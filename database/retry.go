package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

// RetryTxOptions configures the transactional (safe) retry functions
// (RetryPostgresTx and the future RetryMySQLTx / RetrySpannerTx).
type RetryTxOptions struct {
	// TxOptions is passed to (*sql.DB).BeginTx on each attempt. nil = default isolation.
	TxOptions *sql.TxOptions
	// IsRetryable, if non-nil, is consulted IN ADDITION to the backend's default
	// classifier (both the pre-commit and commit phases). Lets consumers add
	// implementation-specific cases on top of the library's known-safe floor.
	// Callers should be careful adding cases that are unsafe at commit time — a
	// commit-phase error may mean the commit already succeeded (see ErrCommitPhase).
	IsRetryable func(err error) bool
}

// RetryUnsafeOptions configures RetryUnsafe.
type RetryUnsafeOptions struct {
	// IsRetryable, if nil, defaults to "retry on any error except
	// context.Canceled / context.DeadlineExceeded". If non-nil, replaces the default.
	IsRetryable func(err error) bool
}

// maxRetryAttempts is the maximum number of attempts for RetryPostgresTx and
// RetryUnsafe (the initial attempt plus 2 retries), matching database/sql's
// maxBadConnRetries+1 convention. It is intentionally not configurable:
// services that need a different retry budget should bound the total time via
// the context deadline (the retry loop exits early when ctx is done) or wrap
// the call in their own retry loop for more attempts.
const maxRetryAttempts = 3

// ErrCommitPhase wraps an error that occurred during the commit phase of a
// retried transaction (i.e., from (*sql.Tx).Commit) and was NOT retried. A
// commit-phase error is ambiguous: the commit may have succeeded on the server
// before the error was returned to the client (e.g., the TCP connection was
// severed after the COMMIT message was sent but before the response was
// received). Retrying such an operation could duplicate the committed work, so
// RetryPostgresTx deliberately does NOT retry commit-phase errors whose type
// could indicate a successful commit (e.g., network errors).
//
// The error is returned wrapped with ErrCommitPhase so the caller can detect
// it via errors.Is(err, database.ErrCommitPhase) and decide whether to alert,
// reconcile, or check whether the commit landed (e.g., via pg_xact_status(),
// which is future work). The underlying error is preserved for inspection via
// errors.Is/errors.As.
var ErrCommitPhase = errors.New("database: error occurred during the commit phase; the transaction may have committed before the error was returned")

// beginTxer is satisfied by *sql.DB so the retry loop is testable with fakes
// without needing a real database driver.
type beginTxer interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// retryClassifier holds the per-phase retryability classifiers used by retryTx.
// preCommit classifies errors from BeginTx or fn (before Commit is called);
// commit classifies errors from (*sql.Tx).Commit. The commit classifier should
// be NARROWER than preCommit because a commit-phase error may mean the commit
// already succeeded (see ErrCommitPhase).
type retryClassifier struct {
	preCommit func(error) bool
	commit    func(error) bool
}

// classify returns whether err is retryable, using the commit classifier when
// the error occurred during the commit phase and the preCommit classifier
// otherwise.
func (c retryClassifier) classify(err error, commitPhase bool) bool {
	if commitPhase {
		return c.commit(err)
	}
	return c.preCommit(err)
}

// RetryUnsafe executes fn up to maxRetryAttempts times, retrying on any error
// that opts.IsRetryable classifies as retryable (default: any error except
// context cancellation/deadline). fn MUST be idempotent — no transaction
// wrapper is provided, so a retry may re-execute work that already committed.
func RetryUnsafe(ctx context.Context, opts RetryUnsafeOptions, fn func() error) error {
	isRetryable := opts.IsRetryable
	if isRetryable == nil {
		isRetryable = isRetryableUnsafeDefault
	}
	var err error
	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}
		if !isRetryable(err) {
			return err
		}
		if !sleepWithJitter(ctx, attempt, maxRetryAttempts) {
			return ctx.Err()
		}
	}
	return err
}

// isRetryableUnsafeDefault retries on any error except context cancellation
// / deadline, since the caller of RetryUnsafe has vouched that fn is idempotent.
func isRetryableUnsafeDefault(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return true
}

// retryTx is the shared transactional retry loop used by RetryPostgresTx and
// the future RetryMySQLTx / RetrySpannerTx implementations. base is the
// backend's per-phase known-safe floor; opts.IsRetryable is additive on top of
// BOTH phases. Commit-phase errors that are not retried are wrapped with
// ErrCommitPhase before being returned to the caller, so the caller can detect
// that the error occurred during the commit phase (where the commit may have
// succeeded before the error was returned).
//
// database/sql already retries driver.ErrBadConn internally for *sql.DB methods
// (up to maxBadConnRetries=2, i.e. 3 total attempts) before surfacing it. That
// internal retry is immediate (no delay) and handles transient bad connections
// on fresh conns; this outer retry layers on top with full-jitter backoff to
// survive longer outages (e.g. an AlloyDB switchover). The composition is
// intentional: the internal retry covers the "connection was bad before I used
// it" case without delay, and this outer retry covers the "the database was
// briefly unavailable" case with jitter to spread fleet-wide retries. For
// *sql.Tx methods (tx.Exec, tx.Commit) there is no internal retry, so this
// outer retry is the only retry.
func retryTx(ctx context.Context, db beginTxer, base retryClassifier, opts RetryTxOptions, fn func(*sql.Tx) error) error {
	classifier := base
	if opts.IsRetryable != nil {
		extra := opts.IsRetryable
		classifier = retryClassifier{
			preCommit: func(err error) bool { return base.preCommit(err) || extra(err) },
			commit:    func(err error) bool { return base.commit(err) || extra(err) },
		}
	}
	var lastErr error
	var lastCommitPhase bool
	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		var commitPhase bool
		lastErr, commitPhase = runOneTxAttempt(ctx, db, opts.TxOptions, fn)
		lastCommitPhase = commitPhase
		if lastErr == nil {
			return nil
		}
		if !classifier.classify(lastErr, commitPhase) {
			break
		}
		if !sleepWithJitter(ctx, attempt, maxRetryAttempts) {
			return ctx.Err()
		}
	}
	if lastCommitPhase {
		return fmt.Errorf("%w: %w", ErrCommitPhase, lastErr)
	}
	return lastErr
}

// runOneTxAttempt runs a single begin/fn/commit attempt and returns the first
// error encountered (from BeginTx, fn, or Commit) along with a commitPhase
// flag indicating whether the error came from (*sql.Tx).Commit. On a
// successful commit it returns (nil, false). The deferred Rollback is a no-op
// after a successful Commit and cleans up the transaction if fn returns an
// error or panics; after a failed Commit the transaction is already in a
// terminal state so Rollback returns sql.ErrTxDone (ignored).
func runOneTxAttempt(ctx context.Context, db beginTxer, txOpts *sql.TxOptions, fn func(*sql.Tx) error) (err error, commitPhase bool) {
	tx, beginErr := db.BeginTx(ctx, txOpts)
	if beginErr != nil {
		return beginErr, false
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if err = fn(tx); err != nil {
		return err, false
	}
	// TODO(future): capture pg_current_xact_id() before commit and consult
	// pg_xact_status() on commit error to detect whether the commit landed.
	// Postgres-only; see RetryPostgresTx and ErrCommitPhase.
	if err = tx.Commit(); err != nil {
		return err, true
	}
	return nil, false
}

// sleepWithJitter waits for a random duration in [0, retryJitterMax) before
// the next retry attempt, respecting ctx. It returns false if ctx was done
// before the sleep completed. It does not sleep after the final attempt.
func sleepWithJitter(ctx context.Context, attempt, maxAttempts int) bool {
	if attempt >= maxAttempts-1 {
		return true
	}
	delay := time.Duration(rand.Int63n(int64(retryJitterMax)))
	select {
	case <-ctx.Done():
		return false
	case <-time.After(delay):
		return true
	}
}
