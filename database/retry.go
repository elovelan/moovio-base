package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

// RetryNonIdempotentOptions configures RetryPostgresNonIdempotent (and future
// RetryMySQLNonIdempotent / RetrySpannerNonIdempotent).
type RetryNonIdempotentOptions struct {
	// TxOptions passed to (*sql.DB).BeginTx each attempt; nil = default isolation.
	TxOptions *sql.TxOptions
	// IsRetryable, if non-nil, is OR'd with the backend's default classifier
	// (both phases). Be careful adding cases unsafe at commit (see ErrCommitPhase).
	IsRetryable func(err error) bool
}

// RetryIdempotentOptions configures RetryIdempotent.
type RetryIdempotentOptions struct {
	// IsRetryable, if nil, retries any error except context.Canceled/DeadlineExceeded.
	// If non-nil, replaces the default.
	IsRetryable func(err error) bool
}

// maxRetryAttempts is the attempt count for RetryPostgresNonIdempotent and RetryIdempotent
// (initial + 2 retries), matching database/sql's maxBadConnRetries+1. Not
// configurable: use the context deadline to bound total time, or wrap for more.
const maxRetryAttempts = 3

// ErrCommitPhase wraps a non-retried commit-phase error whose commit status
// could not be verified — i.e. the commit may or may not have landed (e.g. a
// network error after COMMIT was sent, or pg_xact_status() couldn't determine
// the outcome). The caller should treat it as ambiguous: alert/reconcile, or
// re-check whether the commit landed. The underlying error is preserved.
//
// With commit verification enabled (RetryPostgresNonIdempotent), ErrCommitPhase
// is the fallback when verification is inconclusive; verified-aborted commits
// are retried, verified-committed commits return ErrCommitted.
var ErrCommitPhase = errors.New("database: commit-phase error with inconclusive commit status; the transaction may have committed before the error was returned")

// ErrCommitted wraps a commit-phase error where verification (pg_xact_status)
// confirmed the transaction DID commit on the server before the error was
// returned to the client (e.g. the connection was severed after the commit
// recorded but before the response arrived). The caller must NOT retry (that
// would duplicate the committed work); it should reconcile/alert. The
// underlying commit error is preserved for inspection via errors.Is/errors.As.
var ErrCommitted = errors.New("database: transaction committed before the error was returned; do not retry")

// commitStatus is the verified outcome of an ambiguous commit-phase error.
type commitStatus int

const (
	commitStatusUnknown commitStatus = iota // can't determine → ErrCommitPhase
	commitStatusAborted                      // commit did NOT land → safe to retry
	commitStatusCommitted                    // commit DID land → ErrCommitted, don't retry
)

// beginTxer is satisfied by *sql.DB; the interface lets tests fake BeginTx.
type beginTxer interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// retryDB extends beginTxer with QueryRowContext, so the retry loop can run a
// fresh-connection query (pg_xact_status) to verify an ambiguous commit.
// Satisfied by *sql.DB.
type retryDB interface {
	beginTxer
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// retryClassifier holds per-phase classifiers and optional commit-verification
// hooks. preCommit classifies BeginTx/fn errors; commit classifies
// (*sql.Tx).Commit errors (must be narrower — a commit-phase error may mean
// the commit already succeeded). captureXid/verifyCommit enable commit
// verification for ambiguous commit errors (see ErrCommitPhase/ErrCommitted).
type retryClassifier struct {
	preCommit func(error) bool
	commit    func(error) bool
	// captureXid, if non-nil, is called after fn succeeds and before Commit to
	// capture a transaction identifier for verifyCommit. Returns nil if the
	// transaction has no id (e.g. read-only — safe to retry without verifying).
	captureXid func(*sql.Tx) (any, error)
	// verifyCommit, if non-nil, is called on a commit-phase error the commit
	// classifier rejected, to check whether the commit landed. Runs on a fresh
	// connection (db) since the original may be dead. xid is from captureXid.
	verifyCommit func(ctx context.Context, db retryDB, xid any) (commitStatus, error)
}

// classify picks the commit classifier when commitPhase, else preCommit.
func (c retryClassifier) classify(err error, commitPhase bool) bool {
	if commitPhase {
		return c.commit(err)
	}
	return c.preCommit(err)
}

// RetryIdempotent runs fn up to maxRetryAttempts times, retrying on any error
// opts.IsRetryable says is retryable (default: any except context cancellation/
// deadline). fn MUST be idempotent — no transaction wrapper, so a retry may
// re-execute work that already committed.
func RetryIdempotent(ctx context.Context, opts RetryIdempotentOptions, fn func() error) error {
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

// isRetryableUnsafeDefault retries any error except context cancellation/deadline
// (RetryIdempotent callers vouch fn is idempotent).
func isRetryableUnsafeDefault(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return true
}

// retryTx is the shared retry loop for RetryPostgresNonIdempotent (and future
// MySQL/Spanner implementations). opts.IsRetryable is OR'd with base on both
// phases. Commit-phase errors the commit classifier rejects are verified via
// base.verifyCommit when an xid was captured: verified-aborted → retry,
// verified-committed → ErrCommitted (don't retry), inconclusive → ErrCommitPhase.
// Read-only transactions (no xid) retry without verification (no writes to
// duplicate). Commit-phase errors the classifier accepts (class 40: server
// rolled back) are retried; on exhaustion they return the original error (not
// ErrCommitPhase — they're known non-commit).
//
// database/sql already retries driver.ErrBadConn for *sql.DB methods (immediate,
// up to 3 attempts) before surfacing it; this outer loop layers on top with
// jitter for longer outages. *sql.Tx methods have no internal retry, so this is
// the only retry for tx.Exec/tx.Commit.
func retryTx(ctx context.Context, db retryDB, base retryClassifier, opts RetryNonIdempotentOptions, fn func(*sql.Tx) error) error {
	classifier := base
	if opts.IsRetryable != nil {
		extra := opts.IsRetryable
		classifier.preCommit = func(err error) bool { return base.preCommit(err) || extra(err) }
		classifier.commit = func(err error) bool { return base.commit(err) || extra(err) }
	}
	var lastErr error
	var lastCommitPhase bool
	var lastRetryable bool
	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		var commitPhase bool
		var xid any
		lastErr, commitPhase, xid = runOneTxAttempt(ctx, db, opts.TxOptions, base.captureXid, fn)
		lastCommitPhase = commitPhase
		if lastErr == nil {
			return nil
		}
		retryable := classifier.classify(lastErr, commitPhase)
		if !retryable && commitPhase {
			// Commit-phase error the classifier rejected — verify whether the
			// commit landed, if we captured an xid and have a verifier.
			switch {
			case base.captureXid != nil && xid == nil:
				// Read-only transaction (no xid assigned): no writes to
				// duplicate, so retry is safe without verification.
				retryable = true
			case base.verifyCommit != nil && xid != nil:
				switch status, verr := base.verifyCommit(ctx, db, xid); {
				case verr == nil && status == commitStatusAborted:
					retryable = true
				case verr == nil && status == commitStatusCommitted:
					return fmt.Errorf("%w: %w", ErrCommitted, lastErr)
				default: // commitStatusUnknown or verification error → ambiguous
					retryable = false
				}
			}
		}
		lastRetryable = retryable
		if !retryable {
			break
		}
		if !sleepWithJitter(ctx, attempt, maxRetryAttempts) {
			return ctx.Err()
		}
	}
	// A retryable commit-phase error (class-40 rolled back, verified-aborted,
	// or read-only) is known non-commit, so on exhaustion return the original
	// error — not ErrCommitPhase (which is only for inconclusive commits).
	if lastCommitPhase && !lastRetryable {
		return fmt.Errorf("%w: %w", ErrCommitPhase, lastErr)
	}
	return lastErr
}

// runOneTxAttempt runs one begin/fn/commit attempt, returning the first error,
// whether it came from Commit (commitPhase), and the transaction id captured
// by captureXid (nil if none). The deferred Rollback is a no-op after a
// successful Commit. A captureXid failure is treated as a pre-commit error
// (rolled back, classified by preCommit).
func runOneTxAttempt(ctx context.Context, db beginTxer, txOpts *sql.TxOptions, captureXid func(*sql.Tx) (any, error), fn func(*sql.Tx) error) (err error, commitPhase bool, xid any) {
	tx, beginErr := db.BeginTx(ctx, txOpts)
	if beginErr != nil {
		return beginErr, false, nil
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
		return err, false, nil
	}
	if captureXid != nil {
		xid, err = captureXid(tx)
		if err != nil {
			return err, false, nil
		}
	}
	if err = tx.Commit(); err != nil {
		return err, true, xid
	}
	return nil, false, nil
}

// sleepWithJitter sleeps a random duration in [0, retryJitterMax) before the
// next attempt, respecting ctx. Returns false if ctx is done. No sleep after
// the final attempt.
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
