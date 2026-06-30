package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

// RetryTxOptions configures RetryPostgresTx (and future RetryMySQLTx/SpannerTx).
type RetryTxOptions struct {
	// TxOptions passed to (*sql.DB).BeginTx each attempt; nil = default isolation.
	TxOptions *sql.TxOptions
	// IsRetryable, if non-nil, is OR'd with the backend's default classifier
	// (both phases). Be careful adding cases unsafe at commit (see ErrCommitPhase).
	IsRetryable func(err error) bool
}

// RetryUnsafeOptions configures RetryUnsafe.
type RetryUnsafeOptions struct {
	// IsRetryable, if nil, retries any error except context.Canceled/DeadlineExceeded.
	// If non-nil, replaces the default.
	IsRetryable func(err error) bool
}

// maxRetryAttempts is the attempt count for RetryPostgresTx and RetryUnsafe
// (initial + 2 retries), matching database/sql's maxBadConnRetries+1. Not
// configurable: use the context deadline to bound total time, or wrap for more.
const maxRetryAttempts = 3

// ErrCommitPhase wraps a non-retried error from (*sql.Tx).Commit. A commit-
// phase error is ambiguous: the commit may have succeeded before the error was
// returned (e.g. connection severed after COMMIT sent but before the response).
// RetryPostgresTx doesn't retry such errors (could duplicate the work) and
// wraps them with ErrCommitPhase so the caller can detect them via errors.Is
// and decide whether to alert/reconcile/check if the commit landed (future
// pg_xact_status() work). The underlying error is preserved.
var ErrCommitPhase = errors.New("database: commit-phase error; transaction may have committed before the error was returned")

// beginTxer is satisfied by *sql.DB; the interface lets tests fake BeginTx.
type beginTxer interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// retryClassifier holds per-phase classifiers: preCommit for BeginTx/fn errors,
// commit for (*sql.Tx).Commit errors. commit must be narrower (a commit-phase
// error may mean the commit already succeeded — see ErrCommitPhase).
type retryClassifier struct {
	preCommit func(error) bool
	commit    func(error) bool
}

// classify picks the commit classifier when commitPhase, else preCommit.
func (c retryClassifier) classify(err error, commitPhase bool) bool {
	if commitPhase {
		return c.commit(err)
	}
	return c.preCommit(err)
}

// RetryUnsafe runs fn up to maxRetryAttempts times, retrying on any error
// opts.IsRetryable says is retryable (default: any except context cancellation/
// deadline). fn MUST be idempotent — no transaction wrapper, so a retry may
// re-execute work that already committed.
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

// isRetryableUnsafeDefault retries any error except context cancellation/deadline
// (RetryUnsafe callers vouch fn is idempotent).
func isRetryableUnsafeDefault(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return true
}

// retryTx is the shared retry loop for RetryPostgresTx (and future MySQL/Spanner
// implementations). opts.IsRetryable is OR'd with base on both phases. Non-
// retried commit-phase errors are wrapped with ErrCommitPhase.
//
// database/sql already retries driver.ErrBadConn for *sql.DB methods (immediate,
// up to 3 attempts) before surfacing it; this outer loop layers on top with
// jitter for longer outages. *sql.Tx methods have no internal retry, so this is
// the only retry for tx.Exec/tx.Commit.
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

// runOneTxAttempt runs one begin/fn/commit attempt, returning the first error
// and whether it came from Commit (commitPhase). The deferred Rollback is a
// no-op after a successful Commit.
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
	// TODO(future): capture pg_current_xact_id() pre-commit and consult
	// pg_xact_status() on commit error to detect if the commit landed.
	if err = tx.Commit(); err != nil {
		return err, true
	}
	return nil, false
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
