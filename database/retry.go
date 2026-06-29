package database

import (
	"context"
	"database/sql"
	"errors"
	"math/rand"
	"time"
)

// RetryTxOptions configures the transactional (safe) retry functions
// (RetryPostgresTx and the future RetryMySQLTx / RetrySpannerTx).
type RetryTxOptions struct {
	// MaxAttempts caps the number of transaction attempts. Defaults to 3 if <= 0.
	MaxAttempts int
	// TxOptions is passed to (*sql.DB).BeginTx on each attempt. nil = default isolation.
	TxOptions *sql.TxOptions
	// IsRetryable, if non-nil, is consulted IN ADDITION to the backend's default
	// classifier (e.g. isRetryablePostgresTxError). Lets consumers add
	// implementation-specific cases on top of the library's known-safe floor.
	IsRetryable func(err error) bool
}

// RetryUnsafeOptions configures RetryUnsafe.
type RetryUnsafeOptions struct {
	// MaxAttempts caps the number of attempts. Defaults to 3 if <= 0.
	MaxAttempts int
	// IsRetryable, if nil, defaults to "retry on any error except
	// context.Canceled / context.DeadlineExceeded". If non-nil, replaces the default.
	IsRetryable func(err error) bool
}

// beginTxer is satisfied by *sql.DB so the retry loop is testable with fakes
// without needing a real database driver.
type beginTxer interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// RetryUnsafe executes fn up to MaxAttempts times, retrying on any error that
// opts.IsRetryable classifies as retryable (default: any error except context
// cancellation/deadline). fn MUST be idempotent — no transaction wrapper is
// provided, so a retry may re-execute work that already committed.
func RetryUnsafe(ctx context.Context, opts RetryUnsafeOptions, fn func() error) error {
	maxAttempts := opts.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	isRetryable := opts.IsRetryable
	if isRetryable == nil {
		isRetryable = isRetryableUnsafeDefault
	}
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}
		if !isRetryable(err) {
			return err
		}
		if !sleepWithJitter(ctx, attempt, maxAttempts) {
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
// the future RetryMySQLTx / RetrySpannerTx implementations. defaultClassifier
// is the backend's known-safe floor; opts.IsRetryable is additive on top of it.
func retryTx(ctx context.Context, db beginTxer, defaultClassifier func(error) bool, opts RetryTxOptions, fn func(*sql.Tx) error) error {
	maxAttempts := opts.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	isRetryable := defaultClassifier
	if opts.IsRetryable != nil {
		extra := opts.IsRetryable
		isRetryable = func(err error) bool {
			return defaultClassifier(err) || extra(err)
		}
	}
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		lastErr = runOneTxAttempt(ctx, db, opts.TxOptions, fn)
		if lastErr == nil {
			return nil
		}
		if !isRetryable(lastErr) {
			return lastErr
		}
		if !sleepWithJitter(ctx, attempt, maxAttempts) {
			return ctx.Err()
		}
	}
	return lastErr
}

// runOneTxAttempt runs a single begin/fn/commit attempt and returns the first
// error encountered (from BeginTx, fn, or Commit). On a successful commit it
// returns nil. The deferred Rollback is a no-op after a successful Commit and
// cleans up the transaction if fn returns an error or panics.
func runOneTxAttempt(ctx context.Context, db beginTxer, txOpts *sql.TxOptions, fn func(*sql.Tx) error) (err error) {
	tx, beginErr := db.BeginTx(ctx, txOpts)
	if beginErr != nil {
		return beginErr
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
		return err
	}
	// TODO(stacked-pr): capture pg_current_xact_id() before commit and
	// consult pg_xact_status() on commit error to detect whether the commit
	// landed. Postgres-only; see RetryPostgresTx.
	return tx.Commit()
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
