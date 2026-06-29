package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// safeToRetryErr is a test double for a pgx error that pgconn.SafeToRetry
// flags as safe (i.e. it implements the unexported interface{ SafeToRetry() bool }
// that pgconn.SafeToRetry looks for via errors.As). pgx's own SafeToRetry-flagged
// types have unexported fields, so we construct one directly here.
type safeToRetryErr struct{ msg string }

func (e *safeToRetryErr) Error() string  { return e.msg }
func (e *safeToRetryErr) SafeToRetry() bool { return true }

// mockConnector + mockDriver + mockConn + mockTx form a minimal
// database/sql/driver implementation whose BeginTx/Commit/Rollback/Exec return
// configurable, stateful error sequences. This lets RetryPostgresTx (and the
// shared retryTx helper it delegates to) be driven through a real *sql.DB
// without a Postgres. *sql.Tx is concrete, so a mock driver is the only way
// to control Commit/Rollback errors.
type mockConnector struct{ d *mockDriver }

func (c *mockConnector) Connect(context.Context) (driver.Conn, error) {
	return &mockConn{d: c.d}, nil
}
func (c *mockConnector) Driver() driver.Driver { return c.d }

type mockDriver struct {
	beginErrs     []error
	commitErrs    []error
	rollbackErrs  []error
	execErrs      []error
	mu            sync.Mutex
	beginCalls    int
	commitCalls   int
	rollbackCalls int
	execCalls     int
}

func (d *mockDriver) Open(string) (driver.Conn, error) { return &mockConn{d: d}, nil }

func (d *mockDriver) nthBegin() (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	i := d.beginCalls
	d.beginCalls++
	return i, nthErr(d.beginErrs, i)
}
func (d *mockDriver) nthCommit() (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	i := d.commitCalls
	d.commitCalls++
	return i, nthErr(d.commitErrs, i)
}
func (d *mockDriver) nthRollback() (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	i := d.rollbackCalls
	d.rollbackCalls++
	return i, nthErr(d.rollbackErrs, i)
}

type mockConn struct{ d *mockDriver }

func (c *mockConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	if _, err := c.d.nthBegin(); err != nil {
		return nil, err
	}
	return &mockTx{d: c.d}, nil
}
func (c *mockConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *mockConn) Close() error { return nil }
func (c *mockConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}
func (c *mockConn) PrepareContext(context.Context, string) (driver.Stmt, error) {
	return nil, errors.New("mockDriver: PrepareContext not implemented")
}

type mockTx struct{ d *mockDriver }

func (t *mockTx) Commit() error {
	_, err := t.d.nthCommit()
	return err
}
func (t *mockTx) Rollback() error {
	_, err := t.d.nthRollback()
	return err
}

// nthErr returns errs[i] or nil when i is out of range. Tests pass a sequence
// like []error{err1, err2} to mean "fail twice then succeed".
func nthErr(errs []error, i int) error {
	if i < len(errs) {
		return errs[i]
	}
	return nil
}

func newMockDB(d *mockDriver) *sql.DB {
	return sql.OpenDB(&mockConnector{d: d})
}

func TestRetryPostgresTx(t *testing.T) {
	t.Run("succeeds on first attempt", func(t *testing.T) {
		d := &mockDriver{}
		db := newMockDB(d)
		defer db.Close()

		calls := 0
		err := RetryPostgresTx(context.Background(), db, RetryTxOptions{MaxAttempts: 3}, func(*sql.Tx) error {
			calls++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, calls)
		require.Equal(t, 1, d.beginCalls)
		require.Equal(t, 1, d.commitCalls)
	})

	t.Run("fn error then succeed rolls back between attempts", func(t *testing.T) {
		d := &mockDriver{}
		db := newMockDB(d)
		defer db.Close()

		calls := 0
		err := RetryPostgresTx(context.Background(), db, RetryTxOptions{MaxAttempts: 3}, func(*sql.Tx) error {
			calls++
			if calls < 2 {
				return &pgconn.PgError{Code: "40001"} // serialization_failure -> retryable
			}
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, calls)
		// Two begins, two commits (first attempt's commit is skipped because fn
		// returned an error -> rollback instead).
		require.Equal(t, 2, d.beginCalls)
		require.Equal(t, 1, d.commitCalls) // only the successful attempt commits
		require.Equal(t, 1, d.rollbackCalls)
	})

	t.Run("non-retryable fn error short-circuits", func(t *testing.T) {
		d := &mockDriver{}
		db := newMockDB(d)
		defer db.Close()

		calls := 0
		err := RetryPostgresTx(context.Background(), db, RetryTxOptions{MaxAttempts: 3}, func(*sql.Tx) error {
			calls++
			return &pgconn.PgError{Code: "23505"} // unique_violation -> NOT retryable
		})
		require.Error(t, err)
		require.Equal(t, 1, calls)
		require.Equal(t, 1, d.beginCalls)
		require.Equal(t, 0, d.commitCalls)
		require.Equal(t, 1, d.rollbackCalls)
	})

	t.Run("driver.ErrBadConn from fn is retried (fn-path signal)", func(t *testing.T) {
		// pgx's stdlib adapter converts SafeToRetry-flagged Exec/Query errors to
		// driver.ErrBadConn before fn sees them, so this is the primary retry
		// trigger inside a transaction.
		d := &mockDriver{}
		db := newMockDB(d)
		defer db.Close()

		calls := 0
		err := RetryPostgresTx(context.Background(), db, RetryTxOptions{MaxAttempts: 3}, func(*sql.Tx) error {
			calls++
			if calls < 2 {
				return driver.ErrBadConn
			}
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, calls)
		require.Equal(t, 2, d.beginCalls)
	})

	t.Run("SafeToRetry-flagged Begin failure is retried (Begin/Commit-path signal)", func(t *testing.T) {
		// Begin/Commit errors flow through the stdlib adapter unchanged, so
		// pgconn.SafeToRetry sees the original pgx error. *sql.DB does not
		// internally retry on non-ErrBadConn errors, so our retry loop sees it.
		d := &mockDriver{
			beginErrs: []error{&safeToRetryErr{msg: "pre-send begin failure"}},
		}
		db := newMockDB(d)
		defer db.Close()

		calls := 0
		err := RetryPostgresTx(context.Background(), db, RetryTxOptions{MaxAttempts: 3}, func(*sql.Tx) error {
			calls++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, calls)
		require.Equal(t, 2, d.beginCalls) // first Begin failed, second succeeded
	})

	t.Run("SafeToRetry-flagged Commit failure is retried", func(t *testing.T) {
		d := &mockDriver{
			commitErrs: []error{&safeToRetryErr{msg: "pre-send commit failure"}},
		}
		db := newMockDB(d)
		defer db.Close()

		calls := 0
		err := RetryPostgresTx(context.Background(), db, RetryTxOptions{MaxAttempts: 3}, func(*sql.Tx) error {
			calls++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, calls)
		require.Equal(t, 2, d.beginCalls)
		require.Equal(t, 2, d.commitCalls) // first commit failed, second succeeded
	})

	t.Run("additive opts.IsRetryable extends the default classifier", func(t *testing.T) {
		// sentinelErr is not driver.ErrBadConn, not SafeToRetry-flagged, and not
		// an IsRetryablePostgresError SQLSTATE code, so the default classifier
		// returns false. opts.IsRetryable adds it on top.
		sentinelErr := errors.New("custom retryable sentinel")

		d := &mockDriver{}
		db := newMockDB(d)
		defer db.Close()

		calls := 0
		err := RetryPostgresTx(context.Background(), db, RetryTxOptions{
			MaxAttempts: 3,
			IsRetryable: func(err error) bool { return errors.Is(err, sentinelErr) },
		}, func(*sql.Tx) error {
			calls++
			if calls < 2 {
				return sentinelErr
			}
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, calls)
	})

	t.Run("respects context cancellation between attempts", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		d := &mockDriver{}
		db := newMockDB(d)
		defer db.Close()

		calls := 0
		err := RetryPostgresTx(ctx, db, RetryTxOptions{MaxAttempts: 3}, func(*sql.Tx) error {
			calls++
			if calls == 1 {
				cancel() // cancel after the first attempt runs
				return &pgconn.PgError{Code: "40001"} // retryable, but ctx is now done
			}
			return nil
		})
		// First attempt runs, then context cancellation is detected before the next attempt
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, calls)
	})

	t.Run("exhausts all attempts", func(t *testing.T) {
		d := &mockDriver{}
		db := newMockDB(d)
		defer db.Close()

		calls := 0
		err := RetryPostgresTx(context.Background(), db, RetryTxOptions{MaxAttempts: 3}, func(*sql.Tx) error {
			calls++
			return &pgconn.PgError{Code: "40P01"} // deadlock_detected -> retryable
		})
		require.Error(t, err)
		require.Equal(t, 3, calls)
		require.Equal(t, 3, d.beginCalls)
		require.Equal(t, 0, d.commitCalls) // fn always errors, so no commit
		require.Equal(t, 3, d.rollbackCalls)
	})
}

func TestIsRetryablePostgresTxError(t *testing.T) {
	// Leg 1: driver.ErrBadConn (fn-path signal — pgx stdlib adapter converts
	// SafeToRetry Exec/Query errors to this).
	require.True(t, isRetryablePostgresTxError(driver.ErrBadConn))
	require.True(t, isRetryablePostgresTxError(fmt.Errorf("wrapped: %w", driver.ErrBadConn)))

	// Leg 2: pgconn.SafeToRetry-flagged (Begin/Commit-path signal).
	require.True(t, isRetryablePostgresTxError(&safeToRetryErr{msg: "pre-send"}))

	// Leg 3: IsRetryablePostgresError SQLSTATE codes that guarantee rollback.
	require.True(t, isRetryablePostgresTxError(&pgconn.PgError{Code: "40001"})) // serialization_failure
	require.True(t, isRetryablePostgresTxError(&pgconn.PgError{Code: "40P01"})) // deadlock_detected
	require.True(t, isRetryablePostgresTxError(&pgconn.PgError{Code: "57P01"})) // admin_shutdown

	// Non-retryable: unique_violation is an application error, not a transient one.
	require.False(t, isRetryablePostgresTxError(&pgconn.PgError{Code: "23505"}))
	// Non-retryable: a random error that matches none of the three legs.
	require.False(t, isRetryablePostgresTxError(errors.New("some application error")))
	// Non-retryable: context cancellation is never retryable.
	require.False(t, isRetryablePostgresTxError(context.Canceled))
	// Network errors are retryable via IsRetryablePostgresError's typed checks.
	require.True(t, isRetryablePostgresTxError(io.EOF))
}

func TestRetryMySQLTxNotImplemented(t *testing.T) {
	err := RetryMySQLTx(context.Background(), nil, RetryTxOptions{}, func(*sql.Tx) error { return nil })
	require.Error(t, err)
	require.Contains(t, err.Error(), "not yet implemented")
}

func TestRetrySpannerTxNotImplemented(t *testing.T) {
	err := RetrySpannerTx(context.Background(), nil, RetryTxOptions{}, func(*sql.Tx) error { return nil })
	require.Error(t, err)
	require.Contains(t, err.Error(), "not yet implemented")
}
