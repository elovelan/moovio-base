package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgerrcode"
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
// configurable, stateful error sequences. This lets RetryPostgresNonIdempotent (and the
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
	// Commit-verification scripting (matched by query substring in QueryContext):
	// captureXidResult is returned for pg_current_xact_id_if_assigned (a string
	// xid, or nil for read-only/NULL); verifyStatusResult for pg_xact_status
	// ("aborted"/"committed"/"in progress"/"" for NULL); verifyErr makes the
	// pg_xact_status query fail.
	captureXidResult   any
	verifyStatusResult string
	verifyErr          error
	mu                 sync.Mutex
	beginCalls         int
	commitCalls        int
	rollbackCalls      int
	execCalls          int
	captureCalls       int
	verifyCalls        int
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

// QueryContext implements driver.QueryerContext so *sql.Tx.QueryRow and
// *sql.DB.QueryRowContext work. It recognizes the commit-verification queries
// (by substring) and returns the scripted value from the driver.
func (c *mockConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.d.mu.Lock()
	defer c.d.mu.Unlock()
	switch {
	case strings.Contains(query, "pg_current_xact_id_if_assigned"):
		c.d.captureCalls++
		return &mockRows{val: c.d.captureXidResult}, nil
	case strings.Contains(query, "pg_xact_status"):
		c.d.verifyCalls++
		if c.d.verifyErr != nil {
			return nil, c.d.verifyErr
		}
		return &mockRows{val: c.d.verifyStatusResult}, nil
	}
	return nil, errors.New("mockDriver: unexpected query: " + query)
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

// mockRows is a single-row, single-column result set holding val (a string, or
// nil for SQL NULL). Used by QueryContext for the verification queries.
type mockRows struct {
	val  any
	read bool
}

func (r *mockRows) Columns() []string { return []string{"col"} }
func (r *mockRows) Close() error      { return nil }
func (r *mockRows) Next(dest []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	dest[0] = r.val
	return nil
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

func TestRetryPostgresNonIdempotent(t *testing.T) {
	t.Run("succeeds on first attempt", func(t *testing.T) {
		d := &mockDriver{}
		db := newMockDB(d)
		defer db.Close()

		calls := 0
		err := RetryPostgresNonIdempotent(context.Background(), db, RetryNonIdempotentOptions{}, func(*sql.Tx) error {
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
		err := RetryPostgresNonIdempotent(context.Background(), db, RetryNonIdempotentOptions{}, func(*sql.Tx) error {
			calls++
			if calls < 2 {
				return &pgconn.PgError{Code: pgerrcode.SerializationFailure}
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
		err := RetryPostgresNonIdempotent(context.Background(), db, RetryNonIdempotentOptions{}, func(*sql.Tx) error {
			calls++
			return &pgconn.PgError{Code: pgerrcode.UniqueViolation}
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
		err := RetryPostgresNonIdempotent(context.Background(), db, RetryNonIdempotentOptions{}, func(*sql.Tx) error {
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
		err := RetryPostgresNonIdempotent(context.Background(), db, RetryNonIdempotentOptions{}, func(*sql.Tx) error {
			calls++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, calls)
		require.Equal(t, 2, d.beginCalls) // first Begin failed, second succeeded
	})

	t.Run("SafeToRetry-flagged Commit failure is verified-aborted and retried", func(t *testing.T) {
		// A SafeToRetry-flagged commit error (pre-send) for a write transaction
		// is now verified (not fast-pathed): pg_xact_status says aborted (the
		// COMMIT never sent, so the tx didn't commit) → retry → second attempt
		// succeeds.
		d := &mockDriver{
			captureXidResult:   "1", // write transaction
			verifyStatusResult: "aborted",
			commitErrs:         []error{&safeToRetryErr{msg: "pre-send commit failure"}},
		}
		db := newMockDB(d)
		defer db.Close()

		calls := 0
		err := RetryPostgresNonIdempotent(context.Background(), db, RetryNonIdempotentOptions{}, func(*sql.Tx) error {
			calls++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, calls)
		require.Equal(t, 2, d.commitCalls)
		require.Equal(t, 1, d.verifyCalls) // verified once (first attempt's commit)
	})

	t.Run("commit-phase network error with inconclusive verification is wrapped with ErrCommitPhase", func(t *testing.T) {
		// A network error during commit could mean the commit succeeded but the
		// response was lost. The commit classifier rejects it; verification
		// (pg_xact_status) returns NULL (inconclusive, e.g. xid not yet visible
		// after a failover) → ambiguous → ErrCommitPhase (caller decides).
		d := &mockDriver{
			captureXidResult:   "1", // write transaction, has an xid
			verifyStatusResult: "",  // NULL → inconclusive
			commitErrs:         []error{io.EOF},
		}
		db := newMockDB(d)
		defer db.Close()

		calls := 0
		err := RetryPostgresNonIdempotent(context.Background(), db, RetryNonIdempotentOptions{}, func(*sql.Tx) error {
			calls++
			return nil
		})
		require.Error(t, err)
		require.Equal(t, 1, calls)          // not retried
		require.Equal(t, 1, d.commitCalls)
		require.Equal(t, 0, d.rollbackCalls) // commit failed; rollback is a no-op (ErrTxDone)
		require.Equal(t, 1, d.captureCalls)  // xid captured before commit
		require.Equal(t, 1, d.verifyCalls)   // pg_xact_status queried once
		require.ErrorIs(t, err, ErrCommitPhase)
		require.ErrorIs(t, err, io.EOF) // underlying error preserved
	})

	t.Run("commit-phase admin_shutdown (57P01) with inconclusive verification is wrapped with ErrCommitPhase", func(t *testing.T) {
		// 57P01 is excluded from the commit classifier; verification inconclusive
		// → ErrCommitPhase.
		d := &mockDriver{
			captureXidResult:   "1",
			verifyStatusResult: "",
			commitErrs:         []error{&pgconn.PgError{Code: pgerrcode.AdminShutdown}},
		}
		db := newMockDB(d)
		defer db.Close()

		calls := 0
		err := RetryPostgresNonIdempotent(context.Background(), db, RetryNonIdempotentOptions{}, func(*sql.Tx) error {
			calls++
			return nil
		})
		require.Error(t, err)
		require.Equal(t, 1, calls) // not retried
		require.Equal(t, 1, d.commitCalls)
		require.Equal(t, 1, d.verifyCalls)
		require.ErrorIs(t, err, ErrCommitPhase)
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, pgerrcode.AdminShutdown, pgErr.Code)
	})

	t.Run("commit-phase verify-aborted is retried", func(t *testing.T) {
		// Network error at commit; pg_xact_status says aborted (commit didn't
		// land) → safe to retry → second attempt succeeds.
		d := &mockDriver{
			captureXidResult:   "1",
			verifyStatusResult: "aborted",
			commitErrs:         []error{io.EOF, nil},
		}
		db := newMockDB(d)
		defer db.Close()

		calls := 0
		err := RetryPostgresNonIdempotent(context.Background(), db, RetryNonIdempotentOptions{}, func(*sql.Tx) error {
			calls++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, calls)
		require.Equal(t, 2, d.commitCalls)
		require.Equal(t, 1, d.verifyCalls) // verified once (first attempt's commit)
	})

	t.Run("commit-phase verify-committed returns ErrCommitted (not retried)", func(t *testing.T) {
		// Network error at commit; pg_xact_status says committed (commit DID
		// land) → must NOT retry (would duplicate) → ErrCommitted.
		d := &mockDriver{
			captureXidResult:   "1",
			verifyStatusResult: "committed",
			commitErrs:         []error{io.EOF},
		}
		db := newMockDB(d)
		defer db.Close()

		calls := 0
		err := RetryPostgresNonIdempotent(context.Background(), db, RetryNonIdempotentOptions{}, func(*sql.Tx) error {
			calls++
			return nil
		})
		require.Error(t, err)
		require.Equal(t, 1, calls) // not retried
		require.Equal(t, 1, d.verifyCalls)
		require.ErrorIs(t, err, ErrCommitted)
		require.ErrorIs(t, err, io.EOF)    // underlying commit error preserved
		require.NotErrorIs(t, err, ErrCommitPhase) // not ambiguous — verified committed
	})

	t.Run("commit-phase read-only (no xid) is retried without verifying", func(t *testing.T) {
		// Read-only transaction (pg_current_xact_id_if_assigned returns NULL):
		// no writes to duplicate, so retry is safe without calling pg_xact_status.
		d := &mockDriver{
			captureXidResult: nil, // read-only
			commitErrs:       []error{io.EOF, nil},
		}
		db := newMockDB(d)
		defer db.Close()

		calls := 0
		err := RetryPostgresNonIdempotent(context.Background(), db, RetryNonIdempotentOptions{}, func(*sql.Tx) error {
			calls++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, calls)
		require.Equal(t, 0, d.verifyCalls) // verification skipped (read-only)
		require.Equal(t, 2, d.captureCalls)
	})

	t.Run("commit-phase verify-error is ambiguous (ErrCommitPhase)", func(t *testing.T) {
		// pg_xact_status itself errors (e.g. the fresh connection also failed) →
		// inconclusive → ErrCommitPhase.
		d := &mockDriver{
			captureXidResult: "1",
			verifyErr:        errors.New("verify connection lost"),
			commitErrs:       []error{io.EOF},
		}
		db := newMockDB(d)
		defer db.Close()

		calls := 0
		err := RetryPostgresNonIdempotent(context.Background(), db, RetryNonIdempotentOptions{}, func(*sql.Tx) error {
			calls++
			return nil
		})
		require.Error(t, err)
		require.Equal(t, 1, calls) // not retried
		require.Equal(t, 1, d.verifyCalls)
		require.ErrorIs(t, err, ErrCommitPhase)
		require.ErrorIs(t, err, io.EOF)
	})

	t.Run("commit-phase class-40 exhaustion returns the original error (not ErrCommitPhase)", func(t *testing.T) {
		// serialization_failure (class 40) at commit: the server rolled back,
		// so pg_xact_status says aborted → retry. Retried until exhausted; the
		// original error is returned (NOT ErrCommitPhase — it's known
		// non-commit, just out of retries).
		d := &mockDriver{
			captureXidResult:   "1",
			verifyStatusResult: "aborted", // server rolled back → aborted → retry each time
			commitErrs: []error{
				&pgconn.PgError{Code: pgerrcode.SerializationFailure},
				&pgconn.PgError{Code: pgerrcode.SerializationFailure},
				&pgconn.PgError{Code: pgerrcode.SerializationFailure},
			},
		}
		db := newMockDB(d)
		defer db.Close()

		calls := 0
		err := RetryPostgresNonIdempotent(context.Background(), db, RetryNonIdempotentOptions{}, func(*sql.Tx) error {
			calls++
			return nil
		})
		require.Error(t, err)
		require.Equal(t, 3, calls)
		require.Equal(t, 3, d.commitCalls)
		require.Equal(t, 3, d.verifyCalls) // verified each attempt (always-verify)
		require.NotErrorIs(t, err, ErrCommitPhase) // known non-commit, not ambiguous
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, pgerrcode.SerializationFailure, pgErr.Code)
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
		err := RetryPostgresNonIdempotent(context.Background(), db, RetryNonIdempotentOptions{
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
		err := RetryPostgresNonIdempotent(ctx, db, RetryNonIdempotentOptions{}, func(*sql.Tx) error {
			calls++
			if calls == 1 {
				cancel() // cancel after the first attempt runs
				return &pgconn.PgError{Code: pgerrcode.SerializationFailure}
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
		err := RetryPostgresNonIdempotent(context.Background(), db, RetryNonIdempotentOptions{}, func(*sql.Tx) error {
			calls++
			return &pgconn.PgError{Code: pgerrcode.DeadlockDetected}
		})
		require.Error(t, err)
		require.Equal(t, 3, calls)
		require.Equal(t, 3, d.beginCalls)
		require.Equal(t, 0, d.commitCalls) // fn always errors, so no commit
		require.Equal(t, 3, d.rollbackCalls)
	})
}

func TestIsRetryablePostgresPreCommitError(t *testing.T) {
	// Pre-commit classifier (opt-out): retries everything except context
	// cancellation and permanent SQLSTATE classes (22xxx/23xxx/42xxx).

	// Retried: pre-send guarantees.
	require.True(t, isRetryablePostgresPreCommitError(driver.ErrBadConn))
	require.True(t, isRetryablePostgresPreCommitError(fmt.Errorf("wrapped: %w", driver.ErrBadConn)))
	require.True(t, isRetryablePostgresPreCommitError(&safeToRetryErr{msg: "pre-send"}))

	// Retried: class 40 (server rolled back) and shutdown/cancel codes — the tx
	// didn't commit pre-commit, so retry is safe even though some of these
	// (57P01/57P02/57014) are excluded from the commit classifier.
	require.True(t, isRetryablePostgresPreCommitError(&pgconn.PgError{Code: pgerrcode.SerializationFailure}))
	require.True(t, isRetryablePostgresPreCommitError(&pgconn.PgError{Code: pgerrcode.DeadlockDetected}))
	require.True(t, isRetryablePostgresPreCommitError(&pgconn.PgError{Code: pgerrcode.AdminShutdown}))
	require.True(t, isRetryablePostgresPreCommitError(&pgconn.PgError{Code: pgerrcode.CrashShutdown}))
	require.True(t, isRetryablePostgresPreCommitError(&pgconn.PgError{Code: pgerrcode.CannotConnectNow}))
	require.True(t, isRetryablePostgresPreCommitError(&pgconn.PgError{Code: pgerrcode.TooManyConnections}))
	require.True(t, isRetryablePostgresPreCommitError(&pgconn.PgError{Code: pgerrcode.QueryCanceled}))

	// Retried: typed network errors (non-PgError -> retry under opt-out).
	require.True(t, isRetryablePostgresPreCommitError(io.EOF))
	require.True(t, isRetryablePostgresPreCommitError(io.ErrUnexpectedEOF))
	require.True(t, isRetryablePostgresPreCommitError(&net.OpError{
		Op:  "read",
		Err: errors.New("connection reset by peer"),
	}))

	// Retried: unknown PgError codes (not in the permanent blocklist) and
	// arbitrary non-PgError errors — opt-out assumes retryable unless known
	// permanent.
	require.True(t, isRetryablePostgresPreCommitError(&pgconn.PgError{Code: pgerrcode.InternalError}))
	require.True(t, isRetryablePostgresPreCommitError(errors.New("some application error")))

	// NOT retried: permanent SQLSTATE classes (22xxx/23xxx/42xxx).
	require.False(t, isRetryablePostgresPreCommitError(&pgconn.PgError{Code: pgerrcode.UniqueViolation}))
	require.False(t, isRetryablePostgresPreCommitError(&pgconn.PgError{Code: pgerrcode.ForeignKeyViolation}))
	require.False(t, isRetryablePostgresPreCommitError(&pgconn.PgError{Code: pgerrcode.StringDataRightTruncationDataException}))
	require.False(t, isRetryablePostgresPreCommitError(&pgconn.PgError{Code: pgerrcode.SyntaxError}))
	require.False(t, isRetryablePostgresPreCommitError(&pgconn.PgError{Code: pgerrcode.InsufficientPrivilege}))
	require.False(t, isRetryablePostgresPreCommitError(&pgconn.PgError{Code: pgerrcode.UndefinedTable}))
	require.False(t, isRetryablePostgresPreCommitError(&pgconn.PgError{Code: pgerrcode.UndefinedColumn}))

	// NOT retried: caller's context is done.
	require.False(t, isRetryablePostgresPreCommitError(context.Canceled))
	require.False(t, isRetryablePostgresPreCommitError(context.DeadlineExceeded))
	require.False(t, isRetryablePostgresPreCommitError(nil))
}

func TestRetryMySQLNonIdempotentNotImplemented(t *testing.T) {
	err := RetryMySQLNonIdempotent(context.Background(), nil, RetryNonIdempotentOptions{}, func(*sql.Tx) error { return nil })
	require.Error(t, err)
	require.Contains(t, err.Error(), "not yet implemented")
}

func TestRetrySpannerNonIdempotentNotImplemented(t *testing.T) {
	err := RetrySpannerNonIdempotent(context.Background(), nil, RetryNonIdempotentOptions{}, func(*sql.Tx) error { return nil })
	require.Error(t, err)
	require.Contains(t, err.Error(), "not yet implemented")
}

func TestErrCommitPhase(t *testing.T) {
	// ErrCommitPhase is a sentinel that wraps commit-phase errors so callers
	// can detect them via errors.Is. The underlying error is preserved.
	underlying := errors.New("connection severed during commit")
	wrapped := fmt.Errorf("%w: %w", ErrCommitPhase, underlying)

	require.ErrorIs(t, wrapped, ErrCommitPhase, "caller can detect commit-phase failures")
	require.ErrorIs(t, wrapped, underlying, "underlying error is preserved for inspection")

	// A bare (non-commit-phase) error is NOT ErrCommitPhase.
	require.NotErrorIs(t, underlying, ErrCommitPhase)
	require.NotErrorIs(t, io.EOF, ErrCommitPhase)
}

func TestErrCommitted(t *testing.T) {
	// ErrCommitted wraps a commit error verified to have committed on the server
	// (the response was lost). Callers detect it via errors.Is and must NOT retry.
	underlying := errors.New("connection severed after commit recorded")
	wrapped := fmt.Errorf("%w: %w", ErrCommitted, underlying)

	require.ErrorIs(t, wrapped, ErrCommitted, "caller can detect verified-committed")
	require.ErrorIs(t, wrapped, underlying, "underlying commit error preserved")
	require.NotErrorIs(t, wrapped, ErrCommitPhase) // distinct from ambiguous ErrCommitPhase
	require.NotErrorIs(t, io.EOF, ErrCommitted)
}
