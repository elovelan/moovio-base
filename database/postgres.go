package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"cloud.google.com/go/alloydbconn"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/moov-io/base/log"
)

const (
	// PostgreSQL Error Codes
	// https://www.postgresql.org/docs/current/errcodes-appendix.html
	postgresErrUniqueViolation = "23505"
	postgresErrDeadlockFound   = "40P01"
)

func postgresConnection(ctx context.Context, logger log.Logger, config PostgresConfig, databaseName string) (*sql.DB, error) {
	poolConfig, err := buildPgxPoolConfig(ctx, config, databaseName)
	if err != nil {
		return nil, logger.LogErrorf("building pgx pool config: %w", err).Err()
	}

	// HealthCheckPeriod makes pgxpool ping idle connections in the background.
	// Dead connections (e.g. from an AlloyDB switchover) are evicted before
	// the application ever sees them.
	poolConfig.HealthCheckPeriod = 1 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, logger.LogErrorf("creating pgx pool: %w", err).Err()
	}

	err = pool.Ping(ctx)
	if err != nil {
		pool.Close()
		return nil, logger.LogErrorf("connecting to database: %w", err).Err()
	}

	// Wrap the pgxpool in a *sql.DB so the rest of the codebase doesn't change.
	// pgxpool manages the real pool (with health checks); database/sql pool
	// settings are applied on top via ApplyPostgresConnectionsConfig.
	db := stdlib.OpenDBFromPool(pool)

	return db, nil
}

func buildPgxPoolConfig(ctx context.Context, config PostgresConfig, databaseName string) (*pgxpool.Config, error) {
	if config.Alloy != nil {
		return buildAlloyDBPoolConfig(ctx, config, databaseName)
	}

	connStr, err := getPostgresConnStr(config, databaseName)
	if err != nil {
		return nil, err
	}
	return pgxpool.ParseConfig(connStr)
}

func buildAlloyDBPoolConfig(ctx context.Context, config PostgresConfig, databaseName string) (*pgxpool.Config, error) {
	if config.Alloy == nil {
		return nil, fmt.Errorf("missing alloy config")
	}

	var dialer *alloydbconn.Dialer
	var dsn string

	if config.Alloy.UseIAM {
		d, err := alloydbconn.NewDialer(ctx, alloydbconn.WithIAMAuthN())
		if err != nil {
			return nil, fmt.Errorf("creating alloydb dialer: %v", err)
		}
		dialer = d
		dsn = fmt.Sprintf(
			// sslmode is disabled because the alloy db connection dialer will handle it
			// no password is used with IAM
			"user=%s dbname=%s sslmode=disable",
			config.User, databaseName,
		)
	} else {
		d, err := alloydbconn.NewDialer(ctx)
		if err != nil {
			return nil, fmt.Errorf("creating alloydb dialer: %v", err)
		}
		dialer = d
		dsn = fmt.Sprintf(
			// sslmode is disabled because the alloy db connection dialer will handle it
			"user=%s password=%s dbname=%s sslmode=disable",
			config.User, config.Password, databaseName,
		)
	}

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pgx pool config: %v", err)
	}

	var connOptions []alloydbconn.DialOption
	if config.Alloy.UsePSC {
		connOptions = append(connOptions, alloydbconn.WithPSC())
	}

	poolConfig.ConnConfig.DialFunc = func(ctx context.Context, _ string, _ string) (net.Conn, error) {
		return dialer.Dial(ctx, config.Alloy.InstanceURI, connOptions...)
	}

	return poolConfig, nil
}

func getPostgresConnStr(config PostgresConfig, databaseName string) (string, error) {
	url := fmt.Sprintf("postgres://%s:%s@%s/%s", config.User, config.Password, config.Address, databaseName)

	params := ""

	if config.TLS != nil {
		if len(config.TLS.Mode) < 1 {
			config.TLS.Mode = "verify-full"
		}

		params += "sslmode=" + config.TLS.Mode

		if len(config.TLS.CACertFile) > 0 {
			params += "&sslrootcert=" + config.TLS.CACertFile
		}

		if len(config.TLS.ClientCertFile) > 0 {
			params += "&sslcert=" + config.TLS.ClientCertFile
		}

		if len(config.TLS.ClientKeyFile) > 0 {
			params += "&sslkey=" + config.TLS.ClientKeyFile
		}
	}

	connStr := fmt.Sprintf("%s?%s", url, params)
	return connStr, nil
}

// PostgresUniqueViolation returns true when the provided error matches the Postgres code
// for unique violation.
func PostgresUniqueViolation(err error) bool {
	if err == nil {
		return false
	}

	var pgError *pgconn.PgError
	if errors.As(err, &pgError) && pgError.Code == postgresErrUniqueViolation {
		return true
	}

	return strings.Contains(err.Error(), postgresErrUniqueViolation)
}

// PostgresDeadlockFound returns true when the provided error matches the Postgres code
// for deadlock found.
func PostgresDeadlockFound(err error) bool {
	if err == nil {
		return false
	}

	var pgError *pgconn.PgError
	if errors.As(err, &pgError) && pgError.Code == postgresErrDeadlockFound {
		return true
	}

	return strings.Contains(err.Error(), postgresErrDeadlockFound)
}

// isSafeRetryablePostgresError returns true if the error is safe to retry
// regardless of transaction phase — i.e., the error guarantees no side effects
// occurred (the operation never reached the server, or the server rolled back
// before returning the error). It is the OR of:
//
//   - errors.Is(err, driver.ErrBadConn): pgx's stdlib adapter only produces
//     this for pgconn.SafeToRetry-flagged (pre-send) errors, so the operation
//     never reached the server.
//   - pgconn.SafeToRetry(err): pgx guarantees the error occurred before any
//     data was sent to the server (e.g., during connection acquisition,
//     conn-busy, HA NotPreferredError, pre-send timeouts).
//   - server-returned SQLSTATE codes that guarantee rollback (40001, 40P01,
//     57P01, 57P02, 57P03, 53300, 57014) — the server rolled back the
//     transaction before returning the error.
//
// It is used directly as the commit-phase classifier for RetryPostgresTx.
// The pre-commit classifier (isRetryablePostgresPreCommitError) is a separate
// opt-out classifier that retries a broader set, because pre-commit the
// transaction rolls back so retrying is always safe.
//
// Network-level errors (net.OpError, io.EOF) are intentionally NOT classified
// here: without an explicit transaction there is no way to know whether the
// error occurred before or after the server accepted the query, and at commit
// time a network error could mean the commit succeeded but the response was
// lost (see ErrCommitPhase).
//
// This is unexported because the recommended path for callers is RetryPostgresTx
// (safe, transactional) or RetryUnsafe (idempotent caller), not building a
// custom retry loop on top of this classifier.
func isSafeRetryablePostgresError(err error) bool {
	if err == nil {
		return false
	}
	// driver.ErrBadConn is produced by pgx's stdlib adapter only for
	// pgconn.SafeToRetry-flagged (pre-send) errors from Exec/Query, so the
	// operation never reached the server. Safe to retry at any phase.
	if errors.Is(err, driver.ErrBadConn) {
		return true
	}
	// pgconn.SafeToRetry is pgx's authoritative flag for "occurred before any
	// data was sent to the server". Covers Begin/Commit failures where the
	// original pgx error flows through the stdlib adapter unchanged.
	if pgconn.SafeToRetry(err) {
		return true
	}
	// 57P01 admin_shutdown, 57P02 crash_shutdown: fast shutdown rolls back all
	// active transactions before disconnecting clients. See:
	// https://www.postgresql.org/docs/current/server-shutdown.html
	//
	// 57P03 cannot_connect_now: server is still starting up; the operation never
	// reached a transaction.
	//
	// 40001 serialization_failure, 40P01 deadlock_detected: the server explicitly
	// rolled back the transaction and expects the client to retry. See:
	// https://www.postgresql.org/docs/current/mvcc-serialization-failure-handling.html
	//
	// 53300 too_many_connections: rejected at connect time; no transaction started.
	//
	// 57014 query_canceled: the server rolled back the transaction before returning
	// this error.
	//
	// Note: 08xxx (connection_exception class) codes are intentionally omitted
	// here. pgx surfaces connection-level failures as network errors, not as
	// *pgconn.PgError; the pre-commit classifier (isRetryablePostgresPreCommitError)
	// retries those because pre-commit the transaction rolls back. At commit time
	// they would be wrapped with ErrCommitPhase (conservative — no server response
	// means the commit may have succeeded).
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "57P01", "57P02", "57P03", // admin_shutdown, crash_shutdown, cannot_connect_now
			"40001", "40P01", // serialization_failure, deadlock_detected
			"53300", // too_many_connections
			"57014": // query_canceled
			return true
		}
		return false
	}
	return false
}

// isPermanentPostgresError returns true for PostgreSQL SQLSTATE classes that
// indicate a permanent error which would fail again on retry. Used by the
// pre-commit classifier (isRetryablePostgresPreCommitError) to opt OUT of
// retrying these — pre-commit retrying them would be safe (the transaction
// rolls back) but futile, adding ~200ms of latency before the caller receives
// the error. The classes blocked here all surface at query time (not connection
// time) and don't resolve across retries:
//
//   - 22xxx data exceptions (e.g. string_data_right_truncation 22001)
//   - 23xxx integrity constraint violations (e.g. unique_violation 23505,
//     foreign_key_violation 23503)
//   - 42xxx syntax/access-rule violations (includes 42501
//     insufficient_privilege, 42601 syntax_error, undefined_table 42P01,
//     undefined_column 42703, datatype_mismatch 42804)
//
// Other classes (e.g. 08xxx connection exceptions, 58xxx system errors, XX000
// internal_error) are NOT blocked: they may be transient, and pre-commit
// retrying them is safe. Blocking only the clearly-permanent classes keeps the
// blocklist conservative so we don't accidentally skip a retryable error.
func isPermanentPostgresError(pgErr *pgconn.PgError) bool {
	return strings.HasPrefix(pgErr.Code, "22") || // data exception
		strings.HasPrefix(pgErr.Code, "23") || // integrity constraint violation
		strings.HasPrefix(pgErr.Code, "42") // syntax / access rule violation
}

// retryJitterMax is the upper bound for the random delay between retries.
// Full jitter in [0, retryJitterMax) spreads concurrent retries across the
// fleet rather than letting them all slam the database at the same instant.
// AlloyDB planned maintenance switchovers typically cause less than one second
// of downtime, so three attempts with up to 100ms between each gives a worst-
// case retry window of ~200ms — well within typical service SLAs while still
// spanning the switchover blip. The caller's context deadline is the escape
// hatch for services with tighter latency budgets.
const retryJitterMax = 100 * time.Millisecond

// isRetryablePostgresPreCommitError is the default classifier used by
// RetryPostgresTx for errors that occur BEFORE Commit is called (i.e., from
// BeginTx or fn). Pre-commit, retrying is ALWAYS safe: the transaction rolls
// back, so a retried attempt starts from a clean slate. To avoid missing a
// transient error we haven't enumerated, this classifier OPTS OUT of retrying
// only the errors known to be permanent; everything else is retried.
//
// NOT retried (would fail again on the next attempt, so retrying only adds
// ~200ms of latency before the caller receives the error):
//
//   - context.Canceled / context.DeadlineExceeded: the caller's context is
//     done; retrying would hit the deadline immediately.
//   - Permanent PostgreSQL SQLSTATE classes — see isPermanentPostgresError:
//       * 22xxx data exceptions (e.g. string_data_right_truncation 22001)
//       * 23xxx integrity constraint violations (e.g. unique_violation 23505,
//         foreign_key_violation 23503)
//       * 42xxx syntax/access-rule violations (42501 insufficient_privilege,
//         42601 syntax_error, undefined_table 42P01, undefined_column 42703,
//         datatype_mismatch 42804)
//
// Retried (transient or unknown): everything else — including driver.ErrBadConn,
// pgconn.SafeToRetry-flagged errors, typed network errors (net.OpError, io.EOF),
// the known-transient SQLSTATE codes (40001, 40P01, 57P01, 57P02, 57P03, 53300,
// 57014), any *pgconn.PgError whose code is NOT in the permanent blocklist
// (opt-out — assume retryable unless known permanent, to avoid spurious
// failures on transient errors we haven't enumerated), and any other non-
// PgError error.
//
// The cost of a false positive (retrying a permanent error we didn't blocklist)
// is ~200ms of wasted latency; the cost of a false negative (NOT retrying a
// transient error we didn't allowlist) is a spurious user-facing failure.
// Pre-commit the former is strictly safer, so we opt out.
//
// Callers who want to ALSO opt out of retrying additional errors should pass
// opts.IsRetryable that returns false for them. Note that opts.IsRetryable is
// ADDITIVE (it can only widen the retry set via OR), so to NARROW it you'd need
// a wrapper that re-checks; if narrowing is a common need we can revisit the
// composition semantics.
func isRetryablePostgresPreCommitError(err error) bool {
	if err == nil {
		return false
	}
	// Caller's context is done — retrying would hit the deadline immediately.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	// Permanent SQLSTATE codes would fail again on retry.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return !isPermanentPostgresError(pgErr)
	}
	// Everything else (network errors, driver.ErrBadConn, SafeToRetry-flagged,
	// unknown errors) is retried — pre-commit the transaction rolls back.
	return true
}

// RetryPostgresTx executes fn inside a Postgres transaction, retrying the
// entire transaction on transient errors. fn receives a fresh *sql.Tx each
// attempt; if fn returns an error the tx is rolled back, if nil the tx is
// committed.
//
// Pre-commit errors (from BeginTx or fn) are retried when
// isRetryablePostgresPreCommitError(err) OR opts.IsRetryable(err) is true.
// isRetryablePostgresPreCommitError is an OPT-OUT classifier: pre-commit
// retrying is always safe (the transaction rolls back), so it retries
// everything EXCEPT context.Canceled/DeadlineExceeded and the permanent
// SQLSTATE classes (22xxx data exceptions, 23xxx constraint violations, 42xxx
// syntax/schema/permission). See its doc for the rationale.
//
// Commit-phase errors (from (*sql.Tx).Commit) are retried only when
// isSafeRetryablePostgresError(err) OR opts.IsRetryable(err) is true — a
// narrower OPT-IN set (pre-send + server-rolled-back SQLSTATE; network errors
// excluded), because a commit-phase network error may mean the commit already
// succeeded (e.g., the connection was severed after the COMMIT was sent but
// before the response was received). Commit-phase errors that are NOT retried
// are returned to the caller wrapped with ErrCommitPhase so the caller can
// decide whether to alert, reconcile, or check via pg_xact_status() (future).
//
// db MUST be a *sql.DB backed by the pgx stdlib adapter (e.g. one returned
// by database.New with a PostgresConfig). Using a *sql.DB backed by another
// driver will not produce pgx-specific retries.
func RetryPostgresTx(ctx context.Context, db *sql.DB, opts RetryTxOptions, fn func(*sql.Tx) error) error {
	return retryTx(ctx, db, postgresTxClassifier, opts, fn)
}

// postgresTxClassifier is the per-phase retry classifier used by RetryPostgresTx.
// The preCommit classifier is an OPT-OUT classifier (broad: retry everything
// except permanent errors and context cancellation, because pre-commit the
// transaction rolls back so retrying is always safe); the commit classifier is
// the OPT-IN isSafeRetryablePostgresError (narrow: only pre-send +
// server-rolled-back, because a commit-phase network error may mean the commit
// already succeeded). See ErrCommitPhase.
var postgresTxClassifier = retryClassifier{
	preCommit: isRetryablePostgresPreCommitError,
	commit:    isSafeRetryablePostgresError,
}
