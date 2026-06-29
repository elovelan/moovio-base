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
// It is used directly as the commit-phase classifier for RetryPostgresTx
// (where network errors are NOT safe because the commit may have succeeded —
// see ErrCommitPhase) and as the foundation of the pre-commit classifier
// isRetryablePostgresPreCommitError (which adds network errors via
// isPostgresNetworkError, since pre-commit the transaction rolls back).
//
// Network-level errors (net.OpError, io.EOF) are intentionally NOT classified
// here: without an explicit transaction there is no way to know whether the
// error occurred before or after the server accepted the query, and at commit
// time a network error could mean the commit succeeded but the response was
// lost.
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
	// Note: 08xxx (connection_exception class) codes are intentionally omitted.
	// pgx surfaces connection-level failures as network errors, not as
	// *pgconn.PgError, so those cases are handled by isPostgresNetworkError
	// (used by isRetryablePostgresPreCommitError within a transaction).
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

// isPostgresNetworkError returns true for typed network-level errors that
// indicate the TCP connection was severed. These are safe to retry ONLY within
// an explicit transaction (the server rolls back the uncommitted transaction),
// so this helper is used by isRetryablePostgresPreCommitError and NOT by
// isSafeRetryablePostgresError.
//
// String-matching on error messages is intentionally avoided: pgx flags
// pre-send failures via pgconn.SafeToRetry (which the stdlib adapter converts
// to driver.ErrBadConn), and post-send failures surface as typed net.OpError /
// io errors, so the typed checks are sufficient and far less fragile than
// substring matching.
func isPostgresNetworkError(err error) bool {
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return true
	}
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
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
// BeginTx or fn). It is isSafeRetryablePostgresError OR isPostgresNetworkError:
// pre-commit, typed network errors are also safe to retry because the server
// rolls back the uncommitted transaction.
//
// Permanent application errors are intentionally NOT retried here, even though
// retrying them would be safe (the transaction rolls back): they would fail
// again on the next attempt, so retrying only adds ~200ms of latency (3
// attempts with full-jitter backoff up to retryJitterMax) before the caller
// receives the error. The categories we deliberately do NOT retry are:
//
//   - Constraint violations (class 23xxx) — e.g. unique_violation 23505,
//     foreign_key_violation 23503, check_violation 23514, not_null_violation
//     23502. The data conflict persists across retries.
//   - Syntax errors (42601) — the SQL is malformed; indicates a code bug.
//   - Schema errors (class 42xxx) — e.g. undefined_table 42P01,
//     undefined_column 42703. Indicates a migration drift or code bug.
//   - Permission errors (42501 insufficient_privilege) — permissions don't
//     change mid-query.
//   - Data exceptions (class 22xxx) — e.g. string_data_right_truncation 22001,
//     datatype_mismatch 42804. The data doesn't fit the column/type.
//
// These are discovered at query time (not connection time, where
// incompatibility/handshake failures surface), so they reach fn and would
// recur on retry. Callers who want to retry a broader set (e.g. a custom
// "transaction rolled back" sentinel from a stored procedure) can pass
// opts.IsRetryable, which is additive on top of this classifier.
func isRetryablePostgresPreCommitError(err error) bool {
	return isSafeRetryablePostgresError(err) || isPostgresNetworkError(err)
}

// RetryPostgresTx executes fn inside a Postgres transaction, retrying the
// entire transaction on transient errors. fn receives a fresh *sql.Tx each
// attempt; if fn returns an error the tx is rolled back, if nil the tx is
// committed.
//
// Pre-commit errors (from BeginTx or fn) are retried when
// isRetryablePostgresPreCommitError(err) OR opts.IsRetryable(err) is true — a
// broad set (pre-send + server-rolled-back + network), because the transaction
// rolls back so retrying is safe. Permanent application errors (constraint
// violations, syntax/schema/permission errors) are NOT retried; see
// isRetryablePostgresPreCommitError's doc for the full list.
//
// Commit-phase errors (from (*sql.Tx).Commit) are retried only when
// isSafeRetryablePostgresError(err) OR opts.IsRetryable(err) is true — a
// narrower set (pre-send + server-rolled-back; network errors excluded),
// because a commit-phase network error may mean the commit already succeeded
// (e.g., the connection was severed after the COMMIT was sent but before the
// response was received). Commit-phase errors that are NOT retried are
// returned to the caller wrapped with ErrCommitPhase so the caller can decide
// whether to alert, reconcile, or check via pg_xact_status() (future).
//
// db MUST be a *sql.DB backed by the pgx stdlib adapter (e.g. one returned
// by database.New with a PostgresConfig). Using a *sql.DB backed by another
// driver will not produce pgx-specific retries.
func RetryPostgresTx(ctx context.Context, db *sql.DB, opts RetryTxOptions, fn func(*sql.Tx) error) error {
	return retryTx(ctx, db, postgresTxClassifier, opts, fn)
}

// postgresTxClassifier is the per-phase retry classifier used by RetryPostgresTx.
// The preCommit classifier is broad (pre-send + server-rolled-back + network —
// any transient error is safe because the transaction rolls back); the commit
// classifier is the narrow isSafeRetryablePostgresError (pre-send +
// server-rolled-back; network excluded), because a commit-phase network error
// may mean the commit already succeeded. See ErrCommitPhase.
var postgresTxClassifier = retryClassifier{
	preCommit: isRetryablePostgresPreCommitError,
	commit:    isSafeRetryablePostgresError,
}
