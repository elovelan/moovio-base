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

// IsRetryablePostgresError returns true if the error is a PostgreSQL SQLSTATE
// code that the server guarantees was rolled back before the error was
// returned. These are safe to retry even without an explicit transaction
// because the server has already undone any partial work.
//
// Network-level errors (connection reset, broken pipe, EOF, etc.) are
// intentionally NOT classified as retryable here: without an explicit
// transaction there is no way to know whether the error occurred before or
// after the server accepted the query, so retrying could duplicate committed
// work. Callers who wrap their work in a transaction should use RetryPostgresTx,
// whose classifier (isRetryablePostgresTxError) adds network errors back in
// because the transaction makes them safe to retry.
func IsRetryablePostgresError(err error) bool {
	if err == nil {
		return false
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
	// (used by isRetryablePostgresTxError within a transaction).
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

	// Non-PgError errors (network errors, context errors, application errors)
	// are not retryable here — see the doc comment above.
	return false
}

// isPostgresNetworkError returns true for typed network-level errors that
// indicate the TCP connection was severed. These are safe to retry ONLY within
// an explicit transaction (the server rolls back the uncommitted transaction),
// so this helper is used by isRetryablePostgresTxError and NOT by
// IsRetryablePostgresError.
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

// isRetryablePostgresTxError is the default classifier used by RetryPostgresTx.
// It is the OR of four signals:
//
//  1. errors.Is(err, driver.ErrBadConn) — covers fn's tx.Exec/tx.Query
//     pre-send failures. pgx's stdlib adapter converts pgconn.SafeToRetry-
//     flagged errors from Exec/Query into driver.ErrBadConn before
//     database/sql (and thus fn) sees them, discarding the original pgx
//     error. This is the primary retry trigger inside a transaction.
//  2. pgconn.SafeToRetry(err) — covers Begin/Commit failures where the
//     original pgx error flows through unchanged (connect errors, conn-busy,
//     HA NotPreferredError, pre-send timeouts).
//  3. IsRetryablePostgresError(err) — covers server-returned SQLSTATE codes
//     that guarantee rollback (40001, 40P01, 57P01, 57P02, 57P03, 53300,
//     57014). These are NOT SafeToRetry-flagged by pgx and flow through
//     unchanged.
//  4. isPostgresNetworkError(err) — covers typed network errors (net.OpError,
//     io.EOF, io.ErrUnexpectedEOF) where the TCP connection was severed.
//     These are safe to retry within an explicit transaction because the
//     server rolls back the uncommitted transaction. This leg is intentionally
//     NOT part of IsRetryablePostgresError (which is for use without a
//     transaction, where network errors are unsafe because you can't know if
//     the query landed). The residual timing issue — commit succeeded but the
//     response was lost — is accepted here and will be addressed by the
//     stacked pg_current_xact_id()/pg_xact_status() commit-verification
//     follow-up.
//
// driver.ErrBadConn is backend-agnostic, so the same "retry on bad conn"
// leg will carry over to RetryMySQLTx / RetrySpannerTx when implemented.
func isRetryablePostgresTxError(err error) bool {
	if errors.Is(err, driver.ErrBadConn) {
		return true
	}
	if pgconn.SafeToRetry(err) {
		return true
	}
	if isPostgresNetworkError(err) {
		return true
	}
	return IsRetryablePostgresError(err)
}

// RetryPostgresTx executes fn inside a Postgres transaction, retrying the
// entire transaction on transient errors. fn receives a fresh *sql.Tx each
// attempt; if fn returns an error the tx is rolled back, if nil the tx is
// committed. An error is retried when isRetryablePostgresTxError(err) OR
// opts.IsRetryable(err) is true.
//
// db MUST be a *sql.DB backed by the pgx stdlib adapter (e.g. one returned
// by database.New with a PostgresConfig). Using a *sql.DB backed by another
// driver will not produce pgx-specific retries.
func RetryPostgresTx(ctx context.Context, db *sql.DB, opts RetryTxOptions, fn func(*sql.Tx) error) error {
	return retryTx(ctx, db, isRetryablePostgresTxError, opts, fn)
}
