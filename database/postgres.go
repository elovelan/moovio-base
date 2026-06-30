package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"cloud.google.com/go/alloydbconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/moov-io/base/log"
)

func postgresConnection(ctx context.Context, logger log.Logger, config PostgresConfig, databaseName string) (*sql.DB, error) {
	poolConfig, err := buildPgxPoolConfig(ctx, config, databaseName)
	if err != nil {
		return nil, logger.LogErrorf("building pgx pool config: %w", err).Err()
	}

	// HealthCheckPeriod is how often the background goroutine evicts connections
	// that exceeded MaxConnLifetime or MaxConnIdleTime. It does NOT ping for
	// liveness — dead connections are caught at acquire time by the ResetSession
	// ping (default: ping if idle > 1s), with database/sql retrying on a fresh
	// conn and RetryPostgresTx retrying beyond that.
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
	if errors.As(err, &pgError) && pgError.Code == pgerrcode.UniqueViolation {
		return true
	}

	return strings.Contains(err.Error(), pgerrcode.UniqueViolation)
}

// PostgresDeadlockFound returns true when the provided error matches the Postgres code
// for deadlock found.
func PostgresDeadlockFound(err error) bool {
	if err == nil {
		return false
	}

	var pgError *pgconn.PgError
	if errors.As(err, &pgError) && pgError.Code == pgerrcode.DeadlockDetected {
		return true
	}

	return strings.Contains(err.Error(), pgerrcode.DeadlockDetected)
}

// isSafeRetryablePostgresError is the commit-phase classifier for
// RetryPostgresTx: true iff the error guarantees the commit didn't happen, so
// retry is safe even at commit. Maximally conservative — only errors that can
// NEVER mean "possibly committed":
//
//	- pgconn.SafeToRetry: pgx guarantees the error occurred before any data
//	  was sent to the server (COMMIT message never sent).
//	- pgerrcode.IsTransactionRollback: class 40 SQLSTATE codes (40001
//	  serialization_failure, 40P01 deadlock_detected, 40002, 40003, 40000) —
//	  the server rolled back the transaction before returning the ErrorResponse.
//	  See https://www.postgresql.org/docs/current/mvcc-serialization-failure-handling.html
//
// Excluded (could mean "possibly committed" → wrapped with ErrCommitPhase):
//	- 57P01 admin_shutdown, 57P02 crash_shutdown: die()/quickdie() are signal
//	  handlers that can interrupt after the commit is recorded but before the
//	  response is sent. quickdie() also skips rollback (_exit(2)).
//	- 57014 query_canceled: a cancel arriving after the commit records is a
//	  theoretical edge case.
//	- 57P03/53300: connect-time, won't surface from Commit.
//	- network errors: could mean commit succeeded but the response was lost.
//	- driver.ErrBadConn: pgx's Commit returns native errors (the ErrBadConn
//	  conversion is only in Exec/Query), so the commit path never sees it;
//	  pre-send commit is covered by pgconn.SafeToRetry. *sql.Tx.Commit doesn't
//	  retry ErrBadConn internally (only *sql.DB methods do).
//
// Everything excluded here is still retried pre-commit via
// isRetryablePostgresPreCommitError's opt-out (the tx didn't commit pre-commit).
//
// Unexported: use RetryPostgresTx or RetryUnsafe rather than building on this.
func isSafeRetryablePostgresError(err error) bool {
	if err == nil {
		return false
	}
	if pgconn.SafeToRetry(err) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgerrcode.IsTransactionRollback(pgErr.Code)
	}
	return false
}

// isPermanentPostgresError reports SQLSTATE classes that would fail again on
// retry, used by isRetryablePostgresPreCommitError to opt out. Only
// clearly-permanent classes are listed so unknown/transient codes are retried.
func isPermanentPostgresError(pgErr *pgconn.PgError) bool {
	return pgerrcode.IsDataException(pgErr.Code) || // class 22
		pgerrcode.IsIntegrityConstraintViolation(pgErr.Code) || // class 23
		pgerrcode.IsSyntaxErrororAccessRuleViolation(pgErr.Code) // class 42
}

// retryJitterMax is the upper bound on the random delay between retries. Full
// jitter in [0, retryJitterMax) spreads concurrent retries across the fleet
// rather than slamming the database all at once. AlloyDB maintenance
// switchovers cause <1s downtime, so maxRetryAttempts attempts with up to
// retryJitterMax between each spans the blip; the caller's context deadline is
// the escape hatch for tighter latency budgets.
const retryJitterMax = 100 * time.Millisecond

// isRetryablePostgresPreCommitError is the pre-commit classifier for
// RetryPostgresTx (errors from BeginTx or fn). OPTS OUT: pre-commit retry is
// always safe (the transaction rolls back), so it retries everything EXCEPT
// context.Canceled/DeadlineExceeded and the permanent SQLSTATE classes
// (isPermanentPostgresError). Unknown *pgconn.PgError codes and non-PgError
// errors (network, driver.ErrBadConn, SafeToRetry) are retried — a false
// positive (~200ms wasted on a permanent error) costs less than a false
// negative (spurious user-facing failure), so opt-out is safer than opt-in.
//
// opts.IsRetryable is additive (OR); to narrow the set, wrap with a predicate
// that re-checks.
func isRetryablePostgresPreCommitError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return !isPermanentPostgresError(pgErr)
	}
	return true
}

// RetryPostgresTx runs fn in a Postgres transaction, retrying the whole
// transaction on transient errors. fn gets a fresh *sql.Tx each attempt; if it
// returns an error the tx is rolled back, if nil the tx is committed.
//
// Pre-commit errors use the opt-out isRetryablePostgresPreCommitError; commit
// errors use the narrower opt-in isSafeRetryablePostgresError (a commit-phase
// network error may mean the commit already succeeded). Non-retried commit
// errors are wrapped with ErrCommitPhase. opts.IsRetryable is additive on both.
//
// db must be pgx-backed (e.g. from database.New with a PostgresConfig); other
// drivers won't get pgx-specific retries.
func RetryPostgresTx(ctx context.Context, db *sql.DB, opts RetryTxOptions, fn func(*sql.Tx) error) error {
	return retryTx(ctx, db, postgresTxClassifier, opts, fn)
}

// postgresTxClassifier pairs the pre-commit (opt-out) and commit (opt-in)
// classifiers. See ErrCommitPhase.
var postgresTxClassifier = retryClassifier{
	preCommit: isRetryablePostgresPreCommitError,
	commit:    isSafeRetryablePostgresError,
}
