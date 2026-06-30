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

// isSafeRetryablePostgresError is the commit-phase classifier for
// RetryPostgresTx: true iff the error guarantees the commit didn't happen, so
// retry is safe even at commit.
//
//	- pgconn.SafeToRetry: pgx guarantees the error occurred before any data
//	  was sent to the server.
//	- SQLSTATE codes that guarantee non-commit: 40001, 40P01, 57014 (server
//	  rejected + rolled back), 57P01 (die() aborts the in-flight tx), 57P03,
//	  53300 (connect-time, never started a tx).
//
// 57P02 (crash_shutdown) excluded: quickdie() does _exit(2) WITHOUT rolling
// back (rollback is via crash recovery on restart), and the commit may have
// been recorded before SIGQUIT — so 57P02 at commit is ambiguous. It's still
// retried pre-commit via isRetryablePostgresPreCommitError's opt-out (the tx
// didn't commit). See ErrCommitPhase.
//
// driver.ErrBadConn not checked: pgx's Commit returns native errors (the
// ErrBadConn conversion is only in Exec/Query), so the commit path never sees
// it; pre-send commit is covered by pgconn.SafeToRetry. *sql.Tx.Commit doesn't
// retry ErrBadConn internally (only *sql.DB methods do), so no double-retry.
//
// Network errors excluded: at commit, could mean commit succeeded but the
// response was lost (see ErrCommitPhase).
//
// Unexported: use RetryPostgresTx or RetryUnsafe rather than building on this.
func isSafeRetryablePostgresError(err error) bool {
	if err == nil {
		return false
	}
	if pgconn.SafeToRetry(err) {
		return true
	}
	// SQLSTATE codes that guarantee the commit didn't happen:
	//   40001 serialization_failure, 40P01 deadlock_detected — server rejected
	//   the commit and rolled back (ErrorResponse proves non-commit). See
	//   https://www.postgresql.org/docs/current/mvcc-serialization-failure-handling.html
	//   57P01 admin_shutdown — die() aborts the in-flight tx before sending.
	//   57P03 cannot_connect_now, 53300 too_many_connections — connect-time
	//   rejections (won't surface from Commit, but harmless).
	//   57014 query_canceled — server canceled and rolled back.
	// 57P02 (crash_shutdown) excluded — see doc comment above.
	// 08xxx omitted: pgx surfaces those as network errors, not *pgconn.PgError.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "57P01", "57P03", // admin_shutdown, cannot_connect_now
			"40001", "40P01", // serialization_failure, deadlock_detected
			"53300", // too_many_connections
			"57014": // query_canceled
			return true
		}
	}
	return false
}

// isPermanentPostgresError reports SQLSTATE classes that would fail again on
// retry, used by isRetryablePostgresPreCommitError to opt out. Only
// clearly-permanent classes are listed so unknown/transient codes are retried.
//
//	- 22xxx data exceptions (e.g. 22001 string_data_right_truncation)
//	- 23xxx integrity constraint violations (e.g. 23505 unique_violation)
//	- 42xxx syntax/access-rule violations (42501, 42601, 42P01, 42703, 42804)
func isPermanentPostgresError(pgErr *pgconn.PgError) bool {
	return strings.HasPrefix(pgErr.Code, "22") || // data exception
		strings.HasPrefix(pgErr.Code, "23") || // integrity constraint violation
		strings.HasPrefix(pgErr.Code, "42") // syntax / access rule violation
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
