package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/golang-migrate/migrate/v4/database"
	migspanner "github.com/golang-migrate/migrate/v4/database/spanner"
	_ "github.com/googleapis/go-sql-spanner"
	"google.golang.org/grpc/codes"

	"github.com/moov-io/base/log"
)

func spannerConnection(_ log.Logger, cfg SpannerConfig, databaseName string) (*sql.DB, error) {
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", cfg.Project, cfg.Instance, databaseName))
	if err != nil {
		return nil, err
	}

	return db, nil
}

func SpannerMigrationDriver(cfg SpannerConfig, databaseName string) (database.Driver, error) {
	clean := !cfg.DisableCleanStatements

	s := migspanner.Spanner{}
	return s.Open(fmt.Sprintf("spanner://projects/%s/instances/%s/databases/%s?x-migrations-table=spanner_schema_migrations&x-clean-statements=%t", cfg.Project, cfg.Instance, databaseName, clean))
}

// SpannerUniqueViolation returns true when the provided error matches the Spanner code
// for duplicate entries (violating a unique table constraint).
// Refer to https://cloud.google.com/spanner/docs/error-codes for Spanner error definitions,
// and https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto for error codes
func SpannerUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	return spanner.ErrCode(err) == codes.AlreadyExists ||
		strings.Contains(err.Error(), "AlreadyExists")
}

// RetrySpannerTx is the Spanner equivalent of RetryPostgresTx.
// TODO: consider delegating to spannerdriver.RunTransactionWithOptions for
// ABORTED replay (it replays statements + buffered mutations on a new
// transaction and surfaces ErrAbortedDueToConcurrentModification when the
// replay sees different data), plus network-error retry on top. The shared
// retryTx helper in retry.go already handles driver.ErrBadConn
// (backend-agnostic) and the Begin/fn/Commit/Rollback loop; the missing
// piece is a Spanner-specific default classifier analogous to
// isRetryablePostgresTxError. See
// https://pkg.go.dev/github.com/googleapis/go-sql-spanner
func RetrySpannerTx(ctx context.Context, db *sql.DB, opts RetryTxOptions, fn func(*sql.Tx) error) error {
	_ = ctx
	_ = db
	_ = opts
	_ = fn
	return errors.New("database: RetrySpannerTx is not yet implemented; see TODO")
}
