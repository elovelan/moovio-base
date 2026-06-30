package database_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/moov-io/base"
	"github.com/moov-io/base/database"
	"github.com/moov-io/base/database/testdb"
	"github.com/moov-io/base/log"
	"github.com/stretchr/testify/require"
)

var (
	alloydbInstanceURI    = os.Getenv("ALLOYDB_INSTANCE_URI")
	alloydbDBName         = os.Getenv("ALLOYDB_DBNAME")
	alloydbIAMUser        = os.Getenv("ALLOYDB_IAM_USER")
	alloydbNativeUser     = os.Getenv("ALLOYDB_NATIVE_USER")
	alloydbNativePassword = os.Getenv("ALLOYDB_NATIVE_PASSWORD")
)

func TestPostgres_Basic(t *testing.T) {
	if testing.Short() {
		t.Skip("-short flag enabled")
	}

	config := database.DatabaseConfig{
		DatabaseName: "moov",
		Postgres: &database.PostgresConfig{
			Address:  "127.0.0.1:5432",
			User:     "moov",
			Password: "moov",
			Connections: database.ConnectionsConfig{
				MaxOpen:     4,
				MaxIdle:     4,
				MaxLifetime: time.Minute * 2,
				MaxIdleTime: time.Minute * 2,
			},
		},
	}

	db, err := database.New(context.Background(), log.NewTestLogger(), config)
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()
}

func TestPostgres_TLS(t *testing.T) {
	if testing.Short() {
		t.Skip("-short flag enabled")
	}

	config := database.DatabaseConfig{
		DatabaseName: "moov",
		Postgres: &database.PostgresConfig{
			Address:  "127.0.0.1:5432",
			User:     "moov",
			Password: "moov",
			TLS: &database.PostgresTLSConfig{
				CACertFile:     filepath.Join("..", "testcerts", "root.crt"),
				ClientCertFile: filepath.Join("..", "testcerts", "client.crt"),
				ClientKeyFile:  filepath.Join("..", "testcerts", "client.key"),
			},
		},
	}

	db, err := database.New(context.Background(), log.NewTestLogger(), config)
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()
}

func TestProstres_Alloy(t *testing.T) {
	if testing.Short() {
		t.Skip("-short flag enabled")
	}

	if alloydbInstanceURI == "" || alloydbDBName == "" || alloydbNativeUser == "" || alloydbNativePassword == "" {
		t.Skip("missing required environment variables")
	}

	config := database.DatabaseConfig{
		DatabaseName: alloydbDBName,
		Postgres: &database.PostgresConfig{
			User:     alloydbNativeUser,
			Password: alloydbNativePassword,
			Alloy: &database.PostgresAlloyConfig{
				InstanceURI: alloydbInstanceURI,
				UseIAM:      false,
				UsePSC:      true,
			},
		},
	}

	db, err := database.New(context.Background(), log.NewTestLogger(), config)
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()
}

func TestProstres_Alloy_IAM(t *testing.T) {
	if testing.Short() {
		t.Skip("-short flag enabled")
	}

	if alloydbInstanceURI == "" || alloydbDBName == "" || alloydbIAMUser == "" {
		t.Skip("missing required environment variables")
	}

	config := database.DatabaseConfig{
		DatabaseName: alloydbDBName,
		Postgres: &database.PostgresConfig{
			User: alloydbIAMUser,
			Alloy: &database.PostgresAlloyConfig{
				InstanceURI: alloydbInstanceURI,
				UseIAM:      true,
				UsePSC:      true,
			},
		},
	}

	db, err := database.New(context.Background(), log.NewTestLogger(), config)
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()
}

func Test_Postgres_Embedded_Migration(t *testing.T) {
	if testing.Short() {
		t.Skip("-short flag enabled")
	}

	// create a test postgres db
	config := database.DatabaseConfig{
		DatabaseName: "postgres" + base.ID(),
		Postgres: &database.PostgresConfig{
			Address:  "127.0.0.1:5432",
			User:     "moov",
			Password: "moov",
		},
	}

	err := testdb.NewPostgresDatabase(t, config)
	require.NoError(t, err)

	db, err := database.NewAndMigrate(context.Background(), log.NewDefaultLogger(), config, database.WithEmbeddedMigrations(base.PostgresMigrations))
	require.NoError(t, err)
	defer db.Close()
}

func Test_Postgres_Alloy_Migrations(t *testing.T) {
	if testing.Short() {
		t.Skip("-short flag enabled")
	}

	if alloydbInstanceURI == "" || alloydbDBName == "" || alloydbNativeUser == "" || alloydbNativePassword == "" {
		t.Skip("missing required environment variables")
	}

	config := database.DatabaseConfig{
		DatabaseName: alloydbDBName,
		Postgres: &database.PostgresConfig{
			User:     alloydbNativeUser,
			Password: alloydbNativePassword,
			Alloy: &database.PostgresAlloyConfig{
				InstanceURI: alloydbInstanceURI,
				UseIAM:      false,
				UsePSC:      true,
			},
		},
	}

	// migrating database given by ALLOYDB_DBNAME env var

	db, err := database.NewAndMigrate(context.Background(), log.NewDefaultLogger(), config, database.WithEmbeddedMigrations(base.PostgresMigrations))
	require.NoError(t, err)
	defer db.Close()
}

func TestRetryUnsafe(t *testing.T) {
	t.Run("succeeds on first attempt", func(t *testing.T) {
		calls := 0
		err := database.RetryUnsafe(context.Background(), database.RetryUnsafeOptions{}, func() error {
			calls++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, calls)
	})

	t.Run("retries on any error by default, including non-Pg-retryable ones", func(t *testing.T) {
		// unique_violation is NOT retryable per IsRetryablePostgresError, but
		// RetryUnsafe's default predicate retries any error except context
		// cancellation because the caller has vouched that fn is idempotent.
		calls := 0
		err := database.RetryUnsafe(context.Background(), database.RetryUnsafeOptions{}, func() error {
			calls++
			if calls < 3 {
				return &pgconn.PgError{Code: pgerrcode.UniqueViolation}
			}
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 3, calls)
	})

	t.Run("custom IsRetryable short-circuits non-retryable errors", func(t *testing.T) {
		calls := 0
		neverRetry := func(error) bool { return false }
		err := database.RetryUnsafe(context.Background(), database.RetryUnsafeOptions{IsRetryable: neverRetry}, func() error {
			calls++
			return io.EOF
		})
		require.Error(t, err)
		require.Equal(t, 1, calls)
		require.ErrorIs(t, err, io.EOF)
	})

	t.Run("does not retry context cancellation by default", func(t *testing.T) {
		calls := 0
		err := database.RetryUnsafe(context.Background(), database.RetryUnsafeOptions{}, func() error {
			calls++
			return context.Canceled
		})
		require.Error(t, err)
		require.Equal(t, 1, calls)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("respects context cancellation between attempts", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel immediately

		calls := 0
		err := database.RetryUnsafe(ctx, database.RetryUnsafeOptions{}, func() error {
			calls++
			return io.EOF // retryable, but context is done
		})
		// First call happens, then context cancellation is detected before the next attempt
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, calls)
	})

	t.Run("exhausts all attempts", func(t *testing.T) {
		calls := 0
		err := database.RetryUnsafe(context.Background(), database.RetryUnsafeOptions{}, func() error {
			calls++
			return io.EOF
		})
		require.Error(t, err)
		require.Equal(t, 3, calls)
		require.ErrorIs(t, err, io.EOF)
	})
}

func Test_Postgres_UniqueViolation(t *testing.T) {
	if testing.Short() {
		t.Skip("-short flag enabled")
	}

	// create a test postgres db
	config := database.DatabaseConfig{
		DatabaseName: "postgres" + base.ID(),
		Postgres: &database.PostgresConfig{
			Address:  "127.0.0.1:5432",
			User:     "moov",
			Password: "moov",
		},
	}

	err := testdb.NewPostgresDatabase(t, config)
	require.NoError(t, err)

	db, err := database.New(context.Background(), log.NewDefaultLogger(), config)
	require.NoError(t, err)

	createQry := `CREATE TABLE names (id SERIAL PRIMARY KEY, name VARCHAR(255));`
	_, err = db.Exec(createQry)
	require.NoError(t, err)

	insertQry := `INSERT INTO names (id, name) VALUES ($1, $2);`
	_, err = db.Exec(insertQry, 1, "James")
	require.NoError(t, err)

	_, err = db.Exec(insertQry, 1, "James")
	require.Error(t, err)
	require.True(t, database.UniqueViolation(err))
}
