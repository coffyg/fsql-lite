package fsql

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// TestTransactionCommit tests a basic transaction commit
func TestTransactionCommit(t *testing.T) {
	cleanDatabase(t)
	ctx := context.Background()

	// Start transaction
	tx, err := BeginTx(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert two realms
	realm1UUID := uuid.New().String()
	realm2UUID := uuid.New().String()

	_, err = tx.Exec("INSERT INTO realm (uuid, name) VALUES ($1, $2)", realm1UUID, "Realm 1")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to insert realm 1: %v", err)
	}

	_, err = tx.Exec("INSERT INTO realm (uuid, name) VALUES ($1, $2)", realm2UUID, "Realm 2")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to insert realm 2: %v", err)
	}

	// Commit
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify records exist
	var count int
	row := DB.QueryRow(ctx, "SELECT COUNT(*) FROM realm")
	err = row.Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}

	if count != 2 {
		t.Errorf("Expected 2 records, got %d", count)
	}
}

// TestTransactionRollback tests transaction rollback
func TestTransactionRollback(t *testing.T) {
	cleanDatabase(t)
	ctx := context.Background()

	// Start transaction
	tx, err := BeginTx(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert realm
	realmUUID := uuid.New().String()
	_, err = tx.Exec("INSERT INTO realm (uuid, name) VALUES ($1, $2)", realmUUID, "Test Realm")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to insert realm: %v", err)
	}

	// Rollback
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Verify record doesn't exist
	var count int
	row := DB.QueryRow(ctx, "SELECT COUNT(*) FROM realm")
	err = row.Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}

	if count != 0 {
		t.Errorf("Expected 0 records after rollback, got %d", count)
	}
}

// TestWithTx tests the WithTx helper
func TestWithTx(t *testing.T) {
	cleanDatabase(t)
	ctx := context.Background()

	// Use WithTx helper
	err := WithTx(ctx, func(ctx context.Context, tx *Tx) error {
		realm1UUID := uuid.New().String()
		realm2UUID := uuid.New().String()

		_, err := tx.Exec("INSERT INTO realm (uuid, name) VALUES ($1, $2)", realm1UUID, "Realm 1")
		if err != nil {
			return err
		}

		_, err = tx.Exec("INSERT INTO realm (uuid, name) VALUES ($1, $2)", realm2UUID, "Realm 2")
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		t.Fatalf("WithTx failed: %v", err)
	}

	// Verify records were committed
	var count int
	row := DB.QueryRow(ctx, "SELECT COUNT(*) FROM realm")
	err = row.Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}

	if count != 2 {
		t.Errorf("Expected 2 records, got %d", count)
	}
}

// TestWithTxRollback tests WithTx with rollback on error
func TestWithTxRollback(t *testing.T) {
	cleanDatabase(t)
	ctx := context.Background()

	// Use WithTx with error to cause rollback
	err := WithTx(ctx, func(ctx context.Context, tx *Tx) error {
		realmUUID := uuid.New().String()
		_, err := tx.Exec("INSERT INTO realm (uuid, name) VALUES ($1, $2)", realmUUID, "Test Realm")
		if err != nil {
			return err
		}

		// Return error to trigger rollback
		return errors.New("test error")
	})

	if err == nil {
		t.Fatalf("Expected error from WithTx, got nil")
	}

	// Verify record was not committed
	var count int
	row := DB.QueryRow(ctx, "SELECT COUNT(*) FROM realm")
	err = row.Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}

	if count != 0 {
		t.Errorf("Expected 0 records after rollback, got %d", count)
	}
}

// TestWithTxRetry tests retry logic for retryable errors
func TestWithTxRetry(t *testing.T) {
	cleanDatabase(t)
	ctx := context.Background()

	attemptCount := 0
	err := WithTxRetry(ctx, func(ctx context.Context, tx *Tx) error {
		realmUUID := uuid.New().String()
		_, err := tx.Exec("INSERT INTO realm (uuid, name) VALUES ($1, $2)", realmUUID, "Test Realm")
		if err != nil {
			return err
		}

		attemptCount++
		// Fail first attempt with retryable error
		if attemptCount == 1 {
			return errors.New("deadlock detected")
		}

		return nil
	})

	if err != nil {
		t.Fatalf("WithTxRetry failed: %v", err)
	}

	// Should have retried once
	if attemptCount != 2 {
		t.Errorf("Expected 2 attempts, got %d", attemptCount)
	}

	// Verify final record was committed
	var count int
	row := DB.QueryRow(ctx, "SELECT COUNT(*) FROM realm")
	err = row.Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 record, got %d", count)
	}
}

// TestTransactionIsolationLevels tests different isolation levels
func TestTransactionIsolationLevels(t *testing.T) {
	cleanDatabase(t)
	ctx := context.Background()

	// Test read committed
	opts := DefaultTxOptions
	opts.IsoLevel = pgx.ReadCommitted

	err := WithTxOptions(ctx, opts, func(ctx context.Context, tx *Tx) error {
		_, err := tx.Exec("INSERT INTO realm (uuid, name) VALUES ($1, $2)", uuid.New().String(), "read-committed")
		return err
	})
	if err != nil {
		t.Fatalf("Read committed transaction failed: %v", err)
	}

	// Test repeatable read
	opts.IsoLevel = pgx.RepeatableRead
	err = WithTxOptions(ctx, opts, func(ctx context.Context, tx *Tx) error {
		_, err := tx.Exec("INSERT INTO realm (uuid, name) VALUES ($1, $2)", uuid.New().String(), "repeatable-read")
		return err
	})
	if err != nil {
		t.Fatalf("Repeatable read transaction failed: %v", err)
	}

	// Test serializable
	opts.IsoLevel = pgx.Serializable
	err = WithTxOptions(ctx, opts, func(ctx context.Context, tx *Tx) error {
		_, err := tx.Exec("INSERT INTO realm (uuid, name) VALUES ($1, $2)", uuid.New().String(), "serializable")
		return err
	})
	if err != nil {
		t.Fatalf("Serializable transaction failed: %v", err)
	}

	// Verify all 3 records committed
	var count int
	row := DB.QueryRow(ctx, "SELECT COUNT(*) FROM realm")
	err = row.Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected 3 records, got %d", count)
	}
}

// TestInsertWithTx tests INSERT within transaction
func TestInsertWithTx(t *testing.T) {
	cleanDatabase(t)
	ctx := context.Background()

	err := WithTx(ctx, func(ctx context.Context, tx *Tx) error {
		realmUUID := uuid.New().String()
		values := map[string]interface{}{
			"uuid": realmUUID,
			"name": "TX Realm",
		}

		return InsertWithTx(ctx, tx, "realm", values, "")
	})

	if err != nil {
		t.Fatalf("InsertWithTx failed: %v", err)
	}

	// Verify record exists
	var count int
	row := DB.QueryRow(ctx, "SELECT COUNT(*) FROM realm WHERE name = $1", "TX Realm")
	err = row.Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 record, got %d", count)
	}
}

// TestUpdateWithTx tests UPDATE within transaction
func TestUpdateWithTx(t *testing.T) {
	cleanDatabase(t)
	ctx := context.Background()

	// Insert realm
	realmUUID := uuid.New().String()
	err := Insert(ctx, "realm", map[string]interface{}{
		"uuid": realmUUID,
		"name": "Original",
	}, "")
	if err != nil {
		t.Fatalf("Failed to insert realm: %v", err)
	}

	// Update within transaction
	err = WithTx(ctx, func(ctx context.Context, tx *Tx) error {
		return UpdateWithTx(ctx, tx, "realm", map[string]interface{}{
			"uuid": realmUUID,
			"name": "Updated",
		}, "uuid")
	})

	if err != nil {
		t.Fatalf("UpdateWithTx failed: %v", err)
	}

	// Verify update
	var name string
	row := DB.QueryRow(ctx, "SELECT name FROM realm WHERE uuid = $1", realmUUID)
	err = row.Scan(&name)
	if err != nil {
		t.Fatalf("Failed to fetch realm: %v", err)
	}

	if name != "Updated" {
		t.Errorf("Expected name 'Updated', got '%s'", name)
	}
}

// TestReadOnlyTransaction tests read-only transaction mode
func TestReadOnlyTransaction(t *testing.T) {
	cleanDatabase(t)
	ctx := context.Background()

	// Insert test data
	err := Insert(ctx, "realm", map[string]interface{}{
		"uuid": uuid.New().String(),
		"name": "Test Realm",
	}, "")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Read-only transaction should allow reads
	var count int
	err = WithReadTx(ctx, func(ctx context.Context, tx *Tx) error {
		row := tx.QueryRow("SELECT COUNT(*) FROM realm")
		return row.Scan(&count)
	})

	if err != nil {
		t.Fatalf("Read in read-only tx failed: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 record, got %d", count)
	}

	// Read-only transaction should reject writes
	err = WithReadTx(ctx, func(ctx context.Context, tx *Tx) error {
		_, err := tx.Exec("INSERT INTO realm (uuid, name) VALUES ($1, $2)", uuid.New().String(), "Should Fail")
		return err
	})

	if err == nil {
		t.Error("Expected error from write in read-only transaction, got nil")
	}
}
