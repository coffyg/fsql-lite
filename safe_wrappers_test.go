// safe_wrappers_test.go
package fsql

import (
	"fmt"
	"testing"
	"time"
)

// TestSafeExec tests the SafeExec wrapper function
func TestSafeExec(t *testing.T) {
	cleanDatabase(t)

	// Test SafeExec with valid INSERT
	uuid := GenNewUUID("")
	_, err := SafeExec(
		`INSERT INTO ai_model (uuid, key, name, type, provider) VALUES ($1, $2, $3, $4, $5)`,
		uuid, "test_key", "Test Model", "test_type", "test_provider",
	)
	if err != nil {
		t.Fatalf("SafeExec failed: %v", err)
	}

	// Verify the insert worked
	var count int
	err = Db.Get(&count, "SELECT COUNT(*) FROM ai_model WHERE uuid = $1", uuid)
	if err != nil {
		t.Fatalf("Failed to verify insert: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 record, got %d", count)
	}
}

// TestSafeQuery tests the SafeQuery wrapper function
func TestSafeQuery(t *testing.T) {
	cleanDatabase(t)

	// Insert test data
	uuid1 := GenNewUUID("")
	uuid2 := GenNewUUID("")
	_, err := SafeExec(
		`INSERT INTO ai_model (uuid, key, name, type, provider) VALUES ($1, $2, $3, $4, $5), ($6, $7, $8, $9, $10)`,
		uuid1, "key1", "Model 1", "type1", "provider1",
		uuid2, "key2", "Model 2", "type2", "provider2",
	)
	if err != nil {
		t.Fatalf("Failed to setup test data: %v", err)
	}

	// Test SafeQuery
	rows, err := SafeQuery("SELECT uuid, key FROM ai_model WHERE type LIKE $1", "type%")
	if err != nil {
		t.Fatalf("SafeQuery failed: %v", err)
	}
	defer rows.Close()

	// Count results
	var count int
	for rows.Next() {
		var uuid, key string
		err = rows.Scan(&uuid, &key)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
	}

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}

// TestSafeGet tests the SafeGet wrapper function
func TestSafeGet(t *testing.T) {
	cleanDatabase(t)

	// Insert test data
	uuid := GenNewUUID("")
	_, err := SafeExec(
		`INSERT INTO ai_model (uuid, key, name, type, provider) VALUES ($1, $2, $3, $4, $5)`,
		uuid, "test_key", "Test Model", "test_type", "test_provider",
	)
	if err != nil {
		t.Fatalf("Failed to setup test data: %v", err)
	}

	// Test SafeGet
	var model AIModel
	err = SafeGet(&model, "SELECT uuid, key, name, type, provider FROM ai_model WHERE uuid = $1", uuid)
	if err != nil {
		t.Fatalf("SafeGet failed: %v", err)
	}

	if model.UUID != uuid {
		t.Errorf("Expected UUID %s, got %s", uuid, model.UUID)
	}
	if model.Key != "test_key" {
		t.Errorf("Expected key 'test_key', got %s", model.Key)
	}
}

// TestSafeSelect tests the SafeSelect wrapper function
func TestSafeSelect(t *testing.T) {
	cleanDatabase(t)

	// Insert multiple test records
	for i := 1; i <= 3; i++ {
		uuid := GenNewUUID("")
		_, err := SafeExec(
			`INSERT INTO ai_model (uuid, key, name, type, provider) VALUES ($1, $2, $3, $4, $5)`,
			uuid, fmt.Sprintf("key_%d", i), fmt.Sprintf("Model %d", i), "test_type", "test_provider",
		)
		if err != nil {
			t.Fatalf("Failed to setup test data: %v", err)
		}
	}

	// Test SafeSelect
	var models []AIModel
	err := SafeSelect(&models, "SELECT uuid, key, name, type, provider FROM ai_model WHERE type = $1", "test_type")
	if err != nil {
		t.Fatalf("SafeSelect failed: %v", err)
	}

	if len(models) != 3 {
		t.Errorf("Expected 3 models, got %d", len(models))
	}
}

// TestSafeQueryRow tests the SafeQueryRow wrapper function
func TestSafeQueryRow(t *testing.T) {
	cleanDatabase(t)

	// Insert test data
	uuid := GenNewUUID("")
	_, err := SafeExec(
		`INSERT INTO ai_model (uuid, key, name, type, provider) VALUES ($1, $2, $3, $4, $5)`,
		uuid, "test_key", "Test Model", "test_type", "test_provider",
	)
	if err != nil {
		t.Fatalf("Failed to setup test data: %v", err)
	}

	// Test SafeQueryRow
	var retrievedKey string
	row := SafeQueryRow("SELECT key FROM ai_model WHERE uuid = $1", uuid)
	err = row.Scan(&retrievedKey)
	if err != nil {
		t.Fatalf("SafeQueryRow failed: %v", err)
	}

	if retrievedKey != "test_key" {
		t.Errorf("Expected key 'test_key', got %s", retrievedKey)
	}
}

// TestSafeNamedExec tests the SafeNamedExec wrapper function
func TestSafeNamedExec(t *testing.T) {
	cleanDatabase(t)

	// Test SafeNamedExec with map
	params := map[string]interface{}{
		"uuid":     GenNewUUID(""),
		"key":      "named_test_key",
		"name":     "Named Test Model",
		"type":     "named_test_type",
		"provider": "named_test_provider",
	}

	query := `INSERT INTO ai_model (uuid, key, name, type, provider)
	          VALUES (:uuid, :key, :name, :type, :provider)`

	result, err := SafeNamedExec(query, params)
	if err != nil {
		t.Fatalf("SafeNamedExec failed: %v", err)
	}

	// Check that exactly one row was affected
	rowsAffected := result.RowsAffected()
	if rowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", rowsAffected)
	}

	// Verify the insert
	var count int
	err = SafeGet(&count, "SELECT COUNT(*) FROM ai_model WHERE uuid = $1", params["uuid"])
	if err != nil {
		t.Fatalf("Failed to verify insert: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 record, got %d", count)
	}
}

// TestSafeNamedQuery tests the SafeNamedQuery wrapper function
func TestSafeNamedQuery(t *testing.T) {
	cleanDatabase(t)

	// Insert test data
	for i := 1; i <= 2; i++ {
		uuid := GenNewUUID("")
		_, err := SafeExec(
			`INSERT INTO ai_model (uuid, key, name, type, provider) VALUES ($1, $2, $3, $4, $5)`,
			uuid, fmt.Sprintf("key_%d", i), fmt.Sprintf("Model %d", i), "query_test_type", "test_provider",
		)
		if err != nil {
			t.Fatalf("Failed to setup test data: %v", err)
		}
	}

	// Test SafeNamedQuery with map
	params := map[string]interface{}{
		"search_type": "query_test_type",
	}

	rows, err := SafeNamedQuery("SELECT uuid, key FROM ai_model WHERE type = :search_type", params)
	if err != nil {
		t.Fatalf("SafeNamedQuery failed: %v", err)
	}
	defer rows.Close()

	// Count results
	var count int
	for rows.Next() {
		var uuid, key string
		err = rows.Scan(&uuid, &key)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
	}

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}

// TestSafeBeginx tests the SafeBeginx wrapper function
func TestSafeBeginx(t *testing.T) {
	cleanDatabase(t)

	// Test SafeBeginx transaction
	tx, err := SafeBeginx()
	if err != nil {
		t.Fatalf("SafeBeginx failed: %v", err)
	}

	// Test transaction operations
	uuid := GenNewUUID("")
	_, err = tx.Exec(`INSERT INTO ai_model (uuid, key, name, type, provider)
	                  VALUES ($1, $2, $3, $4, $5)`,
		uuid, "tx_test_key", "TX Test Model", "tx_test_type", "tx_test_provider")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Transaction exec failed: %v", err)
	}

	// Use tx.Get method
	var model AIModel
	err = tx.Get(&model, "SELECT uuid, key, type, provider FROM ai_model WHERE uuid = $1", uuid)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Transaction Get failed: %v", err)
	}
	if model.UUID != uuid {
		tx.Rollback()
		t.Errorf("Expected UUID %s, got %s", uuid, model.UUID)
	}

	// Rollback and verify data doesn't exist outside transaction
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Transaction rollback failed: %v", err)
	}

	var count int
	err = SafeGet(&count, "SELECT COUNT(*) FROM ai_model WHERE uuid = $1", uuid)
	if err != nil {
		t.Fatalf("Failed to verify rollback: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 records after rollback, got %d", count)
	}
}

// TestSafeWrappersTimeout tests that Safe wrappers respect timeouts
func TestSafeWrappersTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping timeout test in short mode")
	}

	// Test that SafeExec times out with a slow query (100ms timeout)
	_, err := SafeExecTimeout(100*time.Millisecond, "SELECT pg_sleep(1)")
	if err == nil {
		t.Error("Expected timeout error, got nil")
	}
}

// TestSafeExecTimeoutCustom tests custom timeout behavior
func TestSafeExecTimeoutCustom(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping timeout test in short mode")
	}

	// Test with very short custom timeout (should fail)
	_, err := SafeExecTimeout(50*time.Millisecond, "SELECT pg_sleep(0.5)")
	if err == nil {
		t.Error("Expected timeout error with 50ms timeout, got nil")
	}

	// Test with sufficient custom timeout (should succeed)
	_, err = SafeExecTimeout(2*time.Second, "SELECT 1")
	if err != nil {
		t.Errorf("Expected success with 2s timeout, got error: %v", err)
	}
}

// TestAllSafeWrappersSuccess tests all Safe functions work correctly without timeout
func TestAllSafeWrappersSuccess(t *testing.T) {
	cleanDatabase(t)

	// Test SafeExec success
	_, err := SafeExec("SELECT 1")
	if err != nil {
		t.Errorf("SafeExec failed: %v", err)
	}

	// Test SafeExecTimeout success
	_, err = SafeExecTimeout(5*time.Second, "SELECT 1")
	if err != nil {
		t.Errorf("SafeExecTimeout failed: %v", err)
	}

	// Test SafeGet success
	var result int
	err = SafeGet(&result, "SELECT 1")
	if err != nil {
		t.Errorf("SafeGet failed: %v", err)
	}
	if result != 1 {
		t.Errorf("Expected result 1, got %d", result)
	}

	// Test SafeGetTimeout success
	err = SafeGetTimeout(5*time.Second, &result, "SELECT 2")
	if err != nil {
		t.Errorf("SafeGetTimeout failed: %v", err)
	}
	if result != 2 {
		t.Errorf("Expected result 2, got %d", result)
	}

	// Test SafeSelect success
	var results []int
	err = SafeSelect(&results, "SELECT 1 UNION SELECT 2")
	if err != nil {
		t.Errorf("SafeSelect failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	// Test SafeSelectTimeout success
	var results2 []int
	err = SafeSelectTimeout(5*time.Second, &results2, "SELECT 3 UNION SELECT 4")
	if err != nil {
		t.Errorf("SafeSelectTimeout failed: %v", err)
	}
	if len(results2) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results2))
	}

	// Test SafeQueryRow success
	row := SafeQueryRow("SELECT 42")
	var value int
	err = row.Scan(&value)
	if err != nil {
		t.Errorf("SafeQueryRow scan failed: %v", err)
	}
	if value != 42 {
		t.Errorf("Expected value 42, got %d", value)
	}

	// Test SafeQueryRowTimeout success
	row = SafeQueryRowTimeout(5*time.Second, "SELECT 99")
	err = row.Scan(&value)
	if err != nil {
		t.Errorf("SafeQueryRowTimeout scan failed: %v", err)
	}
	if value != 99 {
		t.Errorf("Expected value 99, got %d", value)
	}
}
