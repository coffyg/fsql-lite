package fsql

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
)

var benchDataOnce sync.Once

// ensureBenchData populates test data if it doesn't exist (tests may have cleaned it)
func ensureBenchData(b *testing.B) {
	b.Helper()
	benchDataOnce.Do(func() {
		// Check if data exists
		var count int
		err := DB.QueryRow(context.Background(), "SELECT COUNT(*) FROM ai_model WHERE key LIKE 'bench_model_%'").Scan(&count)
		if err != nil || count < 900 {
			b.Log("Inserting benchmark data...")
			// Insert 1000 ai_models
			for i := 1; i <= 1000; i++ {
				modelType := "bench_type_a"
				if i%3 == 1 {
					modelType = "bench_type_b"
				} else if i%3 == 2 {
					modelType = "bench_type_c"
				}
				provider := "provider_x"
				if i%2 == 1 {
					provider = "provider_y"
				}
				_, _ = DB.Exec(context.Background(),
					`INSERT INTO ai_model (uuid, key, name, type, provider, settings)
					 VALUES (uuid_generate_v4(), $1, $2, $3, $4, '{"max_tokens": 1000}'::jsonb)
					 ON CONFLICT DO NOTHING`,
					fmt.Sprintf("bench_model_%d", i),
					fmt.Sprintf("Benchmark Model %d", i),
					modelType, provider)
			}
		}

		// Check realms
		err = DB.QueryRow(context.Background(), "SELECT COUNT(*) FROM realm WHERE name LIKE 'bench_realm_%'").Scan(&count)
		if err != nil || count < 90 {
			for i := 1; i <= 100; i++ {
				_, _ = DB.Exec(context.Background(),
					`INSERT INTO realm (uuid, name) VALUES (uuid_generate_v4(), $1) ON CONFLICT DO NOTHING`,
					fmt.Sprintf("bench_realm_%d", i))
			}
		}

		// Check websites
		err = DB.QueryRow(context.Background(), "SELECT COUNT(*) FROM website WHERE domain LIKE 'bench_%'").Scan(&count)
		if err != nil || count < 400 {
			// Get a realm UUID
			var realmUUID string
			_ = DB.QueryRow(context.Background(), "SELECT uuid FROM realm WHERE name LIKE 'bench_realm_%' LIMIT 1").Scan(&realmUUID)
			if realmUUID != "" {
				for i := 1; i <= 500; i++ {
					_, _ = DB.Exec(context.Background(),
						`INSERT INTO website (uuid, domain, realm_uuid) VALUES (uuid_generate_v4(), $1, $2) ON CONFLICT DO NOTHING`,
						fmt.Sprintf("bench_%d.example.com", i), realmUUID)
				}
			}
		}
	})
}

// BenchmarkScanSingleRow benchmarks scanning a single row into a struct
func BenchmarkScanSingleRow(b *testing.B) {
	// Ensure data exists (tests may have cleaned it)
	ensureBenchData(b)

	query := aiModelBaseQuery + ` WHERE "ai_model".key = $1 LIMIT 1`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var model AIModel
		err := Db.Get(&model, query, "bench_model_1")
		if err != nil {
			b.Fatalf("Get failed: %v", err)
		}
	}
}

// BenchmarkScanMultipleRows benchmarks scanning multiple rows into a slice (333 rows)
func BenchmarkScanMultipleRows(b *testing.B) {
	ensureBenchData(b)

	query := aiModelBaseQuery + ` WHERE "ai_model".type = $1`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var models []AIModel
		err := Db.Select(&models, query, "bench_type_a")
		if err != nil {
			b.Fatalf("Select failed: %v", err)
		}
		if len(models) < 300 {
			b.Fatalf("Expected ~333 models, got %d", len(models))
		}
	}
}

// BenchmarkScanLinkedFields benchmarks scanning with JOINed/linked fields (500 rows)
func BenchmarkScanLinkedFields(b *testing.B) {
	ensureBenchData(b)

	query := websiteBaseQuery + ` WHERE "website".domain LIKE $1`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var websites []Website
		err := Db.Select(&websites, query, "bench_%")
		if err != nil {
			b.Fatalf("Select failed: %v", err)
		}
		if len(websites) < 400 {
			b.Fatalf("Expected ~500 websites, got %d", len(websites))
		}
		// Verify linked field is populated
		if websites[0].Realm == nil {
			b.Fatal("Linked field Realm is nil")
		}
	}
}

// BenchmarkScanLargeResultSet benchmarks scanning all 1000 models
func BenchmarkScanLargeResultSet(b *testing.B) {
	ensureBenchData(b)

	query := aiModelBaseQuery + ` WHERE "ai_model".key LIKE $1`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var models []AIModel
		err := Db.Select(&models, query, "bench_model_%")
		if err != nil {
			b.Fatalf("Select failed: %v", err)
		}
		if len(models) < 900 {
			b.Fatalf("Expected ~1000 models, got %d", len(models))
		}
	}
}

// BenchmarkGetTraversals benchmarks the traversal lookup (called once per query)
func BenchmarkGetTraversals(b *testing.B) {
	// Get type map once (this is what happens in practice)
	tm := mapper.TypeMap(getModelType(AIModel{}))
	columns := []string{"uuid", "key", "name", "description", "type", "provider", "settings", "default_negative_prompt"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = getTraversalsAndScanners(tm, getModelType(AIModel{}), columns)
	}
}

// BenchmarkSetupScanDests benchmarks setting up scan destinations (called per row)
func BenchmarkSetupScanDests(b *testing.B) {
	// Simulate what happens per row
	tm := mapper.TypeMap(getModelType(AIModel{}))
	columns := []string{"uuid", "key", "name", "description", "type", "provider", "settings", "default_negative_prompt"}
	traversals, hasScanner := getTraversalsAndScanners(tm, getModelType(AIModel{}), columns)
	values := make([]interface{}, len(columns))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var model AIModel
		v := getModelValue(&model)
		_ = setupScanDests(v, columns, traversals, hasScanner, values)
	}
}

// BenchmarkRawPgxScan benchmarks raw pgx scanning for comparison
func BenchmarkRawPgxScan(b *testing.B) {
	ensureBenchData(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := DB.Query(context.Background(),
			`SELECT uuid, key, name, description, type, provider, settings, default_negative_prompt FROM ai_model WHERE key = $1`,
			"bench_model_1")
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}

		if rows.Next() {
			var uuid, key, modelType, provider string
			var name, description, settings, negPrompt *string
			err = rows.Scan(&uuid, &key, &name, &description, &modelType, &provider, &settings, &negPrompt)
			if err != nil {
				rows.Close()
				b.Fatalf("Scan failed: %v", err)
			}
		}
		rows.Close()
	}
}

// BenchmarkInsertQuery benchmarks generating INSERT queries
func BenchmarkInsertQuery(b *testing.B) {
	values := map[string]interface{}{
		"uuid":     GenNewUUID(""),
		"key":      "test_key",
		"name":     "Test Name",
		"type":     "test_type",
		"provider": "test_provider",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetInsertQuery("ai_model", values, "uuid")
	}
}

// BenchmarkSelectBase benchmarks building SELECT queries
func BenchmarkSelectBase(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SelectBase("website", "").
			Left("realm", "r", "website.realm_uuid = r.uuid").
			Build()
	}
}

// Helper to get reflect.Value from a struct pointer
func getModelValue(model interface{}) reflect.Value {
	return reflect.ValueOf(model).Elem()
}
