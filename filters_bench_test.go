package fsql

import (
	"testing"
)

// BenchmarkConstructConditions benchmarks filter condition building
func BenchmarkConstructConditions(b *testing.B) {
	filters := &Filter{
		"Type":     "test_type",
		"Provider": "test_provider",
		"Key[$like]": "%test%",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = constructConditions("ai_model", filters, "ai_model")
	}
}

// BenchmarkFilterQuery benchmarks complete filter query building
func BenchmarkFilterQuery(b *testing.B) {
	filters := &Filter{
		"Type":     "test_type",
		"Provider": "test_provider",
	}
	sort := &Sort{
		"Key": "ASC",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = FilterQuery(aiModelBaseQuery, "ai_model", filters, sort, "ai_model", 10, 1)
	}
}

// BenchmarkBuildFilterCount benchmarks count query generation
func BenchmarkBuildFilterCount(b *testing.B) {
	query := aiModelBaseQuery + ` WHERE "ai_model".type = $1 ORDER BY "ai_model".key ASC LIMIT 10 OFFSET 0`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = BuildFilterCount(query)
	}
}

// BenchmarkGetInsertQuery benchmarks insert query generation
func BenchmarkGetInsertQuery(b *testing.B) {
	values := map[string]interface{}{
		"uuid":        GenNewUUID(""),
		"key":         "test_key",
		"name":        "Test Name",
		"description": "Test Description",
		"type":        "test_type",
		"provider":    "test_provider",
		"settings":    nil,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetInsertQuery("ai_model", values, "uuid")
	}
}

// BenchmarkGetUpdateQuery benchmarks update query generation
func BenchmarkGetUpdateQuery(b *testing.B) {
	values := map[string]interface{}{
		"uuid":        "test-uuid",
		"key":         "test_key",
		"name":        "Test Name",
		"description": "Test Description",
		"type":        "test_type",
		"provider":    "test_provider",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetUpdateQuery("ai_model", values, "uuid")
	}
}

// BenchmarkGetSelectFields benchmarks field list generation
func BenchmarkGetSelectFields(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetSelectFields("ai_model", "")
	}
}

// BenchmarkGetSelectFieldsWithAlias benchmarks field list generation with alias
func BenchmarkGetSelectFieldsWithAlias(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetSelectFields("ai_model", "m")
	}
}

// BenchmarkSelectBaseBuild benchmarks the query builder
func BenchmarkSelectBaseBuild(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SelectBase("ai_model", "").Build()
	}
}

// BenchmarkSelectBaseWithJoins benchmarks query builder with joins
func BenchmarkSelectBaseWithJoins(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SelectBase("website", "").
			Left("realm", "r", "website.realm_uuid = r.uuid").
			Build()
	}
}

// BenchmarkIndexCaseInsensitive benchmarks the case-insensitive index function
func BenchmarkIndexCaseInsensitive(b *testing.B) {
	query := `SELECT * FROM ai_model WHERE type = $1 ORDER BY key ASC LIMIT 10 OFFSET 0`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = indexCaseInsensitive(query, " LIMIT ")
		_ = indexCaseInsensitive(query, " ORDER BY ")
		_ = indexCaseInsensitive(query, " OFFSET ")
	}
}
