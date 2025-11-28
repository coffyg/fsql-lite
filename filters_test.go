package fsql

import (
	"fmt"
	"math"
	"testing"
)

func TestBuildFilterCountCustom(t *testing.T) {
	cleanDatabase(t)

	// Create test data by using the existing ai_model table
	for i := 1; i <= 20; i++ {
		model := AIModel{
			Key:      fmt.Sprintf("key_%d", i),
			Type:     "test_type",
			Provider: "test_provider",
		}
		name := fmt.Sprintf("Model %d", i)
		model.Name = &name
		err := model.Insert()
		if err != nil {
			t.Fatalf("Insert error: %v", err)
		}
	}

	// Test various query patterns
	testQueryPatterns(t)
}

func testQueryPatterns(t *testing.T) {
	// Test standard query
	baseQuery := aiModelBaseQuery + " WHERE type = $1"
	baseArgs := []interface{}{"test_type"}
	testCountQuery(t, "standard", baseQuery, baseArgs, 20)

	// Test OR condition query
	baseQuery = aiModelBaseQuery + " WHERE type = $1 OR provider = $2"
	baseArgs = []interface{}{"test_type", "test_provider"}
	testCountQuery(t, "or_condition", baseQuery, baseArgs, 20)

	// Test complex condition query with quoted table name
	baseQuery = aiModelBaseQuery + ` WHERE "ai_model".type = $1`
	baseArgs = []interface{}{"test_type"}
	testCountQuery(t, "quoted_table", baseQuery, baseArgs, 20)

	// Test subquery in condition
	baseQuery = aiModelBaseQuery + ` WHERE type IN (SELECT type FROM ai_model WHERE provider = $1)`
	baseArgs = []interface{}{"test_provider"}
	testCountQuery(t, "subquery", baseQuery, baseArgs, 20)
}

func testCountQuery(t *testing.T, testName string, baseQuery string, baseArgs []interface{}, expectedCount int) {
	t.Helper()

	perPage := 10
	page := 1

	// Apply FilterQueryCustom to create paginated query
	query, args, err := FilterQueryCustom(
		baseQuery,
		"ai_model",
		`"ai_model".key DESC`,
		baseArgs,
		perPage,
		page,
	)
	if err != nil {
		t.Fatalf("FilterQueryCustom error for %s: %v", testName, err)
	}

	// Get some results to verify query works
	var models []AIModel
	err = Db.Select(&models, query, args...)
	if err != nil {
		t.Fatalf("Select error for %s: %v", testName, err)
	}

	// Test BuildFilterCountCustom - this is what we're testing
	countQuery := BuildFilterCountCustom(query)

	// For debugging, log the generated count query
	t.Logf("Test: %s\nCount query: %s", testName, countQuery)

	count, err := GetFilterCount(countQuery, args)
	if err != nil {
		t.Fatalf("GetFilterCount error for %s: %v", testName, err)
	}

	// Verify results
	if count != expectedCount {
		t.Errorf("Test %s: Expected count %d, got %d", testName, expectedCount, count)
	}

	// Verify pagination correct
	pagination := Pagination{
		ResultsPerPage: perPage,
		PageNo:         page,
		Count:          count,
		PageMax:        int(math.Ceil(float64(count) / float64(perPage))),
	}

	expectedPageMax := int(math.Ceil(float64(expectedCount) / float64(perPage)))
	if pagination.PageMax != expectedPageMax {
		t.Errorf("Test %s: Expected page max %d, got %d", testName, expectedPageMax, pagination.PageMax)
	}
}

// TestFilterQuery tests the main FilterQuery function
func TestFilterQuery(t *testing.T) {
	cleanDatabase(t)

	// Create test data
	for i := 1; i <= 30; i++ {
		model := AIModel{
			Key:      fmt.Sprintf("filter_key_%d", i),
			Type:     "filter_type",
			Provider: "filter_provider",
		}
		name := fmt.Sprintf("Filter Model %d", i)
		model.Name = &name
		err := model.Insert()
		if err != nil {
			t.Fatalf("Insert error: %v", err)
		}
	}

	// Test with filters
	filters := &Filter{
		"Type": "filter_type",
	}
	sort := &Sort{
		"Key": "ASC",
	}

	query, args, err := FilterQuery(aiModelBaseQuery, "ai_model", filters, sort, "ai_model", 10, 1)
	if err != nil {
		t.Fatalf("FilterQuery error: %v", err)
	}

	var models []AIModel
	err = Db.Select(&models, query, args...)
	if err != nil {
		t.Fatalf("Select error: %v", err)
	}

	if len(models) != 10 {
		t.Errorf("Expected 10 models, got %d", len(models))
	}

	// Verify count
	countQuery := BuildFilterCount(query)
	count, err := GetFilterCount(countQuery, args)
	if err != nil {
		t.Fatalf("GetFilterCount error: %v", err)
	}

	if count != 30 {
		t.Errorf("Expected total count 30, got %d", count)
	}
}
