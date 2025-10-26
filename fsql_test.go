package fsql

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Test models matching fsql pattern
type AIModel struct {
	UUID                  string   `db:"uuid" dbMode:"i"`
	Key                   string   `db:"key" dbMode:"i,u"`
	Name                  *string  `db:"name" dbMode:"i,u" dbInsertValue:"NULL"`
	Description           *string  `db:"description" dbMode:"i,u" dbInsertValue:"NULL"`
	Type                  string   `db:"type" dbMode:"i,u"`
	Provider              string   `db:"provider" dbMode:"i,u"`
	Settings              *string  `db:"settings" dbMode:"i,u" dbInsertValue:"NULL"`
	DefaultNegativePrompt *string  `db:"default_negative_prompt" dbMode:"i,u" dbInsertValue:"NULL"`
}

type Realm struct {
	UUID      string    `db:"uuid" dbMode:"i"`
	CreatedAt time.Time `db:"created_at" dbMode:"i" dbInsertValue:"NOW()"`
	UpdatedAt time.Time `db:"updated_at" dbMode:"i,u" dbInsertValue:"NOW()"`
	Name      string    `db:"name" dbMode:"i,u"`
}

type Website struct {
	UUID      string    `db:"uuid" dbMode:"i"`
	CreatedAt time.Time `db:"created_at" dbMode:"i" dbInsertValue:"NOW()"`
	UpdatedAt time.Time `db:"updated_at" dbMode:"i,u" dbInsertValue:"NOW()"`
	Domain    string    `db:"domain" dbMode:"i,u"`
	RealmUUID string    `db:"realm_uuid" dbMode:"i"`
	Realm     *Realm    `db:"r" dbMode:"l"`
}

var (
	aiModelBaseQuery   string
	realmBaseQuery     string
	websiteBaseQuery   string
)

func TestMain(m *testing.M) {
	// Parse flags with SAME defaults as fsql
	dbUser := flag.String("dbuser", "test_user", "Database user")
	dbPassword := flag.String("dbpass", "test_password", "Database password")
	dbName := flag.String("dbname", "test_db", "Database name")
	dbHost := flag.String("dbhost", "localhost", "Database host")
	dbPort := flag.String("dbport", "5433", "Database port")
	flag.Parse()

	// Initialize database
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		*dbUser, *dbPassword, *dbHost, *dbPort, *dbName)

	pool, err := InitDB(connStr, 10, 2)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to init DB: %v\n", err)
		os.Exit(1)
	}

	// Initialize model caches
	InitModelTagCache(&AIModel{}, "ai_model")
	InitModelTagCache(&Realm{}, "realm")
	InitModelTagCache(&Website{}, "website")

	// Generate base queries
	aiModelBaseQuery = SelectBase("ai_model", "").Build()
	realmBaseQuery = SelectBase("realm", "").Build()
	websiteBaseQuery = SelectBase("website", "").
		Left("realm", "r", `"website"."realm_uuid" = "r"."uuid"`).
		Build()

	// Run tests
	code := m.Run()

	// Cleanup
	pool.Close()
	os.Exit(code)
}

func cleanDatabase(t *testing.T) {
	ctx := context.Background()
	err := Exec(ctx, `TRUNCATE TABLE website, realm, ai_model RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("Failed to clean database: %v", err)
	}
}

func TestAIModelInsertAndFetch(t *testing.T) {
	cleanDatabase(t)
	ctx := context.Background()

	// Insert AI model
	modelUUID := uuid.New().String()
	values := map[string]interface{}{
		"uuid":     modelUUID,
		"key":      "test_key",
		"name":     "Test Model",
		"type":     "text",
		"provider": "openai",
	}

	err := Insert(ctx, "ai_model", values, "uuid")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Fetch by UUID
	query := aiModelBaseQuery + ` WHERE "ai_model"."uuid" = $1`
	var model AIModel
	err = SelectOne(ctx, &model, query, modelUUID)
	if err != nil {
		t.Fatalf("SelectOne failed: %v", err)
	}

	if model.UUID != modelUUID {
		t.Errorf("Expected UUID %s, got %s", modelUUID, model.UUID)
	}
	if model.Key != "test_key" {
		t.Errorf("Expected key 'test_key', got '%s'", model.Key)
	}
}

func TestListAIModel(t *testing.T) {
	cleanDatabase(t)
	ctx := context.Background()

	// Insert 50 models
	for i := 1; i <= 50; i++ {
		values := map[string]interface{}{
			"uuid":     uuid.New().String(),
			"key":      fmt.Sprintf("key_%d", i),
			"name":     fmt.Sprintf("Model %d", i),
			"type":     "text",
			"provider": "openai",
		}
		err := Insert(ctx, "ai_model", values, "uuid")
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Test pagination
	perPage := 10
	page := 2
	filters := &Filter{
		"Type": "text",
	}
	sort := &Sort{
		"Key": "ASC",
	}

	query, args, err := FilterQuery(aiModelBaseQuery, "ai_model", filters, sort, "ai_model", perPage, page)
	if err != nil {
		t.Fatalf("FilterQuery failed: %v", err)
	}

	var models []AIModel
	err = SelectMany(ctx, &models, query, args...)
	if err != nil {
		t.Fatalf("SelectMany failed: %v", err)
	}

	if len(models) != perPage {
		t.Errorf("Expected %d models, got %d", perPage, len(models))
	}

	// Test count
	countQuery := BuildFilterCount(query)
	count, err := GetFilterCount(countQuery, args)
	if err != nil {
		t.Fatalf("GetFilterCount failed: %v", err)
	}

	expectedCount := 50
	if count != expectedCount {
		t.Errorf("Expected count %d, got %d", expectedCount, count)
	}

	expectedPageMax := int(math.Ceil(float64(count) / float64(perPage)))
	if expectedPageMax != 5 {
		t.Errorf("Expected PageMax 5, got %d", expectedPageMax)
	}
}

func TestLinkedFields(t *testing.T) {
	cleanDatabase(t)
	ctx := context.Background()

	// Insert realm
	realmUUID := uuid.New().String()
	realmValues := map[string]interface{}{
		"uuid": realmUUID,
		"name": "Test Realm",
	}
	err := Insert(ctx, "realm", realmValues, "uuid")
	if err != nil {
		t.Fatalf("Insert realm failed: %v", err)
	}

	// Insert website linked to realm
	websiteUUID := uuid.New().String()
	websiteValues := map[string]interface{}{
		"uuid":       websiteUUID,
		"domain":     "example.com",
		"realm_uuid": realmUUID,
	}
	err = Insert(ctx, "website", websiteValues, "uuid")
	if err != nil {
		t.Fatalf("Insert website failed: %v", err)
	}

	// Fetch website with linked realm
	query := websiteBaseQuery + ` WHERE "website"."uuid" = $1`
	var website Website
	err = SelectOne(ctx, &website, query, websiteUUID)
	if err != nil {
		t.Fatalf("SelectOne failed: %v", err)
	}

	// Verify linked realm
	if website.Realm == nil {
		t.Fatal("Expected linked Realm, got nil")
	}
	if website.Realm.UUID != realmUUID {
		t.Errorf("Expected Realm UUID %s, got %s", realmUUID, website.Realm.UUID)
	}
	if website.Realm.Name != "Test Realm" {
		t.Errorf("Expected Realm Name 'Test Realm', got '%s'", website.Realm.Name)
	}
}

func TestQueryBuilderWhereAndJoin(t *testing.T) {
	cleanDatabase(t)
	ctx := context.Background()

	// Insert realms
	realm1UUID := uuid.New().String()
	realm2UUID := uuid.New().String()

	err := Insert(ctx, "realm", map[string]interface{}{
		"uuid": realm1UUID,
		"name": "Realm One",
	}, "uuid")
	if err != nil {
		t.Fatalf("Insert realm1 failed: %v", err)
	}

	err = Insert(ctx, "realm", map[string]interface{}{
		"uuid": realm2UUID,
		"name": "Realm Two",
	}, "uuid")
	if err != nil {
		t.Fatalf("Insert realm2 failed: %v", err)
	}

	// Insert websites
	websites := []map[string]interface{}{
		{"uuid": uuid.New().String(), "domain": "example.com", "realm_uuid": realm1UUID},
		{"uuid": uuid.New().String(), "domain": "test.com", "realm_uuid": realm2UUID},
		{"uuid": uuid.New().String(), "domain": "sample.com", "realm_uuid": realm1UUID},
	}

	for _, w := range websites {
		err := Insert(ctx, "website", w, "uuid")
		if err != nil {
			t.Fatalf("Insert website failed: %v", err)
		}
	}

	// Build query with WHERE and JOIN
	qb := SelectBase("website", "").
		Left("realm", "r", `"website"."realm_uuid" = "r"."uuid"`).
		Where(`"r"."name" = $1`).
		Where(`"website"."domain" LIKE $2`)
	query := qb.Build()
	args := []interface{}{"Realm One", "%com"}

	// Fetch results
	var results []Website
	err = SelectMany(ctx, &results, query, args...)
	if err != nil {
		t.Fatalf("SelectMany failed: %v", err)
	}

	// Verify results
	if len(results) != 2 {
		t.Errorf("Expected 2 websites, got %d", len(results))
	}

	for _, w := range results {
		if w.Realm == nil {
			t.Errorf("Expected Realm not nil for website %s", w.Domain)
		} else if w.Realm.Name != "Realm One" {
			t.Errorf("Expected Realm Name 'Realm One', got '%s'", w.Realm.Name)
		}
	}
}

func TestFilters(t *testing.T) {
	cleanDatabase(t)
	ctx := context.Background()

	// Insert test models
	types := []string{"text", "image", "audio"}
	for i := 0; i < 15; i++ {
		values := map[string]interface{}{
			"uuid":     uuid.New().String(),
			"key":      fmt.Sprintf("key_%d", i),
			"name":     fmt.Sprintf("Model %d", i),
			"type":     types[i%3],
			"provider": "test",
		}
		err := Insert(ctx, "ai_model", values, "uuid")
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Test $in filter
	filters := &Filter{
		"Type[$in]": []string{"text", "image"},
	}

	query, args, err := FilterQuery(aiModelBaseQuery, "ai_model", filters, nil, "ai_model", 20, 1)
	if err != nil {
		t.Fatalf("FilterQuery failed: %v", err)
	}

	var results []AIModel
	err = SelectMany(ctx, &results, query, args...)
	if err != nil {
		t.Fatalf("SelectMany failed: %v", err)
	}

	if len(results) != 10 {
		t.Errorf("Expected 10 results, got %d", len(results))
	}
}

func TestUpdate(t *testing.T) {
	cleanDatabase(t)
	ctx := context.Background()

	// Insert model
	modelUUID := uuid.New().String()
	values := map[string]interface{}{
		"uuid":     modelUUID,
		"key":      "original_key",
		"name":     "Original Name",
		"type":     "text",
		"provider": "openai",
	}

	err := Insert(ctx, "ai_model", values, "uuid")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Update model
	updateValues := map[string]interface{}{
		"uuid": modelUUID,
		"key":  "updated_key",
		"name": "Updated Name",
	}

	err = Update(ctx, "ai_model", updateValues, "uuid")
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Fetch updated model
	query := aiModelBaseQuery + ` WHERE "ai_model"."uuid" = $1`
	var model AIModel
	err = SelectOne(ctx, &model, query, modelUUID)
	if err != nil {
		t.Fatalf("SelectOne failed: %v", err)
	}

	if model.Key != "updated_key" {
		t.Errorf("Expected key 'updated_key', got '%s'", model.Key)
	}
	if model.Name == nil || *model.Name != "Updated Name" {
		t.Errorf("Expected name 'Updated Name', got '%v'", model.Name)
	}
}

func TestPoolStats(t *testing.T) {
	total, acquired, idle, emptyAcquire, duration := GetPoolStats()

	if total == 0 {
		t.Error("Expected non-zero total connections")
	}

	t.Logf("Pool stats: total=%d, acquired=%d, idle=%d, emptyAcquire=%d, duration=%v",
		total, acquired, idle, emptyAcquire, duration)
}
