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

	// Initialize database - using EXACT same format as original fsql
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		*dbHost, *dbPort, *dbUser, *dbPassword, *dbName)
	InitDB(connStr)

	// Initialize model caches - value not pointer, matching original fsql
	InitModelTagCache(AIModel{}, "ai_model")
	InitModelTagCache(Realm{}, "realm")
	InitModelTagCache(Website{}, "website")

	// Generate base queries
	aiModelBaseQuery = SelectBase("ai_model", "").Build()
	realmBaseQuery = SelectBase("realm", "").Build()
	websiteBaseQuery = SelectBase("website", "").
		Left("realm", "r", "website.realm_uuid = r.uuid").
		Build()

	// Run tests
	code := m.Run()

	// Cleanup
	Db.Close()
	os.Exit(code)
}

func cleanDatabase(t *testing.T) {
	_, err := Db.Exec(`TRUNCATE TABLE ai_model, website, realm RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("Failed to clean database: %v", err)
	}
}

func TestAIModelInsertAndFetch(t *testing.T) {
	cleanDatabase(t)

	// Insert a new AIModel using the same pattern as original fsql
	aiModel := AIModel{
		Key:      "test_key",
		Type:     "test_type",
		Provider: "test_provider",
	}
	name := "Test Model"
	aiModel.Name = &name

	err := aiModel.Insert()
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}

	// Fetch the model by UUID
	fetchedModel, err := AIModelByUUID(aiModel.UUID)
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}

	// Compare the inserted and fetched models
	if aiModel.UUID != fetchedModel.UUID {
		t.Errorf("Expected UUID %v, got %v", aiModel.UUID, fetchedModel.UUID)
	}
	if aiModel.Key != fetchedModel.Key {
		t.Errorf("Expected Key %v, got %v", aiModel.Key, fetchedModel.Key)
	}
}

// AIModelByUUID fetches an AI model by UUID - matching original fsql pattern
func AIModelByUUID(uuidStr string) (*AIModel, error) {
	query := aiModelBaseQuery + ` WHERE "ai_model".uuid = $1 LIMIT 1`
	model := AIModel{}

	err := Db.Get(&model, query, uuidStr)
	if err != nil {
		return nil, err
	}
	return &model, nil
}

// Insert inserts the AI model - matching original fsql pattern
func (m *AIModel) Insert() error {
	query, queryValues := GetInsertQuery("ai_model", map[string]interface{}{
		"uuid":        GenNewUUID(""),
		"key":         m.Key,
		"name":        m.Name,
		"description": m.Description,
		"type":        m.Type,
		"provider":    m.Provider,
		"settings":    m.Settings,
	}, "uuid")

	err := Db.QueryRow(query, queryValues...).Scan(&m.UUID)
	if err != nil {
		return err
	}

	return nil
}

func TestListAIModel(t *testing.T) {
	cleanDatabase(t)

	// Insert multiple AIModels
	for i := 1; i <= 50; i++ {
		aiModel := AIModel{
			Key:      fmt.Sprintf("key_%d", i),
			Type:     "test_type",
			Provider: "test_provider",
		}
		name := fmt.Sprintf("Model %d", i)
		aiModel.Name = &name
		err := aiModel.Insert()
		if err != nil {
			t.Fatalf("Insert error: %v", err)
		}
	}

	// List AIModels with pagination
	perPage := 10
	page := 2
	filters := &Filter{
		"Type": "test_type",
	}
	sort := &Sort{
		"Key": "ASC",
	}
	models, pagination, err := ListAIModel(filters, sort, perPage, page)
	if err != nil {
		t.Fatalf("ListAIModel error: %v", err)
	}

	expectedCount := 50
	if pagination.Count != expectedCount {
		t.Errorf("Expected count %d, got %d", expectedCount, pagination.Count)
	}
	expectedPageMax := int(math.Ceil(float64(expectedCount) / float64(perPage)))
	if pagination.PageMax != expectedPageMax {
		t.Errorf("Expected PageMax %d, got %d", expectedPageMax, pagination.PageMax)
	}
	if len(*models) != perPage {
		t.Errorf("Expected %d models, got %d", perPage, len(*models))
	}
}

// Pagination struct matching original fsql pattern
type Pagination struct {
	ResultsPerPage int
	PageNo         int
	Count          int
	PageMax        int
}

// ListAIModel lists AI models - matching original fsql pattern
func ListAIModel(filters *Filter, sort *Sort, perPage int, page int) (*[]AIModel, *Pagination, error) {
	if sort == nil || len(*sort) == 0 {
		sort = &Sort{
			"Type": "ASC",
		}
	}
	query := aiModelBaseQuery
	query, args, err := FilterQuery(query, "ai_model", filters, sort, "ai_model", perPage, page)
	if err != nil {
		return nil, nil, err
	}

	models := []AIModel{}
	err = Db.Select(&models, query, args...)
	if err != nil {
		return nil, nil, err
	}

	countQuery := BuildFilterCount(query)
	count, err := GetFilterCount(countQuery, args)
	if err != nil {
		return nil, nil, err
	}
	pagination := Pagination{
		ResultsPerPage: perPage,
		PageNo:         page,
		Count:          count,
		PageMax:        int(math.Ceil(float64(count) / float64(perPage))),
	}

	return &models, &pagination, nil
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
