package fsql

import (
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"testing"
	"time"
)

// JSONSettings implements sql.Scanner for JSONB testing
type JSONSettings struct {
	MaxTokens   int    `json:"max_tokens"`
	Temperature float64 `json:"temperature"`
	Model       string  `json:"model"`
}

// Scan implements sql.Scanner - expects []byte from database/sql style drivers
func (j *JSONSettings) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, j)
}

// AIModelWithJSONB uses a Scanner type for the settings column
type AIModelWithJSONB struct {
	UUID     string        `db:"uuid"`
	Key      string        `db:"key"`
	Type     string        `db:"type"`
	Provider string        `db:"provider"`
	Settings *JSONSettings `db:"settings"`
}

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

	// Insert a new Realm
	realm := Realm{
		UUID: GenNewUUID(""),
		Name: "Test Realm",
	}
	query, args := GetInsertQuery("realm", map[string]interface{}{
		"uuid": realm.UUID,
		"name": realm.Name,
	}, "")
	_, err := Db.Exec(query, args...)
	if err != nil {
		t.Fatalf("Failed to insert realm: %v", err)
	}

	// Insert a new Website linked to the Realm
	website := Website{
		UUID:      GenNewUUID(""),
		Domain:    "example.com",
		RealmUUID: realm.UUID,
	}
	query, args = GetInsertQuery("website", map[string]interface{}{
		"uuid":       website.UUID,
		"domain":     website.Domain,
		"realm_uuid": website.RealmUUID,
	}, "")
	_, err = Db.Exec(query, args...)
	if err != nil {
		t.Fatalf("Failed to insert website: %v", err)
	}

	// Fetch the Website along with the linked Realm
	fetchedWebsite, err := GetWebsiteByUUID(website.UUID)
	if err != nil {
		t.Fatalf("Error fetching website: %v", err)
	}

	// Verify that the linked Realm is correctly fetched
	if fetchedWebsite.Realm == nil {
		t.Fatalf("Expected linked Realm, got nil")
	}
	if fetchedWebsite.Realm.UUID != realm.UUID {
		t.Errorf("Expected Realm UUID %s, got %s", realm.UUID, fetchedWebsite.Realm.UUID)
	}
	if fetchedWebsite.Realm.Name != realm.Name {
		t.Errorf("Expected Realm Name %s, got %s", realm.Name, fetchedWebsite.Realm.Name)
	}
}

// GetWebsiteByUUID fetches a website by UUID - matching original fsql pattern
func GetWebsiteByUUID(uuidStr string) (*Website, error) {
	query := websiteBaseQuery + ` WHERE "website".uuid = $1 LIMIT 1`
	website := Website{}

	err := Db.Get(&website, query, uuidStr)
	if err != nil {
		return nil, err
	}

	return &website, nil
}

func TestQueryBuilderWhereAndJoin(t *testing.T) {
	cleanDatabase(t)

	// Insert multiple Realms
	realm1 := Realm{
		UUID: GenNewUUID(""),
		Name: "Realm One",
	}
	realm2 := Realm{
		UUID: GenNewUUID(""),
		Name: "Realm Two",
	}
	insertRealm(t, realm1)
	insertRealm(t, realm2)

	// Insert Websites linked to Realms
	website1 := Website{
		UUID:      GenNewUUID(""),
		Domain:    "example.com",
		RealmUUID: realm1.UUID,
	}
	website2 := Website{
		UUID:      GenNewUUID(""),
		Domain:    "test.com",
		RealmUUID: realm2.UUID,
	}
	website3 := Website{
		UUID:      GenNewUUID(""),
		Domain:    "sample.com",
		RealmUUID: realm1.UUID,
	}
	insertWebsite(t, website1)
	insertWebsite(t, website2)
	insertWebsite(t, website3)

	// Build a query with WHERE and JOIN clauses
	qb := SelectBase("website", "").
		Left("realm", "r", "website.realm_uuid = r.uuid").
		Where("r.name = $1").
		Where("website.domain LIKE $2")
	query := qb.Build()
	args := []interface{}{"Realm One", "%com"}

	// Fetch results
	websites := []Website{}
	err := Db.Select(&websites, query, args...)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Verify results
	if len(websites) != 2 {
		t.Errorf("Expected 2 websites, got %d", len(websites))
	}
	for _, w := range websites {
		if w.Realm == nil {
			t.Errorf("Expected Realm to be not nil for website %s", w.Domain)
		} else if w.Realm.Name != "Realm One" {
			t.Errorf("Expected Realm Name to be 'Realm One', got '%s'", w.Realm.Name)
		}
	}
}

// Helper functions to insert Realm and Website
func insertRealm(t *testing.T, realm Realm) {
	query, args := GetInsertQuery("realm", map[string]interface{}{
		"uuid": realm.UUID,
		"name": realm.Name,
	}, "")
	_, err := Db.Exec(query, args...)
	if err != nil {
		t.Fatalf("Failed to insert realm: %v", err)
	}
}

func insertWebsite(t *testing.T, website Website) {
	query, args := GetInsertQuery("website", map[string]interface{}{
		"uuid":       website.UUID,
		"domain":     website.Domain,
		"realm_uuid": website.RealmUUID,
	}, "")
	_, err := Db.Exec(query, args...)
	if err != nil {
		t.Fatalf("Failed to insert website: %v", err)
	}
}

// TestPoolStats is a simple test to verify pool stats work
func TestPoolStats(t *testing.T) {
	total, acquired, idle, emptyAcquire, duration := GetPoolStats()

	if total == 0 {
		t.Error("Expected non-zero total connections")
	}

	t.Logf("Pool stats: total=%d, acquired=%d, idle=%d, emptyAcquire=%d, duration=%v",
		total, acquired, idle, emptyAcquire, duration)
}

// TestJSONBScanning tests that sql.Scanner types work correctly with pgx
// This tests the pgxScannerWrapper that converts pgx's string returns to []byte
func TestJSONBScanning(t *testing.T) {
	// Clean up first
	_, _ = Db.Exec("DELETE FROM ai_model WHERE key LIKE 'jsonb_test_%'")

	// Insert a model with JSONB settings using uuid_generate_v4()
	settingsJSON := `{"max_tokens": 1000, "temperature": 0.7, "model": "gpt-4"}`

	// Use RETURNING to get the generated UUID
	var testUUID string
	err := Db.QueryRow(
		"INSERT INTO ai_model (uuid, key, type, provider, settings) VALUES (uuid_generate_v4(), $1, $2, $3, $4::jsonb) RETURNING uuid",
		"jsonb_test_key", "llm", "openai", settingsJSON,
	).Scan(&testUUID)
	if err != nil {
		t.Fatalf("Failed to insert test model with JSONB: %v", err)
	}

	// Query using the struct with Scanner type
	var model AIModelWithJSONB
	err = Db.Get(&model, "SELECT uuid, key, type, provider, settings FROM ai_model WHERE uuid = $1", testUUID)
	if err != nil {
		t.Fatalf("Failed to query model with JSONB: %v", err)
	}

	// Verify the Scanner worked correctly
	if model.Settings == nil {
		t.Fatal("Settings should not be nil")
	}
	if model.Settings.MaxTokens != 1000 {
		t.Errorf("Expected MaxTokens 1000, got %d", model.Settings.MaxTokens)
	}
	if model.Settings.Temperature != 0.7 {
		t.Errorf("Expected Temperature 0.7, got %f", model.Settings.Temperature)
	}
	if model.Settings.Model != "gpt-4" {
		t.Errorf("Expected Model 'gpt-4', got %s", model.Settings.Model)
	}

	t.Logf("Successfully scanned JSONB into sql.Scanner type: %+v", model.Settings)

	// Clean up
	_, _ = Db.Exec("DELETE FROM ai_model WHERE uuid = $1", testUUID)
}

// UserProfile for testing plain int scanning
type UserProfile struct {
	UUID           string  `db:"uuid" dbMode:"i"`
	Username       string  `db:"username" dbMode:"i,u"`
	Bio            *string `db:"bio" dbMode:"i,u"`
	UserExperience int     `db:"user_experience" dbMode:"i,u"`
	FollowerCount  int     `db:"follower_count" dbMode:"i,u"`
	FollowingCount int     `db:"following_count" dbMode:"i,u"`
}

// TestPlainIntScanning tests that plain int fields are scanned correctly
// This reproduces a bug where int fields were returning 0
func TestPlainIntScanning(t *testing.T) {
	// Initialize cache
	InitModelTagCache(UserProfile{}, "user_profile")

	// Clean up
	_, _ = Db.Exec("DELETE FROM user_profile")

	// Insert a user with non-zero experience
	var testUUID string
	err := Db.QueryRow(
		`INSERT INTO user_profile (uuid, username, bio, user_experience, follower_count, following_count)
		 VALUES (uuid_generate_v4(), $1, $2, $3, $4, $5) RETURNING uuid`,
		"testuser", "Test bio", 12345, 100, 50,
	).Scan(&testUUID)
	if err != nil {
		t.Fatalf("Failed to insert test user: %v", err)
	}

	// Build the query using SelectBase
	baseQuery := SelectBase("user_profile", "").Build()
	query := baseQuery + ` WHERE "user_profile".uuid = $1 LIMIT 1`

	// Fetch the user
	user := UserProfile{}
	err = Db.Get(&user, query, testUUID)
	if err != nil {
		t.Fatalf("Failed to fetch user: %v", err)
	}

	// Verify int fields are correctly scanned
	if user.UserExperience != 12345 {
		t.Errorf("Expected UserExperience=12345, got %d", user.UserExperience)
	}
	if user.FollowerCount != 100 {
		t.Errorf("Expected FollowerCount=100, got %d", user.FollowerCount)
	}
	if user.FollowingCount != 50 {
		t.Errorf("Expected FollowingCount=50, got %d", user.FollowingCount)
	}

	t.Logf("UserProfile scanned: UUID=%s, Username=%s, XP=%d, Followers=%d, Following=%d",
		user.UUID, user.Username, user.UserExperience, user.FollowerCount, user.FollowingCount)

	// Clean up
	_, _ = Db.Exec("DELETE FROM user_profile WHERE uuid = $1", testUUID)
}

// TestNullInt64Scanning tests scanning COUNT(*) into sql.NullInt64 using SafeGet
// This mimics the exact Soulkyn pattern for UpdatePersonasCountAndExperience
func TestNullInt64Scanning(t *testing.T) {
	cleanDatabase(t)

	// Insert some test ai_models
	for i := 0; i < 5; i++ {
		aiModel := AIModel{
			Key:      fmt.Sprintf("count_test_%d", i),
			Type:     "llm",
			Provider: "openai",
		}
		err := aiModel.Insert()
		if err != nil {
			t.Fatalf("Failed to insert ai_model: %v", err)
		}
	}

	// Test SafeGet with COUNT(*) into sql.NullInt64 - EXACT Soulkyn pattern
	query := "SELECT COUNT(*) FROM ai_model WHERE type = $1"
	var count sql.NullInt64
	err := SafeGet(&count, query, "llm")
	if err != nil {
		t.Fatalf("SafeGet COUNT failed: %v", err)
	}

	if !count.Valid {
		t.Error("Expected count.Valid to be true")
	}
	if count.Int64 != 5 {
		t.Errorf("Expected count=5, got %d", count.Int64)
	}

	t.Logf("SafeGet COUNT(*) into sql.NullInt64: %d (valid=%v)", count.Int64, count.Valid)

	// Test with no results - should return 0, not error
	var countZero sql.NullInt64
	err = SafeGet(&countZero, "SELECT COUNT(*) FROM ai_model WHERE type = $1", "nonexistent")
	if err != nil {
		t.Fatalf("SafeGet COUNT with no results failed: %v", err)
	}
	if countZero.Int64 != 0 {
		t.Errorf("Expected count=0, got %d", countZero.Int64)
	}

	t.Logf("SafeGet COUNT(*) with no matches: %d (valid=%v)", countZero.Int64, countZero.Valid)
}
