package fsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/coffyg/octypes"
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
	_, err := Db.Exec(`TRUNCATE TABLE ai_model, website, realm, user_profile RESTART IDENTITY CASCADE`)
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

// UserProfileFull mimics Soulkyn's UserProfile with octypes
type UserProfileFull struct {
	UUID           string             `db:"uuid" dbMode:"i"`
	Username       string             `db:"username" dbMode:"i,u"`
	Bio            octypes.NullString `db:"bio" dbMode:"i,u"`
	UserExperience int                `db:"user_experience" dbMode:"i,u"`
	FollowerCount  int                `db:"follower_count" dbMode:"i,u"`
	FollowingCount int                `db:"following_count" dbMode:"i,u"`
	PersonasCount  int                `db:"personas_count" dbMode:"i,u"`
}

// TestUserExperienceAndPersonasCount - THE EXACT BUG
func TestUserExperienceAndPersonasCount(t *testing.T) {
	cleanDatabase(t)
	InitModelTagCache(UserProfileFull{}, "user_profile")

	// Insert users with specific XP and PersonasCount
	users := []struct {
		username      string
		xp            int
		personasCount int
	}{
		{"user1", 5000, 10},
		{"user2", 12000, 25},
		{"user3", 300, 3},
	}

	uuids := make([]string, len(users))
	for i, u := range users {
		err := Db.QueryRow(
			`INSERT INTO user_profile (uuid, username, user_experience, follower_count, following_count, personas_count)
			 VALUES (uuid_generate_v4(), $1, $2, 0, 0, $3) RETURNING uuid`,
			u.username, u.xp, u.personasCount,
		).Scan(&uuids[i])
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// SafeSelect all users
	baseQuery := SelectBase("user_profile", "").Build()
	var profiles []UserProfileFull
	err := SafeSelect(&profiles, baseQuery)
	if err != nil {
		t.Fatalf("SafeSelect failed: %v", err)
	}

	t.Logf("Selected %d profiles:", len(profiles))
	for i, p := range profiles {
		t.Logf("  [%d] %s: XP=%d, PersonasCount=%d", i, p.Username, p.UserExperience, p.PersonasCount)

		// Find expected values
		for _, u := range users {
			if u.username == p.Username {
				if p.UserExperience != u.xp {
					t.Errorf("%s: expected XP=%d, got %d", p.Username, u.xp, p.UserExperience)
				}
				if p.PersonasCount != u.personasCount {
					t.Errorf("%s: expected PersonasCount=%d, got %d", p.Username, u.personasCount, p.PersonasCount)
				}
			}
		}
	}

	// Now update each and verify - mimics UpdateAllUsersMeili pattern
	for _, p := range profiles {
		newXP := p.UserExperience + 100
		newPersonas := p.PersonasCount + 1

		query, args := GetUpdateQuery("user_profile", map[string]interface{}{
			"uuid":            p.UUID,
			"username":        p.Username,
			"bio":             p.Bio,
			"user_experience": newXP,
			"follower_count":  p.FollowerCount,
			"following_count": p.FollowingCount,
			"personas_count":  newPersonas,
		}, "uuid")

		t.Logf("Update %s: query=%s args=%v", p.Username, query, args)

		var returnedUUID string
		err = Db.QueryRow(query, args...).Scan(&returnedUUID)
		if err != nil {
			t.Fatalf("Update %s failed: %v", p.Username, err)
		}
	}

	// Fetch again and verify
	var updated []UserProfileFull
	err = SafeSelect(&updated, baseQuery)
	if err != nil {
		t.Fatalf("SafeSelect after update failed: %v", err)
	}

	t.Logf("After update:")
	for i, p := range updated {
		t.Logf("  [%d] %s: XP=%d, PersonasCount=%d", i, p.Username, p.UserExperience, p.PersonasCount)

		for _, u := range users {
			if u.username == p.Username {
				expectedXP := u.xp + 100
				expectedPersonas := u.personasCount + 1
				if p.UserExperience != expectedXP {
					t.Errorf("AFTER UPDATE %s: expected XP=%d, got %d", p.Username, expectedXP, p.UserExperience)
				}
				if p.PersonasCount != expectedPersonas {
					t.Errorf("AFTER UPDATE %s: expected PersonasCount=%d, got %d", p.Username, expectedPersonas, p.PersonasCount)
				}
			}
		}
	}
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

// TestUpdateQueryOrder tests that GetUpdateQuery puts values in correct order
func TestUpdateQueryOrder(t *testing.T) {
	cleanDatabase(t)
	InitModelTagCache(UserProfile{}, "user_profile")

	// Insert a user with known values
	var testUUID string
	err := Db.QueryRow(
		`INSERT INTO user_profile (uuid, username, user_experience, follower_count, following_count)
		 VALUES (uuid_generate_v4(), $1, $2, $3, $4) RETURNING uuid`,
		"testuser", 5000, 100, 50,
	).Scan(&testUUID)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Use GetUpdateQuery with different values
	query, args := GetUpdateQuery("user_profile", map[string]interface{}{
		"uuid":            testUUID,
		"username":        "updated_user",
		"user_experience": 9999,
		"follower_count":  200,
		"following_count": 75,
	}, "uuid")

	t.Logf("Query: %s", query)
	t.Logf("Args: %v", args)

	// Execute update
	var returnedUUID string
	err = Db.QueryRow(query, args...).Scan(&returnedUUID)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Fetch and verify
	var username string
	var xp, followers, following int
	err = Db.QueryRow(
		"SELECT username, user_experience, follower_count, following_count FROM user_profile WHERE uuid = $1",
		testUUID,
	).Scan(&username, &xp, &followers, &following)
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}

	if username != "updated_user" {
		t.Errorf("username: expected 'updated_user', got '%s'", username)
	}
	if xp != 9999 {
		t.Errorf("user_experience: expected 9999, got %d", xp)
	}
	if followers != 200 {
		t.Errorf("follower_count: expected 200, got %d", followers)
	}
	if following != 75 {
		t.Errorf("following_count: expected 75, got %d", following)
	}

	t.Logf("After update: username=%s, xp=%d, followers=%d, following=%d", username, xp, followers, following)
}

// TestSafeSelectThenUpdate - mimics GetAllUsersLimit then Update pattern
func TestSafeSelectThenUpdate(t *testing.T) {
	cleanDatabase(t)
	InitModelTagCache(UserProfile{}, "user_profile")

	// Insert multiple users with different XP values
	uuids := make([]string, 3)
	xpValues := []int{1000, 2000, 3000}
	for i := 0; i < 3; i++ {
		err := Db.QueryRow(
			`INSERT INTO user_profile (uuid, username, user_experience, follower_count, following_count)
			 VALUES (uuid_generate_v4(), $1, $2, $3, $4) RETURNING uuid`,
			fmt.Sprintf("user%d", i), xpValues[i], (i+1)*10, (i+1)*5,
		).Scan(&uuids[i])
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// SafeSelect multiple users - like GetAllUsersLimit
	baseQuery := SelectBase("user_profile", "").Build()
	query := baseQuery + ` ORDER BY user_experience ASC LIMIT 10`

	var profiles []UserProfile
	err := SafeSelect(&profiles, query)
	if err != nil {
		t.Fatalf("SafeSelect failed: %v", err)
	}

	t.Logf("Selected %d profiles", len(profiles))
	for i, p := range profiles {
		t.Logf("  [%d] UUID=%s Username=%s XP=%d Followers=%d Following=%d",
			i, p.UUID, p.Username, p.UserExperience, p.FollowerCount, p.FollowingCount)
	}

	// Verify values are correct
	if len(profiles) != 3 {
		t.Fatalf("Expected 3 profiles, got %d", len(profiles))
	}

	// Check each profile has correct XP (should be sorted ASC)
	for i, p := range profiles {
		expectedXP := xpValues[i]
		if p.UserExperience != expectedXP {
			t.Errorf("Profile %d: expected XP=%d, got %d", i, expectedXP, p.UserExperience)
		}
		expectedFollowers := (i + 1) * 10
		if p.FollowerCount != expectedFollowers {
			t.Errorf("Profile %d: expected Followers=%d, got %d", i, expectedFollowers, p.FollowerCount)
		}
	}

	// Now update each profile and verify values don't get corrupted
	for i, p := range profiles {
		newXP := p.UserExperience + 500
		query, args := GetUpdateQuery("user_profile", map[string]interface{}{
			"uuid":            p.UUID,
			"username":        p.Username,
			"user_experience": newXP,
			"follower_count":  p.FollowerCount,
			"following_count": p.FollowingCount,
		}, "uuid")

		var returnedUUID string
		err = Db.QueryRow(query, args...).Scan(&returnedUUID)
		if err != nil {
			t.Fatalf("Update %d failed: %v", i, err)
		}
	}

	// Fetch again and verify
	var updatedProfiles []UserProfile
	err = SafeSelect(&updatedProfiles, baseQuery+` ORDER BY user_experience ASC LIMIT 10`)
	if err != nil {
		t.Fatalf("SafeSelect after update failed: %v", err)
	}

	t.Logf("After update:")
	for i, p := range updatedProfiles {
		t.Logf("  [%d] UUID=%s Username=%s XP=%d Followers=%d Following=%d",
			i, p.UUID, p.Username, p.UserExperience, p.FollowerCount, p.FollowingCount)

		expectedXP := xpValues[i] + 500
		if p.UserExperience != expectedXP {
			t.Errorf("After update Profile %d: expected XP=%d, got %d", i, expectedXP, p.UserExperience)
		}
	}
}

// TestSafeGetThenUpdate - mimics GetUserBySlug then Update pattern
func TestSafeGetThenUpdate(t *testing.T) {
	cleanDatabase(t)
	InitModelTagCache(UserProfile{}, "user_profile")

	// Insert a user
	var testUUID string
	err := Db.QueryRow(
		`INSERT INTO user_profile (uuid, username, user_experience, follower_count, following_count)
		 VALUES (uuid_generate_v4(), $1, $2, $3, $4) RETURNING uuid`,
		"sluguser", 7777, 88, 44,
	).Scan(&testUUID)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// SafeGet single user - like GetUserBySlug
	baseQuery := SelectBase("user_profile", "").Build()
	query := baseQuery + ` WHERE "user_profile".username = $1 LIMIT 1`

	var profile UserProfile
	err = SafeGet(&profile, query, "sluguser")
	if err != nil {
		t.Fatalf("SafeGet failed: %v", err)
	}

	t.Logf("Got profile: UUID=%s Username=%s XP=%d Followers=%d Following=%d",
		profile.UUID, profile.Username, profile.UserExperience, profile.FollowerCount, profile.FollowingCount)

	// Verify values
	if profile.UserExperience != 7777 {
		t.Errorf("Expected XP=7777, got %d", profile.UserExperience)
	}
	if profile.FollowerCount != 88 {
		t.Errorf("Expected Followers=88, got %d", profile.FollowerCount)
	}
	if profile.FollowingCount != 44 {
		t.Errorf("Expected Following=44, got %d", profile.FollowingCount)
	}

	// Now update and verify
	profile.UserExperience = 8888
	updateQuery, args := GetUpdateQuery("user_profile", map[string]interface{}{
		"uuid":            profile.UUID,
		"username":        profile.Username,
		"user_experience": profile.UserExperience,
		"follower_count":  profile.FollowerCount,
		"following_count": profile.FollowingCount,
	}, "uuid")

	var returnedUUID string
	err = Db.QueryRow(updateQuery, args...).Scan(&returnedUUID)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Fetch again
	var profile2 UserProfile
	err = SafeGet(&profile2, query, "sluguser")
	if err != nil {
		t.Fatalf("SafeGet after update failed: %v", err)
	}

	t.Logf("After update: UUID=%s Username=%s XP=%d Followers=%d Following=%d",
		profile2.UUID, profile2.Username, profile2.UserExperience, profile2.FollowerCount, profile2.FollowingCount)

	if profile2.UserExperience != 8888 {
		t.Errorf("After update: expected XP=8888, got %d", profile2.UserExperience)
	}
	if profile2.FollowerCount != 88 {
		t.Errorf("After update: expected Followers=88, got %d", profile2.FollowerCount)
	}
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

// UserProfileWithNullInt64 - uses octypes.NullInt64 like Soulkyn's UserProfilePublic
type UserProfileWithNullInt64 struct {
	UUID           string             `db:"uuid" dbMode:"i"`
	Username       string             `db:"username" dbMode:"i,u"`
	Bio            octypes.NullString `db:"bio" dbMode:"i,u"`
	UserExperience octypes.NullInt64  `db:"user_experience" dbMode:"i,u"`
	FollowerCount  octypes.NullInt64  `db:"follower_count" dbMode:"i,u"`
	FollowingCount octypes.NullInt64  `db:"following_count" dbMode:"i,u"`
	PersonasCount  octypes.NullInt64  `db:"personas_count" dbMode:"i,u"`
}

// TestOctypesNullInt64Scanning - THE EXACT SOULKYN BUG
// UserProfilePublic uses octypes.NullInt64, not plain int
func TestOctypesNullInt64Scanning(t *testing.T) {
	cleanDatabase(t)
	InitModelTagCache(UserProfileWithNullInt64{}, "user_profile")

	// Insert users with specific XP and PersonasCount
	users := []struct {
		username      string
		xp            int
		personasCount int
	}{
		{"nullint_user1", 5000, 10},
		{"nullint_user2", 12000, 25},
		{"nullint_user3", 300, 3},
	}

	uuids := make([]string, len(users))
	for i, u := range users {
		err := Db.QueryRow(
			`INSERT INTO user_profile (uuid, username, user_experience, follower_count, following_count, personas_count)
			 VALUES (uuid_generate_v4(), $1, $2, 0, 0, $3) RETURNING uuid`,
			u.username, u.xp, u.personasCount,
		).Scan(&uuids[i])
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// SafeSelect all users using octypes.NullInt64 struct
	baseQuery := SelectBase("user_profile", "").Build()
	var profiles []UserProfileWithNullInt64
	err := SafeSelect(&profiles, baseQuery+" WHERE username LIKE 'nullint_%'")
	if err != nil {
		t.Fatalf("SafeSelect failed: %v", err)
	}

	t.Logf("Selected %d profiles with octypes.NullInt64:", len(profiles))
	for i, p := range profiles {
		t.Logf("  [%d] %s: XP=%d (valid=%v), PersonasCount=%d (valid=%v)",
			i, p.Username, p.UserExperience.Int64, p.UserExperience.Valid,
			p.PersonasCount.Int64, p.PersonasCount.Valid)

		// Find expected values
		for _, u := range users {
			if u.username == p.Username {
				if p.UserExperience.Int64 != int64(u.xp) {
					t.Errorf("%s: expected XP=%d, got %d", p.Username, u.xp, p.UserExperience.Int64)
				}
				if p.PersonasCount.Int64 != int64(u.personasCount) {
					t.Errorf("%s: expected PersonasCount=%d, got %d", p.Username, u.personasCount, p.PersonasCount.Int64)
				}
			}
		}
	}

	// SafeGet single user
	var single UserProfileWithNullInt64
	err = SafeGet(&single, baseQuery+` WHERE "user_profile".username = $1 LIMIT 1`, "nullint_user2")
	if err != nil {
		t.Fatalf("SafeGet failed: %v", err)
	}

	t.Logf("SafeGet single: %s XP=%d (valid=%v)", single.Username, single.UserExperience.Int64, single.UserExperience.Valid)
	if single.UserExperience.Int64 != 12000 {
		t.Errorf("SafeGet: expected XP=12000, got %d", single.UserExperience.Int64)
	}

	// NOW TEST UPDATE with octypes.NullInt64 values - THIS MIGHT BE THE BUG
	for _, p := range profiles {
		// Update with modified octypes.NullInt64 values
		newXP := octypes.NewNullInt64(p.UserExperience.Int64 + 100)
		newPersonas := octypes.NewNullInt64(p.PersonasCount.Int64 + 1)

		query, args := GetUpdateQuery("user_profile", map[string]interface{}{
			"uuid":            p.UUID,
			"username":        p.Username,
			"bio":             p.Bio,
			"user_experience": newXP,        // octypes.NullInt64, not int!
			"follower_count":  p.FollowerCount,
			"following_count": p.FollowingCount,
			"personas_count":  newPersonas,  // octypes.NullInt64, not int!
		}, "uuid")

		t.Logf("Update %s with octypes.NullInt64: query=%s args=%v", p.Username, query, args)

		var returnedUUID string
		err = Db.QueryRow(query, args...).Scan(&returnedUUID)
		if err != nil {
			t.Fatalf("Update %s failed: %v", p.Username, err)
		}
	}

	// Fetch again and verify the octypes.NullInt64 update worked
	var updated []UserProfileWithNullInt64
	err = SafeSelect(&updated, baseQuery+" WHERE username LIKE 'nullint_%'")
	if err != nil {
		t.Fatalf("SafeSelect after update failed: %v", err)
	}

	t.Logf("After update with octypes.NullInt64:")
	for i, p := range updated {
		t.Logf("  [%d] %s: XP=%d (valid=%v), PersonasCount=%d (valid=%v)",
			i, p.Username, p.UserExperience.Int64, p.UserExperience.Valid,
			p.PersonasCount.Int64, p.PersonasCount.Valid)

		for _, u := range users {
			if u.username == p.Username {
				expectedXP := int64(u.xp + 100)
				expectedPersonas := int64(u.personasCount + 1)
				if p.UserExperience.Int64 != expectedXP {
					t.Errorf("AFTER UPDATE %s: expected XP=%d, got %d", p.Username, expectedXP, p.UserExperience.Int64)
				}
				if p.PersonasCount.Int64 != expectedPersonas {
					t.Errorf("AFTER UPDATE %s: expected PersonasCount=%d, got %d", p.Username, expectedPersonas, p.PersonasCount.Int64)
				}
			}
		}
	}
}

// TestSelectStar - Test SELECT * like Soulkyn uses in one place
func TestSelectStar(t *testing.T) {
	cleanDatabase(t)
	InitModelTagCache(UserProfileFull{}, "user_profile")

	// Insert test data
	var testUUID string
	err := Db.QueryRow(
		`INSERT INTO user_profile (uuid, username, user_experience, follower_count, following_count, personas_count)
		 VALUES (uuid_generate_v4(), $1, $2, $3, $4, $5) RETURNING uuid`,
		"select_star_user", 7777, 33, 22, 55,
	).Scan(&testUUID)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Use SELECT * like Soulkyn does in CreateOrUpdateUserProfileFromUser
	query := `SELECT * FROM user_profile WHERE uuid = $1 LIMIT 1`
	var profile UserProfileFull
	err = SafeGet(&profile, query, testUUID)
	if err != nil {
		t.Fatalf("SafeGet with SELECT * failed: %v", err)
	}

	t.Logf("SELECT * result: Username=%s, XP=%d, PersonasCount=%d",
		profile.Username, profile.UserExperience, profile.PersonasCount)

	if profile.UserExperience != 7777 {
		t.Errorf("UserExperience: expected 7777, got %d", profile.UserExperience)
	}
	if profile.PersonasCount != 55 {
		t.Errorf("PersonasCount: expected 55, got %d", profile.PersonasCount)
	}
}

// TestMixedStructTypeScanning - THE SOULKYN BUG?
// Query built for UserProfile (int fields) but scanned into UserProfilePublic (NullInt64 fields)
func TestMixedStructTypeScanning(t *testing.T) {
	cleanDatabase(t)

	// Register cache for struct with INT fields (like Soulkyn's UserProfile)
	InitModelTagCache(UserProfileFull{}, "user_profile") // Uses int for XP/PersonasCount

	// Insert test data
	var testUUID string
	err := Db.QueryRow(
		`INSERT INTO user_profile (uuid, username, user_experience, follower_count, following_count, personas_count)
		 VALUES (uuid_generate_v4(), $1, $2, $3, $4, $5) RETURNING uuid`,
		"mixed_test_user", 9999, 50, 25, 42,
	).Scan(&testUUID)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Build query using the INT struct's cache
	baseQuery := SelectBase("user_profile", "").Build()
	t.Logf("Query (built from UserProfileFull/int): %s", baseQuery)

	// But scan into struct with NullInt64 fields - LIKE SOULKYN DOES
	var profile UserProfileWithNullInt64
	err = SafeGet(&profile, baseQuery+` WHERE "user_profile".uuid = $1`, testUUID)
	if err != nil {
		t.Fatalf("SafeGet failed: %v", err)
	}

	t.Logf("Scanned into NullInt64 struct: Username=%s, XP=%d (valid=%v), PersonasCount=%d (valid=%v)",
		profile.Username, profile.UserExperience.Int64, profile.UserExperience.Valid,
		profile.PersonasCount.Int64, profile.PersonasCount.Valid)

	// Check values
	if profile.UserExperience.Int64 != 9999 {
		t.Errorf("UserExperience: expected 9999, got %d", profile.UserExperience.Int64)
	}
	if profile.PersonasCount.Int64 != 42 {
		t.Errorf("PersonasCount: expected 42, got %d", profile.PersonasCount.Int64)
	}

	// Now scan back into INT struct
	var profileInt UserProfileFull
	err = SafeGet(&profileInt, baseQuery+` WHERE "user_profile".uuid = $1`, testUUID)
	if err != nil {
		t.Fatalf("SafeGet into int struct failed: %v", err)
	}

	t.Logf("Scanned into int struct: Username=%s, XP=%d, PersonasCount=%d",
		profileInt.Username, profileInt.UserExperience, profileInt.PersonasCount)

	if profileInt.UserExperience != 9999 {
		t.Errorf("UserExperience (int): expected 9999, got %d", profileInt.UserExperience)
	}
	if profileInt.PersonasCount != 42 {
		t.Errorf("PersonasCount (int): expected 42, got %d", profileInt.PersonasCount)
	}
}

// TestDebugRawScan - debug what pgx actually returns for integer columns
func TestDebugRawScan(t *testing.T) {
	cleanDatabase(t)
	InitModelTagCache(UserProfileFull{}, "user_profile")

	// Insert a user with known values
	var testUUID string
	err := Db.QueryRow(
		`INSERT INTO user_profile (uuid, username, user_experience, follower_count, following_count, personas_count)
		 VALUES (uuid_generate_v4(), $1, $2, $3, $4, $5) RETURNING uuid`,
		"debug_user", 12345, 100, 50, 77,
	).Scan(&testUUID)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Raw query to see exactly what pgx returns
	rows, err := DB.Query(context.Background(),
		"SELECT uuid, username, user_experience, follower_count, following_count, personas_count FROM user_profile WHERE uuid = $1",
		testUUID)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	// Get raw values
	if rows.Next() {
		rawVals, err := rows.Values()
		if err != nil {
			t.Fatalf("Values() failed: %v", err)
		}

		t.Logf("Raw values from pgx:")
		for i, val := range rawVals {
			t.Logf("  [%d] Type=%T, Value=%v", i, val, val)
		}
	}

	// Now test with SafeGet into struct
	baseQuery := SelectBase("user_profile", "").Build()
	t.Logf("SELECT query: %s", baseQuery)

	var profile UserProfileFull
	err = SafeGet(&profile, baseQuery+` WHERE "user_profile".uuid = $1`, testUUID)
	if err != nil {
		t.Fatalf("SafeGet failed: %v", err)
	}

	t.Logf("SafeGet result: Username=%s, XP=%d, Followers=%d, Following=%d, PersonasCount=%d",
		profile.Username, profile.UserExperience, profile.FollowerCount, profile.FollowingCount, profile.PersonasCount)

	if profile.UserExperience != 12345 {
		t.Errorf("UserExperience: expected 12345, got %d", profile.UserExperience)
	}
	if profile.PersonasCount != 77 {
		t.Errorf("PersonasCount: expected 77, got %d", profile.PersonasCount)
	}
}

// TestNullInt64EdgeCases - test various edge cases for NullInt64 scanning
func TestNullInt64EdgeCases(t *testing.T) {
	cleanDatabase(t)

	// Test 1: COUNT with no matching rows should return 0, not error
	var count sql.NullInt64
	err := SafeGet(&count, "SELECT COUNT(*) FROM ai_model WHERE type = $1", "nonexistent_type")
	if err != nil {
		t.Fatalf("COUNT with no matches should not error: %v", err)
	}
	if count.Int64 != 0 {
		t.Errorf("COUNT with no matches: expected 0, got %d", count.Int64)
	}
	t.Logf("COUNT no matches: value=%d valid=%v", count.Int64, count.Valid)

	// Test 2: SUM with no matching rows returns NULL
	var sum sql.NullInt64
	err = SafeGet(&sum, "SELECT SUM(uuid::int) FROM ai_model WHERE type = $1", "nonexistent_type")
	// This will error because uuid can't be cast to int, let's use a different test

	// Insert some test data first
	for i := 0; i < 3; i++ {
		_, err := Db.Exec(`INSERT INTO user_profile (uuid, username, user_experience, follower_count, following_count, personas_count) VALUES (uuid_generate_v4(), $1, $2, 0, 0, $3)`,
			fmt.Sprintf("edge_user_%d", i), 100*(i+1), i+1)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Test 3: SUM with no matching rows returns NULL (valid=false)
	var sumNull sql.NullInt64
	err = SafeGet(&sumNull, "SELECT SUM(user_experience) FROM user_profile WHERE username = $1", "nobody_exists")
	if err != nil {
		t.Fatalf("SUM with no matches should not error: %v", err)
	}
	t.Logf("SUM no matches: value=%d valid=%v", sumNull.Int64, sumNull.Valid)

	// Test 4: SUM with matching rows
	var sumMatch sql.NullInt64
	err = SafeGet(&sumMatch, "SELECT SUM(user_experience) FROM user_profile WHERE username LIKE $1", "edge_user_%")
	if err != nil {
		t.Fatalf("SUM with matches failed: %v", err)
	}
	// Should be 100+200+300 = 600
	if sumMatch.Int64 != 600 {
		t.Errorf("SUM with matches: expected 600, got %d", sumMatch.Int64)
	}
	t.Logf("SUM with matches: value=%d valid=%v", sumMatch.Int64, sumMatch.Valid)

	// Test 5: COUNT with matching rows
	var countMatch sql.NullInt64
	err = SafeGet(&countMatch, "SELECT COUNT(*) FROM user_profile WHERE username LIKE $1", "edge_user_%")
	if err != nil {
		t.Fatalf("COUNT with matches failed: %v", err)
	}
	if countMatch.Int64 != 3 {
		t.Errorf("COUNT with matches: expected 3, got %d", countMatch.Int64)
	}
	t.Logf("COUNT with matches: value=%d valid=%v", countMatch.Int64, countMatch.Valid)
}
