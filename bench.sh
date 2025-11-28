#!/bin/bash

# Make setup script executable
chmod +x setup_test_db.sh

# Setup the test database (reuses existing if available)
./setup_test_db.sh

DB_USER="test_user"
DB_PASS="test_password"
DB_NAME="test_db"
DB_HOST="localhost"
DB_PORT="5433"

echo "Pre-populating database with benchmark data..."

# Clean existing bench data
PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -q -c "DELETE FROM website WHERE domain LIKE 'bench_%';" 2>/dev/null
PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -q -c "DELETE FROM realm WHERE name LIKE 'bench_%';" 2>/dev/null
PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -q -c "DELETE FROM ai_model WHERE key LIKE 'bench_%';" 2>/dev/null

# Insert 1000 ai_models
echo "Inserting 1000 ai_models..."
PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -q -c "
INSERT INTO ai_model (uuid, key, name, type, provider, settings)
SELECT
    uuid_generate_v4(),
    'bench_model_' || i,
    'Benchmark Model ' || i,
    CASE WHEN i % 3 = 0 THEN 'bench_type_a' WHEN i % 3 = 1 THEN 'bench_type_b' ELSE 'bench_type_c' END,
    CASE WHEN i % 2 = 0 THEN 'provider_x' ELSE 'provider_y' END,
    '{\"max_tokens\": 1000, \"temperature\": 0.7}'::jsonb
FROM generate_series(1, 1000) AS i;
"

# Insert 100 realms
echo "Inserting 100 realms..."
PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -q -c "
INSERT INTO realm (uuid, name)
SELECT
    uuid_generate_v4(),
    'bench_realm_' || i
FROM generate_series(1, 100) AS i;
"

# Insert 500 websites linked to realms
echo "Inserting 500 websites..."
PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -q -c "
INSERT INTO website (uuid, domain, realm_uuid)
SELECT
    uuid_generate_v4(),
    'bench_' || i || '.example.com',
    (SELECT uuid FROM realm WHERE name LIKE 'bench_realm_%' ORDER BY random() LIMIT 1)
FROM generate_series(1, 500) AS i;
"

echo "Database pre-populated. Running benchmarks..."

# Run benchmarks with the local database
# -benchmem shows memory allocations
# -count=3 runs each benchmark 3 times for stable results
go test -bench=. -benchmem -count=3 -args -dbuser=test_user -dbpass=test_password -dbname=test_db -dbhost=localhost -dbport=5433
