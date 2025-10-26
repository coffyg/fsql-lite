#!/bin/bash

# Make script executable
chmod +x setup_test_db.sh

# Setup the test database
./setup_test_db.sh

# Run the tests with the local database
go test -v -args -dbuser=test_user -dbpass=test_password -dbname=test_db -dbhost=localhost -dbport=5433
