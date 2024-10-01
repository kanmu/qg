.PHONY: test
test: vet
	go test -v ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: lint
lint:
	golangci-lint run

.PHONY: db
db:
	psql -U postgres -h localhost -d postgres -c 'CREATE USER qgtest;'
	psql -U postgres -h localhost -d postgres -c 'CREATE DATABASE qgtest OWNER qgtest;'

.PHONY: table
table:
	psql -U qgtest -h localhost -d qgtest -f schema.sql
