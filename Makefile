.PHONY: test lint tidy js-test

test:
	go test -race -count=1 ./...

lint:
	go vet ./...
	golangci-lint run ./...

tidy:
	go mod tidy

js-test:
	cd js && npm ci && npm run build && npm test
