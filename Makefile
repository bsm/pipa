default: unit

test: vet errcheck unit

deps:
	go get -t ./...
	go get github.com/kisielk/errcheck

unit:
	go test ./...

vet:
	go tool vet -composites=false ./...

errcheck:
	errcheck ./...

.PHONY: test vet errcheck unit deps
