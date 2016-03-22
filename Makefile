SOURCES=$(shell find . -name '*.go')

default: vet errcheck test

deps:
	go get -t ./...

test:
	go test ./...

vet:
	go tool vet -composites=false $(SOURCES)

errcheck:
	errcheck -ignoretests -ignore 'Close' ./...

.PHONY: default test vet errcheck deps
