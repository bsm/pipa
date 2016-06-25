SOURCES:=$(shell find . -name '*.go' -not -path '*vendor/*')
PACKAGES:=$(shell glide novendor)

default: vet test

test:
	go test $(PACKAGES)

vet:
	go tool vet -composites=false $(SOURCES)

.PHONY: default test vet deps
