SOURCES:=$(shell find . -name '*.go' -not -path '*vendor/*')
PACKAGES:=$(shell glide novendor)

default: vet errcheck test

deps:
	glide install

test:
	go test $(PACKAGES)

vet:
	go tool vet -composites=false $(SOURCES)

errcheck:
	errcheck -ignoretests -ignore 'Close' $(PACKAGES)

.PHONY: default test vet errcheck deps
