.PHONY: bins test clean
PROJECT_ROOT=github.com/uber/cherami-client-go

export PATH := $(GOPATH)/bin:$(PATH)

PROGS = example
export GO15VENDOREXPERIMENT=1
NOVENDOR = $(shell GO15VENDOREXPERIMENT=1 glide novendor)

export PATH := $(GOPATH)/bin:$(PATH)

# Automatically gather all srcs
ALL_SRC := $(shell find . -name "*.go" | grep -v -e Godeps -e vendor \
	-e ".*/\..*" \
	-e ".*/_.*" \
	-e ".*/mocks.*")

# all directories with *_test.go files in them
TEST_DIRS := $(sort $(dir $(filter %_test.go,$(ALL_SRC))))

bins:
	glide install
	go build -i -o example example.go
clean:
	rm -rf example
test:
	@rm -f test	
	@rm -f test.log
	@for dir in $(TEST_DIRS); do \
		go test -coverprofile=$@ "$$dir" | tee -a test.log; \
	done;

test-race:
	@for dir in $(TEST_DIRS); do \
		go test -race "$$dir" | tee -a "$$dir"_test.log; \
	done;
