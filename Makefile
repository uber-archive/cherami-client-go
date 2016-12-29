.PHONY: bins test test-race cover cover_profile cover_ci clean
PROJECT_ROOT=github.com/uber/cherami-client-go

export PATH := $(GOPATH)/bin:$(PATH)

PROGS = example
export GO15VENDOREXPERIMENT=1
NOVENDOR = $(shell GO15VENDOREXPERIMENT=1 glide novendor)
TEST_ARG ?= -race -v -timeout 5m
BUILD := ./build

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

cover_profile: clean bins
	@echo Testing packages:
	@for dir in $(TEST_DIRS); do \
		mkdir -p $(BUILD)/"$$dir"; \
		go test "$$dir" $(TEST_ARG) -coverprofile=$(BUILD)/"$$dir"/coverage.out; \
	done;

cover: cover_profile
	@for dir in $(TEST_DIRS); do \
		go tool cover -html=$(BUILD)/"$$dir"/coverage.out; \
	done

cover_ci: cover_profile
	goveralls -coverprofile=$(BUILD)/coverage.out -service=travis-ci || echo -e "\x1b[31mCoveralls failed\x1b[m"
