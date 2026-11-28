.PHONY: build test demo bench validate clean

BIN := bin/tickdb

build:
	@mkdir -p bin
	go build -o $(BIN) ./src/cmd/tickdb

test:
	go test -v -race ./...

demo: build
	./scripts/run_demo.sh

bench: build
	./scripts/run_benchmarks.sh

validate: build
	./scripts/validate.sh

clean:
	rm -rf bin data/runtime data/bench_runtime
