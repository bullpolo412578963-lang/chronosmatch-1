PYTHON ?= python3

.PHONY: all build clean test benchmark run export fmt help

all: build

build:
	$(PYTHON) src/engine/setup.py build_ext --inplace

clean:
	rm -rf build dist *.egg-info src/engine/*.so src/engine/*.c src/engine/*.cpp src/engine/*.html
	rm -f data/*.db data/*.db-wal data/*.db-shm
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

test: build
	$(PYTHON) -m pytest tests/

benchmark: build
	$(PYTHON) -m benchmarks.benchmark_report

run: build
	$(PYTHON) main.py

export:
	$(PYTHON) main.py --export

help:
	@echo "ChronosMatch Build & Run Targets:"
	@echo "  make build      - Compile Cython matching engine extension"
	@echo "  make test       - Run all unit and integration tests"
	@echo "  make benchmark  - Run performance benchmarks and generate report"
	@echo "  make run        - Launch live interactive trading simulation"
	@echo "  make export     - Export database trade records to CSV"
	@echo "  make clean      - Remove build artifacts and temporary files"
