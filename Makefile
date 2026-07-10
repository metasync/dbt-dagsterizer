.PHONY: sync test lint build clean publish publish-test

sync:
	uv sync --dev

test:
	uv run pytest

lint:
	uv run ruff check .

clean:
	rm -rf dist

build: clean
	uv build

publish: build
	@test -n "$$UV_PUBLISH_TOKEN"
	uv publish

publish-test: build
	@test -n "$$UV_PUBLISH_TOKEN"
	uv publish --publish-url https://test.pypi.org/legacy/ --check-url https://test.pypi.org/simple/
