.PHONY: setup run extract transform dq query test clean
setup:     ; uv sync
run:       ; uv run python -m pipeline run
extract:   ; uv run python -m pipeline extract
transform: ; uv run python -m pipeline transform
dq:        ; uv run python -m pipeline dq
query:     ; uv run python -m pipeline query
test:      ; uv run pytest -q
clean:     ; rm -rf data
