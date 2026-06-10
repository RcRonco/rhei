# rhei-python

Python bindings for rhei (PyO3 + maturin).

## Build & test (dev)

```bash
cd rhei-python
uv venv
uv pip install maturin pyarrow pytest
uv run maturin develop
uv run pytest -q
```
