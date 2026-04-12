# Contributing to Farsight Markets

Thanks for your interest in contributing! This document covers how to get started.

## Getting Started

1. Fork the repository
2. Clone your fork locally
3. Install the package:
   ```bash
   conda create -n farsight-markets python=3.11 -y && conda activate farsight-markets
   # or: python -m venv .venv && source .venv/bin/activate

   pip install -e ".[dev]"
   ```
4. Verify everything works:
   ```bash
   python -m farsight.markets health
   pytest tests/ -v
   ```

No database setup needed — the project uses SQLite for all persistence.

## Making Changes

1. Create a branch from `main`:
   ```bash
   git checkout -b my-feature
   ```
2. Make your changes
3. Run the tests:
   ```bash
   pytest tests/ -v
   ```
4. Push and open a pull request

## What to Contribute

### Good first issues

Look for issues labeled `good first issue` — these are scoped and approachable.

### High-impact areas

- **New signal detectors** — see `services/signal_engine.py` for the pattern
- **New features** — add to `features/` (pure functions, easy to test)
- **New strategies** — follow the composable pipeline pattern in `strategies/base.py`
- **Kalshi integration** — the client exists but needs testing with a real API key
- **WebSocket reliability** — reconnection, gap filling, backpressure handling
- **Documentation** — API quirks, strategy explanations, usage examples

### Out of scope

- Live trading with real money (paper trading only for now)
- UI/frontend code
- Changes that add heavy dependencies (keep it lightweight)

## Code Style

- Python 3.11+ with type hints
- Use `async/await` for I/O operations
- Keep functions focused and testable
- No external state mutation — services take explicit dependencies
- Pydantic for data validation, dataclasses for internal types

## Testing

Every PR should include tests for new functionality:

```bash
# Run all tests
pytest tests/ -v

# Run a specific test file
pytest tests/test_signal_engine.py -v

# Run tests matching a pattern
pytest tests/ -k "test_momentum" -v
```

Tests should be self-contained — mock external API calls, don't depend on network access.

## Pull Request Guidelines

- Keep PRs focused on a single change
- Include a clear description of what and why
- Add/update tests for new behavior
- Don't bundle unrelated changes (formatting, refactors) with feature work
- PRs that add new strategies or signal types should include example output showing the feature in action

## Reporting Issues

Use GitHub Issues with the provided templates. Include:

- What you expected vs what happened
- Steps to reproduce
- Python version and OS
- Relevant error output

## Questions?

Open a GitHub Discussion or an issue tagged `question`.
