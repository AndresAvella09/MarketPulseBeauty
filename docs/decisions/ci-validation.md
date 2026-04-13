# CI validation workflow

## What the workflow validates
- Environment setup from requirements.txt using a fixed Python version.
- Data contracts validation using synthetic samples in tests/sample_data.
- Import checks for critical modules to catch syntax and dependency issues early.
- Unit tests with coverage thresholds for critical modules.

## Why these checks were chosen
- Dependency installation confirms requirements.txt is complete and reproducible.
- Data contracts validation ensures pipeline assumptions hold before processing.
- Import checks detect broken modules without running full scripts.
- Coverage thresholds protect core logic from silent regressions.

## Problems this prevents
- Missing or outdated dependencies in CI or new environments.
- Invalid review or trends data passing downstream steps.
- Syntax errors or missing imports in critical modules.
- Uncovered changes in health score, data contracts, and text cleaning logic.

## Notes
- Sample data lives in tests/sample_data and is designed to be small and fast.
- If CI times grow, consider splitting heavy dependencies into extras.
