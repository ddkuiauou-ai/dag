# Repository Guidelines

## Project Structure & Module Organization
Code lives in the `dag/` package and is grouped by product prefix (`cd_`, `ds_`, `is_`, `ss_`, `nps_`). Asset definitions, jobs, schedules, and shared resources sit in `dag/definitions.py`, `dag/jobs.py`, `dag/schedules.py`, and `dag/resources.py`. Domain notebooks and raw snapshots land under `data/`, while generated artifacts should go to `output_data/` (Git-ignored). Long-form specs and style decisions are captured in `docs/`, notably `docs/coding.md` for asset rules.

## Build, Test, and Development Commands
Use `python -m pip install -e .[dev]` after cloning to pull Dagster, pytest, and dev tooling. Launch the local orchestrator with `dagster dev` and inspect assets through the Dagster UI. Materialize a pipeline with `dagster job launch -j cd_daily_update_job` (replace the job name as needed). Run the current Python test suite with `pytest dag_tests -vv`.

## Coding Style & Naming Conventions
Target Python 3.9–3.12 with four-space indentation and type hints on public functions. Follow the asset-first guidance in `docs/coding.md`: function names must match asset names, every asset sets `group_name` to one of `CD`, `DS`, `IS`, `SS`, or `NPS`, and tier metadata lives in `tags["data_tier"]` only. Keep helper utilities separate from assets, prefer small modules (<1,000 lines), and document meaningful metadata via `MaterializeResult`.

## Testing Guidelines
Add pytest modules under `dag_tests/` using the `test_<feature>.py` pattern. Mock external services and validate Dagster assets with isolated unit tests plus targeted `dagster asset materialize --select asset_name` dry runs when applicable. Fail fast—do not hide errors behind retries—and include regression fixtures for crawler or database IO.

## Commit & Pull Request Guidelines
Commits follow a Conventional Commit-style prefix (`docs:`, `feat:`, `chore:`) as seen in recent history. Squash noisy work-in-progress commits locally. Pull requests should describe the affected assets or jobs, note any new schedules or resources, link to internal tickets, and attach screenshots or logs when changing orchestration behavior.

## Security & Configuration Tips
Load credentials through environment variables or your secret manager; avoid committing `.env` files or raw keys. When adding new resources, document required secrets in `docs/` and reference them via Dagster `EnvVar` helpers.
