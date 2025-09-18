# DAG Project


This is a [Dagster](https://dagster.io/) based data engineering platform that manages multiple data pipelines including stock information (CD), pension data (NPS), and community content aggregation (IS).

> **Documentation moved**  
> Detailed technical specifications and per‑project guides are now centralized in the [`docs/`](docs/overview.md) directory.  
> This README serves as a quick‑start and contribution overview.

## Getting started

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `dag/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

## Projects

For an up‑to‑date description of each pipeline, refer to:

- [`docs/cd.md`](docs/cd.md) — CD (Chundan) stock information service  
- [`docs/nps.md`](docs/nps.md) — NPS (National Pension Service) data pipeline  
- [`docs/is.md`](docs/is.md) — IS (Isshoo) Korean community content aggregator

## Development

### Project Structure

- `dag/` - Main package directory with all asset definitions
  - `cd_*.py` - CD project asset files
  - `nps_*.py` - NPS project asset files
  - `is_*.py` - IS project asset files
- `dag_tests/` - Test directory
- `docs/` - Project documentation
- `data/` - Data storage directory

### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.

### Unit testing

Tests are in the `dag_tests` directory and you can run tests using `pytest`:

```bash
pytest dag_tests
```

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/guides/automate/schedules/) or [Sensors](https://docs.dagster.io/guides/automate/sensors/) for your jobs, the [Dagster Daemon](https://docs.dagster.io/guides/deploy/execution/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.

## Deploy on Dagster+

The easiest way to deploy your Dagster project is to use Dagster+.

Check out the [Dagster+ documentation](https://docs.dagster.io/dagster-plus/) to learn more.
