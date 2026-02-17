# Getting Started

This guide walks you through setting up the Hubverse Dashboard on your local machine.

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| **Node.js** | 20+ | Frontend build and development server ([Download](https://nodejs.org/)) |
| **npm** | (bundled with Node.js) | JavaScript package manager |
| **Git** | Any recent version | Version control and remote data fetching ([Download](https://git-scm.com/)) |
| **Python** | 3.9+ | Data processing pipeline ([Download](https://www.python.org/downloads/)) |

### Windows Users

A Bash-compatible environment is required to run `build_dashboard.sh`. Options include:
- [Git Bash](https://gitforwindows.org/) (recommended, bundled with Git for Windows)
- [WSL (Windows Subsystem for Linux)](https://learn.microsoft.com/en-us/windows/wsl/)

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/mobs-lab/hubverse-dashboards.git
cd hubverse-dashboards
```

### 2. Install JavaScript Dependencies

```bash
npm install
```

### 3. Set Up Python Environment

Using a virtual environment is recommended to isolate this project's dependencies:

```bash
# Create a virtual environment
python3 -m venv .venv

# Activate it
source .venv/bin/activate        # macOS / Linux
source .venv/Scripts/activate    # Windows (Git Bash)

# Install Python dependencies
pip install -r requirements.txt
```

For more details, see [Python's official venv documentation](https://docs.python.org/3/library/venv.html).

### 4. Configure Your Data

Copy the example configuration from `config-file-examples/` and customize it for your data:

```bash
cp config-file-examples/config.yaml.example config.yaml
```

The `config-file-examples/` directory also contains pre-made configurations for specific data hubs (COVID-19, FluSight, RSV) that you can use as starting points.

Then ensure your data directories are set up:

- `target-data/` -- Ground truth / observed data
- `model-output/` -- Forecast submissions (one subdirectory per model)
- `auxiliary-data/` -- Location mappings, custom shapefiles, etc.

Alternatively, configure `link_to_hubverse_compatible_data` in `config.yaml` to point to a remote Hubverse-compatible GitHub repository, and the build script will fetch the data automatically.

See [Configuration Reference](configuration.md) for details on all available options.

### 5. Build the Dashboard

```bash
bash ./build_dashboard.sh
```

Select **Option 1** for a standard full build with evaluations.

The build script provides these options:

| Option | Description |
|--------|-------------|
| **0** | Build & serve HTML documentation (Sphinx) |
| **1** | Full build with evaluations |
| **2** | Build without evaluations (disables Evaluations page) |
| **3** | Development mode build with evaluations |
| **4** | Development mode build without evaluations |
| **5** | Data update -- production mode (incremental) |
| **6** | Data update -- development mode (incremental) |

### 6. View the Dashboard

After processing completes, the script prompts you to start a server:

- **Development server** (hot reload): `npm run dev`
- **Production build**: `npm run build && npm run start`

Visit `http://localhost:3000` in your browser.

---

## Development Mode

Development mode provides an isolated environment for testing so your production data is not affected.

### Setup

1. Create a `development-mode-root/` directory in the project root.
2. Inside it, replicate the standard data structure:

```text
development-mode-root/
├── target-data/
│   └── (your test ground truth files)
├── model-output/
│   ├── ModelA/
│   │   └── 2024-01-01-ModelA.csv
│   └── ModelB/
│       └── 2024-01-01-ModelB.csv
└── auxiliary-data/
    └── locations.csv
```

3. Run `build_dashboard.sh` and choose **Option 3** or **Option 4**.

### How It Works

- **Input**: Read from `development-mode-root/` instead of the project root.
- **Output**: Written to `public/test-data-output/` instead of `public/data/`.
- **Intermediates**: Stored in `development-mode-root/intermediates/`.
- **Frontend**: Automatically loads from `/test-data-output` when you run `npm run dev`.

---

## Data Update Runs (Incremental Processing)

After an initial full build, you can use **Option 5** or **Option 6** to incrementally update data. The system detects changes via MD5 file checksums and only reprocesses what has changed, which is significantly faster.

```{note}
A full build (Options 1--4) must complete successfully before using update mode. The system requires existing intermediates and a `manifest.json` from a prior run.
```

---

## Generating Test Data

If you need synthetic data for testing, use the `dev-tools/generate_test_target_data.py` script:

```bash
python dev-tools/generate_test_target_data.py --help
```

This generates Hubverse-compatible target data files (CSV or Parquet) with configurable date ranges, locations, targets, and `as_of` snapshots.

---

## Next Steps

- [Configuration Reference](configuration.md) -- Full documentation of all `config.yaml` options.
- [Data Preparation](data_preparation.md) -- Details on required data formats and directory structure.
- [Architecture](architecture.md) -- Deep dive into the processing pipeline and system design.
- [UI Customization](ui_customization.md) -- Customize dashboard branding and display options.
