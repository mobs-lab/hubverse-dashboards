# Hubverse Dashboard

Template for quickly spinning up a Hubverse-standard-compatible dashboard, visualizing forecast and evaluation data.

Built with **Next.js** (React) for the frontend and **Python** for data processing, configuration validation, and evaluation metrics.

---

## Technology Requirements

- **Git** ([Download](https://git-scm.com/))
- **Node.js** (v20+) and **npm** ([Download](https://nodejs.org/))
- **Python** (v3.9+) ([Download](https://www.python.org/downloads/))
- **(Windows Users)** A Bash-compatible environment, e.g., [Git Bash](https://gitforwindows.org/)

---

## How To Use This Dashboard

1. **Clone the repository:**

   ```bash
   git clone https://github.com/mobs-lab/hubverse-dashboards.git
   cd hubverse-dashboards
   ```

2. **Install npm dependencies:**

   ```bash
   npm install
   ```

3. **Set up Python and install dependencies** (see [Python Environment Setup](#python-environment-setup) below for detailed instructions):

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

4. **(For Local Data)** Place your data in the project root:
   - `target-data/` -- Ground truth / observed data
   - `model-output/` -- Forecast submissions, one subdirectory per model (e.g., `model-output/MOBS-GLEAM_FLUH/`)
   - `auxiliary-data/` -- Location mapping CSVs, custom shapefiles, etc.

   [See Hubverse.io's documentation on compatible format & standards](https://hubverse.io/tools/data.html)

5. **(For Remote Data)** Specify the link to your Hubverse-compatible GitHub repository in `config.yaml` under `link_to_hubverse_compatible_data`. The build script will clone/update the repo automatically.

6. **Configure the dashboard** by copying the example configuration from `config-file-examples/` and customizing it:

   ```bash
   cp config-file-examples/config.yaml.example config.yaml
   ```

   The `config-file-examples/` directory also contains pre-made configurations for specific data hubs (COVID-19, FluSight, RSV) that you can use as starting points. See [Configuration Reference](docs/source/configuration.md) for full details on each option, or run Option 0 in the build script to browse the HTML documentation.

7. **(Optional)** Make the build script executable:

   ```bash
   chmod +x build_dashboard.sh
   ```

8. **Run the build script:**

   ```bash
   bash ./build_dashboard.sh
   ```

   The interactive menu offers the following options:

   | Option | Description                                           |
   | ------ | ----------------------------------------------------- |
   | **0**  | Build and view the full documentation                 |
   | **1**  | Full build with evaluations (WIS, MAPE, Coverage)     |
   | **2**  | Build without evaluations (disables Evaluations page) |
   | **3**  | Development mode build with evaluations               |
   | **4**  | Development mode build without evaluations            |
   | **5**  | Data update -- production mode (incremental)          |
   | **6**  | Data update -- development mode (incremental)         |

9. **Start the dashboard.** After processing completes, the script prompts you to launch a server:
   - **Development server:** `npm run dev` -- hot reload, `http://localhost:3000`
   - **Production build:** `npm run build && npm run start`

---

## Python Environment Setup

The data processing pipeline (`scripts/`) requires Python 3.9+ with several packages. If you already have Python installed and know how to manage environments, a quick `pip install -r requirements.txt` inside a virtual environment is all you need. Otherwise, read on.

### Installing Python

- **macOS / Linux**: Python 3 is often pre-installed. Verify with `python3 --version`. If not present, install via [python.org](https://www.python.org/downloads/) or your package manager (`brew install python3`, `sudo apt install python3`).
- **Windows**: Download the installer from [python.org](https://www.python.org/downloads/). Make sure to check **"Add Python to PATH"** during installation.

### Setting Up a Virtual Environment (Recommended)

Using a virtual environment keeps this project's dependencies isolated from your system Python.

```bash
# Create a virtual environment in the project root
python3 -m venv .venv

# Activate it
# macOS / Linux:
source .venv/bin/activate
# Windows (Git Bash):
source .venv/Scripts/activate
# Windows (cmd.exe):
.venv\Scripts\activate.bat

# Install dependencies
pip install -r requirements.txt
```

For more details on virtual environments, see [Python's official venv documentation](https://docs.python.org/3/library/venv.html).

> **Note:** Always activate your virtual environment before running `build_dashboard.sh` or any scripts in `scripts/`.

---

## How to Use Development Mode

Development mode lets you work with test data in an isolated directory structure so your production data is not affected.

### Setup

1. Create the `development-mode-root/` directory in the project root (if it does not already exist).
2. Inside `development-mode-root/`, replicate the standard data directory structure:

   ```
   development-mode-root/
   ├── target-data/
   │   └── (your test ground truth files)
   ├── model-output/
   │   ├── ModelA/
   │   └── ModelB/
   └── auxiliary-data/
       └── locations.csv
   ```

3. Run the build script and choose **Option 3** or **Option 4** (dev mode builds).

### How It Works

- **Input data** is read from `development-mode-root/` instead of the project root.
- **Processed output** is written to `public/test-data-output/` (instead of `public/data/`).
- **Intermediates** (manifest, cached parquet files) are stored in `development-mode-root/intermediates/`.
- When you run `npm run dev`, the frontend automatically loads data from `/test-data-output`.

### Data Update Runs (Incremental Processing)

After an initial build, you can use **Option 5** (production) or **Option 6** (dev mode) to run incremental data updates. The system uses a file manifest with MD5 checksums to detect changes in `target-data/`, `model-output/`, and `auxiliary-data/`, and only re-processes what has changed. This is significantly faster than a full rebuild.

> **Prerequisite:** A full build (Options 1--4) must have completed successfully before running a data update. The update mode requires existing intermediates and a `manifest.json`.

---

## Common Errors and Solutions

Below are common issues you may encounter when running `build_dashboard.sh` and how to resolve them.

### `python3: command not found`

**Cause:** Python 3 is not installed or not on your system PATH.

**Solution:** Install Python 3 (see [Python Environment Setup](#python-environment-setup)). On some systems, the command may be `python` instead of `python3` -- you can create an alias or modify `build_dashboard.sh` accordingly.

### `ModuleNotFoundError: No module named 'yaml'` (or `pandas`, `pydantic`, etc.)

**Cause:** Python dependencies are not installed, or your virtual environment is not activated.

**Solution:**

```bash
source .venv/bin/activate   # Activate your virtual environment first
pip install -r requirements.txt
```

### `config.yaml not found in project root`

**Cause:** You have not created a `config.yaml` file yet.

**Solution:**

```bash
cp config-file-examples/config.yaml.example config.yaml
# Then edit config.yaml with your settings
```

### `Configuration validation failed` / Pydantic `ValidationError`

**Cause:** Your `config.yaml` has invalid or missing fields. The Pydantic validator provides detailed error messages indicating which field failed and why.

**Solution:** Read the error messages carefully. They indicate the field path (e.g., `forecast_periods -> 0 -> end_date`) and the specific issue. Cross-reference with the [Configuration Reference](docs/source/configuration.md) or the HTML documentation (Option 0 in the build menu).

### `ERROR: Data update run requires existing artifacts`

**Cause:** You selected a Data Update option (5 or 6) without having completed a full initial build first.

**Solution:** Run a full build first (Options 1--4) to generate the required intermediates (`manifest.json`, cached parquet files). Then use the update options for subsequent runs.

### `Target data file not found: <filename> in target-data/`

**Cause:** The file specified by `single_target_data_file_name` in your `config.yaml` does not exist in the `target-data/` directory (or `development-mode-root/target-data/` in dev mode).

**Solution:** Verify the filename matches exactly (without extension). The system appends `.csv` or `.parquet` based on `target_data_file_format`.

### `npm ERR!` or `next: command not found`

**Cause:** Node.js dependencies are not installed.

**Solution:**

```bash
npm install
```

### `Port 3000 is already in use`

**Cause:** Another process (possibly a previous dashboard instance) is using port 3000.

**Solution:** Kill the existing process or use a different port:

```bash
# Find and kill the process
lsof -i :3000
kill -9 <PID>

# Or use a different port
PORT=3001 npm run dev
```

---

## Warnings and Recommendations

### Use the Latest Python Version

While the minimum requirement is Python 3.9, we strongly recommend using the **latest stable Python release** (3.14+ as of this writing). Newer versions include performance improvements, better error messages, and security patches that benefit the data processing pipeline. You can check your version with `python3 --version`.

### Switching Data Hubs (Complete Configuration Overhaul)

If you are switching the data hub your dashboard is pointed at (e.g., from COVID-19 Forecast Hub to FluSight Hub), this effectively means an entirely different dataset, different model names, different targets, and a completely different `config.yaml`. **We strongly recommend starting clean** rather than trying to incrementally adjust an existing setup:

1. **Delete all data input directories** (`target-data/`, `model-output/`, `auxiliary-data/`, and `development-mode-root/` if present).
2. **Delete all processed output** (`public/data/`, `public/test-data-output/`).
3. **Delete intermediates** (`intermediates/`, `development-mode-root/intermediates/`).

Alternatively, consider **cloning the repository template fresh** into a new directory entirely. This avoids any risk of stale cached data or manifest state contaminating the new build. The incremental update system relies on manifests and intermediates that are tied to a specific data hub's schema -- mixing data from different hubs in the same intermediates will cause errors or incorrect output.

### Do Not Mix Data Hub Artifacts

The manifest (`manifest.json`), intermediate parquet caches, and processed JSON output are all tightly coupled to the configuration they were built with. If you change `config.yaml` significantly (new targets, different column mappings, different models), always run a **full from-scratch build** (Options 1--4) rather than a data-update run (Options 5--6). The data-update mode assumes the schema and configuration are consistent with the previous build.

---

## Tips

### Version Controlling Your Dashboard

If you want to put your configured dashboard under its own Git repository:

1. Remove the existing `.git` folder: `rm -rf .git`
2. Create a new repository on your Git hosting service (e.g., GitHub).
3. Initialize and push:

   ```bash
   git init .
   git add .
   git commit -m "Initial dashboard setup"
   git remote add origin <your-repo-url>
   git push -u origin main
   ```

### Generating Test Data

The `dev-tools/generate_test_target_data.py` script can generate synthetic Hubverse-compatible target data for testing. This is useful for verifying your configuration or testing data-update runs without needing real data.

```bash
python dev-tools/generate_test_target_data.py --help
```

### Inspecting Data Files

The `dev-tools/data_inspector.py` script provides detailed analysis of CSV and Parquet files, including data types, unique values, and quality checks. Useful for debugging data issues.

```bash
python dev-tools/data_inspector.py target-data/your-file.csv
```

### Building HTML Documentation

Developer documentation is built with Sphinx. To build and serve locally:

```bash
pip install -r requirements-dev.txt
cd docs
make html
# Open docs/build/html/index.html in your browser
```

Or use **Option 0** in `build_dashboard.sh` to install doc dependencies and launch a local server automatically.
