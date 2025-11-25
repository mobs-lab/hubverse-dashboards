# Getting Started

This guide will help you set up the Hubverse Dashboard on your local machine.

## Prerequisites

*   **Node.js**: Version 20+ ([Download](https://nodejs.org/))
*   **Git**: Version control ([Download](https://git-scm.com/))
*   **Python**: Version 3.9+ (Recommended for data processing)

## Quick Start

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/mobs-lab/hubverse-dashboards.git
    cd hubverse-dashboards
    ```

2.  **Install Dependencies**:
    ```bash
    npm install
    pip install -r requirements.txt
    ```

3.  **Configure Data**:
    Copy the example configuration to create your own:
    ```bash
    cp config.yaml.example config.yaml
    ```
    
    Ensure your data is placed in the project root:
    *   `target-data/` (Ground truth data)
    *   `model-output/` (Forecast submissions)
	*	`auxiliary-data/` (Locations, map shape file, etc.)

4.  **Build the Dashboard**:
    Run the automated build script:
    ```bash
    ./build_dashboard.sh
    ```
    Select **Option 1** for a standard build.

5.  **Run Development Server**:
    ```bash
    npm run dev
    ```
    Visit `http://localhost:3000` to see your dashboard.

## Development Mode

To test or to develop, you can run the whole process in development mode.

In development mode, the dashboard builder will look for data using the same kind of structure, but inside `/test-data-input` folder, which should be inside project root. Create one if you don't have it, put the data inside it while maintaining the same `target-data`, `model-output` and `auxiliary-data` structure.

Then use the development mode options when running the `build_dashboard.sh`.

The development mode will produce processed data and metadata inside `/public/test-data-output` folder and when you spin up the site locally it will use that as data source.