# Hubverse Dashboard

## About This Project

Template for quickly spinning up Hubverse-standard-compatible dashboard, visualizating forecast and evaluations data.

## Technology Requirements:

- Git
- Node.js (npm)
- (Windows Users) Bash Environment, e.g. Git Bash

## How to spin up the site locally:

1. Install [Node.js and npm](https://docs.npmjs.com/downloading-and-installing-node-js-and-npm) Version 20+
2. Clone the repository to your local machine:
   `git clone https://github.com/mobs-lab/hubverse-dashboards.git`

3. Go to project root directory and install the dependencies:

`cd epistorm-dashboard`

`npm install`

4. Copy `config.yaml.example` to `config.yaml` and [customize it according to your visualization goals](Configurations.md)

5. Put target data inside `target-data/` and model output data in `model-output/`. [See Hubverse.io's documentation on compatible format & standards](https://hubverse.io/tools/data.html)

6. Make sure `build-site.sh` is executable by Bash, then run: `./build-site.sh`

7. Start the development server:

`npm run dev`

8. Or, Start the server, in production mode, after building the project:

`npm run build && npm run start`
