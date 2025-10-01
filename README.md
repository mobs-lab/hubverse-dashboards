# Hubverse Dashboard

## About This Project

Template for quickly spinning up Hubverse-standard-compatible dashboard, visualizating forecast and evaluations data.

## Technology Requirements:

- Git
- Node.js (npm)
- (Windows Users) Bash Environment, e.g. Git Bash

## How To Use This Dashboard:

1. Install [Node.js (and npm)](https://docs.npmjs.com/downloading-and-installing-node-js-and-npm) Version 20+
2. Clone the repository to your local machine:
   `git clone https://github.com/mobs-lab/hubverse-dashboards.git`

3. Go to project root directory and install the dependencies:

`cd epistorm-dashboard`

`npm install`

5. (For Local-Only Setup) Put target data inside `target-data/` and model output data in `model-output/`. Inside `model-output/`, each modelling team should have their own separate subdirectory, e.g. `model-output/MOBS-GLEAM_FLUH/`.
   [See Hubverse.io's documentation on compatible format & standards](https://hubverse.io/tools/data.html)

6. (For Using Hubverse Data Repo Setup) Remember to structure your repo exactly like described above, and specify link to repo in the configuration file (See below).

7. Copy `config.yaml.example` to `config.yaml` and [customize the configurations](Configurations.md) according to your needs

8. Make sure `build_dashboard.sh` is executable by Bash, then run: `./build_dashboard.sh`

9. Start the development server:

`npm run dev`

8. Or, Start the server, in production mode, after building the project:

`npm run build && npm run start`

## Tips

### If you want to version control (using Git) your dashboard after setting it up:

Remove the `.git` folder at root of this project folder.

For example: `cd hubverse-dashboard && rm -r ./.git`.

Then create a new repository on your Git Hosting Service, for example GitHub.

Then come back here and `git init .`

Then follow your Git Hosting Service's guide to push your new local repo to the online repo, after linking them.

## Example site running this template, for reference:

<!--TODO: Add a demo site after finishing the configuration reading and changing the frontend code to work accordingly-->
