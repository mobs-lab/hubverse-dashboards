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

`cd hubverse-dashboards`

`npm install`

4. (For Local-Only Setup) Put target data inside `target-data/` and model output data in `model-output/`. Inside `model-output/`, each modelling team should have their own separate subdirectory, e.g. `model-output/MOBS-GLEAM_FLUH/`.
   [See Hubverse.io's documentation on compatible format & standards](https://hubverse.io/tools/data.html)

5. _**WIP**_ (For Using Hubverse Data Repo Setup) Remember to structure your repo exactly like described above, and specify link to repo in the configuration file (See below).

6. Copy `config.yaml.example` to `config.yaml` and customize the configurations.

7. (Optional) Make sure `build_dashboard.sh` is executable

`chmod +x build_dashboard.sh`

8. Run the `build_dashboard.sh`:

`bash ./build_dashboard.sh`

9. Start the development server (`npm run dev`) and go to `http://localhost:3000` in your browser.

_Or, Start the server, in production mode, after building the project:_

`npm run build && npm run start`

<!--TODO: Add a demo site after finishing the configuration reading and changing the frontend code to work accordingly-->

## How to Use Development Mode

First follow the "How To Use This Dashboard" above but follow the below for your data:

Maintain the supposed structure of data (`/target-data`, `/model-output`, etc.), and put
**input data in: `/test-data-input`**.

And make sure to use the development mode when running dashboard builder script (Option 3 or 4).

Then, the output data will appear in `/public/test-data-output`, from where the dashboard will load them automatically.

Finally, go to `http://localhost:3000` as usual to test.

---

## Tips

### If you want to version control (using Git) your dashboard after setting it up:

Remove the `.git` folder at root of this project folder.

For example: `cd hubverse-dashboard && rm -r ./.git`.

Then create a new repository on your Git Hosting Service, for example GitHub.

Then come back here and `git init .`

Then follow your Git Hosting Service's guide to push your new local repo to the online repo, after linking them.
