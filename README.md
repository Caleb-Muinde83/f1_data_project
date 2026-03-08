# f1_data_project

# OpenF1 Data Engineering Project 🏎️📊

This repository contains the infrastructure, orchestration, and transformation code for a modern data stack that extracts, loads, and transforms Formula 1 telemetry and race data using the [OpenF1 API](https://openf1.org/).

## 🏗️ Project Architecture & Tech Stack

This project implements an ELT (Extract, Load, Transform) pipeline using industry-standard tools:
* **Source:** OpenF1 API (18 endpoints including telemetry, race control, and weather)
* **Infrastructure as Code (IaC):** OpenTofu (GCP Cloud Storage & BigQuery setup)
* **Orchestration:** Apache Airflow (Local Docker execution & GCP Cloud Composer)
* **Transformation:** dbt (Data Build Tool)
* **Version Control & CI/CD:** Git, GitHub Actions



## 💻 Local Development Prerequisites

To run and develop this project locally, install the following system tools:

| Tool | Purpose |
|-----|------|
| **Git** | Version control and collaboration |
| **Python 3.9 – 3.12** | Virtual environments, DAG scripts, and dbt compilation |
| **Docker Desktop** | Running the local Airflow environment |
| **Google Cloud CLI (`gcloud`)** | Authentication and interaction with Google Cloud |
| **OpenTofu** | Infrastructure provisioning (GCP resources) |
| **dbt (Data Build Tool)** | Data transformation and modeling inside BigQuery |

---

## 🛠 Installing Local Development Prerequisites

To run this project locally, install the following tools depending on your operating system.

---

# 🪟 Windows Installation

### 1. Install Git
Download and install Git:

https://git-scm.com/download/win

Verify installation:

```bash
git --version
```

---

### 2. Install Python (3.9 – 3.12)

Download Python:

https://www.python.org/downloads/

During installation:
- ✅ Check **"Add Python to PATH"**

Verify:

```bash
python --version
pip --version
```

---

### 3. Install Docker Desktop

Download:

https://www.docker.com/products/docker-desktop/

After installation verify:

```bash
docker --version
docker compose version
```

---

### 4. Install Google Cloud CLI

Download installer:

https://cloud.google.com/sdk/docs/install

Verify:

```bash
gcloud version
```

Authenticate:

```bash
gcloud auth login
```

---

### 5. Install OpenTofu

Install via Chocolatey (recommended):

```bash
choco install opentofu
```

Or download manually:

https://opentofu.org/docs/intro/install/

Verify:

```bash
tofu version
```

---

### 6. Install dbt (BigQuery Adapter)

Inside your Python environment:

```bash
pip install dbt-bigquery
```

Verify:

```bash
dbt --version
```

---

# 🍎 macOS Installation

### 1. Install Homebrew (Package Manager)

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

---

### 2. Install Git

```bash
brew install git
```

Verify:

```bash
git --version
```

---

### 3. Install Python

```bash
brew install python
```

Verify:

```bash
python3 --version
pip3 --version
```

---

### 4. Install Docker Desktop

Download:

https://www.docker.com/products/docker-desktop/

Verify:

```bash
docker --version
```

---

### 5. Install Google Cloud CLI

```bash
brew install --cask google-cloud-sdk
```

Initialize:

```bash
gcloud init
```

---

### 6. Install OpenTofu

```bash
brew install opentofu
```

Verify:

```bash
tofu version
```

---

### 7. Install dbt

```bash
pip3 install dbt-bigquery
```

Verify:

```bash
dbt --version
```

---

# 🐧 Linux Installation (Ubuntu / Debian)

### 1. Install Git

```bash
sudo apt update
sudo apt install git -y
```

Verify:

```bash
git --version
```

---

### 2. Install Python

```bash
sudo apt install python3 python3-pip python3-venv -y
```

Verify:

```bash
python3 --version
```

---

### 3. Install Docker

```bash
sudo apt install docker.io docker-compose-plugin -y
```

Start Docker:

```bash
sudo systemctl enable docker
sudo systemctl start docker
```

Verify:

```bash
docker --version
```

---

### 4. Install Google Cloud CLI

```bash
curl https://sdk.cloud.google.com | bash
exec -l $SHELL
```

Verify:

```bash
gcloud version
```

---

### 5. Install OpenTofu

```bash
curl -fsSL https://get.opentofu.org/install-opentofu.sh | sh
```

Verify:

```bash
tofu version
```

---

### 6. Install dbt

```bash
pip3 install dbt-bigquery
```

Verify:

```bash
dbt --version
```

---

# ✅ Verify Everything

Run the following commands to ensure everything is installed correctly:

```bash
git --version
python --version
docker --version
gcloud version
tofu version
dbt --version
```

If all commands return versions, your environment is ready to run the project.

## 🔀 Git Workflow & CI/CD

This project follows the **Feature Branch Workflow** to keep the `main` branch stable.

**Important rule:**  
🚫 Never commit directly to `main`.

### Development Workflow

1. Create a feature branch

```bash
git checkout -b feature/your-feature-name
```

2. Write and test your code locally.

3. Commit your changes

```bash
git add .
git commit -m "feat: description of feature"
```

4. Push your branch to GitHub

```bash
git push -u origin feature/your-feature-name
```

5. Open a **Pull Request (PR)** to merge into `main`.

### Automated Deployment

Once the PR is merged into `main`, **GitHub Actions** will automatically run the CI/CD pipeline which:

- Validates the repository
- Runs deployment scripts
- Syncs Airflow DAGs and plugins to **Google Cloud Composer**
- Triggers dbt model execution for transformations

---

## 🚀 Current Project Progress

### Completed

- [x] Defined system architecture and target endpoints  
- [x] Configured local developer environment (Git, Python, Docker, OpenTofu, GCP CLI, dbt)  
- [x] Generated master directory structure  
- [x] Mapped out **18 dbt staging models** for full OpenF1 endpoint coverage  

### In Progress / Next Steps

- [ ] Lock in Python dependencies (`requirements.txt`)
- [ ] Spin up local Airflow (`docker-compose.yaml`)
- [ ] Provision GCP infrastructure with **OpenTofu**
- [ ] Implement extraction scripts and Airflow DAGs
- [ ] Implement dbt transformations


## 📂 Repository Structure

The codebase is organized into distinct domains to separate infrastructure, orchestration, and business logic:

For a **README.md**, the cleanest way is to present the directory structure inside a **Markdown code block** so the spacing is preserved. Here’s a properly formatted version:

````markdown
## Project Structure
```text
f1_data_project
├── .gitignore                          # Ignores raw data, keys, Python cache, and dbt target/ folders
├── Makefile                            # Shortcuts (e.g., `make up` for Airflow, `make apply` for Tofu)
├── README.md                           # Project documentation
│
├── .github/                            # Automated CI/CD Pipelines
│   └── workflows/
│       └── deploy_to_composer.yml      # Automatically syncs Airflow code to GCP on merge to main
│
├── scripts/                            # Manual / Helper Scripts
│   └── deploy_to_composer.sh           # Bash script to manually sync DAGs/Plugins to Composer bucket
│
├── opentofu/                           # Infrastructure as Code (GCP Setup)
│   ├── main.tf                         # Defines GCS (Data Lake) and BigQuery datasets
│   ├── provider.tf                     # GCP provider configuration for OpenTofu
│   └── variables.tf                    # GCP Project IDs, region settings, bucket names
│   
├── airflow/                            # Open-Source Local Airflow Setup
│   ├── docker-compose.yaml             # Spins up local Airflow webserver, scheduler, and Postgres
│   ├── requirements.txt                # Python dependencies (astronomer-cosmos, dbt-bigquery, requests)
│   │
│   ├── plugins/
│   │   └── hooks/
│   │       └── openf1_hook.py                 # Centralized API connection manager
│   │
│   ├── logs/                                  # Local Airflow execution logs
│   │
│   └── dags/                                  # Airflow workflows
│       ├── .airflowignore                     # Tells Airflow scheduler to ignore the modules folder
│       ├── extract_openf1_dag.py              # Main DAG: Orchestrates modular extraction tasks
│       ├── dbt_cosmos_dag.py                  # Cosmos DAG: Parses dbt models into Airflow tasks
│       │
│       └── modules/                           # Modularized Python Business Logic (Full OpenF1 Coverage)
│           ├── __init__.py
│           ├── event_extractor.py             # Functions for: /meetings, /sessions
│           ├── competitor_extractor.py        # Functions for: /drivers, /championship_drivers, /championship_teams
│           ├── results_extractor.py           # Functions for: /starting_grid, /session_result
│           ├── performance_extractor.py       # Functions for: /laps, /pit, /stints, /intervals, /position, /overtakes
│           ├── telemetry_extractor.py         # Functions for: /car_data, /location, /weather
│           ├── race_events_extractor.py       # Functions for: /race_control, /team_radio
│           └── gcs_loader.py                  # Uploads extracted data to Google Cloud Storage
│
├── dbt_f1/                                    # dbt Transformation Project
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── profiles.yml
│   │
│   ├── macros/                                # Reusable SQL macros
│   │
│   └── models/
│       ├── staging/                           # Raw → Clean transformations
│       │   ├── stg_car_data.sql               # Cleans 3.7Hz telemetry data
│       │   ├── stg_championship_drivers.sql   # Cleans driver standings data
│       │   ├── stg_championship_teams.sql     # Cleans constructor standings data
│       │   ├── stg_drivers.sql                # Cleans driver info (names, acronyms, team colors)
│       │   ├── stg_intervals.sql              # Cleans time gaps between cars and to the leader
│       │   ├── stg_laps.sql                   # Cleans lap durations, sector times, and speed traps
│       │   ├── stg_location.sql               # Cleans X, Y, Z coordinates of cars on track
│       │   ├── stg_meetings.sql               # Cleans overarching event/weekend data
│       │   ├── stg_overtakes.sql              # Cleans overtaking events and track positions
│       │   ├── stg_pit.sql                    # Cleans pit lane times and stationary stop durations
│       │   ├── stg_position.sql               # Cleans running track positions
│       │   ├── stg_race_control.sql           # Cleans flags, safety cars, and race director messages
│       │   ├── stg_sessions.sql               # Cleans session data (Practice, Qualifying, Race)
│       │   ├── stg_session_result.sql         # Cleans final classifications, DNFs, and DSQs
│       │   ├── stg_starting_grid.sql          # Cleans starting grid positions
│       │   ├── stg_stints.sql                 # Cleans tyre compound usage and stint lengths
│       │   ├── stg_team_radio.sql             # Cleans radio recording URLs
│       │   ├── stg_weather.sql                # Cleans track temperature, rainfall, and wind data
│       │   └── schema.yml                     # Documentation and data tests (e.g., uniqueness on keys)
│       │
│       ├── intermediate/
│       │   └── int_driver_lap_telemetry.sql   # Combines lap data with telemetry for deeper analysis
│       │
│       └── marts/                             # Final analytics-ready tables
│           ├── dim_drivers.sql                # Driver dimension table
│           ├── fct_race_performance.sql       # Fact table for race performance analytics
│           └── schema.yml                     # Documentation and tests for marts
```
