# **Formula 1 Data Engineering: From API to Cloud Warehouse**

***A Step-by-Step Course Book and Project Documentation***

**Repository:** [github.com/Caleb-Muinde83/f1_data_project](https://github.com/Caleb-Muinde83/f1_data_project)

This book turns the raw build logs of a real project into a **curriculum**. You will start with an empty folder and finish with a working **Formula 1 analytics warehouse**: raw data pulled from the **OpenF1 API** by **Apache Airflow**, stored as **Parquet**, cleaned and modelled by **dbt** inside **DuckDB**, explored with SQL tools, visualised in **Power BI**, and prepared for the cloud with **OpenTofu** on **Google Cloud**.

Every chapter follows the same rhythm: **why** the technology exists, **what** you will build, **how** to build it step by step, and a **Learner Pitfalls & Troubleshooting** section made of errors that were actually hit while building this project, each with its root cause and fix.

---

## **How to Use This Book**

* **Follow the chapters in order.** Each chapter assumes the files and tools from the one before it.
* **Type the commands, read the explanations.** Every code block says which file it belongs to. Every concept is explained the first time it appears.
* **Do not skip the pitfalls.** They are the most valuable part. Most of the real learning in this project came from things breaking.
* **Look for the status tags.** Not everything in the project is at the same level of completion. See the next section.
* **Replace the placeholder path.** Every absolute path uses `F:/DaTech/Production/f1_data_project` as the **project root**. Swap in the absolute path of your own clone (for example `/home/you/f1_data_project` on Linux or macOS).

---

## **Project Status: What Is Built and What Is Not**

This book separates **what the build logs prove** from **what is only scaffolded or planned**, so you always know how much to trust each part.

| Tag | Meaning |
|---|---|
| **[VERIFIED]** | Code was run and the build logs show it working. |
| **[CODE FINAL, RUN NOT CONFIRMED]** | Code was reviewed and finalised, but the logs do not show a successful end-to-end run. |
| **[SCAFFOLDED]** | The file or folder exists in the project skeleton, but no working implementation appears in the build logs. |
| **[ADDED FOR THIS BOOK]** | Not in the build logs. Added to close a gap, and explained where it appears. |

| Layer | Status |
|---|---|
| Project skeleton, Git workflow | **[VERIFIED]** |
| Airflow in Docker, hook, pool, universal extractor | **[VERIFIED]** |
| Extraction hardening (chunking, backoff, sanitiser, Parquet) | **[VERIFIED]** |
| dbt + DuckDB: sources, 18 staging models, marts, tests | **[VERIFIED]** (a first clean run of 33 models, later 34 with `fct_tire_strategy`; all data tests passing at the last recorded run) |
| Telemetry export to a partitioned Parquet lake | **[VERIFIED]** (a 43 minute export) |
| Harlequin, DBeaver, Power BI over ODBC (16 tables) | **[VERIFIED]** |
| dbt triggered from inside Airflow (`run_dbt_telemetry`) | **[CODE FINAL, RUN NOT CONFIRMED]** |
| OpenTofu: `tofu init` and `tofu plan` for a GCS bucket | **[VERIFIED]** |
| OpenTofu: `tofu apply`, BigQuery datasets | **[SCAFFOLDED]** |
| Cosmos DAG, `gcs_loader.py`, `deploy_to_composer.sh`, CI/CD workflow, Cloud Composer | **[SCAFFOLDED]** |

> **Honest note on the title.** The warehouse that actually runs today is **local** (DuckDB). The cloud half of the architecture is designed and scaffolded, and Chapter 10 shows the one cloud step that was executed. Treat the cloud chapters as the **launch pad for your own migration**, not as a finished deployment.

---

## **The Architecture at a Glance**

```
  OpenF1 REST API (18 endpoints)
          |
          |  HTTP + JSON      (rate limits, 422/429/404 handling)
          v
  Apache Airflow (Docker)  ----  Hook -> Universal Extractor -> Dynamic Task Mapping
          |
          |  Parquet files (snappy compressed)
          v
  Local data lake   airflow/data/raw/<endpoint>_<session>_extracted.parquet
          |
          |  DuckDB reads Parquet directly  (the "Load" in ELT)
          v
  dbt + DuckDB      sources -> staging (18 views) -> marts (dim_ + fct_ tables)
          |                                   \
          |                                    \--> fct_telemetry -> partitioned Parquet export
          v
  f1_local_warehouse.duckdb
          |
          +--> Harlequin / DBeaver / SQLTools   (explore with SQL)
          +--> Power BI via DuckDB ODBC         (dashboards)
          +--> OpenTofu -> GCS bucket           (cloud launch pad)
```

## **The Tech Stack and Why Each Piece Exists**

| Tool | Role in this project | Why it was chosen |
|---|---|---|
| **OpenF1 API** | Data source | Free, rich: telemetry, laps, weather, radio, standings |
| **Apache Airflow** | Orchestration | Schedules, retries, parallelises and monitors extraction |
| **Docker / Docker Compose** | Runs Airflow locally | Same environment locally and in the cloud |
| **Python + Parquet + PyArrow** | Extraction and raw storage | Columnar, compressed, typed storage |
| **dbt** | Transformation | Modular SQL, tests, lineage, documentation |
| **DuckDB** | Warehouse engine | Fast analytics on a laptop, reads Parquet natively |
| **Power BI** | Visualisation | Dashboards over a star schema |
| **OpenTofu (open-source Terraform)** | Infrastructure as Code | Reproducible cloud resources |
| **Git + GitHub** | Version control, workflow | Branch, review, merge |

## **Chapter Map**

1. Project Set Up and Initialization
2. Local Orchestration and API Extraction (Airflow and Docker)
3. Hardening the Extraction Pipeline
4. Data Modelling Concepts: ELT, Layers, Joins and the Star Schema
5. Data Transformation I: dbt, DuckDB and the Staging Layer
6. Data Transformation II: Building the Analytical Marts
7. Taming High-Volume Telemetry
8. Closing the Loop: Running dbt From Airflow
9. Serving the Data: SQL Tools and Power BI
10. Infrastructure as Code (OpenTofu and GCP)
11. Professional Workflow: Git and CI/CD
12. Operations and Troubleshooting Playbook

Appendices: **A** Final directory tree, **B** the 18 OpenF1 endpoints, **C** glossary.

---

## **Chapter 1: Project Set Up and Initialization**

### **Chapter Overview**

Before any data moves, a data engineering project needs a **home**: a Git repository, an isolated Python environment, and a folder layout that keeps orchestration, transformation and infrastructure code organised. In this chapter you will install and verify your tools, create the repository, and generate the full project skeleton with one script.

**Concepts introduced in this chapter**

* **Monorepo:** one repository that holds *all* the code for a project (here: Airflow, dbt and OpenTofu). It keeps the pieces versioned together, so a change to a dbt model and the DAG that triggers it ship in the same commit.
* **Virtual environment (venv):** a private copy of Python and its packages for this project only, so library versions never clash with other projects on your machine.
* **Git Bash:** a Linux-style terminal for Windows. Every command in this book is written for Git Bash. On macOS or Linux, use your normal terminal.

### **Prerequisites**

* **Operating System:** Windows (with WSL2), macOS, or Linux.
* **Software:** Docker Desktop, Python 3.9 to 3.12 (3.9 to 3.11 is the most stable range for Airflow and dbt), Git, the Google Cloud CLI, and OpenTofu.
* **GitHub Account:** to host the project repository.
* **Editor:** Visual Studio Code.

### **Step by Step Procedures**

#### **Task 1: Verify Your Tools**

Run this single command in your terminal. It prints the version of every required tool, one after another.

```bash
echo "--- Checking Prerequisites ---" && \
echo "1. Git:" && git --version && echo "" && \
echo "2. Python:" && python --version && echo "" && \
echo "3. Docker:" && docker --version && docker compose version && echo "" && \
echo "4. Google Cloud CLI:" && gcloud --version | head -n 1 && echo "" && \
echo "5. OpenTofu:" && tofu --version
```

**What to look for in the output**

* **Git:** any version (for example `git version 2.34.1`).
* **Python:** ideally 3.9.x to 3.11.x. On macOS or Linux you may need `python3 --version`.
* **Docker:** versions for both `docker` and `docker compose`.
* **Google Cloud CLI:** the first line prints the Google Cloud SDK version.
* **OpenTofu:** a version such as `OpenTofu v1.6.0`.

Once all five tools answer, your machine is ready.

#### **Task 2: Create the Repository and Clone It**

The skeleton script in Task 3 builds the folders, but first you need a Git repository.

**Step 1: Create the empty repo on GitHub**

1. Sign in to GitHub and click **New** to create a repository.
2. Name it **f1_data_project**.
3. **Important:** tick **Add a README file**. A repository with at least one commit is easier to clone and branch.
4. For the **.gitignore** template choose **Python**.
5. Click **Create repository**.

**Step 2: Clone it to your machine**

1. On the repository page click **Code** and copy the **HTTPS** URL.
2. Open **Git Bash**, then move to the folder where you keep projects.

```bash
cd /f/DaTech/Production
git clone https://github.com/<your-username>/f1_data_project.git
cd f1_data_project
code .
```

`code .` opens the current folder in VS Code (the dot means "this directory").

#### **Task 3: Create the Virtual Environment**

1. From the project root, create and activate the environment:

```bash
python -m venv venv
source venv/Scripts/activate     # Git Bash on Windows
# source venv/bin/activate       # macOS / Linux
```

You should now see **(venv)** at the start of your prompt.

2. Point VS Code at it. Press **Ctrl + Shift + P**, type **Python: Select Interpreter**, and choose the entry that mentions **venv**.

> **Why a local venv if Airflow runs in Docker?** Airflow will run *inside a container* with its own Python. The local venv is for **editor support only**: it lets VS Code understand `import airflow` so you get autocomplete instead of red squiggly lines. It is also where you will install **dbt** in Chapter 5.

#### **Task 4: Generate the Project Skeleton**

From the project root, run the script below. It creates every folder and blank file used in the rest of the book.

```bash
# 1. Root-level files
touch .gitignore Makefile README.md

# 2. CI/CD workflow  [SCAFFOLDED]
mkdir -p .github/workflows
touch .github/workflows/deploy_to_composer.yml

# 3. Airflow
mkdir -p airflow/dags/modules airflow/logs airflow/plugins/hooks airflow/data/raw
touch airflow/docker-compose.yaml airflow/requirements.txt
touch airflow/plugins/__init__.py airflow/plugins/hooks/__init__.py
touch airflow/plugins/hooks/openf1_hook.py
touch airflow/dags/.airflowignore airflow/dags/extract_openf1_dag.py
touch airflow/dags/dbt_cosmos_dag.py                      # [SCAFFOLDED]
touch airflow/dags/modules/universal_extractor.py
touch airflow/dags/modules/event_extractor.py
touch airflow/dags/modules/gcs_loader.py                  # [SCAFFOLDED]

# 4. dbt project (filled in during Chapter 5)
mkdir -p dbt_f1_transformations/macros
mkdir -p dbt_f1_transformations/models/staging
mkdir -p dbt_f1_transformations/models/intermediate
mkdir -p dbt_f1_transformations/models/marts/core
mkdir -p dbt_f1_transformations/models/marts/analytics

# 5. OpenTofu
mkdir -p opentofu
touch opentofu/main.tf opentofu/provider.tf opentofu/variables.tf opentofu/terraform.tfvars

# 6. Helper scripts
mkdir -p scripts
touch scripts/deploy_to_composer.sh                       # [SCAFFOLDED]
chmod +x scripts/deploy_to_composer.sh

echo "F1 Data Project directory structure created successfully!"
```

The full resulting tree, with a note on what goes where, is in **Appendix A**.

**Edit your `.gitignore` now.** Raw Parquet files, the DuckDB warehouse and dbt build artefacts are large and re-creatable. Keep them out of Git:

```gitignore
venv/
.venv/
airflow/logs/
airflow/data/
output/
*.duckdb
*.duckdb.wal
.duckdb_tmp/
duckdb_tmp/
dbt_f1_transformations/target/
dbt_f1_transformations/dbt_packages/
dbt_f1_transformations/logs/
opentofu/.terraform/
*.tfstate
*.tfstate.backup
.env
```

#### **Task 5 (Optional): Install Airflow Locally for Editor Support**

If you want VS Code to stop underlining `airflow` imports, install the library into the **local venv**:

```bash
pip install apache-airflow==2.8.1
```

**Know what this does and does not do.** It is **cosmetic**: it only teaches your editor what `import airflow` means. Airflow itself runs in Docker (Chapter 2). The build logs recommended this step but do **not** show it completing, and Airflow does not officially support running natively on Windows, so if the install fails, skip it. Your pipeline is unaffected.

### **Learner Pitfalls & Troubleshooting**

* **The Problem:** `docker: command not found` in Git Bash right after installing Docker Desktop.
* **Why it Happens:** terminal sessions read the system `PATH` once, when they start. A tool installed afterwards is invisible to sessions that were already open.
* **The Solution:** fully close and reopen VS Code (and any Git Bash windows), then open a fresh terminal. If Docker is installed and running but still invisible, confirm Docker Desktop is started and that you are in Git Bash, not PowerShell.

* **The Problem:** a command that works in one terminal fails in another with syntax errors (for example the `&&` and `\` combined check command).
* **Why it Happens:** Windows PowerShell and Git Bash are different shells with different syntax. The commands in this book are Bash.
* **The Solution:** run them in **Git Bash** (the prompt shows `MINGW64`), not in a prompt that starts with `PS`.

* **The Problem:** `tofu` is not recognised after installing OpenTofu with a package manager.
* **Why it Happens:** the package ID for OpenTofu differs from the command name, so guessing the ID fails.
* **The Solution:** follow the install instructions on the official OpenTofu website for your operating system, then reopen the terminal and run `tofu --version`.

---

## **Chapter 2: Local Orchestration and API Extraction (Airflow and Docker)**

### **Chapter Overview**

In this chapter you will build the **heart of the pipeline**: a local **Apache Airflow** environment running in **Docker** that extracts **18 data endpoints** from the **OpenF1 API**. OpenF1 exposes telemetry, lap times, weather and team radio, but it also imposes real-world constraints: rate limits, very large payloads, and endpoints that sometimes go offline. Handling those constraints is what separates a script from a pipeline.

**Concepts introduced in this chapter**

* **Airflow:** a workflow orchestrator. You describe work as a **DAG** (Directed Acyclic Graph): tasks plus the order they must run in. Airflow schedules them, retries failures and shows everything in a web UI.
* **Scheduler, Webserver, Metadata database:** the three moving parts of Airflow. The **Scheduler** decides what runs and when, the **Webserver** is the UI you click on, and the **database** (PostgreSQL here) remembers the state of every task.
* **TaskFlow API:** the modern way to write DAGs. You decorate plain Python functions with `@task`, and Airflow turns them into tasks and wires data between them.
* **Hook:** one reusable class that owns the connection to an external system, so you never scatter `requests.get(...)` across your code.
* **Pool:** a named set of **slots** that limits how many tasks may run at the same time. This is how you stay under an API rate limit.
* **Docker, image, container, volume, Compose:** an **image** is a packaged application, a **container** is a running copy of it, a **volume** shares a folder between your machine and a container, and **Docker Compose** starts several containers from one blueprint file.

### **Prerequisites**

* Chapter 1 complete: the repository, the skeleton from Task 4, and Docker Desktop running.
* Port **8080** free on your machine.

### **Step by Step Procedures**

#### **Task 1: Configure the Airflow Environment**

We run Airflow inside Docker so that your laptop looks like the cloud environment you will deploy to later. Three files do the work: **requirements.txt**, **docker-compose.yaml**, and the DAG.

**Step 1: Declare the dependencies**

Open **airflow/requirements.txt** and add:

```text
apache-airflow-providers-google==10.12.0
requests==2.31.0
```

> **Correction to earlier drafts.** The original file also pinned `pandas==2.1.4`. Pandas 2.1 requires Python 3.9 or newer, while the `apache/airflow:2.8.1` image runs **Python 3.8** (you can see `python3.8` in its log paths). The pandas pin was never exercised, because the compose file only **mounts** this file into the container; nothing runs `pip install` on it. The extractor in this book does not use pandas, so it was removed.

**Step 2: Configure local Airflow (Docker Compose)**

Open **airflow/docker-compose.yaml** and paste the following. It is a streamlined version of the official Apache Airflow Compose setup, trimmed for a laptop: a **LocalExecutor**, one **PostgreSQL** database, an **init** job that creates the database tables and the login user, a **webserver** and a **scheduler**.

```yaml
x-airflow-common:
  &airflow-common
  image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.8.1}
  environment:
    &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    - ./requirements.txt:/opt/airflow/requirements.txt
    - ./data:/opt/airflow/data
  user: "${AIRFLOW_UID:-50000}:0"

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5

  airflow-init:
    <<: *airflow-common
    command: version
    environment:
      <<: *airflow-common-env
      _AIRFLOW_DB_UPGRADE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'
      _AIRFLOW_WWW_USER_USERNAME: airflow
      _AIRFLOW_WWW_USER_PASSWORD: airflow
    depends_on:
      postgres:
        condition: service_healthy

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    depends_on:
      airflow-init:
        condition: service_completed_successfully
```

**Read the file like an engineer**

* **`x-airflow-common` and `<<: *airflow-common`:** a YAML *anchor* and *merge*. Settings are defined once and reused by every Airflow service, so the webserver and scheduler cannot drift apart.
* **`volumes`:** the **file bridge**. `./dags:/opt/airflow/dags` makes your local `dags` folder appear *inside* the container. Edit a file in VS Code and the container sees it instantly, with no rebuild. `./data:/opt/airflow/data` is where Parquet files will land (Chapter 3).
* **`depends_on` with `condition`:** enforces boot order. Postgres must be **healthy**, then `airflow-init` must **complete successfully**, and only then do the webserver and scheduler start.
* **No `version:` line.** Newer Docker Compose prints a warning that the `version` attribute is obsolete, so it is omitted here.

**Step 3: Write the DAG skeleton**

Before adding real API logic, prove that orchestration works using **dummy tasks**. Open **airflow/dags/extract_openf1_dag.py** and paste:

```python
from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="extract_openf1_data",
    default_args=default_args,
    description="Extracts 18 endpoints from the OpenF1 API",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["openf1", "extraction"],
)
def extract_openf1_dag():

    @task
    def start_extraction():
        logging.info("Starting OpenF1 API extraction pipeline...")
        return "Started"

    @task
    def extract_placeholder(endpoint: str):
        logging.info(f"Extracting /{endpoint}")

    with TaskGroup("event_and_competitor_data") as event_group:
        for ep in ["meetings", "sessions", "drivers"]:
            extract_placeholder.override(task_id=f"extract_{ep}")(ep)

    with TaskGroup("telemetry_and_performance") as telemetry_group:
        for ep in ["car_data", "location", "laps", "pit"]:
            extract_placeholder.override(task_id=f"extract_{ep}")(ep)

    @task
    def verify_and_load():
        logging.info("All data extracted. Ready for the next stage.")
        return "Finished"

    start = start_extraction()
    end = verify_and_load()
    start >> event_group >> telemetry_group >> end

dag_instance = extract_openf1_dag()
```

**What this teaches**

* **`@dag` and `@task`** turn ordinary Python into a DAG.
* **`TaskGroup`** folds related tasks into a collapsible box in the UI.
* **`.override(task_id=...)`** lets you reuse **one** task definition many times with different names and inputs. Remember it: it is the seed of the *universal extractor* later in this chapter.
* **`start >> event_group >> telemetry_group >> end`** is dependency notation: "run left to right".

**Step 4: Boot it and test**

```bash
cd airflow
docker compose up -d
```

The first run downloads the PostgreSQL and Airflow images and can take **2 to 5 minutes**. When your prompt returns:

1. Open **http://localhost:8080**.
2. Log in with **Username `airflow`** and **Password `airflow`**.
3. Find **extract_openf1_data**, **unpause** it (the toggle on the left), then click **Play** and **Trigger DAG**.

When every task square turns **green**, you have a working orchestrator.

#### **Task 2: Build the Hook and the First Real Extraction**

Now replace the dummy logic with real API calls, in three moves: the **Hook** (the connection), an **extractor** (the logic), and the **DAG wiring**.

**Step 1: Build the OpenF1 Hook**

A Hook centralises *how* you talk to the API: base URL, headers, error handling. Adding authentication later means changing **one file**. Open **airflow/plugins/hooks/openf1_hook.py** and paste:

```python
import logging

import requests
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook


class OpenF1Hook(BaseHook):
    """
    Interact with the OpenF1 API.
    Base URL: https://api.openf1.org/v1
    """

    def __init__(self, conn_id="openf1_default"):
        super().__init__()
        self.conn_id = conn_id
        self.base_url = "https://api.openf1.org/v1"

    def get_endpoint(self, endpoint: str, params: dict = None) -> list:
        """Calls an OpenF1 endpoint and returns the decoded JSON payload."""
        url = f"{self.base_url}{endpoint}"

        # 1. Identify ourselves so the API does not mistake us for a spam bot
        headers = {
            "User-Agent": "F1DataProject/1.0 (Airflow Data Pipeline)",
            "Accept": "application/json",
        }

        # If you later sponsor OpenF1 and receive a token, add it here:
        # headers["Authorization"] = "Bearer YOUR_ACCESS_TOKEN"

        logging.info(f"Making request to: {url} with params: {params}")
        response = requests.get(url, params=params, headers=headers)

        # 2. Live-session lockout: skip the task instead of failing the run
        if response.status_code == 401:
            logging.warning(
                "401 Unauthorized: OpenF1 restricts free access while a live "
                "F1 session is active."
            )
            raise AirflowSkipException(
                "Skipping task: OpenF1 API is locked down (401). "
                "Try again after the live session finishes."
            )

        # 3. Any other HTTP error (404, 422, 429, 500...) fails the task
        response.raise_for_status()

        data = response.json()
        logging.info(f"Successfully retrieved {len(data)} records from {endpoint}")
        return data
```

**Why it is written this way**

* **`User-Agent` header:** many public APIs reject the default Python `requests` signature.
* **`AirflowSkipException` on 401:** a temporary third-party lockout should not turn your dashboard red. The task shows as **skipped** (pink) and you simply re-run later.
* **`raise_for_status()`:** every other error becomes a real failure that Airflow can retry. Chapter 3 teaches the extractor to *inspect* those failures (404, 422, 429) and react.

**Step 2: Write your first extractor**

Open **airflow/dags/modules/event_extractor.py** and paste. It calls `/meetings` for 2023 and saves the JSON into the `logs/` folder, which is mounted to your machine so you can see the result immediately.

```python
import json
import logging

from hooks.openf1_hook import OpenF1Hook


def extract_and_save_meetings(**kwargs):
    """Pulls meeting (race weekend) data from OpenF1 and saves it as JSON."""
    hook = OpenF1Hook()

    records = hook.get_endpoint("/meetings", params={"year": 2023})

    output_path = "/opt/airflow/logs/meetings_2023.json"
    with open(output_path, "w") as f:
        json.dump(records, f, indent=4)

    logging.info(f"Saved {len(records)} meetings to {output_path}")
    return output_path
```

> **Why JSON first, Parquet later?** This is how the project really evolved. JSON is human-readable, so it is the best way to *see* what the API returns while learning. Chapter 3 upgrades storage to columnar **Parquet**. Earlier drafts of this book mixed the two (writing JSON text into a file named `.parquet`), which cannot work, so the formats are now kept separate.

**Step 3: Wire it into the DAG, with a lazy import**

In **extract_openf1_dag.py**, replace the `extract_placeholder` task with a task that imports the extractor **inside the function**:

```python
    @task
    def extract_meetings(**kwargs):
        # Import INSIDE the task so only the Worker runs it, never the Scheduler
        from modules.event_extractor import extract_and_save_meetings
        return extract_and_save_meetings(**kwargs)
```

**Why the import must be inside the task:** the Scheduler re-reads every file in `dags/` roughly every 30 seconds just to learn which DAGs exist. Code at the top level of a DAG file runs **during that read**. If your extractor is imported or called at the top level, the Scheduler itself will hit the API, and any error (such as a 401) will break DAG parsing. See the pitfall below.

**Step 4: Test the live extraction**

Because `./dags` is mounted, you do **not** need to restart Docker. Trigger the DAG again, wait for **extract_meetings** to turn green, then look in **airflow/logs/** on your machine for **meetings_2023.json**.


#### **Task 3: Throttle the API with an Airflow Pool**

Extracting one endpoint works. Extracting 18 at once exposes the first real-world constraint. By default Airflow runs every ready task **at the same time**. Eighteen simultaneous requests trigger **HTTP 429 Too Many Requests** and the API blocks you.

Think of a **Pool** as the cashier lanes at a grocery store. If the pool has **2 slots**, only **2 tasks** may be running against the API at once. The rest wait in the **queued** state. When one finishes it gives its slot back and the next task in line starts.

**Step 1: Create the pool in the UI**

1. In the Airflow UI open **Admin > Pools**.
2. Click **+** to add a record.
3. Name it exactly **openf1_api_pool** and give it **2 slots**. Save.

*Alternative from the terminal (from the `airflow/` folder):*

```bash
docker compose exec airflow-scheduler airflow pools set openf1_api_pool 2 "OpenF1 API throttle"
```

**Step 2: Remember the rule**

The name in Python must match the name in the UI **character for character**. If the pool does not exist, tasks sit in **queued** forever with no error message.

#### **Task 4: Build the Universal Extractor**

Instead of 18 scripts, write **one** extractor and reuse it with `.override()`.

**Step 1: Create the module.** Open **airflow/dags/modules/universal_extractor.py**:

```python
import json
import logging

from hooks.openf1_hook import OpenF1Hook


def extract_and_save_endpoint(endpoint: str, api_params: dict = None, **kwargs):
    """Pulls any OpenF1 endpoint and saves it as JSON in the logs folder."""
    hook = OpenF1Hook()

    if not api_params:
        api_params = {"year": 2023}

    logging.info(f"Extracting data from /{endpoint} with params {api_params}...")
    records = hook.get_endpoint(f"/{endpoint}", params=api_params)

    output_path = f"/opt/airflow/logs/{endpoint}_extracted.json"
    with open(output_path, "w") as f:
        json.dump(records, f, indent=4)

    logging.info(f"Success! Saved {len(records)} records to {output_path}")
    return output_path
```

**Step 2: Replace the DAG.** Replace **extract_openf1_dag.py** with the version below. The pool is set in `default_args`, so **every** task obeys it. Session keys here are hard-coded on purpose: it keeps this stage simple, and Chapter 3 removes them.

```python
from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "pool": "openf1_api_pool",  # at most 2 tasks talk to the API at once
}

SESSION = {"session_key": 9158}

EVENT_ENDPOINTS = {
    "meetings": None,
    "sessions": None,
    "drivers": SESSION,
    "championship_drivers": {"session_key": "latest"},  # beta: no 2023 history
    "championship_teams": {"session_key": "latest"},
}
PERFORMANCE_ENDPOINTS = {
    "car_data": {**SESSION, "driver_number": 1},   # one driver: avoids HTTP 422
    "location": {**SESSION, "driver_number": 1},
    "laps": SESSION,
    "pit": SESSION,
    "stints": SESSION,
    "position": SESSION,
    "intervals": {"session_key": "latest"},
    "overtakes": {"session_key": "latest"},
}
RESULT_ENDPOINTS = {
    "starting_grid": {"session_key": 7783},
    "session_result": SESSION,
    "race_control": SESSION,
    "team_radio": SESSION,
    "weather": SESSION,
}


@dag(
    dag_id="extract_openf1_data",
    default_args=default_args,
    description="Extracts all 18 endpoints with a Universal Extractor",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["openf1", "extraction"],
)
def extract_openf1_dag():

    @task
    def start_extraction():
        logging.info("Starting full OpenF1 extraction...")
        return "Started"

    @task
    def extract_endpoint_data(endpoint: str, api_params: dict = None):
        from modules.universal_extractor import extract_and_save_endpoint
        return extract_and_save_endpoint(endpoint=endpoint, api_params=api_params)

    def build_group(name, endpoints):
        with TaskGroup(name) as group:
            for ep, params in endpoints.items():
                extract_endpoint_data.override(task_id=f"extract_{ep}")(ep, api_params=params)
        return group

    event_group = build_group("event_and_competitor_data", EVENT_ENDPOINTS)
    telemetry_group = build_group("telemetry_and_performance", PERFORMANCE_ENDPOINTS)
    results_group = build_group("race_results_and_events", RESULT_ENDPOINTS)

    @task
    def verify_and_load():
        logging.info("All 18 endpoints extracted. Ready for the next stage.")
        return "Finished"

    start_extraction() >> event_group >> [telemetry_group, results_group] >> verify_and_load()


dag_instance = extract_openf1_dag()
```

Trigger the DAG. Two tasks run at a time, the rest wait, and eighteen JSON files appear in **airflow/logs/**.

#### **Task 5: See What Airflow Is Doing**

Airflow can feel like a black box. Two features open it up.

**1. The big picture: watch the colours.** In the **Grid** or **Graph** view, switch on **Auto refresh** (top right). Then:

* **Lime green:** running.
* **Dark green:** success.
* **Red:** failed.
* **Gold/yellow:** failed and waiting to retry.
* **Pink:** skipped (for example by the 401 handler).
* **Orange:** upstream failed, so this task never ran.
* **Grey:** queued or scheduled.

When **verify_and_load** turns dark green, the run is finished.

**2. Deep visibility: read the logs.** Colours say *whether* a task is running, not *what it is doing*. Click a task square, then the **Logs** tab. You will see your own `logging.info(...)` messages, for example:

```text
INFO - Extracting data from /sessions with params {'year': 2023}...
INFO - Success! Saved 142 records to /opt/airflow/logs/sessions_extracted.json
```

The logs are your source of truth. A slow task shows whether it is waiting on the API; a failed task prints its exact Python error at the bottom.

#### **Task 6: Understand Why We Used Docker**

Look at a task log: it says it ran on a host such as `d844f45e111b`. That is a **Docker container ID**, not an Airflow concept. The task executed inside an isolated Linux container, yet its output appeared on your Windows drive. That bridge is the point of Docker.

**Three reasons to Dockerise Airflow**

* **It works on every machine.** Airflow needs a database, specific library versions and more. Containers bundle all of that so nothing conflicts with your computer.
* **Separation of concerns.** The webserver, scheduler and database are separate containers. If the webserver crashes, the database is safe.
* **Cloud readiness.** Managed services such as **Google Cloud Composer** and **Amazon MWAA** run Airflow the same way, so your code moves with little change.

**Looking inside the containers**

* **Visual way:** Docker Desktop > **Containers**, expand the project, click a container, then use the **Logs** or **Exec** tabs.
* **Terminal way:**

```bash
docker ps                          # list running containers and their IDs
docker logs <CONTAINER_ID>         # raw container logs
docker exec -it <CONTAINER_ID> bash
```

On Git Bash for Windows, `docker exec -it <id> /bin/bash` can fail because Git Bash rewrites paths starting with `/`. Use `//bin/bash` instead, or prefix the command with `MSYS_NO_PATHCONV=1`. Type `exit` to leave the container.

You rarely need this just to read task logs. The Airflow UI already collects them.

#### **Shutting Down and Starting Up**

```bash
cd airflow
docker compose down      # stop everything (data in Postgres is kept in the container volume)
docker compose up -d     # start again
```

Re-activate your venv in a new terminal with `source venv/Scripts/activate` before running Python or dbt commands.

### **Learner Pitfalls & Troubleshooting**

* **The Problem:** `docker compose up -d` fails with `failed to extract layer ... input/output error` while pulling the Airflow image.
* **Why it Happens:** the large image download was corrupted or interrupted in Docker's Windows (WSL2) storage layer.
* **The Solution:** retry `docker compose pull`. In the build log, a retry completed both images, after which `docker compose up -d` worked.

* **The Problem:** the DAG shows **Broken DAG** in the UI.
* **Why it Happens:** Python only treats a folder as an importable package if it contains `__init__.py`. Without these files Airflow cannot import `hooks.openf1_hook`.
* **The Solution:** `touch airflow/plugins/__init__.py airflow/plugins/hooks/__init__.py` from the project root, then wait 10 to 20 seconds for the scheduler to rescan. (Chapter 1's skeleton script already creates them.)

* **The Problem:** `AttributeError: 'NoneType' object has no attribute 'params'` when clicking Play, or an "Ooops, something bad has happened" page.
* **Why it Happens:** the webserver could not load the DAG because the scheduler rejected the file. The UI hides stack traces on purpose.
* **The Solution:** ask Airflow directly: `docker compose exec airflow-scheduler airflow dags list-import-errors`. Fix what it reports. `No data found` means the file parses cleanly. A simple race (clicking Play before the scheduler finished parsing) is cured by a hard refresh (**Ctrl + F5**) and a 30 second wait.

* **The Problem:** the DAG would not load because the extractor ran and hit a **401 Unauthorized**.
* **Why it Happens:** the extractor was called at the **top level** of the DAG file, so the **Scheduler** executed it while merely *reading* the file. A live F1 session was active, so OpenF1 returned 401.
* **The Solution:** import and call extractors **inside** `@task` functions (as in Task 2, Step 3), and handle 401 with `AirflowSkipException` in the Hook.

* **The Problem:** tasks (even `start_extraction`) sit in **queued** forever.
* **Why it Happens:** `default_args` assigns every task to `openf1_api_pool`, but the pool does not exist in the UI or the name has a typo or a trailing space.
* **The Solution:** create the pool (Task 3), then in the Grid view select the stuck run and click **Clear** (the broom icon), or fail the run and trigger a new one.

* **The Problem:** only `meetings_2023.json` appears in `logs/`.
* **Why it Happens:** only the `meetings` task had been wired to real code; the rest were still dummies.
* **The Solution:** use the universal extractor (Task 4) so all 18 tasks do real work.

* **The Problem:** `422 Client Error: Unprocessable Entity` on `/car_data` or `/location`.
* **Why it Happens:** asking for a whole session of high-frequency telemetry is too large for the server to process.
* **The Solution:** filter the request (`session_key` plus `driver_number`) as the v1 DAG does. Chapter 3 replaces the manual filter with automatic chunking.

* **The Problem:** extraction returns 404 for `championship_drivers`, `championship_teams`, `intervals` or `overtakes` with old session keys.
* **Why it Happens:** these endpoints are in beta and lack older historical data.
* **The Solution:** use `session_key: "latest"` for them, or target recent sessions.

---


## **Chapter 3: Hardening the Extraction Pipeline**

### **Chapter Overview**

The v1 pipeline from Chapter 2 works, but only for **one hard-coded session** and it writes JSON. A production pipeline must pull **any session or the full history**, survive API failures, and store data in an analytics-friendly format. In this chapter you will upgrade the extractor and the DAG with five production patterns. **[VERIFIED]**

**Concepts introduced in this chapter**

* **Parquet:** a **columnar** file format. Instead of storing row after row, it stores each column together, with types and **snappy compression**. Analytical queries ("average lap time") read only the columns they need. It is typically many times smaller and faster than JSON, and DuckDB reads it directly.
* **Dynamic Task Mapping (`.expand()`):** Airflow creates **one task instance per item in a list that is only known at runtime**. If the list has 25 session keys, you get 25 parallel tasks without writing 25 tasks.
* **Exponential backoff:** after a rate-limit error, wait a little, then longer on each retry (30 s, then 60 s) instead of hammering the server.
* **Idempotency:** running the same job twice gives the same result. Files are named by endpoint **and** session key, so re-running overwrites rather than duplicates.
* **Fan-out guard:** a cap on how many mapped tasks a single run is allowed to create.

### **Prerequisites**

* Chapter 2 complete and Airflow running.
* The pool **openf1_api_pool** exists.

### **Step by Step Procedures**

#### **Task 1: Write the Production Extractor**

Replace the contents of **airflow/dags/modules/universal_extractor.py** with the version below. Read the code in the order the explanation follows it.

```python
import os
import time
import logging
import json
import pyarrow as pa
import pyarrow.parquet as pq
from collections import defaultdict
from datetime import datetime, timedelta
from hooks.openf1_hook import OpenF1Hook

def sanitize_mixed_and_nested_columns(records):
    """
    Dynamically detects mixed-type columns and nested arrays/dicts.
    Casts them to strings/JSON to prevent PyArrow schema crashes.
    """
    col_types = defaultdict(set)
    for row in records:
        for k, v in row.items():
            if v is not None:
                col_types[k].add(type(v).__name__)
    
    # Identify mixed columns OR columns containing lists/dicts (e.g., duration arrays in Qualifying)
    problematic_cols = {
        col for col, types in col_types.items() 
        if len(types) > 1 or 'list' in types or 'dict' in types
    }
    
    for row in records:
        for col in problematic_cols:
            if col in row and row[col] is not None:
                if isinstance(row[col], (list, dict)):
                    row[col] = json.dumps(row[col]) # Safe JSON stringification for nested F1 data
                else:
                    row[col] = str(row[col])
    return records

def extract_and_save_endpoint(endpoint: str, target_session="latest", api_params: dict = None, is_global: bool = False):
    """
    Universally pulls data from any OpenF1 endpoint. 
    Handles dynamic routing, 422/429 protection, strict-typing sanitization, Parquet conversion,
    and fallback Time-Window chunking for ultra-dense data.
    """
    hook = OpenF1Hook()
    params = api_params.copy() if api_params else {}
    ultra_heavy_endpoints = ["car_data", "location"]

    if not is_global:
        params["session_key"] = target_session

    session_val = params.get("session_key", "global")
    records = []

    # ==========================================
    # RESOLVE "LATEST"
    # ==========================================
    if str(session_val).lower() == "latest" and endpoint not in ["sessions", "meetings"]:
        logging.info("Resolving 'latest' to an actual session ID...")
        try:
            latest_session_data = hook.get_endpoint('/sessions', params={'session_key': 'latest'})
            if latest_session_data and isinstance(latest_session_data, list):
                session_val = latest_session_data[-1]['session_key']
                params['session_key'] = session_val
        except Exception as e:
            logging.warning(f"Could not resolve 'latest' session: {e}")

    # ==========================================
    # ULTRA-HEAVY ENDPOINT PROTECTION & RETRIES
    # ==========================================
    if endpoint in ultra_heavy_endpoints and str(session_val).isdigit():
        logging.info(f"/{endpoint} is ultra-heavy. Sub-chunking session {session_val} by driver...")
        
        # 1. Fetch Drivers (Protected)
        drivers = None
        for attempt in range(3):
            try:
                drivers = hook.get_endpoint('/drivers', params={'session_key': session_val})
                break
            except Exception as e:
                error_msg = str(e)
                if "404" in error_msg:
                    logging.warning(f"No driver data for session {session_val}. Skipping /{endpoint}.")
                    return None
                elif "429" in error_msg and attempt < 2:
                    wait = 30 * (attempt + 1)
                    logging.warning(f"429 on /drivers for session {session_val}. Sleeping {wait}s (attempt {attempt+1}/3)...")
                    time.sleep(wait)
                else:
                    if attempt == 2 or "429" not in error_msg:
                        raise e
        
        if not drivers:
            logging.warning(f"Failed to retrieve drivers for session {session_val}. Skipping /{endpoint}.")
            return None
            
        # 2. Fetch Session Timestamps (Used if Time-Window Chunking is triggered)
        session_start, session_end = None, None
        try:
            session_details = hook.get_endpoint('/sessions', params={'session_key': session_val})
            if session_details and isinstance(session_details, list):
                session_start = session_details[0].get('date_start')
                session_end = session_details[0].get('date_end')
        except Exception as e:
            logging.warning(f"Could not retrieve session timestamps for time-window fallback: {e}")

        driver_numbers = list(set([d['driver_number'] for d in drivers if 'driver_number' in d]))
        
        # 3. Main Extraction Loop
        for driver in driver_numbers:
            chunk_params = params.copy()
            chunk_params["driver_number"] = driver
            success = False
            
            for attempt in range(3):
                try:
                    time.sleep(2) # Base pacing
                    driver_data = hook.get_endpoint(f'/{endpoint}', params=chunk_params)
                    if driver_data:
                        records.extend(driver_data)
                    success = True
                    break # Success, break out of retry loop
                except Exception as e:
                    error_msg = str(e)
                    if "404" in error_msg:
                        logging.warning(f"🚨 404 ERROR: Driver {driver} missing. Skipping.")
                        success = True # Acknowledge handled so fallback doesn't trigger
                        break 
                    elif "422" in error_msg:
                        logging.warning(f"🚨 422 ERROR: Driver {driver} payload too large. Triggering Time-Window Chunking...")
                        break # Break loop early to trigger the fallback below
                    elif "429" in error_msg and attempt < 2:
                        sleep_duration = 30 * (attempt + 1)
                        logging.warning(f"🚦 429 RATE LIMIT: Driver {driver}. Retrying in {sleep_duration}s...")
                        time.sleep(sleep_duration)
                    else:
                        if attempt == 2:
                            raise e

            # ==========================================
            # 4. TIME-WINDOW CHUNKING FALLBACK
            # ==========================================
            if not success and session_start and session_end:
                logging.info(f"Initiating 15-minute time chunks for Driver {driver} in session {session_val}...")
                
                try:
                    # OpenF1 returns standard ISO 8601 with trailing Z or timezone offsets
                    start_dt = datetime.fromisoformat(session_start.replace('Z', '+00:00'))
                    end_dt = datetime.fromisoformat(session_end.replace('Z', '+00:00'))
                    current_dt = start_dt
                    
                    while current_dt < end_dt:
                        next_dt = current_dt + timedelta(minutes=15)
                        
                        time_params = chunk_params.copy()
                        # F1 API range filters: date>= and date<
                        time_params['date>='] = current_dt.isoformat()
                        time_params['date<'] = next_dt.isoformat()
                        
                        for sub_attempt in range(3):
                            try:
                                time.sleep(1) # Pacing between time chunks
                                time_data = hook.get_endpoint(f'/{endpoint}', params=time_params)
                                if time_data:
                                    records.extend(time_data)
                                break
                            except Exception as e:
                                sub_error = str(e)
                                if "404" in sub_error:
                                    break # Chunk is just empty, move to next
                                elif "422" in sub_error:
                                    logging.error(f"FATAL 422: 15-min chunk STILL too large for Driver {driver} at {current_dt}. Skipping chunk.")
                                    break # Skip this specific 15 min chunk to avoid looping endlessly
                                elif "429" in sub_error and sub_attempt < 2:
                                    time.sleep(30 * (sub_attempt + 1))
                                else:
                                    if sub_attempt == 2:
                                        raise e
                                        
                        current_dt = next_dt
                except Exception as ex:
                    logging.error(f"Time-window fallback failed for Driver {driver}: {ex}")
            
            # POLISH ADDED: Explicit warning if fallback couldn't execute due to missing timestamps
            elif not success:
                logging.warning(f"⚠️ Driver {driver} on /{endpoint} had a 422 payload issue, but session timestamps were unavailable for time-window fallback. Skipping driver data.")

    # ==========================================
    # STANDARD EXTRACTION EXECUTION
    # ==========================================
    else:
        for attempt in range(3):
            try:
                logging.info(f"Extracting data from /{endpoint} with params {params} (Attempt {attempt+1}/3)...")
                records = hook.get_endpoint(f'/{endpoint}', params=params)
                break
            except Exception as e:
                error_msg = str(e)
                if "404" in error_msg or "422" in error_msg:
                    logging.warning(f"Endpoint /{endpoint} unavailable for session {session_val}. Skipping gracefully.")
                    return None
                elif "429" in error_msg and attempt < 2:
                    wait = 30 * (attempt + 1)  # 30s, 60s
                    logging.warning(f"429 on /{endpoint} session {session_val}. Sleeping {wait}s (attempt {attempt+1}/3)...")
                    time.sleep(wait)
                else:
                    if attempt == 2 or "429" not in error_msg:
                        raise e

    if not records:
        logging.info(f"No records found for /{endpoint} session {session_val}. Skipping save.")
        return None

    # ==========================================
    # SANITIZE & SAVE TO DISK AS PARQUET
    # ==========================================
    records = sanitize_mixed_and_nested_columns(records)

    base_path = os.getenv("OPENF1_OUTPUT_PATH", "/opt/airflow/data/raw")
    os.makedirs(base_path, exist_ok=True)
    
    output_path = f"{base_path}/{endpoint}_{session_val}_extracted.parquet"
    
    try:
        table = pa.Table.from_pylist(records)
        # POLISH ADDED: Re-enabled snappy compression to optimize storage performance
        pq.write_table(table, output_path, compression='snappy')
        logging.info(f"Success! Saved {len(records)} records to {output_path}")
        return output_path
    except Exception as e:
        logging.error(f"Failed to save Parquet file for {endpoint}: {str(e)}")
        raise e
```

**How it works, section by section**

1. **`sanitize_mixed_and_nested_columns`:** Parquet requires every column to have **one type**. OpenF1 sometimes returns a number in one row and text in another (for example gap values such as `1.2` and `"+1 LAP"`), or a list inside a column (qualifying durations). The sanitiser finds those columns and converts every value to a string (lists and dictionaries become JSON text). Without it, `pa.Table.from_pylist` crashes with a type error.
2. **Resolving `"latest"`:** `latest` is a convenience alias in the API. The extractor looks up the real session number first, so the filename (`laps_9158_extracted.parquet`) always holds a real ID.
3. **Ultra-heavy endpoint protection:** `car_data` and `location` produce several readings per second per car. The extractor therefore requests **one driver at a time**, with a 2-second pause between requests.
4. **Error handling by status code:**
   * **404:** the data does not exist for that session or driver. Skip gracefully and return `None`.
   * **422:** the request was too large. Switch to **time-window chunking**: ask for 15-minute slices (`date>=` and `date<` filters) between the session start and end.
   * **429:** rate limited. Wait 30 s, then 60 s, then give up.
   * **Anything else:** raise, so Airflow's own retries take over.
5. **Writing the file:** the output is `{endpoint}_{session}_extracted.parquet` in `/opt/airflow/data/raw` (overridable through the `OPENF1_OUTPUT_PATH` environment variable), compressed with snappy.

> **Why `/opt/airflow/data/raw`?** Compose mounts `./data` into the container (Chapter 2). Files written there appear on your machine in **airflow/data/raw/**, which is exactly where dbt will read them in Chapter 5.

#### **Task 2: Make the DAG Session-Aware**

Replace **airflow/dags/extract_openf1_dag.py** with the DAG below. It adds a **parameter** (`target_session_key`), a metadata task that decides *what* to extract, and mapped extraction tasks.

```python
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator

# Import your refactored extractor module and hook
from modules.universal_extractor import extract_and_save_endpoint
from hooks.openf1_hook import OpenF1Hook

# ==========================================
# DEFAULT ARGS
# ==========================================
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# ==========================================
# DAG DEFINITION
# ==========================================
with DAG(
    dag_id="extract_openf1_data",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        "target_session_key": Param(
            default="latest",
            type="string",
            description="Enter a session_key integer, 'latest', or 'all'."
        )
    },
    tags=["openf1", "ingestion"]
) as dag:

    start = EmptyOperator(task_id="start_extraction")

    # ==========================================
    # TASK: FETCH SESSION METADATA
    # ==========================================
    @task
    def fetch_session_metadata(**context):
        """Fetches raw session metadata from the OpenF1 API using the custom Hook."""
        hook = OpenF1Hook()
        target = context["params"]["target_session_key"]

        if str(target).lower() == "all":
            params = {}  # No filter — API returns full history
        elif str(target).lower() == "latest":
            params = {"session_key": "latest"}
        else:
            params = {"session_key": int(target) if str(target).isdigit() else target}

        logging.info(
            f"Fetching session metadata for: "
            f"{params if params else 'ALL historical sessions'}"
        )

        sessions = hook.get_endpoint("/sessions", params=params)

        if not sessions:
            logging.warning("No sessions found for the given parameter.")
            return []

        # ==========================================
        # FAN-OUT GUARD: Protect worker from massive 'all' payload
        # ==========================================
        if str(target).lower() == "all":
            # OpenF1 session keys are sequential integers; 9000+ corresponds to ~2023 onward
            filtered_sessions = [s for s in sessions if s.get("session_key", 0) >= 9000]
            logging.warning(
                f"Historical load requested. Capping {len(sessions)} total sessions "
                f"to {len(filtered_sessions)} (ID >= 9000) to prevent worker OOM and API limits."
            )
            return filtered_sessions

        return sessions

    # ==========================================
    # TASKS: FILTER SESSIONS FOR DYNAMIC MAPPING
    # ==========================================
    @task
    def get_all_keys(sessions):
        """Returns session_key for every session."""
        return [s["session_key"] for s in sessions]

    @task
    def get_race_keys(sessions):
        """Returns session_key for Race sessions only."""
        return [s["session_key"] for s in sessions if s.get("session_type") == "Race"]

    @task
    def get_quali_and_race_keys(sessions):
        """Returns session_key for Race, Qualifying, Sprint, and Sprint Qualifying sessions."""
        valid_types = {"Race", "Qualifying", "Sprint", "Sprint Qualifying"}
        return [
            s["session_key"] for s in sessions
            if s.get("session_type") in valid_types
        ]

    # ==========================================
    # TASK: EXTRACTION WRAPPER (Dynamically Mapped)
    # ==========================================
    @task(pool="openf1_api_pool")
    def extract_mapped_data(endpoint: str, target_session: str, is_global: bool = False):
        """Universal extraction task — mapped over session keys per endpoint."""
        return extract_and_save_endpoint(
            endpoint=endpoint,
            target_session=target_session,
            is_global=is_global
        )

    # ==========================================
    # WORKFLOW: RESOLVE SESSIONS
    # ==========================================

    # Call resolve tasks EXACTLY ONCE each — XComArgs are reused across groups
    raw_sessions      = fetch_session_metadata()
    all_sessions      = get_all_keys(raw_sessions)
    race_sessions     = get_race_keys(raw_sessions)
    quali_race_sessions = get_quali_and_race_keys(raw_sessions)

    # ==========================================
    # TASK GROUP 1: GLOBAL / REFERENCE DATA
    # Endpoints that don't vary by session — extracted once.
    # ==========================================
    with TaskGroup("global_and_reference") as event_group:
        global_endpoints = ["meetings", "drivers", "sessions"]
        event_results = []
        for ep in global_endpoints:
            res = extract_mapped_data.override(task_id=f"extract_{ep}").partial(
                endpoint=ep, is_global=True
            ).expand(target_session=["global"])
            event_results.append(res)

    # ==========================================
    # TASK GROUP 2: HEAVY TELEMETRY
    # Mapped to ALL sessions — highest fan-out, pool-throttled.
    # ==========================================
    with TaskGroup("telemetry_and_performance") as telemetry_group:
        telemetry_endpoints = ["car_data", "location"]
        telemetry_results = []
        for ep in telemetry_endpoints:
            res = extract_mapped_data.override(task_id=f"extract_{ep}").partial(
                endpoint=ep
            ).expand(target_session=all_sessions)
            telemetry_results.append(res)

    # ==========================================
    # TASK GROUP 3: RACE RESULTS & EVENTS
    # Each endpoint mapped only to the sessions where its data exists.
    # ==========================================
    with TaskGroup("race_results_and_events") as results_group:
        results_out = []

        # All sessions
        all_session_endpoints = [
            "weather", "team_radio", "race_control", "pit",
            "laps", "stints", "position", "session_result"
        ]
        for ep in all_session_endpoints:
            res = extract_mapped_data.override(task_id=f"extract_{ep}_all").partial(
                endpoint=ep
            ).expand(target_session=all_sessions)
            results_out.append(res)

        # Race sessions only
        race_endpoints = [
            "intervals", "overtakes", "championship_drivers", "championship_teams"
        ]
        for ep in race_endpoints:
            res = extract_mapped_data.override(task_id=f"extract_{ep}_race").partial(
                endpoint=ep
            ).expand(target_session=race_sessions)
            results_out.append(res)

        # Qualifying + Race + Sprint sessions only
        res_grid = extract_mapped_data.override(task_id="extract_starting_grid").partial(
            endpoint="starting_grid"
        ).expand(target_session=quali_race_sessions)
        results_out.append(res_grid)

    # ==========================================
    # TASK: VERIFY AND LOAD
    # Aggregates results from all groups.
    # ==========================================
    @task
    def verify_and_load(event_results_in, telemetry_results_in, results_out_in):
        """Flattens and counts successfully extracted files across all task groups."""
        flat_results = []
        for group in [event_results_in, telemetry_results_in, results_out_in]:
            for item in group:
                if isinstance(item, list):
                    flat_results.extend(item)
                else:
                    flat_results.append(item)

        saved_files = [r for r in flat_results if r is not None]

        if not saved_files:
            logging.warning(
                "Pipeline completed, but NO files were successfully extracted or saved."
            )
        else:
            logging.info(
                f"✅ Success! {len(saved_files)} Parquet files extracted and saved. "
                f"Ready for dbt."
            )

    load_task = verify_and_load(event_results, telemetry_results, results_out)

    # ==========================================
    # EXPLICIT DEPENDENCY GRAPH
    # ==========================================

    # Start -> fetch metadata -> derive filter lists
    start >> raw_sessions >> [all_sessions, race_sessions, quali_race_sessions]

    # Global extractions have no session dependency — run immediately after start
    start >> event_group

    # Each group waits only on the filter(s) it actually uses
    all_sessions >> telemetry_group
    [all_sessions, race_sessions, quali_race_sessions] >> results_group

    # All groups must complete before load aggregation
    [event_group, telemetry_group, results_group] >> load_task
```

**What changed and why**

* **`schedule_interval=None` and `Param`:** the DAG runs on demand. When you click **Trigger DAG w/ config**, the form asks for `target_session_key`:
  * a session number such as `9158`, to extract one session;
  * **`latest`**, for the newest session;
  * **`all`**, for the historical backfill.
* **`fetch_session_metadata`:** asks `/sessions` which sessions match. This list drives everything else.
* **Fan-out guard:** with `all`, the full session history would create thousands of mapped tasks. The guard keeps only `session_key >= 9000` (roughly 2023 onward). This is a project-specific cap, so change the number if you need older seasons.
* **Three filter tasks:** `get_all_keys`, `get_race_keys`, `get_quali_and_race_keys` split the session list by type. This is **smart routing**: `intervals` and `overtakes` only exist for races, and `starting_grid` only for qualifying, sprint and race sessions. Sending them elsewhere would just produce 404s.
* **`.partial(...).expand(target_session=...)`:** `partial` fixes the constant arguments (the endpoint name), `expand` supplies the list that fans out into parallel tasks.
* **`@task(pool="openf1_api_pool")`:** the pool is applied **only to the extraction tasks**, so the cheap metadata tasks are never stuck in a queue behind them.
* **Three task groups:** `global_and_reference` (data that does not vary per session, extracted once), `telemetry_and_performance` (the heavy endpoints, for all sessions) and `race_results_and_events`.

> **A note on imports.** This DAG imports `extract_and_save_endpoint` at the top of the file, unlike the lazy imports of Chapter 2. That is fine here because importing the module only *defines* functions; nothing calls the API until a task runs. The rule is "never **execute** API calls at the top level", not "never import".

#### **Task 3: Run and Verify**

1. Restart nothing: the DAG folder is mounted. Wait about 30 seconds for the scheduler to re-parse.
2. In the UI choose **Trigger DAG w/ config**, leave `latest`, and run it.
3. Watch the **Graph** view. The extraction nodes show a **mapped task** badge such as `[1]`.
4. Open **airflow/data/raw/** on your machine and confirm the Parquet files exist. Check one:

```bash
python - <<'PY'
import pyarrow.parquet as pq
t = pq.read_table("airflow/data/raw/laps_9158_extracted.parquet")   # use a file you have
print(t.schema)
print(t.num_rows, "rows")
PY
```

5. Run the DAG a second time with the same session. File names do not change: the run is **idempotent**.

For a full backfill, trigger with `all`. Expect it to take a long time, because pacing and backoff are deliberate (the long export times in Chapter 7 belong to the *export* step, not to extraction).

### **Learner Pitfalls & Troubleshooting**

* **The Problem:** `ArrowInvalid` / "could not convert" when writing Parquet.
* **Why it Happens:** a column mixed numbers and strings, or contained a list. PyArrow infers one type for the whole column from the data.
* **The Solution:** run every payload through the sanitiser before `pa.Table.from_pylist`. Better to store a column as text and clean it in dbt than to lose the whole file.

* **The Problem:** one driver's 422 or 429 makes the whole telemetry task fail.
* **Why it Happens:** a single oversized or rate-limited request was treated as fatal.
* **The Solution:** treat each status code differently (the table in Task 1): skip on 404, chunk by time on 422, back off on 429, raise otherwise.

* **The Problem:** `all` creates an enormous number of task instances and the worker runs out of memory.
* **Why it Happens:** mapping fans out over **every session in the history**, each with heavy telemetry downloads.
* **The Solution:** the fan-out guard in `fetch_session_metadata`. Scope to what you need.

* **The Problem:** a new session key fails with 404 for some endpoints but not others.
* **Why it Happens:** not every endpoint has data for every session type, or the session has not published it yet.
* **The Solution:** this is expected. The extractor returns `None` and `verify_and_load` counts only real files.

* **The Problem (confirmed in the final log of the build):** a run fails with `404` on `/drivers?year=2023` while you believe you are running the new DAG.
* **Why it Happens (inference):** the log line `universal_extractor.py:15 ... params {'year': 2023}` comes from the **old** Chapter 2 extractor, whose default was `{"year": 2023}`. The new DAG was running against a stale module file. The DAG and the module it imports must be updated **together**.
* **The Solution:** replace `universal_extractor.py` with the Task 1 version, save, wait for the scheduler to re-parse, and confirm the first log line of a task reads `Extracting data from /... (Attempt 1/3)...`.

---

## **Chapter 4: Data Modelling Concepts: ELT, Layers, Joins and the Star Schema**

### **Chapter Overview**

You now have Parquet files. Before writing any SQL, you need the mental models that organise it. This chapter has no commands to type, but every later chapter relies on it.

### **ETL versus ELT**

* **ETL (Extract, Transform, Load):** transform data *before* it reaches the warehouse. Traditional, rigid, and the transformation code is hard to inspect.
* **ELT (Extract, Load, Transform):** land the **raw** data first, then transform it **inside** the warehouse with SQL. Raw data is preserved, transformations are versioned SQL, and you can rebuild anything.

This project is **ELT**. Airflow extracts and lands Parquet (the E and the L). **dbt** transforms it inside **DuckDB** (the T). DuckDB reads the Parquet files in place, so there is no separate loading step.

### **The Three Layers**

| Layer | Folder | Purpose | Materialisation |
|---|---|---|---|
| **Sources** | `models/staging/sources.yml` | Declares where raw data lives | n/a |
| **Staging** | `models/staging/` | One model per raw table. Rename, cast types, light cleaning. **No joins.** | view |
| **Marts** | `models/marts/` | Business-ready tables. `dim_` (who/what) and `fct_` (events/measures) | table (or incremental) |

**Materialisation** means *how* dbt saves a model: a **view** stores only the query (always fresh, recomputed on read), a **table** stores the result (fast to read), and an **incremental** model only adds new rows on each run.

### **Star Schema: Dimensions and Facts**

* **Dimension tables (`dim_`)** describe entities and change slowly: `dim_drivers`, `dim_sessions`.
* **Fact tables (`fct_`)** record events and measurements and grow constantly: `fct_lap_performance`, `fct_pit_stops`, `fct_telemetry`.
* Facts carry **keys** (`session_key`, `driver_number`) that point to dimensions. Drawn out, facts sit in the middle with dimensions around them like a star.

**Why bother?** BI tools such as Power BI are built for this shape. Filtering a dashboard by driver means filtering the dimension; every fact table follows.

### **Joins You Will Use**

A join combines two tables on a matching key. Using F1 examples:

| Join | Keeps | F1 example |
|---|---|---|
| **INNER JOIN** | Only rows that match in both tables | Laps that have a matching driver |
| **LEFT JOIN** | Every row on the left, matches (or NULL) on the right | All laps, with the pit stop for that lap if there was one |
| **FULL OUTER JOIN** | Everything from both sides | Audit: which keys exist in one table but not the other |
| **ASOF JOIN** | Each left row matched with the **nearest earlier** right row by time | Each car-data reading with the most recent location (x, y, z) reading at or before it (Chapter 7) |

A plain join needs an exact match. Car-data and location readings come from separate feeds with their own timestamps, so a speed reading almost never shares an exact timestamp with a position reading. The ASOF join exists for exactly this "closest in time" problem.

### **Why dbt?**

* **Modularity:** each model is one `SELECT` in one file. `{{ ref('stg_laps') }}` links models, and dbt works out the order.
* **Lineage:** because models reference each other, dbt draws the dependency graph for you.
* **Testing:** assertions such as `not_null` and `unique` run with one command.
* **Documentation:** descriptions in YAML become a browsable website.

---


## **Chapter 5: Data Transformation I: dbt, DuckDB and the Staging Layer**

### **Chapter Overview**

Airflow has left you a folder of Parquet files. In this chapter you will install **dbt** and **DuckDB**, point dbt at the raw files with a **sources** declaration, and build the **staging layer**: **18 views**, one per endpoint, that rename columns and cast types. **[VERIFIED]** (the full project compiled and ran cleanly).

**Concepts introduced in this chapter**

* **DuckDB:** an analytical database that runs **inside your process** and stores everything in one file. It can query Parquet files directly with SQL, which is why this project needs no separate "load" step.
* **dbt (data build tool):** you write `SELECT` statements; dbt compiles them, runs them in the right order, and materialises the results.
* **Adapter (`dbt-duckdb`):** the plugin that teaches dbt how to speak to DuckDB.
* **`profiles.yml`:** tells dbt *where* the database is (outside the project, because it may hold credentials).
* **`dbt_project.yml`:** tells dbt about the project itself and sets defaults per folder.
* **Source:** a named declaration of a raw table, referenced with `{{ source('openf1_raw', 'laps') }}`. Pointing every staging model at one `sources.yml` means that moving your raw data requires editing **one file**, not 18 models.
* **Glob (`*`):** `laps_*_extracted.parquet` matches every file Airflow produced, one per session, so DuckDB reads them all as a single table.

### **Prerequisites**

* Chapters 1 to 4, with Parquet files in **airflow/data/raw/** (run the DAG with `latest` at least once; use `all` for a realistic dataset).
* Your venv active.

### **Step by Step Procedures**

#### **Task 1: Install and Connect**

**Step 1: Install the adapter** (this also installs `dbt-core`):

```bash
pip install dbt-duckdb
dbt --version
```

**Step 2: Create `profiles.yml`.** dbt looks for it in your user folder by default (`~/.dbt/profiles.yml`; on Windows `C:\Users\<you>\.dbt\profiles.yml`).

```yaml
dbt_f1_transformations:
  target: dev
  outputs:
    dev:
      type: duckdb
      # A local DuckDB file; replace with the absolute path to your project root
      path: "F:/DaTech/Production/f1_data_project/f1_local_warehouse.duckdb"
      threads: 1
      # Redirect spill-to-disk files (see Chapter 7)
      settings:
        temp_directory: "F:/DaTech/Production/f1_data_project/.duckdb_tmp"
```

> **The first word must match.** `dbt_f1_transformations` here must be identical to the `profile:` line in `dbt_project.yml`. A mismatch gives "Could not find profile named ...".

**Step 3: Create `dbt_project.yml`** inside **dbt_f1_transformations/**:

```yaml
name: 'dbt_f1_transformations'
version: '1.0.0'
profile: 'dbt_f1_transformations'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - "target"
  - "dbt_packages"

models:
  dbt_f1_transformations:
    staging:
      +materialized: view
    marts:
      +materialized: table
```

This sets the rule from Chapter 4: everything in `staging/` is a **view**, everything in `marts/` is a **table**.

**Step 4: Test the connection**

```bash
cd dbt_f1_transformations
dbt debug
```

Look for **All checks passed!** at the bottom.

#### **Task 2: Declare the Raw Sources**

Create **models/staging/sources.yml**. Replace the root path with your own absolute path. Each table points at a glob of Parquet files.

```yaml
version: 2

sources:
  - name: openf1_raw
    description: "Raw Parquet files extracted locally from the OpenF1 API via Airflow"
    tables:
      - name: drivers
        description: "Raw driver information and details"
        config:
          external_location: "F:/DaTech/Production/f1_data_project/airflow/data/raw/drivers_*_extracted.parquet"
      - name: championship_drivers
        description: "Driver championship standings and points"
        config:
          external_location: "F:/DaTech/Production/f1_data_project/airflow/data/raw/championship_drivers_*_extracted.parquet"
      - name: championship_teams
        description: "Constructor championship standings and points"
        config:
          external_location: "F:/DaTech/Production/f1_data_project/airflow/data/raw/championship_teams_*_extracted.parquet"
      - name: meetings
        description: "Grand Prix weekend metadata (locations, dates)"
        config:
          external_location: "F:/DaTech/Production/f1_data_project/airflow/data/raw/meetings_*_extracted.parquet"
      - name: sessions
        description: "Session metadata (practice, qualifying, race)"
        config:
          external_location: "F:/DaTech/Production/f1_data_project/airflow/data/raw/sessions_*_extracted.parquet"
      - name: session_result
        description: "Final classification and results per session"
        config:
          external_location: "read_parquet('F:/DaTech/Production/f1_data_project/airflow/data/raw/session_result_*_extracted.parquet', union_by_name=true)"
      - name: starting_grid
        description: "Starting grid positions per session"
        config:
          external_location: "F:/DaTech/Production/f1_data_project/airflow/data/raw/starting_grid_*_extracted.parquet"
      - name: laps
        description: "Lap-by-lap timing, sectors and speeds"
        config:
          external_location: "F:/DaTech/Production/f1_data_project/airflow/data/raw/laps_*_extracted.parquet"
      - name: stints
        description: "Tyre stints per driver"
        config:
          external_location: "F:/DaTech/Production/f1_data_project/airflow/data/raw/stints_*_extracted.parquet"
      - name: pit
        description: "Pit stop events and durations"
        config:
          external_location: "F:/DaTech/Production/f1_data_project/airflow/data/raw/pit_*_extracted.parquet"
      - name: position
        description: "Track position changes over time"
        config:
          external_location: "F:/DaTech/Production/f1_data_project/airflow/data/raw/position_*_extracted.parquet"
      - name: intervals
        description: "Gaps to the leader and the car ahead"
        config:
          external_location: "read_parquet('F:/DaTech/Production/f1_data_project/airflow/data/raw/intervals_*_extracted.parquet', union_by_name=true)"
      - name: overtakes
        description: "Overtake events"
        config:
          external_location: "F:/DaTech/Production/f1_data_project/airflow/data/raw/overtakes_*_extracted.parquet"
      - name: race_control
        description: "Flags, safety cars and incident messages"
        config:
          external_location: "read_parquet('F:/DaTech/Production/f1_data_project/airflow/data/raw/race_control_*_extracted.parquet', union_by_name=true)"
      - name: team_radio
        description: "Team radio recording metadata"
        config:
          external_location: "F:/DaTech/Production/f1_data_project/airflow/data/raw/team_radio_*_extracted.parquet"
      - name: weather
        description: "Track and air conditions over time"
        config:
          external_location: "F:/DaTech/Production/f1_data_project/airflow/data/raw/weather_*_extracted.parquet"
      - name: car_data
        description: "High-frequency telemetry: speed, rpm, gear, throttle, brake, DRS"
        config:
          external_location: "F:/DaTech/Production/f1_data_project/airflow/data/raw/car_data_*_extracted.parquet"
      - name: location
        description: "High-frequency x/y/z car positions"
        config:
          external_location: "F:/DaTech/Production/f1_data_project/airflow/data/raw/location_*_extracted.parquet"
```

**Why three tables use `read_parquet(..., union_by_name=true)`**

Each Parquet file is written separately, and DuckDB normally infers a column's type from the **first** file it reads. If `intervals` for session A stores gaps as numbers but session B contains `"+1 LAP"` strings, DuckDB hits a conversion error when it reaches session B. `union_by_name=true` tells DuckDB to **line files up by column name and reconcile their types**. In the build, this was confirmed as the fix for `intervals`. The same wrapper was recommended for `race_control` and `session_result` because they showed the same kind of cross-file drift (empty files and mixed duration shapes); that recommendation was not separately confirmed by a re-run in the logs. If you see a similar conversion error on another table, wrap that table the same way. **Chapter 9 tells the full story.**

#### **Task 3: Write Your First Staging Model**

A staging model does only three jobs: **rename** columns to clear names, **cast** them to proper types, and apply **light cleaning**. No joins. Create **models/staging/stg_laps.sql** (shown in the full list below), then run it:

```bash
dbt run --select stg_laps
```

dbt compiles the file (replacing `{{ source(...) }}` with the real read of your Parquet files) and creates a **view** in DuckDB.

#### **Task 4: The Complete Staging Layer (18 Models)**

Create each file below in **dbt_f1_transformations/models/staging/**. The pattern is identical every time: a CTE called `raw_<name>` selects from the source, and a second CTE `renamed_and_casted` renames and casts.

**`models/staging/stg_drivers.sql`**

```sql
WITH raw_drivers AS (
    SELECT * FROM {{ source('openf1_raw', 'drivers') }}
),

renamed_and_casted AS (
    SELECT
        CAST(driver_number AS INTEGER) AS driver_number,
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        
        full_name,
        name_acronym AS driver_acronym,
        team_name,
        team_colour AS team_color_hex,
        country_code,
        
        headshot_url

    FROM raw_drivers
    WHERE driver_number IS NOT NULL
)

SELECT * FROM renamed_and_casted
```

**`models/staging/stg_championship_drivers.sql`**

```sql
WITH raw_championship_drivers AS (
    SELECT * FROM {{ source('openf1_raw', 'championship_drivers') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        position_start,
        position_current,
        CAST(points_start AS DOUBLE) AS points_start,
        CAST(points_current AS DOUBLE) AS points_current
    FROM raw_championship_drivers
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted
```

**`models/staging/stg_championship_teams.sql`**

```sql
WITH raw_championship_teams AS (
    SELECT * FROM {{ source('openf1_raw', 'championship_teams') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        
        team_name,
        position_start,
        position_current,
        CAST(points_start AS DOUBLE) AS points_start,
        CAST(points_current AS DOUBLE) AS points_current
    FROM raw_championship_teams
)
SELECT * FROM renamed_and_casted
```

**`models/staging/stg_meetings.sql`**

```sql
WITH raw_meetings AS (
    SELECT * FROM {{ source('openf1_raw', 'meetings') }}
),
renamed_and_casted AS (
    SELECT
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(year AS INTEGER) AS year,
        CAST(circuit_key AS INTEGER) AS circuit_key,
        
        meeting_name,
        meeting_official_name,
        location,
        country_key,
        country_code,
        circuit_short_name,
        CAST(date_start AS TIMESTAMP) AS date_start,
        gmt_offset
    FROM raw_meetings
    WHERE meeting_key IS NOT NULL
)
SELECT * FROM renamed_and_casted
```

**`models/staging/stg_sessions.sql`**

```sql
WITH raw_sessions AS (
    SELECT * FROM {{ source('openf1_raw', 'sessions') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(circuit_key AS INTEGER) AS circuit_key,
        CAST(year AS INTEGER) AS year,
        
        session_name,
        session_type,
        CAST(date_start AS TIMESTAMP) AS date_start,
        CAST(date_end AS TIMESTAMP) AS date_end,
        gmt_offset
    FROM raw_sessions
    WHERE session_key IS NOT NULL
)
SELECT * FROM renamed_and_casted
```

**`models/staging/stg_session_result.sql`**

```sql
WITH raw_session_result AS (
    SELECT * FROM {{ source('openf1_raw', 'session_result') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        CAST(position AS INTEGER) AS position,
        
        -- Defensive fix: Read as VARCHAR to accept string arrays or standalone doubles,
        -- then use regexp to isolate the first core duration metric if formatted as an array.
        CASE 
            WHEN REGEXP_MATCHES(CAST(duration AS VARCHAR), '^\[.*\]$') 
            THEN TRY_CAST(STR_SPLIT(REPLACE(REPLACE(CAST(duration AS VARCHAR), '[', ''), ']', ''), ',')[1] AS DOUBLE)
            ELSE TRY_CAST(CAST(duration AS VARCHAR) AS DOUBLE)
        END AS duration,
        
        dns,
        dsq
        
    FROM raw_session_result
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted
```

**`models/staging/stg_starting_grid.sql`**

```sql
WITH raw_starting_grid AS (
    SELECT * FROM {{ source('openf1_raw', 'starting_grid') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        -- Changed from grid_position to position based on likely API schema
        CAST(position AS INTEGER) AS grid_position
        
    FROM raw_starting_grid
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted
```

**`models/staging/stg_laps.sql`**

```sql
WITH raw_laps AS (
    SELECT * FROM {{ source('openf1_raw', 'laps') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        CAST(lap_number AS INTEGER) AS lap_number,
        lap_duration,
        
        -- The actual time spent in each sector
        duration_sector_1,
        duration_sector_2,
        duration_sector_3,
        
        -- The micro-sector arrays (useful for detailed telemetry later)
        segments_sector_1,
        segments_sector_2,
        segments_sector_3,
        
        is_pit_out_lap
        
    FROM raw_laps
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted
```

**`models/staging/stg_stints.sql`**

```sql
WITH raw_stints AS (
    SELECT * FROM {{ source('openf1_raw', 'stints') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        CAST(stint_number AS INTEGER) AS stint_number,
        compound AS tyre_compound,
        CAST(tyre_age_at_start AS INTEGER) AS tyre_age_at_start,
        CAST(lap_start AS INTEGER) AS lap_start,
        CAST(lap_end AS INTEGER) AS lap_end
    FROM raw_stints
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted
```

**`models/staging/stg_pit.sql`**

```sql
WITH raw_pit AS (
    SELECT * FROM {{ source('openf1_raw', 'pit') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        CAST(lap_number AS INTEGER) AS lap_number,
        pit_duration
    FROM raw_pit
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted
```

**`models/staging/stg_position.sql`**

```sql
WITH raw_position AS (
    SELECT * FROM {{ source('openf1_raw', 'position') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        CAST(position AS INTEGER) AS position,
        CAST(date AS TIMESTAMP) AS position_timestamp
    FROM raw_position
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted
```

**`models/staging/stg_intervals.sql`**

```sql
WITH raw_intervals AS (
    SELECT * FROM {{ source('openf1_raw', 'intervals') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        -- Force these to strings so DuckDB doesn't accidentally 
        -- infer them as doubles and crash on "+2 LAPS"
        CAST(interval AS VARCHAR) AS interval_raw,
        CAST(gap_to_leader AS VARCHAR) AS gap_to_leader_raw
        
    FROM raw_intervals
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted
```

**`models/staging/stg_overtakes.sql`**

```sql
WITH raw_overtakes AS (
    SELECT * FROM {{ source('openf1_raw', 'overtakes') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        
        CAST(overtaking_driver_number AS INTEGER) AS overtaking_driver_number,
        CAST(overtaken_driver_number AS INTEGER) AS overtaken_driver_number,
        
        -- Removed lap_number and added position based on the API's actual schema
        CAST(position AS INTEGER) AS position
        
    FROM raw_overtakes
)
SELECT * FROM renamed_and_casted
```

**`models/staging/stg_race_control.sql`**

```sql
WITH raw_race_control AS (
    SELECT * FROM {{ source('openf1_raw', 'race_control') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        
        -- Defensive fix: TRY_CAST prevents type-inference crashes 
        -- if an empty parquet file sets the column layout to NULL
        TRY_CAST(lap_number AS INTEGER) AS lap_number,
        
        CAST(date AS TIMESTAMP) AS race_control_timestamp,
        category,
        message,
        flag
    FROM raw_race_control
)
SELECT * FROM renamed_and_casted
```

**`models/staging/stg_team_radio.sql`**

```sql
WITH raw_team_radio AS (
    SELECT * FROM {{ source('openf1_raw', 'team_radio') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        CAST(date AS TIMESTAMP) AS radio_timestamp,
        recording_url
    FROM raw_team_radio
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted
```

**`models/staging/stg_weather.sql`**

```sql
WITH raw_weather AS (
    SELECT * FROM {{ source('openf1_raw', 'weather') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        
        CAST(date AS TIMESTAMP) AS weather_timestamp,
        CAST(air_temperature AS DOUBLE) AS air_temperature,
        CAST(track_temperature AS DOUBLE) AS track_temperature,
        CAST(humidity AS DOUBLE) AS humidity,
        CAST(rainfall AS INTEGER) AS rainfall_flag,
        CAST(wind_speed AS DOUBLE) AS wind_speed,
        CAST(wind_direction AS INTEGER) AS wind_direction
    FROM raw_weather
)
SELECT * FROM renamed_and_casted
```

**`models/staging/stg_car_data.sql`**

```sql
WITH raw_car_data AS (
    SELECT * FROM {{ source('openf1_raw', 'car_data') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        CAST(date AS TIMESTAMP) AS telemetry_timestamp,
        CAST(rpm AS INTEGER) AS rpm,
        CAST(speed AS INTEGER) AS speed,
        CAST(n_gear AS INTEGER) AS gear,
        CAST(throttle AS INTEGER) AS throttle,
        CAST(brake AS INTEGER) AS brake,
        CAST(drs AS INTEGER) AS drs
    FROM raw_car_data
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted
```

**`models/staging/stg_location.sql`**

```sql
WITH raw_location AS (
    SELECT * FROM {{ source('openf1_raw', 'location') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        
        CAST(date AS TIMESTAMP) AS location_timestamp,
        CAST(x AS DOUBLE) AS x_coordinate,
        CAST(y AS DOUBLE) AS y_coordinate,
        CAST(z AS DOUBLE) AS z_coordinate
    FROM raw_location
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted
```


**Notes on specific models**

* **`stg_car_data` and `stg_location`:** `date` is renamed to `telemetry_timestamp` or similar, to avoid the reserved-feeling word `date`.
* **`stg_intervals`:** values are kept as **text** (`interval_raw`, `gap_to_leader_raw`) because the API mixes numbers and strings such as `+1 LAP`. The marts convert them safely.
* **`stg_race_control`:** `TRY_CAST` turns un-castable values into `NULL` instead of crashing, which matters when an empty Parquet file makes DuckDB guess the wrong type.
* **`stg_session_result`:** `duration` can be a single number or an array of qualifying times. The `CASE` handles both and keeps the first value.

#### **Task 5: Build and Verify the Layer**

```bash
dbt run --select staging
```

Expect 18 successes. Then look inside the database with the DuckDB command line (or any tool from Chapter 9):

```sql
SELECT COUNT(*) FROM stg_laps;
SELECT * FROM stg_drivers LIMIT 5;
```

### **Learner Pitfalls & Troubleshooting**

* **The Problem:** `Could not find profile named 'dbt_f1_transformations'`.
* **Why it Happens:** the `profile:` value in `dbt_project.yml` does not match the top-level key in `profiles.yml`, or `profiles.yml` is not where dbt looks.
* **The Solution:** make the two names identical and keep the file in `~/.dbt/`, or pass `--profiles-dir <folder>`.

* **The Problem:** `IO Error: No files found that match the pattern ...`.
* **Why it Happens:** the glob in `sources.yml` does not match any file: wrong root path, wrong file naming, or Airflow has not produced that endpoint yet.
* **The Solution:** list the folder (`ls airflow/data/raw | head`) and compare the names. The pattern is `<endpoint>_*_extracted.parquet`.

* **The Problem:** the editor shows `UnusedConfigKey` warnings on `sources.yml`.
* **Why it Happens:** `external_location` was placed at the wrong nesting level.
* **The Solution:** nest it under `config:` exactly as shown.

* **The Problem:** staging models contained hard-coded file paths, so moving the data meant editing 18 files.
* **Why it Happens:** early models used `read_parquet('F:/...')` directly.
* **The Solution:** always `SELECT * FROM {{ source('openf1_raw', '<table>') }}`. Paths then live in one file.

* **The Problem:** `IO Error: Could not set lock on file ... f1_local_warehouse.duckdb`.
* **Why it Happens:** DuckDB allows **one writer** at a time. A SQL tool, Power BI or another dbt run still holds the file open.
* **The Solution:** close the other connection before running dbt (Chapter 9 shows how to stay out of dbt's way).

* **The Problem:** `Binder Error: Referenced column "..." not found`.
* **Why it Happens:** the model names a column the raw data does not have (a typo, or a field assumed from API documentation that the files do not contain).
* **The Solution:** inspect first: `DESCRIBE SELECT * FROM {{ source }}` equivalent, i.e. `SELECT * FROM read_parquet('<glob>') LIMIT 1`, then write the model from what actually exists.

* **The Problem:** dbt reports "Compilation Error" on a model you just edited, but the file looks right.
* **Why it Happens:** the file was not saved in the editor, or a stale `target/` folder is being used.
* **The Solution:** save, then run `dbt clean` followed by `dbt run`.

---


## **Chapter 6: Data Transformation II: Building the Analytical Marts**

### **Chapter Overview**

Staging gave you clean tables. The **marts** turn them into answers: *Who gained the most places? How old were the tyres on each lap? Which pit stops were slowest?* You will build **2 dimensions and 13 fact tables** here (the 14th fact table, telemetry, gets Chapter 7), test them, and generate lineage documentation. **[VERIFIED]**

**Concepts introduced in this chapter**

* **`ref()`:** `{{ ref('stg_laps') }}` is how one dbt model depends on another. dbt reads these references to build the dependency graph and to run models in the right order.
* **Window functions:** calculations across related rows without collapsing them (`ROW_NUMBER()`, `LAG()`, `AVG() OVER`). `PARTITION BY` defines the groups; `ORDER BY` defines the order inside each group.
* **Range join:** joining on `BETWEEN` instead of equality, for example matching a lap to the stint whose `lap_start` and `lap_end` contain it.
* **`TRY_CAST`:** like `CAST`, but returns `NULL` instead of failing when a value does not convert (`"+1 LAP"` as a number).
* **Data tests:** assertions dbt runs as SQL, such as `not_null` and `unique`.

### **Prerequisites**

* Chapter 5 complete: all 18 staging views build.

### **Step by Step Procedures**

#### **Task 1: Plan the Five Pillars of Analysis**

Marts are designed around questions, not around source tables. This project groups them into five pillars:

| Pillar | Question | Models |
|---|---|---|
| **Core dimensions** | Who and what? | `dim_drivers`, `dim_sessions` |
| **Pace and tyres** | How fast, on which rubber? | `fct_lap_performance`, `fct_fastest_laps`, `fct_tire_strategy` |
| **Race dynamics** | Who passed whom, who stopped? | `fct_overtakes`, `fct_driver_positions`, `fct_pit_stops`, `fct_race_intervals` |
| **Results and standings** | Who won, who leads? | `fct_race_results`, `fct_championship_standings`, `fct_constructor_standings` |
| **Context and incidents** | What was happening? | `fct_weather_impact`, `fct_lap_incidents`, `fct_team_radio` |

The 16th mart, **`fct_telemetry`**, is large enough to get its own chapter (Chapter 7).

#### **Task 2: Create the Models**

Create the files in **models/marts/core/** (dimensions) and **models/marts/analytics/** (facts). The default from `dbt_project.yml` makes them **tables**; a few add explicit `config` blocks.

**`models/marts/core/dim_drivers.sql`**

**Dimension.** One row per driver (their most recent session wins), so the driver list never has duplicates.

```sql
WITH staging_drivers AS (
    SELECT * FROM {{ ref('stg_drivers') }}
),

deduplicated_drivers AS (
    SELECT
        driver_number,
        full_name,
        driver_acronym,
        team_name,
        team_color_hex,
        country_code,
        headshot_url,
        -- This ranks a driver's rows, putting their most recent session as #1
        ROW_NUMBER() OVER (PARTITION BY driver_number ORDER BY session_key DESC) as row_num
    FROM staging_drivers
)

SELECT
    driver_number,
    full_name,
    driver_acronym,
    team_name,
    team_color_hex,
    country_code,
    headshot_url
FROM deduplicated_drivers
WHERE row_num = 1
```

**`models/marts/core/dim_sessions.sql`**

**Dimension.** Every session enriched with its Grand Prix, circuit and country through a join to meetings.

```sql
WITH sessions AS (
    SELECT * FROM {{ ref('stg_sessions') }}
),

meetings AS (
    SELECT * FROM {{ ref('stg_meetings') }}
),

enriched_sessions AS (
    SELECT
        -- Session specific details
        s.session_key,
        s.session_name,
        s.session_type,
        s.date_start AS session_start_time,
        s.date_end AS session_end_time,
        
        -- Meeting & Location details brought in from the join
        s.meeting_key,
        m.meeting_name,
        m.meeting_official_name,
        s.year,
        m.circuit_key,
        m.circuit_short_name,
        m.location,
        m.country_code,
        s.gmt_offset
        
    FROM sessions s
    LEFT JOIN meetings m
        ON s.meeting_key = m.meeting_key
)

SELECT * FROM enriched_sessions
```

**`models/marts/analytics/fct_lap_performance.sql`**

Each lap joined to the tyre **stint** it belongs to, with a calculated tyre age for that lap. Uses a **range join** (`BETWEEN`).

```sql
WITH laps AS (
    SELECT * FROM {{ ref('stg_laps') }}
),

stints AS (
    SELECT * FROM {{ ref('stg_stints') }}
)

SELECT
    l.session_key,
    l.meeting_key,
    l.driver_number,
    l.lap_number,
    l.lap_duration AS lap_time_seconds,
    
    -- Bringing in the Tyre Strategy Data
    s.stint_number,
    s.tyre_compound, -- Updated to match the column name coming out of stg_stints
    
    -- Calculate exactly how many laps this set of tyres has done!
    s.tyre_age_at_start + (l.lap_number - s.lap_start) AS tyre_age_this_lap

FROM laps l
-- Range Join
LEFT JOIN stints s
    ON l.session_key = s.session_key
    AND l.driver_number = s.driver_number
    AND l.lap_number BETWEEN s.lap_start AND s.lap_end
```

**`models/marts/analytics/fct_fastest_laps.sql`**

The single best lap per driver per session, found with `ROW_NUMBER()` over a window.

```sql
WITH laps AS (
    SELECT * FROM {{ ref('stg_laps') }}
),

ranked_laps AS (
    SELECT
        session_key,
        meeting_key,
        driver_number,
        lap_number,
        lap_duration AS fastest_lap_time_seconds,
        -- Row number orders laps from quickest to slowest per driver, per session
        ROW_NUMBER() OVER (
            PARTITION BY session_key, driver_number 
            ORDER BY lap_duration ASC
        ) AS lap_rank
    FROM laps
    WHERE lap_duration IS NOT NULL
)

-- Grab only the #1 fastest lap for every driver in each session
SELECT
    session_key,
    meeting_key,
    driver_number,
    lap_number,
    fastest_lap_time_seconds
FROM ranked_laps
WHERE lap_rank = 1
```

**`models/marts/analytics/fct_pit_stops.sql`**

Pit lane time per stop, cast to a number, with incomplete records removed.

```sql
WITH pit_stops AS (
    SELECT * FROM {{ ref('stg_pit') }}
)

SELECT
    session_key,
    meeting_key,
    driver_number,
    lap_number,
    CAST(pit_duration AS DOUBLE) AS pit_lane_time_seconds
FROM pit_stops
WHERE pit_duration IS NOT NULL
```

**`models/marts/analytics/fct_overtakes.sql`**

Who passed whom, and the position after the pass.

```sql
WITH overtakes AS (
    SELECT * FROM {{ ref('stg_overtakes') }}
)

SELECT
    session_key,
    meeting_key,
    overtaking_driver_number,
    overtaken_driver_number,
    position AS post_overtake_position
FROM overtakes
```

**`models/marts/analytics/fct_driver_positions.sql`**

Position over time with `LAG()` to flag whether a driver gained or lost a place.

```sql
{{ config(
    materialized = 'table'
) }}

WITH positions_data AS (
    SELECT * FROM {{ ref('stg_position') }}
),

session_context AS (
    SELECT * FROM {{ ref('dim_sessions') }}
)

SELECT
    p.session_key,
    p.meeting_key,
    p.driver_number,
    
    -- UPDATE THIS LINE: Use the actual column name from your staging model
    p.position_timestamp, 
    
    p.position AS live_position,
    
    -- Update the ORDER BY clauses here too
    LAG(p.position) OVER (
        PARTITION BY p.session_key, p.driver_number 
        ORDER BY p.position_timestamp
    ) AS previous_position,
    
    CASE 
        WHEN LAG(p.position) OVER (PARTITION BY p.session_key, p.driver_number ORDER BY p.position_timestamp) > p.position THEN TRUE
        ELSE FALSE
    END AS gained_position,
    
    CASE 
        WHEN LAG(p.position) OVER (PARTITION BY p.session_key, p.driver_number ORDER BY p.position_timestamp) < p.position THEN TRUE
        ELSE FALSE
    END AS lost_position

FROM positions_data p
```

**`models/marts/analytics/fct_team_radio.sql`**

Radio messages enriched with the driver and team names.

```sql
{{ config(
    materialized = 'table'
) }}

WITH radio_data AS (
    SELECT * FROM {{ ref('stg_team_radio') }}
),

drivers AS (
    SELECT DISTINCT driver_number, team_name, full_name 
    FROM {{ ref('dim_drivers') }}
)

SELECT
    r.session_key,
    r.meeting_key,
    r.driver_number,
    d.full_name AS driver_name,
    d.team_name,
    
    -- UPDATE THIS LINE: Use the actual column name from your staging model
    r.radio_timestamp,
    
    r.recording_url

FROM radio_data r
LEFT JOIN drivers d
    ON r.driver_number = d.driver_number
```

**`models/marts/analytics/fct_weather_impact.sql`**

Each lap with **session-level average** air and track temperature and a rain flag.

```sql
WITH session_weather AS (
    SELECT
        session_key,
        AVG(air_temperature) AS avg_air_temperature,
        AVG(track_temperature) AS avg_track_temperature,
        MAX(rainfall_flag) AS rained_during_session
    FROM {{ ref('stg_weather') }}
    GROUP BY session_key
),

laps AS (
    SELECT * FROM {{ ref('stg_laps') }}
)

SELECT
    l.session_key,
    l.meeting_key,
    l.driver_number,
    l.lap_number,
    -- Aliased to match your schema.yml testing expectations perfectly
    l.lap_duration AS lap_time_seconds,
    w.avg_air_temperature,
    w.avg_track_temperature,
    w.rained_during_session
FROM laps l
LEFT JOIN session_weather w
    ON l.session_key = w.session_key
WHERE l.lap_duration IS NOT NULL
```

**`models/marts/analytics/fct_championship_standings.sql`**

Driver championship position and points.

```sql
WITH driver_standings AS (
    SELECT * FROM {{ ref('stg_championship_drivers') }}
)

SELECT
    session_key,
    meeting_key,
    driver_number,
    position_start AS season_starting_position,
    position_current AS current_championship_position,
    points_current AS total_points
FROM driver_standings
```

**`models/marts/analytics/fct_constructor_standings.sql`**

Constructor championship position and points.

```sql
WITH team_standings AS (
    SELECT * FROM {{ ref('stg_championship_teams') }}
)

SELECT
    session_key,
    meeting_key,
    team_name,
    position_start AS season_starting_position,
    position_current AS current_championship_position,
    points_current AS total_points
FROM team_standings
```

**`models/marts/analytics/fct_lap_incidents.sql`**

Laps flagged when race control reported an incident on that lap. Materialised as a table.

```sql
{{ config(
    materialized='table'
) }}

WITH laps AS (
    SELECT * FROM {{ ref('stg_laps') }}
    WHERE lap_duration IS NOT NULL
),

race_control_events AS (
    SELECT 
        session_key,
        lap_number,
        STRING_AGG(DISTINCT category, ', ') AS incident_categories,
        MAX(flag) AS primary_flag
    FROM {{ ref('stg_race_control') }}
    WHERE lap_number IS NOT NULL
    GROUP BY 
        session_key, 
        lap_number
)

SELECT
    l.session_key,
    l.meeting_key,
    l.driver_number,
    l.lap_number,
    l.lap_duration AS lap_time_seconds,
    
    CASE 
        WHEN r.incident_categories IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS is_affected_by_incident,
    
    r.incident_categories,
    r.primary_flag

FROM laps l
LEFT JOIN race_control_events r
    ON l.session_key = r.session_key
    AND l.lap_number = r.lap_number
```

**`models/marts/analytics/fct_race_results.sql`**

Final classification joined to the starting grid, with positions gained or lost. Materialised as a table.

```sql
{{ config(
    materialized='table'
) }}

WITH session_results AS (
    SELECT * FROM {{ ref('stg_session_result') }}
),

starting_grid AS (
    SELECT * FROM {{ ref('stg_starting_grid') }}
),

race_results_calculated AS (
    SELECT
        r.session_key,
        r.meeting_key,
        r.driver_number,
        g.grid_position,
        r.position AS final_position,
        
        CASE 
            WHEN g.grid_position IS NOT NULL AND r.position IS NOT NULL 
            THEN (g.grid_position - r.position)
            ELSE NULL 
        END AS positions_gained_or_lost,
        
        r.duration AS race_duration,
        r.dns AS did_not_start,
        r.dsq AS disqualified
        
    FROM session_results r
    LEFT JOIN starting_grid g
        ON r.session_key = g.session_key
        AND r.driver_number = g.driver_number
)

SELECT * FROM race_results_calculated
```

**`models/marts/analytics/fct_race_intervals.sql`**

Gaps to the leader and car ahead as numbers, with lapped drivers kept as text and a DRS-range flag. Uses `TRY_CAST`.

```sql
WITH intervals AS (
    SELECT * FROM {{ ref('stg_intervals') }}
)

SELECT
    session_key,
    meeting_key,
    driver_number,
    
    -- Safely convert to double. Text like "+2 LAPS" safely becomes NULL.
    TRY_CAST(gap_to_leader_raw AS DOUBLE) AS gap_to_leader_seconds,
    TRY_CAST(interval_raw AS DOUBLE) AS gap_to_car_ahead_seconds,
    
    -- Optional: Preserve the raw text so you can flag lapped drivers in Power BI
    CASE 
        WHEN gap_to_leader_raw LIKE '%LAP%' THEN gap_to_leader_raw
        ELSE NULL 
    END AS lapped_status,
    
    -- Business Logic: Identify if the driver is in DRS range
    -- We use the safe numeric conversion here for the math
    CASE 
        WHEN TRY_CAST(interval_raw AS DOUBLE) <= 1.000 THEN TRUE 
        ELSE FALSE 
    END AS is_in_drs_range

FROM intervals
-- Filter out the leader, as they don't have a gap to the car ahead!
WHERE interval_raw IS NOT NULL
```

**`models/marts/analytics/fct_tire_strategy.sql`**

Every lap mapped to its stint, tyre compound and current tyre age.

```sql
{{ config(
    materialized='table'
) }}

with laps as (
    select
        session_key,
        meeting_key,
        driver_number,
        lap_number,
        lap_duration,
        -- DuckDB handles boolean expressions directly without an IIF or CASE statement
        (lap_duration is not null) as is_valid_lap
    from {{ ref('stg_laps') }}
),

stints as (
    select
        session_key,
        driver_number,
        stint_number,
        tyre_compound,
        lap_start,
        lap_end,
        tyre_age_at_start
    from {{ ref('stg_stints') }}
),

tire_strategy_mapping as (
    select
        l.session_key,
        l.meeting_key,
        l.driver_number,
        l.lap_number,
        l.lap_duration,
        l.is_valid_lap,
        s.stint_number,
        s.tyre_compound,
        s.lap_start as stint_lap_start,
        s.lap_end as stint_lap_end,
        s.tyre_age_at_start,
        -- Calculate real-time tire age for each lap in the strategy
        s.tyre_age_at_start + (l.lap_number - s.lap_start) as current_tyre_age
    from laps l
    inner join stints s
        on l.session_key = s.session_key
        and l.driver_number = s.driver_number
        and l.lap_number >= s.lap_start
        and l.lap_number <= s.lap_end
)

select * from tire_strategy_mapping
```


> **Design note on `fct_weather_impact`.** Weather is sampled every minute or so, while laps are discrete events. The model keeps it simple: it averages weather **per session** and attaches those averages to every lap. That is easy to explain and cheap, but it cannot say "it was raining on lap 41". A true **time-aligned** version would use an ASOF join on the lap start time. Chapter 7 teaches ASOF on telemetry; applying the same pattern to weather is a good **exercise** (not implemented in the project).

#### **Task 3: Run the Marts**

```bash
dbt run --select marts
dbt run                       # everything: staging + marts
```

The first fully clean run built **33 models**; with `fct_tire_strategy` added, the project has **18 staging + 2 dimension + 14 fact models** once telemetry (Chapter 7) is included. Power BI later connects to those 16 mart tables.

#### **Task 4: Add Tests and Documentation**

Create **models/marts/schema.yml**. Descriptions here become your documentation site, and the `tests:` entries become checks.

```yaml
version: 2

models:
  - name: fct_race_results
    description: "A fact table containing the final race results for each session, with grid position and positions gained or lost."
    columns:
      - name: session_key
        description: "The unique identifier for the race session."
        tests:
          - not_null

      - name: driver_number
        description: "The unique number assigned to the driver."
        tests:
          - not_null

      - name: final_position
        description: "Where the driver finished the race."

  - name: fct_overtakes
    description: "A chronological record of every overtake during a session."
    columns:
      - name: session_key
        tests:
          - not_null

      - name: overtaking_driver_number
        tests:
          - not_null

      - name: overtaken_driver_number
        tests:
          - not_null

  - name: dim_drivers
    description: "Master list of drivers with their latest team and biographical info."
    columns:
      - name: driver_number
        description: "The permanent racing number of the driver."
        tests:
          - unique
          - not_null
      - name: full_name
        tests:
          - not_null

  - name: fct_fastest_laps
    description: "The single fastest lap per driver in each session."
    columns:
      - name: session_key
        tests:
          - not_null
      - name: driver_number
        tests:
          - not_null
      - name: fastest_lap_time_seconds
        description: "The duration of the lap in seconds."
        tests:
          - not_null

  - name: fct_pit_stops
    description: "Pit lane time for each completed pit stop."
    columns:
      - name: session_key
        tests:
          - not_null
      - name: driver_number
        tests:
          - not_null
      - name: pit_lane_time_seconds
        description: "Total time spent in the pit lane."
        tests:
          - not_null

  - name: fct_weather_impact
    description: "Each lap joined to session-level average weather (air temperature, track temperature, rain flag)."
    columns:
      - name: session_key
        tests:
          - not_null
      - name: driver_number
        tests:
          - not_null
      - name: lap_time_seconds
        tests:
          - not_null
      - name: avg_track_temperature
        description: "Average track temperature in Celsius across the whole session."

  - name: fct_tire_strategy
    description: "Granular fact table mapping every completed lap to its respective tire stint, compound type, and real-time tire age."
    columns:
      - name: session_key
        description: "Unique identifier for the session."
        tests:
          - not_null
      - name: driver_number
        description: "The racing number of the driver."
      - name: lap_number
        description: "The specific lap number."
      - name: tyre_compound
        description: "The tire compound used (SOFT, MEDIUM, HARD, INTERMEDIATE, WET)."
      - name: current_tyre_age
        description: "Calculated age of the tire (in laps completed) during this specific lap."
```

Run the tests:

```bash
dbt test
```

At the last recorded test run, all data tests passed. Each test is a query that returns the **bad rows**; zero rows means pass.

> **Documentation drift, a lesson in itself.** An earlier version of this file described `is_overall_fastest_lap` and `is_slow_stop` columns and an ASOF join for weather. **None of those exist in the models.** dbt did not complain because only the *tested* columns are checked, so the docs lied quietly. The version above is corrected to match the SQL. Treat `schema.yml` as code: review it whenever a model changes.

Generate and browse the documentation site:

```bash
dbt docs generate
dbt docs serve
```

Open the **lineage graph** (the blue circle, bottom right): it shows every source flowing through staging into the marts. You get this for free from `ref()` and `source()`.

### **Learner Pitfalls & Troubleshooting**

* **The Problem:** `Conversion Error: Could not convert string '+2 LAPS' to DOUBLE` when building or querying interval data.
* **Why it Happens:** gap columns hold numbers for most drivers and text for lapped cars.
* **The Solution:** keep the raw columns as text in staging, convert with `TRY_CAST` in the mart so text becomes `NULL`, and keep the original text in a separate `lapped_status` column. If the error persists at the *source* (the first file says DOUBLE, a later file says VARCHAR), the cause is file-to-file type drift; see `union_by_name` in Chapter 5 and the full story in Chapter 9.

* **The Problem:** a column used in `ORDER BY` does not exist (`position_timestamp`, `radio_timestamp`).
* **Why it Happens:** the mart used the raw API name, but staging had renamed it.
* **The Solution:** marts read **staging** columns, never raw ones. Open the staging model to see the final names.

* **The Problem:** `dbt test` fails on `unique` for `dim_drivers.driver_number`.
* **Why it Happens:** drivers appear once per session in the raw data, and changing teams creates several rows.
* **The Solution:** deduplicate with `ROW_NUMBER() OVER (PARTITION BY driver_number ORDER BY session_key DESC)` and keep row 1, as `dim_drivers` does.

* **The Problem:** a model builds but returns zero rows.
* **Why it Happens:** an `INNER JOIN` (as in `fct_tire_strategy`) drops laps without a matching stint, or the staging view reads an empty glob.
* **The Solution:** count rows at each step (`SELECT COUNT(*)` on each CTE), switch to `LEFT JOIN` to see what is being dropped.

* **The Problem:** the dependency "dangles": a model exists but nothing references it, and it builds out of order or not at all.
* **Why it Happens:** the model used a hard-coded table name instead of `ref()`.
* **The Solution:** always `{{ ref('model_name') }}`; check the lineage graph for orphans.

---


## **Chapter 7: Taming High-Volume Telemetry**

### **Chapter Overview**

Everything so far handles thousands or millions of rows. **Telemetry** (`car_data` and `location`) samples every car several times per second for an entire race: **tens of millions of rows**. Naive SQL that works on laps will exhaust an 8 GB machine. This chapter is the project's best case study in **memory-aware data engineering**, told as the sequence of failures that produced the final design.

* The incident chain and the streaming export: **[VERIFIED]** (the logs show the failures, and later successful exports of about 21 and 43 minutes).
* The final incremental ASOF model and variable-gated macro below: **[CODE FINAL, RUN NOT CONFIRMED]**. Review them against your own runs.

**Concepts introduced in this chapter**

* **Out-of-memory (OOM):** the query engine needs more RAM than it has.
* **Blocking (pipeline-breaking) operator:** an operation that must see all of its input before it can produce output, such as a hash join's build side or a global `ORDER BY`. Two of them in one query compete for memory.
* **Spill to disk:** DuckDB can write temporary data to a **temp directory** when memory is full, if you give it a place to do so.
* **ASOF join:** match each row to the **closest earlier** row. Cheaper than a many-to-many equality join on high-frequency time series.
* **Incremental model:** after the first build, each run processes only new data.
* **Partitioned Parquet:** one folder per value of chosen columns (`meeting_key=.../session_key=.../driver_number=.../`). Queries that filter on those columns read only the matching folders (**partition pruning**).
* **Macro:** a reusable Jinja function in `macros/`. It can run SQL that is not a model, such as a `COPY` export.

### **Prerequisites**

* Chapters 5 and 6 complete, with `car_data` and `location` Parquet files for at least one session.

### **Step by Step Procedures**

#### **Task 1: Decide Whether Partitioning Is Worth It**

Partitioning has a cost (many small files), so the build started with a design question rather than code. The reasoning that held up:

* Analysts almost always filter by **meeting, session and driver**, so those three columns match how data is read.
* Partition **columns must be low-cardinality enough** to avoid millions of tiny files. Three nested levels (meeting, session, driver) produce a few thousand folders for a season, which is fine. Partitioning by timestamp would be ruinous.

#### **Task 2: The Failure Chain (Read This Before Writing Code)**

Each step below is a real error from the build, in order. Do not skip them: every later design choice answers one of them.

| # | Attempt | Error | Root cause | Response |
|---|---|---|---|---|
| 1 | `materialized='external'` with `location='output/telemetry_data/'` | `Invalid path ... \\.tmp` | DuckDB stages partitioned writes in a `.tmp` folder. A trailing slash produced a double slash, which Windows reads as a **UNC network path** | Remove the trailing slash |
| 2 | Same, with a SQL comment (`-- note`) inside `{{ config(...) }}` | `invalid syntax for function call expression` | The braces are **Jinja**, not SQL. `--` comments are not valid there | Use `{# comment #}` or no comment |
| 3 | Relative path | Same `.tmp` error | Relative paths were mis-resolved by partition workers | Use an **absolute path** |
| 4 | Absolute path | Same error again | A defect in the adapter's `external` materialisation on Windows (the diagnosis the build converged on) | **Stop using `external`.** Separate *modelling* from *exporting* |
| 5 | `materialized='table'` | `Out of Memory` | An 8 GB limit cannot hold the full car/location join while writing it into the database file | Make it a **view** and stream it out with a `COPY` macro |
| 6 | `COPY` into a nested folder | `IO Error` | `COPY` does not create missing **parent** folders (`output/` did not exist) | `mkdir output` first |
| 7 | `COPY ... ORDER BY` over a `LEFT JOIN` | `Out of Memory Error: Allocation failure` after ~30 s | Two blocking operators (hash join plus global sort) at once. The temp directory in `profiles.yml` also pointed to a folder that did not exist | Remove the global `ORDER BY`, create the temp folder, replace the join with an **ASOF join** |
| 8 | Streaming `COPY` with ASOF, ordering relaxed | **Success** (about 21 minutes in the first logged run) | | Keep this design |

**The lessons**

1. **Separate modelling from exporting.** dbt builds models; DuckDB's native `COPY` is the best tool for writing files.
2. **Do not stack blocking operators** over large inputs.
3. **Give the engine somewhere to spill** (`temp_directory`) and make sure that folder exists.
4. **Pick the join that matches the data's shape.** Time series want ASOF.

#### **Task 3: Build the Telemetry Model**

Create **models/marts/analytics/fct_telemetry.sql**.

```sql
{{ config(
    materialized = 'incremental',
    unique_key = ['session_key', 'driver_number', 'telemetry_timestamp']
) }}

WITH car_data AS (
    SELECT * FROM {{ ref('stg_car_data') }}
    ORDER BY session_key, driver_number, telemetry_timestamp
),
location_data AS (
    SELECT * FROM {{ ref('stg_location') }}
    ORDER BY session_key, driver_number, location_timestamp
)
SELECT
    c.session_key,
    c.meeting_key,
    c.driver_number,
    c.telemetry_timestamp,
    
    -- Performance Metrics
    c.rpm, 
    c.speed, 
    c.gear, 
    c.throttle, 
    c.brake, 
    c.drs,
    
    -- Track Position (mapped to nearest preceding timestamp)
    l.x_coordinate AS x,
    l.y_coordinate AS y,
    l.z_coordinate AS z
    
FROM car_data c
ASOF LEFT JOIN location_data l
    ON c.session_key = l.session_key
    AND c.driver_number = l.driver_number
    AND c.telemetry_timestamp >= l.location_timestamp

{% if is_incremental() %}
-- Defensive incremental logic using NOT EXISTS to avoid the NOT IN NULL trap.
-- Ensures safe appending without relying on external variables.
WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ this }} t 
    WHERE t.session_key = c.session_key
)
{% endif %}
```

**Reading it line by line**

* **`incremental` and `unique_key`:** the first run builds the table; later runs only add rows. The key identifies one reading.
* **`ASOF LEFT JOIN ... ON ... AND c.telemetry_timestamp >= l.location_timestamp`:** for each car-data row, find the latest location row for the same session and driver that is **not later** than the reading. The `>=` condition is what makes it an ASOF join.
* **`{% if is_incremental() %} WHERE NOT EXISTS (...)`:** on subsequent runs, skip any session already present in the table. `NOT EXISTS` is used instead of `NOT IN` because `NOT IN` returns nothing at all if the subquery contains a `NULL`, a classic trap.
* **Column aliases (`x`, `y`, `z`):** location coordinates arrive from staging as `x_coordinate` and so on.

Build it:

```bash
dbt run --select fct_telemetry
```

To rebuild from scratch (for example after changing the logic), use `--full-refresh`.

#### **Task 4: The Export Macro**

Create **macros/export_fct_telemetry.sql**. It writes the table out as a partitioned Parquet data lake, only when you ask it to.

```jinja
{% macro export_fct_telemetry() %}

  {% if execute %}
    
    {# 1. Guardrail: Only run if explicitly told to export #}
    {% set should_export = var('export_data', false) %}
    
    {% if should_export %}
      
      {# 2. Capture target parameters #}
      {% set target_session = var('target_session_key', none) %}
      {% set target_meeting = var('target_meeting_key', none) %}

      {% do log("=======================================================================", info=True) %}
      {% if target_session %}
        {% do log("ATTENTION: Starting INCREMENTAL Parquet export for Session: " ~ target_session, info=True) %}
      {% elif target_meeting %}
        {% do log("ATTENTION: Starting INCREMENTAL Parquet export for Meeting: " ~ target_meeting, info=True) %}
      {% else %}
        {% do log("ATTENTION: Starting FULL historical Parquet export for fct_telemetry.", info=True) %}
      {% endif %}
      {% do log("PLEASE WAIT... Streaming data to local storage.", info=True) %}
      {% do log("=======================================================================", info=True) %}

      {# 3. DuckDB Optimizations #}
      {% do run_query("SET preserve_insertion_order = false;") %}
      {% do run_query("SET partitioned_write_max_open_files = 10;") %}

      {# 4. Build Export Query #}
      {% set export_query %}
          COPY (
              SELECT * FROM {{ ref('fct_telemetry') }}
              WHERE 1=1
              {% if target_session %}
                  AND session_key = {{ target_session }}
              {% endif %}
              {% if target_meeting %}
                  AND meeting_key = {{ target_meeting }}
              {% endif %}
          )
          TO 'F:/DaTech/Production/f1_data_project/output/telemetry_data'
          (
              FORMAT PARQUET,
              PARTITION_BY (meeting_key, session_key, driver_number),
              OVERWRITE_OR_IGNORE TRUE
          );
      {% endset %}

      {% do run_query(export_query) %}
      {% do log("SUCCESS: Telemetry export completed successfully!", info=True) %}
      {% do log("=======================================================================", info=True) %}
      
    {% else %}
      {% do log("Skipping export — export_data flag not set to true.", info=True) %}
    {% endif %}

  {% else %}
    {% do log("Skipping export execution during dbt parsing phase.", info=False) %}
  {% endif %}

{% endmacro %}
```

**What each section does**

1. **`{% if execute %}`:** dbt *parses* macros before running. Without this guard, the export would fire during parsing.
2. **`var('export_data', false)`:** a **safety switch**. Nothing is exported unless you pass `export_data: true`.
3. **Scoping variables:** `target_session_key` or `target_meeting_key` limit the export to new data (incremental export). With neither, it exports everything.
4. **DuckDB settings:** `preserve_insertion_order = false` lets DuckDB stream without remembering row order (a large memory saver); `partitioned_write_max_open_files = 10` caps simultaneously open partition files.
5. **`COPY (...) TO ... PARTITION_BY (meeting_key, session_key, driver_number) OVERWRITE_OR_IGNORE TRUE`:** writes the folder tree, replacing files that already exist.

Create the output folder first, then run:

```bash
mkdir -p output/telemetry_data

# Export one session
dbt run-operation export_fct_telemetry --vars '{export_data: true, target_session_key: 9158}'

# Export a whole Grand Prix weekend
dbt run-operation export_fct_telemetry --vars '{export_data: true, target_meeting_key: 1219}'

# Export everything (long: tens of minutes)
dbt run-operation export_fct_telemetry --vars '{export_data: true}'
```

> **`--vars`, not `--args`.** The macro reads **project variables** with `var()`, so values must arrive through `--vars`. `--args` feeds a macro's *function parameters*, which this macro does not declare, and the export would silently print "Skipping export". The Airflow task in Chapter 8 uses `--vars` for the same reason.

**Read the result.** The output looks like:

```text
output/telemetry_data/
  meeting_key=1219/
    session_key=9158/
      driver_number=1/
        data_0.parquet
```

Any tool (DuckDB, Power BI via a connector, Spark) can read this folder. **Partition pruning** means `WHERE session_key = 9158` reads one folder, not the whole lake.

#### **Task 5: Safe Memory and Temp Settings**

In `profiles.yml` (Chapter 5) the temp folder is already set. Create it so spilling can work:

```bash
mkdir -p .duckdb_tmp
```

If you need to cap memory (for example on a shared machine), DuckDB accepts `memory_limit` in `settings`:

```yaml
      settings:
        memory_limit: "8GB"
        temp_directory: "F:/DaTech/Production/f1_data_project/.duckdb_tmp"
```

### **Learner Pitfalls & Troubleshooting**

* **The Problem:** `Out of Memory Error: Allocation failure`.
* **Why it Happens:** a join plus sort, or a join between two high-frequency tables, needs more RAM than available.
* **The Solution:** use ASOF for time series, drop unnecessary global `ORDER BY`, set `preserve_insertion_order = false` for exports, ensure `temp_directory` exists, and process one session at a time (the incremental model and the export scoping do exactly this).

* **The Problem:** a **view** over telemetry feels slow every time you query it.
* **Why it Happens:** views store only the query, so the expensive join is recomputed on each read.
* **The Solution:** use an incremental **table** for repeated reads, or export once to partitioned Parquet and query that.

* **The Problem:** `IO Error: Cannot open file ... No such file or directory` during `COPY`.
* **Why it Happens:** `COPY` will not create missing parent folders.
* **The Solution:** `mkdir -p output/telemetry_data` before the first export.

* **The Problem:** `dbt run` finishes in seconds and the table is empty after adding new sessions.
* **Why it Happens:** an incremental model with `NOT EXISTS (session_key)` skips sessions already present, so you see nothing new unless the session is genuinely new.
* **The Solution:** add the new session's staging data first, or use `--full-refresh` when the logic changes.

* **The Problem:** the export prints "Skipping export".
* **Why it Happens:** the safety switch `export_data` was not set, or was passed with `--args` instead of `--vars`.
* **The Solution:** pass `--vars '{export_data: true}'` and confirm the log line "Starting ... Parquet export".

---

## **Chapter 8: Closing the Loop: Running dbt From Airflow**

### **Chapter Overview**

So far you trigger Airflow, then separately trigger dbt by hand. In this chapter the DAG calls dbt itself, passing the sessions it just extracted so the telemetry export is **scoped to new data**. **[CODE FINAL, RUN NOT CONFIRMED]** The build logs contain the final DAG code, but no log of a successful end-to-end run with the dbt task, and the last logged DAG run failed for an unrelated reason (see Chapter 3's last pitfall).

**Concepts introduced in this chapter**

* **Orchestration boundary:** Airflow decides *when and what*, dbt decides *how to transform*. Airflow should call dbt, not reimplement it.
* **`subprocess`:** running a command-line program from Python.
* **`shlex.quote`:** safely escapes a value before placing it in a shell command, preventing injection.
* **Task dependency (`>>`):** the dbt task waits for `verify_and_load`.

### **Step by Step Procedures**

#### **Task 1: Make dbt Available Inside the Airflow Container**

The final DAG runs `dbt` **inside the Airflow container**, and it expects two locations: the project at **`/opt/airflow/dbt/f1_data_project`** and a profiles folder at **`/opt/airflow/.dbt`** (override with the `DBT_PROFILES_DIR` environment variable). The compose file in Chapter 2 provides none of this, so the steps below are **[ADDED FOR THIS BOOK]**:

1. **Install dbt in the image.** For example set `_PIP_ADDITIONAL_REQUIREMENTS: "dbt-duckdb"` under the `environment:` block in `docker-compose.yaml` (convenient for development; build a custom image for production).
2. **Mount the project and a container profile.** Add under `volumes:`
   * `- ../dbt_f1_transformations:/opt/airflow/dbt/f1_data_project`
   * `- ./dbt_profiles:/opt/airflow/.dbt` (a folder containing a container-specific `profiles.yml`)
3. **Use Linux paths inside the container.** The `F:/...` paths from Chapters 5 and 7 do **not exist in the container**. The container profile must point the DuckDB file, the Parquet glob and the export folder at mounted paths (for example `/opt/airflow/data/raw/...`), and `sources.yml` and the export macro need the same treatment (for example by moving the root path into a dbt variable).

> **Why this matters.** This is the most likely reason the dbt task would fail on first run. It is an inference from the project files, not an error recorded in the logs.

#### **Task 2: Add the dbt Task to the DAG**

Add the block below inside the `with DAG(...)` body of **extract_openf1_dag.py**, after `load_task` is defined, and add `import os`, `import shlex`, `import subprocess` and `from airflow.exceptions import AirflowException` at the top of the file, plus these two constants below the imports:

```python
DBT_PROJECT_DIR = "/opt/airflow/dbt/f1_data_project"
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/.dbt")
```

Then add `"dbt"` to the DAG's `tags` list, and paste the task:

```python
    # ==========================================
    # TASK: DBT TRANSFORMATION & EXPORT
    # Receives all_sessions XComArg — same single reference used throughout.
    # ==========================================
    @task
    def run_dbt_telemetry(session_keys_list: list):
        """
        Runs dbt incrementally against fct_telemetry, then exports to Parquet
        via the export_fct_telemetry macro.

        Incremental logic lives in the SQL model (NOT EXISTS against {{ this }}),
        so no --vars are needed for the build step.
        --vars are passed only to the run-operation export macro.
        """
        if not session_keys_list:
            logging.info("No session keys to process. Skipping dbt.")
            return

        env = {**os.environ, "DBT_PROFILES_DIR": DBT_PROFILES_DIR}

        def run_cmd(cmd: str):
            logging.info(f"Executing: {cmd}")
            result = subprocess.run(
                cmd, shell=True, capture_output=True,
                text=True, cwd=DBT_PROJECT_DIR, env=env
            )
            if result.returncode != 0:
                raise AirflowException(
                    f"Command failed:\n{result.stderr}\n{result.stdout}"
                )
            logging.info(result.stdout)

        # Step 1: Incremental build — model uses NOT EXISTS against {{ this }},
        # so it automatically skips sessions already present in the table.
        run_cmd("dbt run --select fct_telemetry")

        # Step 2: Export via macro — vars determine session scope for the COPY statement.
        if len(session_keys_list) == 1:
            vars_json = (
                f'{{"target_session_key": {session_keys_list[0]}, "export_data": true}}'
            )
            logging.info(f"Incremental Parquet export for session: {session_keys_list[0]}")
        else:
            vars_json = '{"export_data": true}'
            logging.info(
                f"Full historical Parquet export for {len(session_keys_list)} sessions."
            )

        run_cmd(
            f"dbt run-operation export_fct_telemetry --vars {shlex.quote(vars_json)}"
        )
        logging.info("✅ dbt transformation and export completed successfully.")

    dbt_task = run_dbt_telemetry(all_sessions)
```

Then connect it after the loading step:

```python
    # dbt runs after load is confirmed; receives all_sessions for export scoping
    load_task >> dbt_task
```

**What to notice**

* **Scoping:** the dbt *build* is incremental by itself (Chapter 7's `NOT EXISTS`). The *export* is scoped only when the run extracted **exactly one** session; with several sessions it falls back to a full export, so single-session runs are the cheap path.
* **`shlex.quote`:** session values are untrusted strings once they reach a shell command; always quote them.
* **Failing loudly:** a non-zero dbt exit code raises `AirflowException`, so the task turns red and retries rather than silently succeeding.
* **Separation:** the task shells out to the same dbt commands you ran by hand, so you can debug them in a terminal.

#### **Task 3: Where Cosmos Fits (Planned)**

`airflow/dags/dbt_cosmos_dag.py` was scaffolded for **Astronomer Cosmos**, a library that renders each dbt model as its own Airflow task so lineage appears in the Airflow graph. **[SCAFFOLDED]** The file is empty in the build logs; the production DAG uses the subprocess approach above. Treat Cosmos as the next learning step once the subprocess version works.

### **Learner Pitfalls & Troubleshooting**

* **The Problem:** `dbt: command not found` in the task log.
* **Why it Happens:** dbt is installed on your host's venv, not inside the container.
* **The Solution:** install it in the image (Task 1) and re-create the containers.

* **The Problem:** the task cannot find `profiles.yml` or the DuckDB file.
* **Why it Happens:** paths point to the Windows host, or the folder is not mounted.
* **The Solution:** mount the project and use container-valid paths.

* **The Problem:** the DuckDB file is "locked" when dbt runs from Airflow.
* **Why it Happens:** a SQL tool, Power BI or a second dbt process holds the file (DuckDB allows one writer).
* **The Solution:** disconnect interactive tools before the scheduled run, or schedule dbt when nobody is querying.

---

## **Chapter 9: Serving the Data: SQL Tools and Power BI**

### **Chapter Overview**

A warehouse nobody can query is a hobby. This chapter connects tools to the DuckDB file: a terminal UI (**Harlequin**), a GUI (**DBeaver**), and **Power BI** over **ODBC**. It also contains the best data-quality incident of the project. **[VERIFIED]** (all 16 mart tables loaded into Power BI).

**Concepts introduced in this chapter**

* **ODBC:** a standard driver interface that lets BI tools talk to databases.
* **Single-writer rule:** DuckDB lets one process **write** at a time. Open the file read-only for analysis whenever you can.
* **Schema drift:** the shape or type of data changing between files.
* **Materialising a view as a table:** storing the result so BI tools read ready values instead of recomputing a heavy query.

### **Step by Step Procedures**

#### **Task 1: Explore With Harlequin (Terminal)**

```bash
pip install harlequin harlequin-duckdb
harlequin "F:/DaTech/Production/f1_data_project/f1_local_warehouse.duckdb"
```

Use the left panel to browse tables and the editor to run SQL (**Ctrl + Enter**). Quit with **Ctrl + Q**. Add `--read-only` (if your version supports it) so you never block dbt.

#### **Task 2: Explore With DBeaver (GUI)**

1. Install DBeaver, choose **New Database Connection > DuckDB**.
2. Set **Path** to the `.duckdb` file. Tick **Open in read-only mode** if offered.
3. After a dbt run, **disconnect** in DBeaver so the lock is released.

#### **Task 3: Connect Power BI**

1. Install the **DuckDB ODBC driver** and create a **System DSN** pointing at the `.duckdb` file.
2. In Power BI: **Get Data > ODBC**, choose the DSN, and select the **16 mart tables** (2 dimensions and 14 facts).
3. In the relationship view, link facts to dimensions on `session_key` and `driver_number` (the star schema from Chapter 4).

For laptops with limited RAM, reduce parallel loading in Power BI's Data Load options (for example **parallel loading = 1**) and consider setting a DuckDB `temp_directory` (Chapter 5) so large queries can spill.

#### **Task 4: The `+2 LAPS` Incident (Schema Drift in Practice)**

This is the full story, in order, because every step teaches something.

1. **Symptom.** Power BI refresh fails loading interval data.
2. **First diagnosis.** Interval gaps are numbers like `1.2` but lapped cars have text like `+2 LAPS`. Comparing text to a number fails.
3. **First fix (necessary, not sufficient).** Staging keeps `interval` and `gap_to_leader` as **text**; the mart converts with `TRY_CAST`, keeps the text in `lapped_status`, and the model is materialised as a **table**.
4. **The error came back**, now at model creation: `Conversion Error: Could not convert string '+2 LAPS' to DOUBLE`.
5. **Real root cause.** The problem was never in the models. `read_parquet` on a glob infers each column's type from the **first file**. If that file stores gaps as numbers (DOUBLE) and a later file has strings, the later file cannot be read. The Airflow sanitiser fixes mixing **inside** one file but not **between** files.
6. **Real fix.** `union_by_name=true` on the source (Chapter 5), which reconciles types across files.
7. **Verification.** `dbt run --select stg_intervals+` then refresh Power BI.

**What it teaches**

* A fix at the wrong layer can look right and fail later. Read the error's *location*.
* Prevent the class of error where it begins: **schema contracts at the source**.
* Keep raw text columns raw, convert in the mart with `TRY_CAST`, and keep the original text for analysis.

### **Learner Pitfalls & Troubleshooting**

* **The Problem:** the VS Code SQLTools extension for DuckDB fails to load or connect.
* **Why it Happens:** the Node-based DuckDB driver bundled with the extension is compiled for specific Node versions and often breaks on Windows.
* **The Solution:** use Harlequin or DBeaver, which this project used successfully.

* **The Problem:** Power BI hangs or is killed on a large table.
* **Why it Happens:** too many parallel loads plus memory pressure.
* **The Solution:** lower parallelism, import only the tables you need, aggregate heavy tables (telemetry) before importing, or read the exported partitioned Parquet.

* **The Problem:** dbt cannot write because the file is locked.
* **Why it Happens:** a tool opened the file read-write.
* **The Solution:** open read-only, or disconnect before running dbt.

---


## **Chapter 10: Infrastructure as Code (OpenTofu and GCP)**

### **Chapter Overview**

Clicking around a cloud console builds infrastructure nobody can reproduce. **Infrastructure as Code (IaC)** describes servers and storage in text files you can version, review and rebuild. In this chapter you write the OpenTofu configuration for the project's **cloud data lake** on Google Cloud Storage.

**Status.** `tofu init` and `tofu plan` are **[VERIFIED]** in the build logs. The logs end before `tofu apply` is confirmed, and BigQuery datasets, Cloud Composer and Cloud Run are **[SCAFFOLDED]** (named in the plan, with no configuration).

**Concepts introduced in this chapter**

* **OpenTofu:** the open-source fork of Terraform. It reads `.tf` files written in **HCL** and makes the cloud match them. It uses the same provider registry, which is why the source below still says `hashicorp/google`.
* **Provider:** the plugin that talks to one platform (Google Cloud here).
* **Resource:** one thing to create (a bucket).
* **Variable and `terraform.tfvars`:** inputs to the configuration, and the file that supplies them.
* **Plan versus apply:** `plan` previews changes and changes nothing; `apply` makes them.
* **State:** a record of what OpenTofu has created. Never commit it.

### **Prerequisites**

* A Google Cloud project with billing enabled and its **Project ID** (not the display name).
* `gcloud` and `tofu` installed (Chapter 1).

### **Step by Step Procedures**

#### **Task 1: Authenticate**

```bash
gcloud auth login
gcloud auth application-default login
```

The second command creates the credentials OpenTofu uses.

#### **Task 2: Write the Configuration**

In **opentofu/provider.tf**:

```hcl
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
  zone    = var.gcp_zone
}
```

In **opentofu/variables.tf**:

```hcl
variable "gcp_project_id" {
  description = "The ID of your Google Cloud project"
  type        = string
  # No default, so the value is supplied explicitly
}

variable "gcp_region" {
  description = "The GCP region to deploy resources to"
  type        = string
  default     = "us-central1"
}

variable "gcp_zone" {
  description = "The specific zone within the region"
  type        = string
  default     = "us-central1-a"
}
```

In **opentofu/main.tf**:

```hcl
resource "google_storage_bucket" "data_lake" {
  # Bucket names are globally unique, so the project ID is appended
  name     = "falcon-fleet-f1-data-lake-${var.gcp_project_id}"
  location = var.gcp_region

  # Lets OpenTofu delete a non-empty bucket. Fine for development,
  # turn it off in production.
  force_destroy = true

  uniform_bucket_level_access = true
}

output "data_lake_bucket_name" {
  value       = google_storage_bucket.data_lake.name
  description = "The name of the F1 data lake bucket"
}
```

In **opentofu/terraform.tfvars** (use your real ID):

```hcl
gcp_project_id = "my-real-project-id-123456"
```

#### **Task 3: Init, Plan, Apply**

```bash
cd opentofu
tofu init       # downloads the Google provider and writes .terraform.lock.hcl
tofu plan       # previews: "Plan: 1 to add, 0 to change, 0 to destroy."
tofu apply      # type "yes" to create the bucket
```

**Commit `.terraform.lock.hcl`** so everyone gets the same provider version. Do **not** commit `*.tfstate` or the `.terraform/` folder (the `.gitignore` from Chapter 1 already excludes them).

#### **Task 4: Where the Cloud Pieces Go Next (Planned)**

The project's design points to these steps, none of which has working code in the logs:

* **`gcs_loader.py`:** upload Parquet from `airflow/data/raw` to the bucket.
* **BigQuery** as a cloud warehouse with `dbt-bigquery` replacing `dbt-duckdb` in a new profile target.
* **Cloud Composer** (managed Airflow) running the same DAGs.

Each is an extension exercise. Because the local pipeline is already modular (sources, staging, marts), swapping the warehouse mostly means changing `sources.yml` and the profile, plus any DuckDB-specific SQL such as `ASOF JOIN` that BigQuery does not support in the same form.

### **Learner Pitfalls & Troubleshooting**

* **The Problem:** the plan shows a bucket name containing the text `your-actual-gcp-project-id`.
* **Why it Happens:** the placeholder from the instructions was pasted literally into `terraform.tfvars`. This happened in the build.
* **The Solution:** replace it with the real Project ID before `apply`. A plan is the cheap place to catch this.

* **The Problem:** `Error 403` or `could not find default credentials`.
* **Why it Happens:** the application-default credentials were never created, or belong to another project.
* **The Solution:** run `gcloud auth application-default login` and confirm the project.

* **The Problem:** `bucket name already exists`.
* **Why it Happens:** bucket names are global across all of Google Cloud.
* **The Solution:** keep the project ID suffix, or add your own unique suffix.

---

## **Chapter 11: Professional Workflow: Git and CI/CD**

### **Chapter Overview**

Version control is how a project survives mistakes. This chapter covers the **branch-based workflow** used in the build **[VERIFIED]** and explains the CI/CD pieces that were scaffolded **[SCAFFOLDED]**.

### **Step by Step Procedures**

#### **Task 1: The Feature Branch Workflow**

1. Update `main`: `git checkout main && git pull`
2. Branch: `git checkout -b feature/openf1-extraction` (the build worked on this branch)
3. Work, then stage and commit in small logical steps:

```bash
git add airflow/dags airflow/plugins
git commit -m "feat: dynamic task mapping for OpenF1 extraction"
```

4. Push the branch: `git push -u origin feature/openf1-extraction`
5. Open a **Pull Request** on GitHub, review the diff, and **merge into `main`**.

Good commits are small, named for *what and why*, and never include data (`airflow/data`, `*.duckdb`, `output/`) or secrets. If you committed something large by mistake, remove it from tracking with `git rm --cached <path>` and add it to `.gitignore`.

#### **Task 2: Understand the CI/CD Scaffold**

`.github/workflows/deploy_to_composer.yml` and `scripts/deploy_to_composer.sh` exist as **empty files**. The intent recorded in the build: when a pull request is **merged into `main`**, GitHub Actions should (1) check the code and (2) copy the DAGs and plugins into the Composer bucket. The notion of a pipeline that validates before it deploys is the key idea:

* **Continuous Integration (CI):** on every pull request, run automatic checks (Python syntax, `dbt parse`, `dbt compile`).
* **Continuous Deployment (CD):** on merge to `main`, ship the code to the environment.

Below is a **reference sketch** to complete the exercise. It is **[ADDED FOR THIS BOOK]** and has **not** been run; it also assumes a Composer environment, a bucket and credentials that this project does not yet have.

```yaml
name: ci
on:
  pull_request:
    branches: [main]
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install dbt-duckdb
      - run: python -m compileall airflow/dags airflow/plugins
```

Keep deployment (the Composer upload) as a second, separate job that only runs on pushes to `main`, and store credentials in **GitHub Actions secrets**, never in the repository.

### **Learner Pitfalls & Troubleshooting**

* **The Problem:** `fatal: Unable to create '.git/index.lock': File exists`.
* **Why it Happens:** a previous Git process crashed or is still running and left a lock file.
* **The Solution:** make sure no Git process or editor integration is running, then delete `.git/index.lock`.

* **The Problem:** a push is rejected because a file is too large.
* **Why it Happens:** a Parquet or `.duckdb` file was committed.
* **The Solution:** remove it from history or tracking and rely on `.gitignore`; data is re-creatable from the pipeline.

---

## **Chapter 12: Operations and Troubleshooting Playbook**

### **Daily Start-Up and Shut-Down**

```bash
# Start
cd airflow && docker compose up -d
source venv/Scripts/activate        # from the project root, if not already active

# Stop
cd airflow && docker compose down
```

Before running dbt, **close** SQL tools that hold the DuckDB file.

### **The Full Run Order**

1. Trigger the Airflow DAG with `latest` (or a specific `session_key`).
2. Confirm Parquet files in `airflow/data/raw/`.
3. `dbt run` (staging and marts), then `dbt test`.
4. For telemetry: `dbt run --select fct_telemetry`, then the export with `--vars`.
5. Refresh Power BI or query with Harlequin.

### **Deep Clean**

When dbt behaves strangely after many edits, clear generated artefacts, then rebuild:

```bash
cd dbt_f1_transformations
dbt clean
dbt deps        # only if you use packages
dbt run
```

### **Consolidated Error Index**

| Symptom | Root cause | Chapter |
|---|---|---|
| `docker: command not found` | Terminal started before install | 1 |
| Broken DAG | Missing `__init__.py` or top-level API code | 2 |
| Tasks stuck in queued | Pool missing or misspelled | 2 |
| 401 on every call | Live-session lockout | 2 |
| 422 on telemetry | Request too large | 2, 3 |
| 429 | Rate limit | 2, 3 |
| `ArrowInvalid` | Mixed types in a column | 3 |
| Old extractor behaviour | Stale module under a new DAG | 3 |
| `Could not find profile` | Profile name mismatch | 5 |
| No files match pattern | Wrong glob or root path | 5 |
| Lock on `.duckdb` | Another process holds it | 5, 9 |
| `+2 LAPS` conversion error | Type drift between Parquet files | 5, 9 |
| `\\.tmp` path error | Partitioned `external` write on Windows | 7 |
| Out of memory | Stacked blocking operators | 7 |
| dbt task fails in Airflow | dbt or paths not available in the container | 8 |
| Plan shows placeholder ID | `tfvars` not edited | 10 |
| `index.lock` exists | Stale Git lock | 11 |

---

## **Appendix A: Final Directory Tree**

```text
f1_data_project/
├── .github/workflows/deploy_to_composer.yml        [SCAFFOLDED]
├── .gitignore
├── Makefile
├── README.md
├── airflow/
│   ├── docker-compose.yaml
│   ├── requirements.txt
│   ├── dags/
│   │   ├── .airflowignore
│   │   ├── extract_openf1_dag.py
│   │   ├── dbt_cosmos_dag.py                       [SCAFFOLDED]
│   │   └── modules/
│   │       ├── event_extractor.py
│   │       ├── universal_extractor.py
│   │       └── gcs_loader.py                       [SCAFFOLDED]
│   ├── plugins/hooks/openf1_hook.py
│   ├── data/raw/                                   (Parquet files from Airflow)
│   └── logs/
├── dbt_f1_transformations/
│   ├── dbt_project.yml
│   ├── macros/export_fct_telemetry.sql
│   └── models/
│       ├── staging/        sources.yml + 18 stg_*.sql
│       ├── intermediate/   (empty; reserved)
│       └── marts/
│           ├── core/       dim_drivers.sql, dim_sessions.sql
│           ├── analytics/  14 fct_*.sql
│           └── schema.yml
├── opentofu/               main.tf, provider.tf, variables.tf, terraform.tfvars
├── output/telemetry_data/  (partitioned Parquet lake)
├── scripts/deploy_to_composer.sh                   [SCAFFOLDED]
└── f1_local_warehouse.duckdb
```

## **Appendix B: The 18 OpenF1 Endpoints**

| Endpoint | Content | Extraction scope |
|---|---|---|
| `meetings` | Grand Prix weekends | Global |
| `sessions` | Practice, qualifying, sprint, race | Global |
| `drivers` | Driver and team details | Global |
| `car_data` | Speed, rpm, gear, throttle, brake, DRS | All sessions (per driver, chunked) |
| `location` | x, y, z position on track | All sessions (per driver, chunked) |
| `laps` | Lap and sector times | All sessions |
| `pit` | Pit stops | All sessions |
| `stints` | Tyre stints | All sessions |
| `position` | Race positions over time | All sessions |
| `weather` | Air and track conditions | All sessions |
| `team_radio` | Radio recordings | All sessions |
| `race_control` | Flags and incidents | All sessions |
| `session_result` | Final classification | All sessions |
| `intervals` | Gaps between cars | Race sessions |
| `overtakes` | Passing events | Race sessions |
| `championship_drivers` | Driver standings | Race sessions |
| `championship_teams` | Constructor standings | Race sessions |
| `starting_grid` | Grid positions | Qualifying, sprint, race |

## **Appendix C: Glossary**

* **ASOF join:** a join that matches each row to the nearest earlier row in another table, by time.
* **Backoff:** waiting progressively longer between retries.
* **Columnar storage:** storing data by column, as Parquet does.
* **DAG:** Directed Acyclic Graph, a set of tasks with a fixed order and no loops.
* **Dimension / Fact:** descriptive table versus event or measurement table.
* **ELT:** extract, load, then transform inside the warehouse.
* **Fan-out:** one task producing many parallel tasks.
* **Glob:** a filename pattern with `*`.
* **Hook:** a reusable connection class in Airflow.
* **Idempotent:** safe to run repeatedly with the same result.
* **Incremental model:** a model that only processes new data after the first build.
* **Materialisation:** how dbt stores a model (view, table, incremental).
* **Partition pruning:** skipping folders that cannot match a filter.
* **Pool:** an Airflow limit on concurrent tasks.
* **Schema drift:** data shape or types changing across files.
* **Slot:** one unit of capacity in a pool.
* **Spill to disk:** using temporary files when memory is full.
* **Star schema:** facts surrounded by dimensions.
* **XCom:** Airflow's mechanism for passing small values between tasks.
