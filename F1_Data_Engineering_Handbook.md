# **Formula 1 Data Engineering: From API to Cloud Warehouse**

***A Step-by-Step Course Book and Project Documentation***

**Repository:** [github.com/Caleb-Muinde83/f1_data_project](https://github.com/Caleb-Muinde83/f1_data_project)

This book turns the raw build logs of a real project into a **curriculum**. You will start with an empty folder and finish with a working **Formula 1 analytics warehouse**: raw data pulled from the **OpenF1 API** by **Apache Airflow**, stored as **Parquet**, cleaned and modelled by **dbt** inside **DuckDB**, explored with SQL tools, visualised in **Power BI**, and prepared for the cloud with **OpenTofu** on **Google Cloud**.

**This is the third edition of the handbook.** The chapters and the order are unchanged: new material was added *inside* the chapter where it belongs, and a few code blocks were replaced with the current version from the repository. Section **What Changed in This Edition** (just below the chapter map) lists what the second edition moved, and the table **What Changed in the Third Edition** under it lists the third edition's changes, which are confined to Chapters 1, 3, 5, 6, 7, 8, 11 and 12, one renamed variable in Chapter 10, and the appendices. The third edition is about **session-scoped incremental builds**: each extraction run now builds only its own sessions.

Every chapter follows the same rhythm: **why** the technology exists, **what** you will build, **how** to build it step by step, and a **Learner Pitfalls & Troubleshooting** section made of errors that were actually hit while building this project, each with its root cause and fix.

---

## **How to Use This Book**

* **Follow the chapters in order.** Each chapter assumes the files and tools from the one before it.
* **Type the commands, read the explanations.** Every code block says which file it belongs to. Every concept is explained the first time it appears.
* **Do not skip the pitfalls.** They are the most valuable part. Most of the real learning in this project came from things breaking.
* **Look for the status tags.** Not everything in the project is at the same level of completion. See the next section.
* **Run the test suite before you change anything.** `make ci-test` builds the whole project on a generated dataset (no API, no Docker) and takes about a minute. Chapter 11 explains it; use it to prove your own copy works before you start editing.
* **Replace the placeholder path.** Every absolute path uses `F:/DaTech/Production/f1_data_project` as the **project root**. Swap in the absolute path of your own clone (for example `/home/you/f1_data_project` on Linux or macOS).

---

## **Project Status: What Is Built and What Is Not**

This book separates **what the build logs prove** from **what is only scaffolded or planned**, so you always know how much to trust each part.

| Tag | Meaning |
|---|---|
| **[VERIFIED]** | Code was run and the build logs show it working. |
| **[CODE FINAL, RUN NOT CONFIRMED]** | Code was reviewed and finalised, but the logs do not show a successful end-to-end run. |
| **[SCAFFOLDED]** | The file or folder exists in the project skeleton, but no working implementation appears in the build logs. |
| **[VERIFIED ON SYNTHETIC DATA]** | New in this edition. The code builds and passes its tests on a **generated** OpenF1-shaped dataset (`make ci-test`), but has not been proven on real OpenF1 data. |
| **[ADDED FOR THIS BOOK]** | Not in the build logs. Added to close a gap, and explained where it appears. |

| Layer | Status |
|---|---|
| Project skeleton, Git workflow | **[VERIFIED]** |
| Airflow in Docker, hook, pool, universal extractor | **[VERIFIED]** |
| Extraction hardening (chunking, backoff, sanitiser, Parquet) | **[VERIFIED]** |
| Extraction additions: run metadata, atomic writes, skip-existing, fan-out guard, manifest, failure alerts | **[VERIFIED ON SYNTHETIC DATA]** (unit tests; DAG parsed with real Airflow 2.10.5). The third edition's structured results, manifest statuses and scoped dbt step were then run in Docker on real data: see Chapter 8 |
| dbt + DuckDB: sources, staging, marts, tests | **[VERIFIED]** on real data in the build logs; the current 143-node project (56 models, 1 seed, 86 tests) is **[VERIFIED ON SYNTHETIC DATA]**: `make ci-test` passed on DuckDB 1.5.6 and 2.0.0 for the second edition, and the third edition's full fixtures build passed on DuckDB 2.0.0 (142 pass, 1 warning) |
| Pit strategy: compound pace model, pit loss, undercut/overcut | **[VERIFIED ON SYNTHETIC DATA]** (also built on real data; see Chapter 6 pitfalls) |
| Safety-car risk model and the Monte Carlo strategy simulator | **[VERIFIED ON SYNTHETIC DATA]** (the simulator is unit-tested against hand-calculated totals; **not validated against real races**) |
| Telemetry: ASOF joins, distance axis, derived corners and apex speeds | **[VERIFIED ON SYNTHETIC DATA]** (a full real-data build of `fct_telemetry` took 1 h 25 min: see Chapter 7) |
| Telemetry export to a partitioned Parquet lake | **[VERIFIED]** (a 43 minute export) |
| Team radio transcription (faster-whisper) and `fct_radio_messages` | **[CODE FINAL, RUN NOT CONFIRMED]** (logic unit-tested with a fake model; real speech-to-text never run) |
| Harlequin, DBeaver, Power BI over ODBC (16 mart tables in the first-edition build) | **[VERIFIED]** |
| Streamlit dashboards | **[VERIFIED ON SYNTHETIC DATA]** (104 headless tests; `docs/CHANGES.md` records further fixes made after running the pages on real data) |
| MetricFlow semantic layer (7 semantic models, 22 metrics) | **[VERIFIED ON SYNTHETIC DATA]** (config validation plus 7 metrics cross-checked against hand-written SQL) |
| dbt triggered from inside Airflow (`run_dbt_build`), scoped to the sessions a run wrote | **[VERIFIED]** on real data in Docker (one `latest` run; the multi-chunk plan, `dbt_scope=full` and the full-refresh guard are unit-tested only) |
| Session-scoped incremental telemetry (replace-by-session, scope-aware tests, truncation guard, equivalence check) | **[VERIFIED]**: equivalence check on the generated dataset, a damage-and-restore test on a copy of the real warehouse, and one live Docker run that left every other session's row counts unchanged. BigQuery: compile and full build only |
| OpenTofu: `tofu init` and `tofu plan` for a GCS bucket | **[VERIFIED]** |
| OpenTofu: BigQuery datasets | **[CODE FINAL, RUN NOT CONFIRMED]** (no `tofu` binary was available to validate it) |
| BigQuery: portable SQL, raw loader, `dbt-bigquery` target | **[VERIFIED ON SYNTHETIC DATA]** on the BigQuery **emulator**: all 56 tables identical to DuckDB, as of that run. The repository's `docs/CHANGES.md` records later model changes for which `make bq-emulator-test` has not been re-run. Real BigQuery, and incremental re-runs, are untested |
| `gcs_loader.py`, `deploy_to_composer.sh`, Cosmos DAG, CI workflows | **[CODE FINAL, RUN NOT CONFIRMED]** (the main CI workflow's steps were run locally; the GitHub Actions runs themselves were not) |
| Cloud Composer environment | **[SCAFFOLDED]** |

> **Honest note on the title.** The warehouse that actually runs today is **local** (DuckDB). Chapter 10 now also contains a **BigQuery port** of the whole project, verified table-for-table against DuckDB on a BigQuery *emulator*, but no real cloud warehouse was deployed. Treat the cloud chapters as the **launch pad for your own migration**, not as a finished deployment.

> **How the real-data runs fit in.** The core models were also built on the author's real OpenF1 data. Those runs found defects in the first version of this edition's code, each described as a pitfall where it happened: DuckDB 1.5's `INTEGER`/`BIGINT` optimizer crash and `temp_directory` re-set after a spill (Chapter 5), overlapping stints duplicating laps and practice garage visits counted as pit stops (Chapter 6). Later real-data rounds (sprints, starting grids, race trace, VSC periods, served penalties, telemetry lap windows, the dashboards) are listed in the repository's `docs/CHANGES.md` and `docs/ISSUES_CHECKLIST.md`, and the code in this book is the repository's code for them. The code was re-verified for this edition by running `make ci-test` on synthetic data; it has not been re-run on the real data.

---

## **The Architecture at a Glance**

```
  OpenF1 REST API (18 endpoints)
          |
          |  HTTP + JSON      (rate limits, 422/429/404 handling)
          v
  Apache Airflow (Docker)  ----  Hook -> Universal Extractor -> Dynamic Task Mapping -> run manifest
          |                       \--(opt-in)--> faster-whisper: team radio audio -> transcripts
          |                       \--(opt-in)--> dbt build scoped to the sessions this run wrote (session_keys)
          |  Parquet files (snappy compressed)
          v
  Local data lake   airflow/data/raw/<endpoint>_<session>_extracted.parquet
          |                       \--(opt-in)--> scripts/load_raw_to_bigquery.py -> BigQuery f1_raw
          |  DuckDB reads Parquet directly  (the "Load" in ELT)
          v
  dbt + DuckDB      sources -> staging (views) -> intermediate (int_lap_context) -> marts (dim_ + fct_ tables)
          |             |                                   \
          |             |                                    \--> fct_telemetry -> distance trace -> detected corners
          |             \--> pit strategy, safety-car risk, strategy simulator (Python), radio
          v
  f1_local_warehouse.duckdb   (the same project also builds on BigQuery: --target bigquery)
          |
          +--> Harlequin / DBeaver / SQLTools   (explore with SQL)
          +--> Power BI via DuckDB ODBC         (dashboards)
          +--> Streamlit dashboards             (race overview, pace, strategy, telemetry, safety car, radio)
          +--> MetricFlow semantic layer        (22 governed metrics)
          +--> OpenTofu -> GCS bucket + BigQuery datasets   (cloud launch pad)
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
| **pytest** | Unit tests for Python code | Fast tests that need no API, Docker or cloud |
| **NumPy** | Monte Carlo strategy simulator | Vectorised simulation of thousands of races |
| **faster-whisper** | Optional radio transcription | Self-hosted speech-to-text, in keeping with the project's no-managed-services stance |
| **Streamlit + Plotly** | Interactive dashboards | Python-only, reads DuckDB directly |
| **MetricFlow** | Semantic layer | One governed definition per metric |
| **BigQuery (+ emulator)** | Optional cloud warehouse | The project's SQL is portable; the emulator makes that testable without credentials |

## **Chapter Map**

1. Project Set Up and Initialization
2. Local Orchestration and API Extraction (Airflow and Docker) *(now with secrets in `.env` and dbt mounted)*
3. Hardening the Extraction Pipeline *(now with run metadata, atomic writes, a fan-out guard, a manifest and structured `written`/`skipped`/`empty` results)*
4. Data Modelling Concepts: ELT, Layers, Joins and the Star Schema
5. Data Transformation I: dbt, DuckDB and the Staging Layer *(now with environment-driven profiles and a DuckDB 1.5 pitfall)*
6. Data Transformation II: Building the Analytical Marts *(now with the lap-context layer, pit strategy, safety-car risk and the strategy simulator)*
7. Taming High-Volume Telemetry *(now with the distance axis, derived corners and session-scoped, replace-by-session builds)*
8. Closing the Loop: Running dbt From Airflow *(now implemented, and scoped to the sessions each run wrote)*
9. Serving the Data: SQL Tools, Power BI, Dashboards and the Semantic Layer
10. Infrastructure as Code (OpenTofu and GCP) *(now with the BigQuery port)*
11. Professional Workflow: Git, CI/CD and Testing Everything *(now with the truncation guard and the scoped-build equivalence check)*
12. Operations and Troubleshooting Playbook

Appendices: **A** Final directory tree, **B** the 18 OpenF1 endpoints, **C** glossary.

## **What Changed in This Edition**

If you already worked through the first edition, this is where to look. Nothing was renumbered.

| Chapter | What was added or replaced |
|---|---|
| 1 | Skeleton gains `analytics/`, `dashboards/`, `seeds/`, `tests/`, `ci/` and `docs/`; `.gitignore` additions |
| 2 | `docker-compose.yaml` replaced: secrets from `.env`, a `Dockerfile` (Python 3.11 image with dbt, `pyarrow` and `faster-whisper` baked in), resource limits, dbt project and warehouse mounted. New pitfalls |
| 3 | Extractor and DAG replaced (run metadata, atomic writes, skip-existing, guards, manifest, Slack failure alerts, opt-in dbt/GCS/radio tasks). New Tasks 4 and 5 |
| 5 | `profiles.yml` moves into the project with several targets; sources read an environment path; staging models updated (lap start time, interval timestamps, overtake time) and a radio staging model added. New pitfalls: DuckDB 1.5, `temp_directory` |
| 6 | New intermediate layer (`int_lap_context`); marts replaced and extended (season-aware points, sprint handling, grid mapping, served-penalty handling); new Tasks: pit strategy, safety-car risk, the strategy simulator, radio messages. New pitfalls: overlapping stints, practice pit stops |
| 7 | `fct_telemetry` replaced (portable ASOF, lap window, `telemetry_only_sessions`); new Tasks: distance axis, per-lap KPIs, derived corners and apex speeds. New pitfalls |
| 8 | dbt-inside-Airflow is now implemented rather than described as a to-do |
| 9 | New Tasks 5 and 6: role-based Streamlit dashboards and the MetricFlow semantic layer |
| 10 | OpenTofu gains BigQuery datasets; **the planned cloud steps are replaced by the BigQuery port** (Tasks 4 to 7) |
| 11 | The CI/CD sketch is replaced by the real workflows; new Task 3 runs every test locally |
| 12 | Start-up, run order and error index updated |
| Appendices | Directory tree updated; glossary gains the new terms |

## **What Changed in the Third Edition**

The third edition adds one idea, **session-scoped incremental builds**, and brings the code blocks of the chapters it touches up to the current repository. Chapters 2, 4 and 9 are unchanged.

| Chapter | What was added or replaced |
|---|---|
| 1 | The skeleton script gains `dbt_scope.py`, `dbt_runner.py`, `macros/scope.sql`, and the new CI scripts |
| 3 | Extractor, DAG and `pipeline_utils.py` replaced: structured `written` / `skipped` / `empty` results, statuses in the manifest, three new dbt parameters, the new order of tasks. New pitfall: Airflow hands over lazy sequences, not lists |
| 5 | `dbt_project.yml` replaced (the `session_keys` variable, the `telemetry_group` tags, and two telemetry variables the book had never listed); `stg_car_data` and `stg_location` become the only scoped views |
| 6 | `schema.yml` replaced; scope-aware generic and singular tests; the table of singular tests corrected (32 tests, including three that were never described) |
| 7 | `fct_telemetry`, the distance trace, the lap summary, the delta trace and both corner models replaced with the current files; the scope macros; replace-by-session; the generalised delete macro; masking of unavailable and frozen readings and the nearest-bin corner geometry, which the repository had but the book did not describe; new pitfalls |
| 8 | `run_dbt_build` rewritten around two new Airflow-free modules (`dbt_scope.py`, `dbt_runner.py`); the behaviour table, parameters and expected log; status upgraded to verified; new pitfalls |
| 10 | One renamed variable (`telemetry_only_sessions` is now `session_keys`) |
| 11 | `ci.yml` replaced; new Task 4: the truncation guard and the equivalence check; new pitfalls |
| 12 | Run order and error index updated |
| Appendices | Directory tree updated; glossary gains the new terms |

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

# 2. CI/CD workflows (Chapter 11)
mkdir -p .github/workflows
touch .github/workflows/deploy_to_composer.yml

# 3. Airflow
mkdir -p airflow/dags/modules airflow/plugins/hooks airflow/data/raw
touch airflow/docker-compose.yaml airflow/requirements.txt airflow/Dockerfile
touch airflow/plugins/__init__.py airflow/plugins/hooks/__init__.py
touch airflow/plugins/hooks/openf1_hook.py airflow/plugins/slack_alerts.py
touch airflow/dags/.airflowignore airflow/dags/extract_openf1_dag.py
touch airflow/dags/dbt_cosmos_dag.py                      # Chapter 8 (optional)
touch airflow/dags/modules/universal_extractor.py
touch airflow/dags/modules/dbt_scope.py airflow/dags/modules/dbt_runner.py   # Chapter 8: scope builder and dbt runner
touch airflow/dags/modules/event_extractor.py
touch airflow/dags/modules/gcs_loader.py                  # Chapter 10

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
touch scripts/deploy_to_composer.sh
chmod +x scripts/deploy_to_composer.sh
touch scripts/load_raw_to_bigquery.py                     # Chapter 10

# 7. Added in the second edition: analytics, dashboards, tests, CI helpers, docs
mkdir -p analytics/f1_strategy analytics/tests            # Chapter 6: strategy simulator
mkdir -p dashboards/f1_dash dashboards/tests              # Chapter 9 (f1_dash grows charts/, components/ and workspaces/)
mkdir -p airflow/tests                                    # Chapter 3
mkdir -p dbt_f1_transformations/seeds dbt_f1_transformations/tests/generic
mkdir -p dbt_f1_transformations/ci dbt_f1_transformations/models/semantic
mkdir -p dbt_f1_transformations/ci/tests                  # Chapter 11: tests for the CI helpers
touch dbt_f1_transformations/macros/scope.sql             # Chapter 7: the session scope
mkdir -p docs
touch airflow/.env.example

echo "F1 Data Project directory structure created successfully!"
```

The full resulting tree, with a note on what goes where, is in **Appendix A**.

**Edit your `.gitignore` now.** Raw Parquet files, the DuckDB warehouse and dbt build artefacts are large and re-creatable, and `airflow/.env` (introduced in Chapter 2) holds passwords. Keep them all out of Git:

```gitignore
venv/
.venv/
airflow/data/
airflow/.env
output/
.ci_raw/
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

We run Airflow inside Docker so that your laptop looks like the cloud environment you will deploy to later. Four files do the work: **requirements.txt**, the **Dockerfile**, **docker-compose.yaml**, and the DAG.

**Step 1: Declare the dependencies**

Open **airflow/requirements.txt** and add:

```text
apache-airflow-providers-google==10.12.0
requests==2.31.0
pyarrow
faster-whisper
```

> **Correction to earlier drafts.** The original file also pinned `pandas==2.1.4`. Pandas 2.1 requires Python 3.9 or newer, while the `apache/airflow:2.8.1` image runs **Python 3.8** (you can see `python3.8` in its log paths). The pandas pin was never exercised, because the first edition's compose file only **mounted** this file into the container; nothing ran `pip install` on it. The extractor in this book does not use pandas, so it was removed.
>
> **Second edition.** `pyarrow` is now listed because the extractor imports it, and `faster-whisper` because the optional radio-transcription task uses it. The **Dockerfile** below installs this file into the image, so nothing is installed when a container starts.

**Step 1b: Bake the dependencies into an image (the Dockerfile)**

Open **airflow/Dockerfile** and paste:

```dockerfile
FROM apache/airflow:2.8.1-python3.11

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt "dbt-duckdb>=1.10,<2" \
    && python -c "from faster_whisper import WhisperModel; WhisperModel('small.en', device='cpu', compute_type='int8')"
```

It starts from the official Airflow image on **Python 3.11**, installs `requirements.txt` plus `dbt-duckdb`, and downloads the `small.en` speech-to-text model once, at build time, so the first transcription does not have to. The compose file builds this image (`build: .`).

**Step 2a: Create your `.env` file (new in the second edition)**

The first edition shipped the database password, the Airflow login and a default secret directly inside the compose file. Anyone with a copy of the repository had them. The compose file now **refuses to start** until you supply them yourself, from a file Git ignores:

```bash
cp airflow/.env.example airflow/.env
```

Open **airflow/.env** and fill in the passwords. Generate the two keys with Python:

```bash
python -c "import secrets; print(secrets.token_hex(32))"                              # AIRFLOW_WEBSERVER_SECRET_KEY
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"   # AIRFLOW_FERNET_KEY (optional)
```

```bash
# Copy to airflow/.env (git-ignored) and fill in. docker compose refuses to start while required values are missing.
AIRFLOW_UID=50000
POSTGRES_PASSWORD=change-me
AIRFLOW_ADMIN_USER=airflow
AIRFLOW_ADMIN_PASSWORD=change-me-too
# python -c "import secrets; print(secrets.token_hex(32))"
AIRFLOW_WEBSERVER_SECRET_KEY=
# python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
AIRFLOW_FERNET_KEY=
# Optional
GCS_RAW_BUCKET=
ALERT_WEBHOOK_URL=
```

**Step 2b: Configure local Airflow (Docker Compose)**

Open **airflow/docker-compose.yaml** and paste the following. It is a streamlined version of the official Apache Airflow Compose setup, trimmed for a laptop: a **LocalExecutor**, one **PostgreSQL** database, an **init** job that creates the database tables and the login user, a **webserver** and a **scheduler**.

```yaml
x-airflow-common:
  &airflow-common
  build: .
  environment:
    &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER:-airflow}:${POSTGRES_PASSWORD:?copy .env.example to .env and set POSTGRES_PASSWORD}@postgres/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY:-}
    AIRFLOW__WEBSERVER__SECRET_KEY: ${AIRFLOW_WEBSERVER_SECRET_KEY:?set AIRFLOW_WEBSERVER_SECRET_KEY in .env}
    AIRFLOW__CORE__PARALLELISM: '2'
    AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: '1'
    AIRFLOW__CORE__DAG_FILE_PROCESSOR_TIMEOUT: '180'
    AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT: '120'
    AIRFLOW__SCHEDULER__SCHEDULER_HEALTH_CHECK_THRESHOLD: '60'
    OMP_NUM_THREADS: '2'
    MKL_NUM_THREADS: '2'
    _PIP_ADDITIONAL_REQUIREMENTS: ''
    # Where dbt, DuckDB and the pipeline look for things INSIDE the container
    DBT_PROJECT_DIR: /opt/airflow/dbt/f1_data_project
    DBT_PROFILES_DIR: /opt/airflow/dbt/f1_data_project
    DBT_TARGET: container
    F1_RAW_DATA_PATH: /opt/airflow/data/raw
    F1_DUCKDB_PATH: /opt/airflow/data/warehouse/f1_local_warehouse.duckdb
    F1_EXPORT_PATH: /opt/airflow/data/export/telemetry
    OPENF1_OUTPUT_PATH: /opt/airflow/data/raw
    GCS_RAW_BUCKET: ${GCS_RAW_BUCKET:-}
    ALERT_WEBHOOK_URL: ${ALERT_WEBHOOK_URL:-}
  volumes:
    - ./dags:/opt/airflow/dags
    - airflow-logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    - ./data:/opt/airflow/data
    - ../dbt_f1_transformations:/opt/airflow/dbt/f1_data_project
  user: "${AIRFLOW_UID:-50000}:0"

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-airflow}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?copy .env.example to .env and set POSTGRES_PASSWORD}
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
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
      _AIRFLOW_WWW_USER_USERNAME: ${AIRFLOW_ADMIN_USER:-airflow}
      _AIRFLOW_WWW_USER_PASSWORD: ${AIRFLOW_ADMIN_PASSWORD:?copy .env.example to .env and set AIRFLOW_ADMIN_PASSWORD}
    depends_on:
      postgres:
        condition: service_healthy

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "127.0.0.1:8080:8080"
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    depends_on:
      airflow-init:
        condition: service_completed_successfully

volumes:
  postgres-db-volume:
  airflow-logs:
```

**Read the file like an engineer**

* **`x-airflow-common` and `<<: *airflow-common`:** a YAML *anchor* and *merge*. Settings are defined once and reused by every Airflow service, so the webserver and scheduler cannot drift apart.
* **`volumes`:** the **file bridge**. `./dags:/opt/airflow/dags` makes your local `dags` folder appear *inside* the container. Edit a file in VS Code and the container sees it instantly, with no rebuild. `./data:/opt/airflow/data` is where Parquet files will land (Chapter 3).
* **`depends_on` with `condition`:** enforces boot order. Postgres must be **healthy**, then `airflow-init` must **complete successfully**, and only then do the webserver and scheduler start.
* **No `version:` line.** Newer Docker Compose prints a warning that the `version` attribute is obsolete, so it is omitted here.
* **`${POSTGRES_PASSWORD:?message}`:** the `:?` makes Compose **stop with your message** if the variable is empty, instead of silently starting with a blank or default password. `${AIRFLOW_UID:-50000}` (with `:-`) means "use this default if unset".
* **`build: .`:** every Airflow service is built from the `airflow/Dockerfile` above, which starts from `apache/airflow:2.8.1-python3.11`. The plain `2.8.1` tag runs **Python 3.8**, and current `dbt-duckdb` does not install on it (see the pitfall below). The `-python3.11` tag is the same Airflow on a newer Python.
* **`_PIP_ADDITIONAL_REQUIREMENTS: ''`:** a feature of the official image that runs `pip install` on every container start. It is left empty on purpose: the packages are baked into the image by the Dockerfile, so containers start quickly.
* **The resource limits:** `AIRFLOW__CORE__PARALLELISM: '2'` and `AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: '1'` cap how many tasks run at once, `OMP_NUM_THREADS` and `MKL_NUM_THREADS` cap the threads each task may use, and the two timeouts give the scheduler longer to parse the DAG file. They keep a laptop usable while the extraction runs.
* **`airflow-logs`:** the logs live in a **named Docker volume**, not in a folder of your repository. `plugins/slack_alerts.py` (Chapter 3) reads the failed task's log from there.
* **The dbt and data mounts:** `../dbt_f1_transformations` appears inside the container at `/opt/airflow/dbt/f1_data_project`, and `./data` at `/opt/airflow/data`. The `DBT_*`, `F1_*` and `OPENF1_OUTPUT_PATH` variables tell dbt, DuckDB and the extractor where things are **inside the container** (Chapter 8 uses them).
* **`127.0.0.1:8080:8080`:** the web UI is bound to your own machine only. Without the `127.0.0.1:` prefix Docker publishes it on every network interface.

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
2. Log in with the **`AIRFLOW_ADMIN_USER`** and **`AIRFLOW_ADMIN_PASSWORD`** you set in `airflow/.env`.
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

Because `./dags` is mounted, you do **not** need to restart Docker. Trigger the DAG again, wait for **extract_meetings** to turn green, then look inside the container for **meetings_2023.json**: `docker compose exec airflow-scheduler ls /opt/airflow/logs` (the logs folder is a Docker volume, so it does not appear in your project folder).


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

Trigger the DAG. Two tasks run at a time, the rest wait, and eighteen JSON files appear in **/opt/airflow/logs** inside the container (`docker compose exec airflow-scheduler ls /opt/airflow/logs`).

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
docker compose up -d --build   # rebuild the image first after changing requirements.txt or the Dockerfile
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

* **The Problem (second edition):** `docker compose up` is expected to stop immediately with `required variable POSTGRES_PASSWORD is missing a value: copy .env.example to .env and set POSTGRES_PASSWORD`.
* **Why it Happens:** that is the `:?` guard working as designed. The compose file no longer contains any password.
* **The Solution:** create `airflow/.env` from `airflow/.env.example` (Step 2a). Run `docker compose` from the `airflow/` folder, because that is where Compose looks for `.env`.

* **The Problem (anticipated, not an error anyone recorded):** installing `dbt-duckdb` into the container fails because no release supports the container's Python.
* **Why it Happens (an inference):** the default `apache/airflow:2.8.1` image runs **Python 3.8** (visible as `python3.8` in its file paths in the build logs), and `dbt-duckdb` 1.10 needs a newer Python. This was reasoned from the version requirements, not reproduced inside Docker.
* **The Solution:** use the `-python3.11` image tag, as the Dockerfile above does (`FROM apache/airflow:2.8.1-python3.11`). If you change the `FROM` line, keep a tag with Python 3.9 or newer.

---


## **Chapter 3: Hardening the Extraction Pipeline**

### **Chapter Overview**

The v1 pipeline from Chapter 2 works, but only for **one hard-coded session** and it writes JSON. A production pipeline must pull **any session or the full history**, survive API failures, and store data in an analytics-friendly format. In this chapter you will upgrade the extractor and the DAG with five production patterns. **[VERIFIED]**

**Concepts introduced in this chapter**

* **Parquet:** a **columnar** file format. Instead of storing row after row, it stores each column together, with types and **snappy compression**. Analytical queries ("average lap time") read only the columns they need. It is typically many times smaller and faster than JSON, and DuckDB reads it directly.
* **Dynamic Task Mapping (`.expand()`):** Airflow creates **one task instance per item in a list that is only known at runtime**. If the list has 25 session keys, you get 25 parallel tasks without writing 25 tasks.
* **Exponential backoff:** after a rate-limit error, wait a little, then longer on each retry (30 s, then 60 s) instead of hammering the server.
* **Idempotency:** running the same job twice gives the same result. Files are named by endpoint **and** session key, so re-running overwrites rather than duplicates.
* **Fan-out guard (now configurable):** with `all`, the full session history would create thousands of mapped tasks. Three parameters control it: `min_session_key` (only sessions at or above this key; the first edition hard-coded 9000), `max_sessions` (default 150) and `allow_large_backfill`. If more sessions would be extracted than `max_sessions`, the task **fails immediately with a message that names the fix** instead of quietly fanning out; tick `allow_large_backfill` only when you mean it.
* **`skip_existing`, `run_dbt_build`, `upload_to_gcs`, `transcribe_radio`:** four more parameters, all default off. They appear in the trigger form with a description each. Three more, `dbt_scope`, `dbt_full_refresh` and `dbt_chunk_size`, only matter when `run_dbt_build` is on and are explained in Chapter 8.
* **Structured extraction results (third edition):** the extractor no longer returns just a path. Every endpoint-and-session extraction returns `{endpoint, session_key, path, status}` where `status` is **`written`** (a Parquet file was written by this run), **`skipped`** (`skip_existing` found the file already on disk) or **`empty`** (the API had nothing). The orchestrator needs the difference: only `written` files change what dbt has to rebuild (Chapter 8).
* **Retries and timeouts:** `retry_exponential_backoff` (3 minutes, then 6, up to 30) gives a rate-limited API time to recover, and `execution_timeout` (3 hours) stops one hung HTTP call from holding a pool slot forever.
* **`on_failure_callback`:** the DAG imports `send_slack_failure_alert` from `plugins/slack_alerts.py`. Once a task has used up its retries it posts a Slack message with the DAG, task, run id, attempt, exception and the tail of the task log, if `ALERT_WEBHOOK_URL` is set (Task 4).
* **`verify_and_load` now writes a manifest:** a JSON audit of the run, including the status of every extraction (Task 4).

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
from datetime import datetime, timedelta, timezone
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

def build_output_path(endpoint: str, session_val) -> str:
    """Deterministic file name: re-running the same endpoint+session overwrites instead of duplicating (idempotent)."""
    base_path = os.getenv("OPENF1_OUTPUT_PATH", "/opt/airflow/data/raw")
    return f"{base_path}/{endpoint}_{session_val}_extracted.parquet"


# Outcome of one endpoint+session extraction. The orchestrator builds the dbt scope from these, so they are explicit:
#   written : a Parquet file was (re)written by this run
#   skipped : skip_existing=True and the file was already on disk; nothing was downloaded
#   empty   : the API had nothing (404/422/no rows); no file was written by this run
WRITTEN, SKIPPED, EMPTY = "written", "skipped", "empty"


def _result(endpoint: str, session_val, path, status: str) -> dict:
    """Structured, JSON-serialisable extraction result (safe to pass through Airflow XCom)."""
    session_key = int(session_val) if str(session_val).isdigit() else str(session_val)
    return {"endpoint": endpoint, "session_key": session_key, "path": path, "status": status}


def extract_and_save_endpoint(endpoint: str, target_session="latest", api_params: dict = None, is_global: bool = False,
                              skip_existing: bool = False, run_id: str = None):
    """Backwards-compatible wrapper: returns the Parquet path, or None when nothing was written or found.
    New code should call extract_endpoint_result, which also says whether the file was written or skipped."""
    return extract_endpoint_result(endpoint, target_session, api_params, is_global, skip_existing, run_id)["path"]


def extract_endpoint_result(endpoint: str, target_session="latest", api_params: dict = None, is_global: bool = False,
                            skip_existing: bool = False, run_id: str = None) -> dict:
    """
    Same extraction as extract_and_save_endpoint, but returns {endpoint, session_key, path, status} with
    status one of written / skipped / empty.
    Universally pulls data from any OpenF1 endpoint. 
    skip_existing: return the existing file untouched when it is already on disk (cheap incremental backfills).
    run_id: optional orchestrator run id, stamped into every row as _airflow_run_id for lineage/audit.
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
    # SKIP FILES THAT ALREADY EXIST (opt-in)
    # ==========================================
    if skip_existing and str(session_val).isdigit():
        existing = build_output_path(endpoint, session_val)
        if os.path.exists(existing):
            logging.info(f"skip_existing=True and {existing} already exists. Skipping /{endpoint} for session {session_val}.")
            return _result(endpoint, session_val, existing, SKIPPED)

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
                    return _result(endpoint, session_val, None, EMPTY)
                elif "429" in error_msg and attempt < 2:
                    wait = 30 * (attempt + 1)
                    logging.warning(f"429 on /drivers for session {session_val}. Sleeping {wait}s (attempt {attempt+1}/3)...")
                    time.sleep(wait)
                else:
                    if attempt == 2 or "429" not in error_msg:
                        raise e
        
        if not drivers:
            logging.warning(f"Failed to retrieve drivers for session {session_val}. Skipping /{endpoint}.")
            return _result(endpoint, session_val, None, EMPTY)
            
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
                    return _result(endpoint, session_val, None, EMPTY)
                elif "429" in error_msg and attempt < 2:
                    wait = 30 * (attempt + 1)  # 30s, 60s
                    logging.warning(f"429 on /{endpoint} session {session_val}. Sleeping {wait}s (attempt {attempt+1}/3)...")
                    time.sleep(wait)
                else:
                    if attempt == 2 or "429" not in error_msg:
                        raise e

    if not records:
        logging.info(f"No records found for /{endpoint} session {session_val}. Skipping save.")
        return _result(endpoint, session_val, None, EMPTY)

    # ==========================================
    # SANITIZE & SAVE TO DISK AS PARQUET
    # ==========================================
    records = sanitize_mixed_and_nested_columns(records)

    # Extraction metadata: lets you audit when/why a file was produced and reconcile row counts later
    extracted_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for row in records:
        row["_extracted_at_utc"] = extracted_at
        row["_source_endpoint"] = endpoint
        row["_airflow_run_id"] = run_id

    output_path = build_output_path(endpoint, session_val)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Write to a temp file then rename, so a crash mid-write can never leave a truncated Parquet file
    # that dbt would later try to read.
    tmp_path = output_path + ".tmp"
    try:
        table = pa.Table.from_pylist(records)
        pq.write_table(table, tmp_path, compression='snappy')
        os.replace(tmp_path, output_path)
        logging.info(f"Success! Saved {len(records)} records to {output_path}")
        return _result(endpoint, session_val, output_path, WRITTEN)
    except Exception as e:
        logging.error(f"Failed to save Parquet file for {endpoint}: {str(e)}")
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise e
```

**How it works, section by section**

1. **`sanitize_mixed_and_nested_columns`:** Parquet requires every column to have **one type**. OpenF1 sometimes returns a number in one row and text in another (for example gap values such as `1.2` and `"+1 LAP"`), or a list inside a column (qualifying durations). The sanitiser finds those columns and converts every value to a string (lists and dictionaries become JSON text). Without it, `pa.Table.from_pylist` crashes with a type error.
2. **Resolving `"latest"`:** `latest` is a convenience alias in the API. The extractor looks up the real session number first, so the filename (`laps_9158_extracted.parquet`) always holds a real ID.
3. **Ultra-heavy endpoint protection:** `car_data` and `location` produce several readings per second per car. The extractor therefore requests **one driver at a time**, with a 2-second pause between requests.
4. **Error handling by status code:**
   * **404:** the data does not exist for that session or driver. Skip gracefully and report the extraction as `empty`.
   * **422:** the request was too large. Switch to **time-window chunking**: ask for 15-minute slices (`date>=` and `date<` filters) between the session start and end.
   * **429:** rate limited. Wait 30 s, then 60 s, then give up.
   * **Anything else:** raise, so Airflow's own retries take over.
5. **Run metadata (new in the second edition):** every row gets three extra columns, `_extracted_at_utc`, `_source_endpoint` and `_airflow_run_id`. When a number in the warehouse looks wrong you can trace it to the exact run and moment that produced it. dbt ignores the columns it does not select.
6. **Skipping files that already exist (`skip_existing`):** with the DAG parameter switched on, a numeric session whose file is already on disk is reported as `skipped` (its path is returned untouched) and **no API call is made**. This makes a large backfill resumable. It is deliberately off by default, and it never applies to `latest`, whose data is still changing.
7. **Atomic writes:** the Parquet file is written to `<name>.tmp` and then renamed with `os.replace`. A crash mid-write can therefore never leave a half-written Parquet file that dbt would later fail to read; the temporary file is removed if the write fails.
8. **Writing the file:** the output is `{endpoint}_{session}_extracted.parquet` in `/opt/airflow/data/raw` (overridable through the `OPENF1_OUTPUT_PATH` environment variable), compressed with snappy. The name is built by `build_output_path`, a small function the skip check and the writer share, so they can never disagree about where a file lives.

9. **Structured results (third edition):** the real work now lives in `extract_endpoint_result`, which returns a small dictionary `{endpoint, session_key, path, status}` (`session_key` is the string `global` for files that are not per session). `extract_and_save_endpoint` is kept as a thin wrapper that returns only the path (or `None`), so older callers and tests keep working. Every early exit that used to `return None` now returns an `empty` result, and a skipped file returns `skipped`. The three statuses are the whole contract between extraction and the dbt step, so they are worth getting exactly right: a session that was *re-extracted* is `written`, one that was found on disk is `skipped`, and one the API had nothing for is `empty`.

> **Why `/opt/airflow/data/raw`?** Compose mounts `./data` into the container (Chapter 2). Files written there appear on your machine in **airflow/data/raw/**, which is exactly where dbt will read them in Chapter 5.

#### **Task 2: Make the DAG Session-Aware**

Replace **airflow/dags/extract_openf1_dag.py** with the DAG below. It adds a **parameter** (`target_session_key`), a metadata task that decides *what* to extract, and mapped extraction tasks.

> **The second edition's DAG is the complete one.** It also contains three **opt-in** tasks that do nothing unless you switch them on in the trigger form: `transcribe_team_radio` (explained in Chapter 6), `upload_raw_to_gcs` (Chapter 10) and `run_dbt_build` (Chapter 8). Read past them for now; every opt-in task is skipped by default, so the DAG behaves exactly like the extraction-only version until you ask for more.
>
> **One file the DAG needs before it will parse (third edition).** The DAG imports `modules/dbt_scope.py` at the top of the file (its `Param` defaults and error type live there). The code for it is in **Chapter 8, Task 2, Step 2**. Create that file before you save this DAG, or the Airflow UI shows an import error for it. `modules/dbt_runner.py` (Step 3) is imported lazily inside the task, so it only has to exist when you tick `run_dbt_build`.

```python
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from hooks.openf1_hook import OpenF1Hook
from slack_alerts import send_slack_failure_alert
from modules.dbt_scope import (
    DEFAULT_CHUNK_SIZE, ScopeError, build_dbt_scope, flatten_results, plan_dbt_commands, radio_transcripts_exist,
)

# ==========================================
# DBT LOCATIONS (inside the Airflow container; see docker-compose.yaml for the mounts)
# ==========================================
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/f1_data_project")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)
DBT_TARGET = os.environ.get("DBT_TARGET", "container")
RAW_DIR = os.environ.get("OPENF1_OUTPUT_PATH", "/opt/airflow/data/raw")

# ==========================================
# DEFAULT ARGS
# ==========================================
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,      # 3 min, 6 min, ... so a rate-limited API gets time to recover
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=3),  # a hung HTTP call must not hold a pool slot forever
    "on_failure_callback": send_slack_failure_alert,    # structured log line + optional webhook (ALERT_WEBHOOK_URL)
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
            default="latest", type="string",
            description="Enter a session_key, 'latest', or 'all'.",
        ),
        "min_session_key": Param(
            default=0, type="integer",
            description="Only extract sessions with session_key >= this value (use with 'all' to bound a backfill).",
        ),
        "max_sessions": Param(
            default=150, type="integer",
            description="Fan-out guard: abort if more sessions than this would be extracted.",
        ),
        "allow_large_backfill": Param(
            default=False, type="boolean",
            description="Tick to override the max_sessions guard on purpose.",
        ),
        "skip_existing": Param(
            default=False, type="boolean",
            description="Do not re-download endpoint/session files that already exist (cheap backfills; never use for a live session).",
        ),
        "run_dbt_build": Param(
            default=False, type="boolean",
            description="Run `dbt build` after extraction (needs dbt + the dbt project mounted in the container).",
        ),
        "dbt_scope": Param(
            default="auto", type="string", enum=["auto", "full"],
            description="auto: dbt rebuilds only the sessions whose files were written in this run. "
                        "full: whole graph with no session scope (the telemetry tables still load only sessions they lack).",
        ),
        "dbt_full_refresh": Param(
            default=False, type="boolean",
            description="Deliberate `dbt build --full-refresh` of EVERY model, including the 200M+ row telemetry tables. "
                        "Also needs allow_large_backfill.",
        ),
        "dbt_chunk_size": Param(
            default=DEFAULT_CHUNK_SIZE, type="integer", minimum=1,
            description="Multi-session runs build the telemetry group this many sessions at a time.",
        ),
        "transcribe_radio": Param(
            default=False, type="boolean",
            description="Transcribe team radio audio with faster-whisper (needs the package + outbound HTTPS; slow on CPU).",
        ),
        "upload_to_gcs": Param(
            default=False, type="boolean",
            description="Upload extracted Parquet to GCS_RAW_BUCKET after extraction.",
        ),
    },
    tags=["openf1", "ingestion"],
) as dag:

    start = EmptyOperator(task_id="start_extraction")

    # ==========================================
    # TASK: FETCH SESSION METADATA (+ fan-out guard)
    # ==========================================
    @task
    def fetch_session_metadata(**context):
        """Fetches the raw session metadata from the API using the custom Hook, then applies the backfill guard."""
        hook = OpenF1Hook()
        p = context["params"]
        target = p["target_session_key"]

        if str(target).lower() == "all":
            params = {}  # omit session_key entirely: API returns full history + future
        elif str(target).lower() == "latest":
            params = {"session_key": "latest"}
        else:
            params = {"session_key": int(target) if str(target).isdigit() else target}

        logging.info(f"Fetching session metadata for: {params if params else 'ALL historical sessions'}")
        sessions = hook.get_endpoint('/sessions', params=params)

        if not sessions:
            logging.warning("No sessions found for the given parameter.")
            return []

        min_key = int(p["min_session_key"])
        if min_key:
            sessions = [s for s in sessions if int(s["session_key"]) >= min_key]

        if len(sessions) > int(p["max_sessions"]) and not p["allow_large_backfill"]:
            raise AirflowFailException(
                f"{len(sessions)} sessions would be extracted (guard max_sessions={p['max_sessions']}). "
                "Narrow the range with min_session_key, or tick allow_large_backfill to proceed deliberately."
            )
        logging.info(f"{len(sessions)} session(s) selected for extraction.")
        return sessions

    # ==========================================
    # TASKS: FILTER SESSIONS FOR DYNAMIC MAPPING
    # ==========================================
    @task
    def get_all_keys(sessions):
        return [s['session_key'] for s in sessions]

    @task
    def get_race_keys(sessions):
        return [s['session_key'] for s in sessions if s.get('session_type') == 'Race']

    @task
    def get_quali_and_race_keys(sessions):
        valid_types = {'Race', 'Qualifying', 'Sprint', 'Sprint Qualifying'}
        return [s['session_key'] for s in sessions if s.get('session_type') in valid_types]

    # ==========================================
    # TASK: EXTRACTION WRAPPER (Mapped)
    # ==========================================
    @task(pool='openf1_api_pool')
    def extract_mapped_data(endpoint: str, target_session: str, is_global: bool = False, **context):
        from modules.universal_extractor import extract_endpoint_result
        return extract_endpoint_result(
            endpoint=endpoint,
            target_session=target_session,
            is_global=is_global,
            skip_existing=bool(context["params"]["skip_existing"]),
            run_id=context["run_id"],
        )

    # ==========================================
    # WORKFLOW DEFINITION
    # ==========================================
    raw_sessions = fetch_session_metadata()
    all_sessions = get_all_keys(raw_sessions)
    race_sessions = get_race_keys(raw_sessions)
    quali_race_sessions = get_quali_and_race_keys(raw_sessions)

    with TaskGroup("global_and_reference") as event_group:
        global_endpoints = ["meetings", "drivers", "sessions"]
        event_results = []
        for ep in global_endpoints:
            res = extract_mapped_data.override(task_id=f"extract_{ep}").partial(
                endpoint=ep, is_global=True
            ).expand(target_session=["global"])
            event_results.append(res)

    with TaskGroup("telemetry_and_performance") as telemetry_group:
        telemetry_endpoints = ["car_data", "location"]
        telemetry_results = []
        for ep in telemetry_endpoints:
            res = extract_mapped_data.override(task_id=f"extract_{ep}").partial(
                endpoint=ep
            ).expand(target_session=all_sessions)
            telemetry_results.append(res)

    with TaskGroup("race_results_and_events") as results_group:
        results_out = []

        all_session_endpoints = [
            "weather", "team_radio", "race_control", "pit",
            "laps", "stints", "position", "session_result"
        ]
        for ep in all_session_endpoints:
            res = extract_mapped_data.override(task_id=f"extract_{ep}_all").partial(endpoint=ep).expand(target_session=all_sessions)
            results_out.append(res)

        race_endpoints = ["intervals", "overtakes", "championship_drivers", "championship_teams"]
        for ep in race_endpoints:
            res = extract_mapped_data.override(task_id=f"extract_{ep}_race").partial(endpoint=ep).expand(target_session=race_sessions)
            results_out.append(res)

        res_grid = extract_mapped_data.override(task_id="extract_starting_grid").partial(endpoint="starting_grid").expand(target_session=quali_race_sessions)
        results_out.append(res_grid)

    # ==========================================
    # TASK: VERIFY, RECONCILE AND WRITE A MANIFEST
    # ==========================================
    @task
    def verify_and_load(event_results, telemetry_results, results_out, **context):
        from modules.pipeline_utils import build_manifest

        results = flatten_results(event_results, telemetry_results, results_out)
        counts = {st: sum(1 for r in results if r.get("status") == st) for st in ("written", "skipped", "empty")}
        # a skipped file is already on disk and still belongs in the manifest / GCS upload, as before
        saved_files = [r["path"] for r in results if r.get("path") and r.get("status") in ("written", "skipped")]

        if not saved_files:
            logging.warning("Pipeline completed, but NO files were successfully extracted or saved.")
            return {"files": [], "manifest_path": None, "results": results}

        manifest_path = build_manifest(saved_files, RAW_DIR, context["run_id"], results=results)
        logging.info(f"{len(saved_files)} Parquet files ready ({counts['written']} written, {counts['skipped']} skipped, "
                     f"{counts['empty']} empty). Manifest written to {manifest_path}. Ready for dbt!")
        return {"files": saved_files, "manifest_path": manifest_path, "results": results}

    load_task = verify_and_load(event_results, telemetry_results, results_out)

    # ==========================================
    # OPTIONAL: UPLOAD RAW FILES TO GCS
    # ==========================================
    @task
    def upload_raw_to_gcs(load_result, **context):
        if not context["params"]["upload_to_gcs"]:
            raise AirflowSkipException("upload_to_gcs is off for this run.")
        bucket = os.environ.get("GCS_RAW_BUCKET")
        if not bucket:
            raise AirflowFailException("upload_to_gcs=True but env var GCS_RAW_BUCKET is not set.")
        from modules.gcs_loader import upload_raw_files
        return upload_raw_files(load_result["files"], bucket=bucket)

    # ==========================================
    # OPTIONAL: RUN DBT AFTER EXTRACTION (scoped to what this run changed)
    # ==========================================
    # Runs after transcription too (radio transcript files feed fct_radio_messages). The trigger rule lets it run when
    # the mapped transcription tasks were SKIPPED (transcribe_radio=false); a failed transcription still blocks it.
    @task(trigger_rule=TriggerRule.NONE_FAILED)
    def run_dbt_build(load_result, selected_session_keys, **context):
        from modules.dbt_runner import DbtRunError, run_dbt_plan

        p = context["params"]
        if not p["run_dbt_build"]:
            raise AirflowSkipException("run_dbt_build is off for this run.")
        try:
            scope = build_dbt_scope(
                load_result["results"], selected_session_keys,
                dbt_scope=p["dbt_scope"], dbt_full_refresh=bool(p["dbt_full_refresh"]),
                chunk_size=int(p["dbt_chunk_size"]), allow_large_backfill=bool(p["allow_large_backfill"]),
                non_telemetry_changed=bool(p["transcribe_radio"]),   # new transcript files change fct_radio_messages
            )
            if scope["mode"] == "none":
                raise AirflowSkipException(f"dbt skipped: {scope['reason']}.")
            steps = plan_dbt_commands(
                scope, target=DBT_TARGET, profiles_dir=DBT_PROFILES_DIR,
                radio_transcripts=radio_transcripts_exist(RAW_DIR), chunk_size=int(p["dbt_chunk_size"]),
                dbt_executable=os.environ.get("DBT_EXECUTABLE", "dbt"),
            )
            summary = run_dbt_plan(steps, cwd=DBT_PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR, scope=scope)
        except ScopeError as exc:
            raise AirflowFailException(str(exc)) from exc
        except DbtRunError as exc:
            raise AirflowFailException(str(exc)) from exc
        return summary

    # ==========================================
    # OPTIONAL: TRANSCRIBE TEAM RADIO (one session at a time: CPU heavy)
    # ==========================================
    @task(max_active_tis_per_dag=1)
    def transcribe_team_radio(target_session, **context):
        if not context["params"]["transcribe_radio"]:
            raise AirflowSkipException("transcribe_radio is off for this run.")
        from modules.radio_transcriber import transcribe_session
        return transcribe_session(target_session, RAW_DIR, model_name=os.environ.get("RADIO_WHISPER_MODEL", "small.en"))

    radio_task = transcribe_team_radio.expand(target_session=all_sessions)

    upload_task = upload_raw_to_gcs(load_task)
    dbt_task = run_dbt_build(load_task, all_sessions)

    # ==========================================
    # EXPLICIT DEPENDENCY GRAPH
    # ==========================================
    start >> raw_sessions >> [all_sessions, race_sessions, quali_race_sessions]
    start >> event_group
    all_sessions >> telemetry_group
    [all_sessions, race_sessions, quali_race_sessions] >> results_group
    [event_group, telemetry_group, results_group] >> load_task
    results_group >> radio_task
    radio_task >> dbt_task      # dbt waits for transcription (trigger rule tolerates the skipped mapped tasks)
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
* **Mapped tasks now return result dictionaries (third edition):** `extract_mapped_data` calls `extract_endpoint_result`, so every mapped instance returns `{endpoint, session_key, path, status}`. `verify_and_load` flattens all of them with `flatten_results`, builds the manifest (with the statuses), and returns the list of files **and** the list of results. Only the results travel on to `run_dbt_build`, which turns them into the dbt scope (Chapter 8).
* **Three new dbt parameters and a new order:** `dbt_scope`, `dbt_full_refresh` and `dbt_chunk_size` join the trigger form, and `run_dbt_build` now also waits for the radio transcription task (with a trigger rule that tolerates it being skipped). Both are explained in Chapter 8.
* **Three task groups:** `global_and_reference` (data that does not vary per session, extracted once), `telemetry_and_performance` (the heavy endpoints, for all sessions) and `race_results_and_events`.

> **A note on imports.** The DAG imports the pure helpers `modules.dbt_scope` at the top of the file, unlike the lazy imports of Chapter 2. That is fine because that module only *defines* functions and has no Airflow, network or dbt dependency; nothing is called until a task runs. The heavier imports (`universal_extractor`, `pipeline_utils`, `dbt_runner`, the radio transcriber) stay **inside** the task functions. The rule is "never **execute** API calls at the top level", not "never import".

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

#### **Task 4: Know What a Run Produced (Manifest and Failure Alerts)**

New in the second edition. Two small helpers live in **airflow/dags/modules/pipeline_utils.py** (the manifest builder and a generic `notify_failure` callback). They deliberately import nothing from Airflow, so they can be unit tested without it.

```python
"""Small, Airflow-free helpers so they can be unit tested: run manifest + failure notification."""
import json
import logging
import os
import urllib.request
from datetime import datetime, timezone

import pyarrow.parquet as pq


def build_manifest(files, raw_dir: str, run_id: str, results=None) -> str:
    """Write raw_dir/_manifests/<run_id>.json listing every file with its row count and size.
    dbt tests / reviewers can reconcile what was extracted in a run against what reached the warehouse.
    results (optional): the extractor results {endpoint, session_key, path, status}; their status
    (written / skipped / empty) is recorded so a manifest shows what the run actually changed."""
    entries = []
    for f in files:
        try:
            rows = pq.ParquetFile(f).metadata.num_rows
        except Exception as exc:  # a corrupt file must be visible in the manifest, not crash the run
            logging.error(f"Could not read Parquet metadata for {f}: {exc}")
            rows = None
        entries.append({"file": os.path.basename(f), "rows": rows, "bytes": os.path.getsize(f) if os.path.exists(f) else None})
    manifest = {
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "file_count": len(entries),
        "total_rows": sum(e["rows"] or 0 for e in entries),
        "files": sorted(entries, key=lambda e: e["file"]),
    }
    if results is not None:
        clean = [{"endpoint": r.get("endpoint"), "session_key": r.get("session_key"),
                  "file": os.path.basename(r["path"]) if r.get("path") else None, "status": r.get("status")}
                 for r in results if isinstance(r, dict)]
        manifest["status_counts"] = {st: sum(1 for r in clean if r["status"] == st) for st in ("written", "skipped", "empty")}
        manifest["results"] = sorted(clean, key=lambda r: (str(r["endpoint"]), str(r["session_key"])))
    out_dir = os.path.join(raw_dir, "_manifests")
    os.makedirs(out_dir, exist_ok=True)
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in str(run_id))
    path = os.path.join(out_dir, f"{safe}.json")
    with open(path, "w") as fh:
        json.dump(manifest, fh, indent=2)
    return path


def notify_failure(context):
    """on_failure_callback: always logs one structured line; also POSTs to ALERT_WEBHOOK_URL if configured
    (Slack-compatible {'text': ...} payload). Never raises: alerting must not mask the original failure."""
    try:
        ti = context.get("task_instance")
        info = {
            "dag_id": getattr(ti, "dag_id", None), "task_id": getattr(ti, "task_id", None),
            "run_id": context.get("run_id"), "try_number": getattr(ti, "try_number", None),
            "exception": str(context.get("exception"))[:500],
        }
        logging.error("PIPELINE_FAILURE " + json.dumps(info))
        url = os.environ.get("ALERT_WEBHOOK_URL")
        if url:
            body = json.dumps({"text": f"OpenF1 pipeline failure: {info['dag_id']}.{info['task_id']} ({info['run_id']}): {info['exception']}"}).encode()
            req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10)
    except Exception as exc:
        logging.error(f"notify_failure itself failed: {exc}")
```

**Why it is written this way**

* **The manifest** (`airflow/data/raw/_manifests/<run_id>.json`) lists every file the run saved with its **row count and size**. A later reviewer, or a dbt test, can reconcile "what was extracted" against "what reached the warehouse". Since the third edition it also records, when `build_manifest` is given the extractor results, a `results` list (endpoint, session, file name, `written` / `skipped` / `empty`) and a `status_counts` summary, so the manifest shows what the run actually **changed**, not just which files exist. A manifest written the old way (no results) is unchanged.
* **The run id is sanitised** before it becomes a file name. Airflow run ids contain `:` (for example `manual__2026-01-01T00:00:00+00:00`), which Windows refuses in file names, so the helper replaces any other character first. A unit test pins that behaviour; it was written as a precaution, not in response to a failure.
* **A corrupt file is recorded, not fatal:** if a Parquet file cannot be read, its entry gets `rows: null` and the manifest is still written. An audit log that crashes on the thing it is auditing is useless.
* **`notify_failure` never raises.** Alerting runs *while something is already failing*; if the webhook is down, the original error must stay visible, not be replaced by a second one. Any Slack-compatible webhook works because the payload is just `{"text": "..."}`. The DAG itself uses the richer Slack callback described next; `notify_failure` stays in `pipeline_utils.py` as the dependency-free alternative and is covered by a unit test.

**The Slack failure alert.** The DAG's failure callback is **airflow/plugins/slack_alerts.py**. It stays silent while retries remain, does nothing (but logs a warning) when `ALERT_WEBHOOK_URL` is empty, and otherwise posts a Slack message built from Block Kit blocks: the DAG, task (with its map index), run id and attempt, the exception text (cut to 500 characters), the tail of the task's log file (cut to 2,400 characters to stay under Slack's section limit) and a link to the task log in the Airflow UI. Like `notify_failure`, it never raises: if the post fails it only logs an error.

```python
import logging
import os
from pathlib import Path
import requests
from airflow.configuration import conf

log = logging.getLogger(__name__)

MAX_LOG_CHARS = 2400  # Keeps Slack section blocks safely under Slack's 3,000-char limit


def _read_task_log_tail(ti) -> str:
    base_log_folder = conf.get("logging", "base_log_folder", fallback="/opt/airflow/logs")
    parts = [
        base_log_folder,
        f"dag_id={ti.dag_id}",
        f"run_id={ti.run_id}",
        f"task_id={ti.task_id}",
    ]
    map_index = getattr(ti, "map_index", -1)
    if map_index is not None and map_index >= 0:
        parts.append(f"map_index={map_index}")

    log_dir = Path(*parts)
    candidate = log_dir / f"attempt={ti.try_number}.log"

    if not candidate.exists() and log_dir.exists():
        attempts = sorted(log_dir.glob("attempt=*.log"))
        if attempts:
            candidate = attempts[-1]

    if not candidate.exists():
        return f"Log file not found at {candidate} (task may have been killed before writing logs)."

    text = candidate.read_text(encoding="utf-8", errors="replace")
    if len(text) > MAX_LOG_CHARS:
        return "... [truncated] ...\n" + text[-MAX_LOG_CHARS:]
    return text or "(empty log file)"


def send_slack_failure_alert(context: dict) -> None:
    ti = context["task_instance"]
    if ti.try_number <= ti.max_tries:
        return

    webhook_url = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    if not webhook_url:
        log.warning("ALERT_WEBHOOK_URL is not set; skipping Slack failure alert.")
        return

    exception = context.get("exception")
    map_index = getattr(ti, "map_index", -1)
    task_label = f"{ti.task_id}[{map_index}]" if map_index is not None and map_index >= 0 else ti.task_id
    log_tail = _read_task_log_tail(ti)

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":x: Airflow Task Failed: {ti.dag_id}.{task_label}",
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*DAG:*\n`{ti.dag_id}`"},
                {"type": "mrkdwn", "text": f"*Task:*\n`{task_label}`"},
                {"type": "mrkdwn", "text": f"*Run ID:*\n`{ti.run_id}`"},
                {"type": "mrkdwn", "text": f"*Attempt:*\n`{ti.try_number}`"},
            ],
        },
    ]

    if exception:
        exc_text = str(exception)
        if len(exc_text) > 500:
            exc_text = exc_text[:500] + "..."
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Exception:*\n`{exc_text}`"},
            }
        )

    blocks.extend(
        [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Log Tail:*\n```{log_tail}```"},
            },
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"<{ti.log_url}|Open Task Log in Airflow UI>"},
                ],
            },
        ]
    )

    try:
        resp = requests.post(webhook_url, json={"blocks": blocks}, timeout=10)
        resp.raise_for_status()
    except Exception as err:
        log.error("Failed to send Slack alert: %s", err)
```

The payload is Slack Block Kit, so it needs a **Slack** webhook; `notify_failure`'s plain `{"text": ...}` payload works with any Slack-compatible endpoint.

#### **Task 5: Run the Python Tests**

New in the second edition. The extractor is the part of the project that talks to a flaky external API, which makes it the part most worth testing, and it can be tested **without the API**. **airflow/tests/conftest.py** replaces the real hook with a scripted `FakeHook`; each test then decides what the "API" answers:

```python
fake_hook.script["/car_data"] = car      # a function that raises "422" unless a time window is requested
path = ue.extract_and_save_endpoint("car_data", target_session=7)
assert pq.read_table(path).num_rows == 3   # the extractor fell back to three 15-minute windows

res = ue.extract_endpoint_result("laps", target_session=1)           # third edition: the structured form
assert res["status"] == "written" and res["session_key"] == 1
```

```bash
python -m pytest -q airflow/tests
```

The tests cover the sanitiser, 404 skipping, 422 time-window chunking, 429 backoff, idempotent file names, `skip_existing`, atomic writes, the structured `written` / `skipped` / `empty` results, the manifest (with and without statuses), the dbt scope builder and runner (Chapter 8), the generic failure callback, the GCS loader (Chapter 10), the radio transcriber (Chapter 6) and the BigQuery loader (Chapter 10). The 422 and 429 paths existed in the first edition but had never been tested; writing the tests did not find a bug there, which is itself useful information.

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
* **The Solution:** this is expected. The extractor reports the extraction as `empty` (for example `starting_grid` for a session type that has none) and `verify_and_load` counts only files that exist.

* **The Problem (confirmed in the final log of the build):** a run fails with `404` on `/drivers?year=2023` while you believe you are running the new DAG.
* **Why it Happens (inference):** the log line `universal_extractor.py:15 ... params {'year': 2023}` comes from the **old** Chapter 2 extractor, whose default was `{"year": 2023}`. The new DAG was running against a stale module file. The DAG and the module it imports must be updated **together**.
* **The Solution:** replace `universal_extractor.py` with the Task 1 version, save, wait for the scheduler to re-parse, and confirm the first log line of a task reads `Extracting data from /... (Attempt 1/3)...`.

* **The Problem (second edition):** a run fails at `fetch_session_metadata` with `... sessions would be extracted (guard max_sessions=150). Narrow the range with min_session_key, or tick allow_large_backfill`.
* **Why it Happens:** that is the fan-out guard doing its job: `all` matched more sessions than `max_sessions`.
* **The Solution:** pass `min_session_key` to bound the backfill (for example the first session key of the season you want), or tick `allow_large_backfill` if the size is intentional.

* **The Problem (third edition):** `verify_and_load` logs `NO files were successfully extracted` and returns `'results': []`, although the extraction tasks ran for minutes and the Parquet files are on disk. `run_dbt_build` then reports `keys=[]`.
* **Why it Happens:** the DAG passes each task group's outputs as a **list of mapped-task results**, and Airflow resolves each mapped output to a **lazy sequence**, which is not a Python `list` or `tuple`. The first version of `flatten_results` only unpacked lists and tuples, so it silently threw every result away. The unit tests passed because they used plain lists.
* **The Solution:** `flatten_results` now recurses into anything iterable (except dictionaries and strings), drops `None` for skipped mapped instances, and **raises** on any other leaf instead of ignoring it. The lesson is general: a test double must have the same *shape* as what the real framework passes, and code that discards unexpected input hides exactly the data downstream steps depend on. The test now uses a stand-in for Airflow's lazy sequence.

* **The Problem (second edition):** with `skip_existing` on, a session's data looks **stale** or incomplete.
* **Why it Happens:** an earlier run extracted that session while it was still live, and the file is now being skipped because it exists.
* **The Solution:** delete that session's files (or run once with `skip_existing` off) after the session has finished. Never use `skip_existing` for `latest`.

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

**Step 2: Create `profiles.yml`.** In the first edition this file lived in your user folder (`~/.dbt/`). In the second edition it lives **inside the dbt project**, so your laptop, Airflow and CI all use the same file. dbt looks in the folder you run it from **before** it looks in `~/.dbt/`, so running `dbt` from `dbt_f1_transformations/` just works; the `--target` flag picks which block below is used (the default is `dev`).

```yaml
# Profiles live in the project so Airflow, CI and your laptop all use the same file:
#   dbt build --profiles-dir . --target dev        (host)
#   dbt build --profiles-dir . --target container  (inside the Airflow container)
#   dbt build --profiles-dir . --target ci         (GitHub Actions, synthetic fixtures)
#   dbt build --profiles-dir . --target bigquery   (cloud warehouse; needs `gcloud auth application-default login`)
#   dbt build --profiles-dir . --target bigquery_emulator   (local BigQuery emulator; see ci/bq_emulator_runner.py)
dbt_f1_transformations:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('F1_DUCKDB_PATH', 'F:/DaTech/f1_data_project/f1_local_warehouse.duckdb') }}"
      threads: 1
    container:
      type: duckdb
      path: "{{ env_var('F1_DUCKDB_PATH', '/opt/airflow/data/warehouse/f1_local_warehouse.duckdb') }}"
      threads: 1
    ci:
      type: duckdb
      path: "{{ env_var('F1_DUCKDB_PATH', '/tmp/f1_ci.duckdb') }}"
      threads: 1
    bigquery:
      type: bigquery
      method: oauth                       # application-default credentials (gcloud auth application-default login)
      project: "{{ env_var('F1_BQ_PROJECT') }}"
      dataset: "{{ env_var('F1_BQ_DATASET', 'f1_analytics') }}"   # where dbt builds staging, intermediate and marts
      location: "{{ env_var('F1_BQ_LOCATION', 'US') }}"
      threads: 4
    bigquery_emulator:
      type: bigquery
      method: oauth
      project: "{{ env_var('F1_BQ_PROJECT', 'f1test') }}"
      dataset: "{{ env_var('F1_BQ_DATASET', 'f1_analytics') }}"
      location: US
      api_endpoint: "{{ env_var('F1_BQ_API_ENDPOINT', 'http://localhost:9050') }}"
      threads: 1
```

> **The first word must match.** `dbt_f1_transformations` here must be identical to the `profile:` line in `dbt_project.yml`. A mismatch gives "Could not find profile named ...".

**Read the profile like an engineer**

* **Several kinds of target.** `dev` is your laptop. `container` is the same project run *inside* the Airflow container (Chapter 8). `ci` is the synthetic dataset used by the automated tests (Chapter 11). `bigquery` and `bigquery_emulator` are the cloud warehouse and its local test double (Chapter 10). You only need `dev` today.
* **`{{ env_var('NAME', 'default') }}`:** the value comes from an environment variable when it is set and falls back to the default (your `F:/` paths) when it is not. That is how one file serves a laptop, a container and CI without edits.
* **`config_options` versus `settings`:** the shipped profile sets no `temp_directory`, so DuckDB uses its default spill location. If you add one, put it under `config_options`, which DuckDB applies **once, when the connection opens**. Under `settings` it looks equivalent and is not; see the pitfall at the end of this chapter.

**Step 3: Create `dbt_project.yml`** inside **dbt_f1_transformations/**:

```yaml
name: 'dbt_f1_transformations'
version: '1.1.0'
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

# ---------------------------------------------------------------------------
# Tunable analytics assumptions. Change them here, not inside the SQL.
# ---------------------------------------------------------------------------
vars:
  # SESSION SCOPE (macros/scope.sql). Airflow passes the sessions a run changed: --vars '{session_keys: [9158]}'. Empty = no scope, which
  # behaves as before (the telemetry tables then load only the sessions they do not hold yet). `telemetry_only_sessions` is the old name.
  session_keys: []
  # Fuel burn lap-time effect (s per lap of fuel burned). ASSUMPTION: ~0.03-0.04 s/lap is a commonly
  # quoted figure; real values vary by circuit/era. Used only for fuel_corrected_lap_time_seconds.
  fuel_effect_seconds_per_lap: 0.035
  # A lap is "representative" if within this multiple of the driver's median clean-lap time.
  representative_lap_threshold: 1.07
  drs_range_seconds: 1.0            # within this gap to the car ahead => "in DRS range"
  dirty_air_seconds: 2.0            # within this gap => aerodynamic wake effects likely
  slow_stop_margin_seconds: 3.0     # pit lane time above session median + margin => slow stop
  min_stint_laps_for_degradation: 5 # regression needs at least this many representative laps
  distance_bin_metres: 10           # resolution of the distance-domain telemetry trace
  telemetry_max_unknown_lap_seconds: 200   # telemetry window for a lap whose duration OpenF1 omits
  telemetry_lap_tolerance_seconds: 2       # grace after lap_start + lap_duration before a sample stops belonging to the lap
  min_plausible_lap_seconds: 30            # sanity floor used by tests (no F1 lap is shorter)
  max_unclosed_vsc_seconds: 600            # an SC/VSC with no closing race-control message is capped (VSC 10 min, SC 20 min)
  max_unclosed_sc_seconds: 1200
  fastest_lap_bonus_last_year: 2024 # the bonus point was removed from the 2025 season
  # --- derived corner detection (no official corner data exists in OpenF1) ---
  corner_smooth_bins: 2              # speed smoothing half-window (bins)
  corner_min_separation_bins: 15     # two apexes closer than this many bins collapse to the lowest one
  corner_prominence_window_bins: 50  # window used to measure how far speed drops into a corner
  corner_min_speed_drop_kph: 25      # a corner = speed minimum at least this far below the surrounding peak
  corner_entry_distance_m: 150
  corner_exit_distance_m: 100
  corner_brake_lookback_m: 100       # how far before entry a brake application still belongs to this corner
  stale_telemetry_min_samples: 8      # this many consecutive identical non-zero speed+gear samples (about 2 s) are a frozen reading, not driving
  corner_geometry_search_bins: 5     # nearest bin (with a position) searched for a corner's entry and exit point; the axis is sparse where the car is fast
  corner_slow_kph: 120               # apex speed classes
  corner_fast_kph: 220
  # --- undercut / overcut ---
  undercut_window_laps: 8            # two stops within this many laps are considered related
  undercut_max_gap_seconds: 8.0      # attacker must be within this gap of the defender before the first stop
  # --- safety car model ---
  sc_prior_laps: 300                 # strength of the pull toward the all-circuit rate (empirical Bayes)
  # Where the telemetry export writes partitioned Parquet (relative to the dbt process, or absolute)
  telemetry_export_path: "{{ env_var('F1_EXPORT_PATH', 'output/telemetry_data') }}"
  # BigQuery only: telemetry tables are partitioned with EXACTLY ONE session per partition so insert_overwrite can only
  # ever replace the session being loaded. Sessions outside [start, end) land in a catch-all partition: widen the range
  # (max 4000 partitions) before the season counter outgrows it.
  bq_session_key_start: 9000
  bq_session_key_end: 13000
  bq_incremental_strategy: insert_overwrite   # use 'merge' where scripting (insert_overwrite) is unavailable
  export_data: false
  # Build the radio transcript models. Leave false until radio_transcripts_*_extracted.parquet files exist.
  enable_radio_transcripts: false

models:
  dbt_f1_transformations:
    staging:
      +materialized: view
      # The telemetry group = the scoped views plus every model that reads them (directly or through fct_telemetry). Airflow selects it
      # per chunk of sessions (`--select tag:telemetry_group`) and builds everything else once (`--exclude tag:telemetry_group`).
      # Nothing outside this group depends on it. Keep this list in sync with ci/check_scoped_views.py (it fails on a mismatch).
      stg_car_data:
        +tags: [telemetry_group]
      stg_location:
        +tags: [telemetry_group]
    intermediate:
      +materialized: table
    marts:
      +materialized: table
      analytics:
        fct_telemetry:
          +tags: [telemetry_group]
        fct_telemetry_distance_trace:
          +tags: [telemetry_group]
        fct_telemetry_lap_summary:
          +tags: [telemetry_group]
        fct_lap_time_delta_trace:
          +tags: [telemetry_group]
        dim_session_corners:
          +tags: [telemetry_group]
        fct_corner_performance:
          +tags: [telemetry_group]
```

This sets the rule from Chapter 4: everything in `staging/` is a **view**, everything in `intermediate/` and `marts/` is a **table**. There is one deliberate exception, the **telemetry group**: `stg_car_data`, `stg_location` and the six telemetry models downstream of them (Chapters 6 and 7) carry the tag `telemetry_group`, and the six models are **incremental** because telemetry is far too large to rebuild on every run.

**Session scope (third edition).** The first variable, `session_keys`, is empty by default. When it is set, for example `--vars '{session_keys: [9158]}'`, dbt builds the telemetry group for **only those sessions and replaces them** instead of appending (Chapter 7 explains how); Airflow sets it for you (Chapter 8). `telemetry_only_sessions` is the first edition's name for the same variable and still works as an alias. The tags under `models:` are how Airflow selects the group (`--select tag:telemetry_group`) or leaves it out (`--exclude tag:telemetry_group`).

The long `vars:` block is new in the second edition. It holds every **tunable assumption** in one place (the fuel-burn effect, the DRS-range threshold, the corner-detection thresholds and so on), so that changing an assumption never means editing SQL. Each variable is explained in the chapter that uses it, with the reason for its default.

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
    schema: "{{ env_var('F1_RAW_SCHEMA', 'f1_raw') }}"   # BigQuery dataset loaded by scripts/load_raw_to_bigquery.py (DuckDB ignores it)
    description: "Raw Parquet files extracted locally from the OpenF1 API via Airflow"
    tables:
      # 1. Drivers & Teams
      - name: drivers
        description: "Raw driver information and details"
        config:
          meta:
            external_location: "read_parquet('{{ env_var('F1_RAW_DATA_PATH', 'F:/DaTech/f1_data_project/airflow/data/raw') }}/drivers_*_extracted.parquet', union_by_name=true)"
          
      - name: championship_drivers
        description: "Driver championship standings and points"
        config:
          meta:
            external_location: "read_parquet('{{ env_var('F1_RAW_DATA_PATH', 'F:/DaTech/f1_data_project/airflow/data/raw') }}/championship_drivers_*_extracted.parquet', union_by_name=true)"
          
      - name: championship_teams
        description: "Constructor championship standings and points"
        config:
          meta:
            external_location: "read_parquet('{{ env_var('F1_RAW_DATA_PATH', 'F:/DaTech/f1_data_project/airflow/data/raw') }}/championship_teams_*_extracted.parquet', union_by_name=true)"

      # 2. Race Weekend Structure
      - name: meetings
        description: "Grand Prix weekend metadata (locations, dates)"
        config:
          meta:
            external_location: "read_parquet('{{ env_var('F1_RAW_DATA_PATH', 'F:/DaTech/f1_data_project/airflow/data/raw') }}/meetings_*_extracted.parquet', union_by_name=true)"
          
      - name: sessions
        description: "Specific session metadata (FP1, Quali, Race)"
        config:
          meta:
            external_location: "read_parquet('{{ env_var('F1_RAW_DATA_PATH', 'F:/DaTech/f1_data_project/airflow/data/raw') }}/sessions_*_extracted.parquet', union_by_name=true)"
          
      - name: session_result
        description: "Final classification and results of a session"
        config:
          meta:
            external_location: "read_parquet('{{ env_var('F1_RAW_DATA_PATH', 'F:/DaTech/f1_data_project/airflow/data/raw') }}/session_result_*_extracted.parquet', union_by_name=true)"
          
      - name: starting_grid
        description: "Starting grid positions for the race or sprint"
        config:
          meta:
            external_location: "read_parquet('{{ env_var('F1_RAW_DATA_PATH', 'F:/DaTech/f1_data_project/airflow/data/raw') }}/starting_grid_*_extracted.parquet', union_by_name=true)"

      # 3. Track Action & Timing
      - name: laps
        description: "Detailed lap times, sectors, and speed traps"
        config:
          meta:
            external_location: "read_parquet('{{ env_var('F1_RAW_DATA_PATH', 'F:/DaTech/f1_data_project/airflow/data/raw') }}/laps_*_extracted.parquet', union_by_name=true)"
          
      - name: intervals
        description: "Time gaps between cars"
        config:
          meta:
            external_location: "read_parquet('{{ env_var('F1_RAW_DATA_PATH', 'F:/DaTech/f1_data_project/airflow/data/raw') }}/intervals_*_extracted.parquet', union_by_name=true)"
          
      - name: position
        description: "Track position updates and leaderboard changes"
        config:
          meta:
            external_location: "read_parquet('{{ env_var('F1_RAW_DATA_PATH', 'F:/DaTech/f1_data_project/airflow/data/raw') }}/position_*_extracted.parquet', union_by_name=true)"
          
      - name: overtakes
        description: "Logged overtakes during a session"
        config:
          meta:
            external_location: "read_parquet('{{ env_var('F1_RAW_DATA_PATH', 'F:/DaTech/f1_data_project/airflow/data/raw') }}/overtakes_*_extracted.parquet', union_by_name=true)"

      # 4. Telemetry & Environment
      - name: car_data
        description: "High-frequency telemetry (speed, throttle, brake, RPM, gear)"
        config:
          meta:
            external_location: "read_parquet('{{ env_var('F1_RAW_DATA_PATH', 'F:/DaTech/f1_data_project/airflow/data/raw') }}/car_data_*_extracted.parquet', union_by_name=true)"
          
      - name: location
        description: "GPS coordinate track locations for each car"
        config:
          meta:
            external_location: "read_parquet('{{ env_var('F1_RAW_DATA_PATH', 'F:/DaTech/f1_data_project/airflow/data/raw') }}/location_*_extracted.parquet', union_by_name=true)"
          
      - name: weather
        description: "Track and air temperature, wind, and rainfall"
        config:
          meta:
            external_location: "read_parquet('{{ env_var('F1_RAW_DATA_PATH', 'F:/DaTech/f1_data_project/airflow/data/raw') }}/weather_*_extracted.parquet', union_by_name=true)"

      # 5. Pitlane & Strategy
      - name: pit
        description: "Pit lane timing and stop durations"
        config:
          meta:
            external_location: "read_parquet('{{ env_var('F1_RAW_DATA_PATH', 'F:/DaTech/f1_data_project/airflow/data/raw') }}/pit_*_extracted.parquet', union_by_name=true)"
          
      - name: stints
        description: "Tire compound usage and stint lengths"
        config:
          meta:
            external_location: "read_parquet('{{ env_var('F1_RAW_DATA_PATH', 'F:/DaTech/f1_data_project/airflow/data/raw') }}/stints_*_extracted.parquet', union_by_name=true)"

      # 6. Communications & Control
      - name: team_radio
        description: "Driver-to-pit radio communication transcripts/links"
        config:
          meta:
            external_location: "read_parquet('{{ env_var('F1_RAW_DATA_PATH', 'F:/DaTech/f1_data_project/airflow/data/raw') }}/team_radio_*_extracted.parquet', union_by_name=true)"
          
      - name: race_control
        description: "Flags, safety cars, and incident messages"
        config:
          meta:
            external_location: "read_parquet('{{ env_var('F1_RAW_DATA_PATH', 'F:/DaTech/f1_data_project/airflow/data/raw') }}/race_control_*_extracted.parquet', union_by_name=true)"

      - name: radio_transcripts
        description: "OPTIONAL speech-to-text of team radio, written by modules/radio_transcriber.py (only exists when transcription is enabled)"
        config:
          meta:
            external_location: "read_parquet('{{ env_var('F1_RAW_DATA_PATH', 'F:/DaTech/f1_data_project/airflow/data/raw') }}/radio_transcripts_*_extracted.parquet', union_by_name=true)"
```

**Why every table now uses `read_parquet(..., union_by_name=true)`**

Each Parquet file is written separately, and DuckDB normally infers a column's type from the **first** file it reads. If `intervals` for session A stores gaps as numbers but session B contains `"+1 LAP"` strings, DuckDB hits a conversion error when it reaches session B. `union_by_name=true` tells DuckDB to **line files up by column name and reconcile their types**. In the first edition only `intervals`, `race_control` and `session_result` had it; the second edition applies it to **all** tables. The extractor now adds run-metadata columns (Chapter 3), so files written before and after that change have different column sets, and without `union_by_name` DuckDB would have to guess. **Chapter 9 tells the full story of the `+2 LAPS` incident.**

**Three other changes to this file in the second edition**

* **`F1_RAW_DATA_PATH`:** the raw folder comes from an environment variable (default: your existing `F:/` path), so the same file works in the Airflow container and in CI.
* **`schema: ...` (line 5):** the name of the BigQuery dataset the raw tables are loaded into (Chapter 10). DuckDB ignores it, because it reads the files through `external_location`.
* **`config: meta: external_location`:** dbt 1.10 and later warn when `meta` sits directly on a source table and ask for it under `config`. Moving it silences the warning, and dbt-duckdb reads it from the same place.

#### **Task 3: Write Your First Staging Model**

A staging model does only three jobs: **rename** columns to clear names, **cast** them to proper types, and apply **light cleaning**. No joins. Create **models/staging/stg_laps.sql** (shown in the full list below), then run it:

```bash
dbt run --select stg_laps
```

dbt compiles the file (replacing `{{ source(...) }}` with the real read of your Parquet files) and creates a **view** in DuckDB.

#### **Task 4: The Complete Staging Layer (19 Models)**

Create each file below in **dbt_f1_transformations/models/staging/**. The pattern is identical every time: a CTE called `raw_<name>` selects from the source, and a second CTE `renamed_and_casted` renames and casts.

**`models/staging/stg_drivers.sql`**

```sql
WITH raw_drivers AS (
    -- dbt will compile this source function into a DuckDB command
    -- to read your local files based on the sources.yml meta tag!
    SELECT * FROM {{ source('openf1_raw', 'drivers') }}
),

renamed_and_casted AS (
    SELECT
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        
        full_name,
        name_acronym AS driver_acronym,
        team_name,
        team_colour AS team_color_hex,
        country_code, -- Fixed: Added missing comma here to prevent aliasing
        
        -- Passed the URL through cleanly
        headshot_url

    FROM raw_drivers
    -- Let's filter out any null driver numbers just in case 
    -- the API returned an empty row
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
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,
        
        position_start,
        position_current,
        CAST(points_start AS {{ f1_type('double') }}) AS points_start,
        CAST(points_current AS {{ f1_type('double') }}) AS points_current
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
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        
        team_name,
        position_start,
        position_current,
        CAST(points_start AS {{ f1_type('double') }}) AS points_start,
        CAST(points_current AS {{ f1_type('double') }}) AS points_current
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
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(year AS {{ f1_type('int') }}) AS year,
        CAST(circuit_key AS {{ f1_type('int') }}) AS circuit_key,
        
        meeting_name,
        meeting_official_name,
        location,
        country_key,
        country_code,
        circuit_short_name,
        CAST(date_start AS {{ f1_type('timestamp') }}) AS date_start,
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
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(circuit_key AS {{ f1_type('int') }}) AS circuit_key,
        CAST(year AS {{ f1_type('int') }}) AS year,
        
        session_name,
        {# OpenF1 reports a Sprint with session_type 'Race' and only session_name 'Sprint'. Everything downstream (points scale, grid mapping, race-only
           models) expects session_type 'Sprint', so normalise here. Without it sprints were scored on the Race points scale. #}
        CASE WHEN session_name = 'Sprint' THEN 'Sprint' ELSE session_type END AS session_type,
        CAST(date_start AS {{ f1_type('timestamp') }}) AS date_start,
        CAST(date_end AS {{ f1_type('timestamp') }}) AS date_end,
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
typed AS (
    SELECT
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,
        {{ f1_try_cast_fn() }}(position AS {{ f1_type('int') }}) AS position,
        {{ f1_try_cast_fn() }}(number_of_laps AS {{ f1_type('int') }}) AS number_of_laps,
        COALESCE(CAST(dnf AS {{ f1_type('bool') }}), FALSE) AS dnf,
        COALESCE(CAST(dns AS {{ f1_type('bool') }}), FALSE) AS dns,
        COALESCE(CAST(dsq AS {{ f1_type('bool') }}), FALSE) AS dsq,
        -- Race: a number or text. Qualifying: a JSON array string like '[q1, q2, q3]'.
        CAST(duration AS {{ f1_type('string') }}) AS duration_raw,
        CAST(gap_to_leader AS {{ f1_type('string') }}) AS gap_to_leader_raw
    FROM raw_session_result
    WHERE driver_number IS NOT NULL
)
SELECT
    *,
    CASE WHEN duration_raw LIKE '[%' THEN NULL ELSE {{ f1_try_cast_fn() }}(duration_raw AS {{ f1_type('double') }}) END AS race_time_seconds,
    {{ f1_try_cast_fn() }}({{ f1_json_scalar("CASE WHEN duration_raw LIKE '[%' THEN duration_raw END", '$[0]') }} AS {{ f1_type('double') }}) AS q1_seconds,
    {{ f1_try_cast_fn() }}({{ f1_json_scalar("CASE WHEN duration_raw LIKE '[%' THEN duration_raw END", '$[1]') }} AS {{ f1_type('double') }}) AS q2_seconds,
    {{ f1_try_cast_fn() }}({{ f1_json_scalar("CASE WHEN duration_raw LIKE '[%' THEN duration_raw END", '$[2]') }} AS {{ f1_type('double') }}) AS q3_seconds,
    CASE WHEN gap_to_leader_raw LIKE '[%' THEN NULL ELSE {{ f1_try_cast_fn() }}(gap_to_leader_raw AS {{ f1_type('double') }}) END AS gap_to_leader_seconds,
    CASE WHEN gap_to_leader_raw LIKE '+%LAP%' THEN gap_to_leader_raw END AS gap_to_leader_lapped_text
FROM typed
```

**`models/staging/stg_starting_grid.sql`**

```sql
WITH raw_starting_grid AS (
    SELECT * FROM {{ source('openf1_raw', 'starting_grid') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,
        
        -- Changed from grid_position to position based on likely API schema
        CAST(position AS {{ f1_type('int') }}) AS grid_position
        
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
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,

        CAST(lap_number AS {{ f1_type('int') }}) AS lap_number,
        -- Lap start time: the key that lets weather, position, gaps and telemetry be aligned to a lap
        CAST(date_start AS {{ f1_type('timestamp') }}) AS date_start,
        {{ f1_try_cast_fn() }}(lap_duration AS {{ f1_type('double') }}) AS lap_duration,

        {{ f1_try_cast_fn() }}(duration_sector_1 AS {{ f1_type('double') }}) AS duration_sector_1,
        {{ f1_try_cast_fn() }}(duration_sector_2 AS {{ f1_type('double') }}) AS duration_sector_2,
        {{ f1_try_cast_fn() }}(duration_sector_3 AS {{ f1_type('double') }}) AS duration_sector_3,

        -- Speed trap readings (km/h): intermediate 1, intermediate 2, and the finish-line speed trap
        {{ f1_try_cast_fn() }}(i1_speed AS {{ f1_type('int') }}) AS speed_trap_i1,
        {{ f1_try_cast_fn() }}(i2_speed AS {{ f1_type('int') }}) AS speed_trap_i2,
        {{ f1_try_cast_fn() }}(st_speed AS {{ f1_type('int') }}) AS speed_trap_finish,

        segments_sector_1,
        segments_sector_2,
        segments_sector_3,

        COALESCE(is_pit_out_lap, FALSE) AS is_pit_out_lap
    FROM raw_laps
    WHERE driver_number IS NOT NULL
),
deduplicated_laps AS (
    -- First, filter out exact duplicate lap numbers
    SELECT * FROM renamed_and_casted
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY session_key, driver_number, lap_number 
        ORDER BY date_start ASC
    ) = 1
)
-- Finally, filter out chronological anomalies using LAG
SELECT * 
FROM deduplicated_laps
QUALIFY LAG(date_start) OVER (
    PARTITION BY session_key, driver_number 
    ORDER BY lap_number
) IS NULL 
OR date_start > LAG(date_start) OVER (
    PARTITION BY session_key, driver_number 
    ORDER BY lap_number
)
```

**`models/staging/stg_stints.sql`**

```sql
WITH raw_stints AS (
    SELECT * FROM {{ source('openf1_raw', 'stints') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,
        
        CAST(stint_number AS {{ f1_type('int') }}) AS stint_number,
        compound AS tyre_compound,
        CAST(tyre_age_at_start AS {{ f1_type('int') }}) AS tyre_age_at_start,
        CAST(lap_start AS {{ f1_type('int') }}) AS lap_start,
        CAST(lap_end AS {{ f1_type('int') }}) AS lap_end
    FROM raw_stints
    WHERE driver_number IS NOT NULL
)
SELECT * 
FROM renamed_and_casted
WHERE lap_end >= lap_start 
  AND tyre_age_at_start >= 0
```

**`models/staging/stg_pit.sql`**

```sql
WITH raw_pit AS (
    SELECT * FROM {{ source('openf1_raw', 'pit') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,

        CAST(lap_number AS {{ f1_type('int') }}) AS lap_number,
        CAST(date AS {{ f1_type('timestamp') }}) AS pit_timestamp,
        {{ f1_try_cast_fn() }}(pit_duration AS {{ f1_type('double') }}) AS pit_duration
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
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,
        
        CAST(position AS {{ f1_type('int') }}) AS position,
        CAST(date AS {{ f1_type('timestamp') }}) AS position_timestamp
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
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,
        CAST(date AS {{ f1_type('timestamp') }}) AS interval_timestamp,

        -- Kept as text: the API mixes numbers with strings such as '+1 LAP' (see fct_race_intervals)
        CAST({{ adapter.quote('interval') }} AS {{ f1_type('string') }}) AS interval_raw,
        CAST(gap_to_leader AS {{ f1_type('string') }}) AS gap_to_leader_raw
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
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(date AS {{ f1_type('timestamp') }}) AS overtake_timestamp,

        CAST(overtaking_driver_number AS {{ f1_type('int') }}) AS overtaking_driver_number,
        CAST(overtaken_driver_number AS {{ f1_type('int') }}) AS overtaken_driver_number,
        CAST(position AS {{ f1_type('int') }}) AS position
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
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        -- TRY_CAST: an all-empty Parquet column can be typed as text, which would crash a plain CAST
        {{ f1_try_cast_fn() }}(lap_number AS {{ f1_type('int') }}) AS lap_number,
        {{ f1_try_cast_fn() }}(driver_number AS {{ f1_type('int') }}) AS driver_number,

        CAST(date AS {{ f1_type('timestamp') }}) AS race_control_timestamp,
        category,
        message,
        flag,
        scope,
        {{ f1_try_cast_fn() }}(sector AS {{ f1_type('int') }}) AS sector
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
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,
        
        CAST(date AS {{ f1_type('timestamp') }}) AS radio_timestamp,
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
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,

        CAST(date AS {{ f1_type('timestamp') }}) AS weather_timestamp,
        CAST(air_temperature AS {{ f1_type('double') }}) AS air_temperature,
        CAST(track_temperature AS {{ f1_type('double') }}) AS track_temperature,
        CAST(humidity AS {{ f1_type('double') }}) AS humidity,
        {{ f1_try_cast_fn() }}(pressure AS {{ f1_type('double') }}) AS pressure,
        CAST(rainfall AS {{ f1_type('int') }}) AS rainfall_flag,
        CAST(wind_speed AS {{ f1_type('double') }}) AS wind_speed,
        CAST(wind_direction AS {{ f1_type('int') }}) AS wind_direction
    FROM raw_weather
)
SELECT * FROM renamed_and_casted
```

**`models/staging/stg_car_data.sql`**

```sql
{# SCOPED VIEW: with var session_keys set it exposes only those sessions (see macros/scope.sql); unset, it exposes everything.
   Only this view and stg_location are scoped. A `table` model must never read them (ci/check_scoped_views.py). #}
WITH raw_car_data AS (
    SELECT * FROM {{ source('openf1_raw', 'car_data') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,
        
        CAST(date AS {{ f1_type('timestamp') }}) AS telemetry_timestamp,
        CAST(rpm AS {{ f1_type('int') }}) AS rpm,
        CAST(speed AS {{ f1_type('int') }}) AS speed,
        CAST(n_gear AS {{ f1_type('int') }}) AS gear,
        CAST(throttle AS {{ f1_type('int') }}) AS throttle,
        CAST(brake AS {{ f1_type('int') }}) AS brake,
        CAST(drs AS {{ f1_type('int') }}) AS drs
    FROM raw_car_data
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted
WHERE {{ in_scope('session_key') }}
```

**`models/staging/stg_location.sql`**

```sql
{# SCOPED VIEW: with var session_keys set it exposes only those sessions (see macros/scope.sql); unset, it exposes everything.
   Only this view and stg_location are scoped. A `table` model must never read them (ci/check_scoped_views.py). #}
WITH raw_location AS (
    SELECT * FROM {{ source('openf1_raw', 'location') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,
        
        CAST(date AS {{ f1_type('timestamp') }}) AS location_timestamp,
        CAST(x AS {{ f1_type('double') }}) AS x_coordinate,
        CAST(y AS {{ f1_type('double') }}) AS y_coordinate,
        CAST(z AS {{ f1_type('double') }}) AS z_coordinate
    FROM raw_location
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted
WHERE {{ in_scope('session_key') }}
```

**`models/staging/stg_radio_transcripts.sql`**

```sql
{# OPTIONAL source: when var enable_radio_transcripts is false (default) this is an empty, correctly typed table, so the
   project builds before any transcript file exists (a source glob with no files is a hard error in DuckDB). #}
{% if var('enable_radio_transcripts', false) %}
SELECT
    {{ f1_cast('session_key', 'int') }} AS session_key,
    {{ f1_cast('meeting_key', 'int') }} AS meeting_key,
    {{ f1_cast('driver_number', 'int') }} AS driver_number,
    {{ f1_cast('date', 'timestamp') }} AS radio_timestamp,
    recording_url,
    {{ f1_cast('transcript', 'string') }} AS transcript,
    {{ f1_cast('language', 'string') }} AS language,
    {{ f1_try_cast('audio_duration_seconds', 'double') }} AS audio_duration_seconds,
    {{ f1_cast('error', 'string') }} AS transcription_error
FROM {{ source('openf1_raw', 'radio_transcripts') }}
WHERE driver_number IS NOT NULL
{% else %}
SELECT
    {{ f1_cast('NULL', 'int') }} AS session_key,
    {{ f1_cast('NULL', 'int') }} AS meeting_key,
    {{ f1_cast('NULL', 'int') }} AS driver_number,
    {{ f1_cast('NULL', 'timestamp') }} AS radio_timestamp,
    {{ f1_cast('NULL', 'string') }} AS recording_url,
    {{ f1_cast('NULL', 'string') }} AS transcript,
    {{ f1_cast('NULL', 'string') }} AS language,
    {{ f1_cast('NULL', 'double') }} AS audio_duration_seconds,
    {{ f1_cast('NULL', 'string') }} AS transcription_error
FROM (SELECT 1) AS one_row
WHERE FALSE
{% endif %}
```

**Notes on specific models**

* **`stg_car_data` and `stg_location`:** `date` is renamed to `telemetry_timestamp` or similar, to avoid the reserved-feeling word `date`. They are also the **only scoped views** (third edition): the last line, `WHERE {{ in_scope('session_key') }}`, keeps every row when no scope is set and keeps only the scoped sessions when `session_keys` is set. The macro is in Chapter 7. The rule that makes this safe: **a `table` model must never read a scoped view**, because a table rebuilt from a scoped view would silently lose every session outside the scope. `ci/check_scoped_views.py` (Chapter 11) enforces it.
* **`stg_laps` (changed):** keeps `date_start`, the **lap start time**. The first edition dropped it, and that single omission meant weather, position, gaps and telemetry could not be aligned to a lap at all: it is the root cause of most of the new analytics in Chapter 6. It also keeps the three speed-trap readings.
* **`stg_intervals` (changed):** values stay **text** (`interval_raw`, `gap_to_leader_raw`) because the API mixes numbers and strings such as `+1 LAP`, and it now keeps the sample **timestamp**. The `interval` column is quoted through `adapter.quote`, because `interval` is a reserved word in BigQuery.
* **`stg_pit` and `stg_overtakes` (changed):** both keep their event timestamps, which the undercut and overtake analysis in Chapter 6 joins on.
* **`stg_sessions` (changed):** OpenF1 reports a Sprint with `session_type = 'Race'` and only `session_name = 'Sprint'`. The model rewrites those rows to `session_type = 'Sprint'`, because everything downstream (the points scale, grid mapping, race-only models) expects it. Without this, sprints were scored on the Race points scale.
* **`stg_laps` (changed):** keeps one row per `(session_key, driver_number, lap_number)` (the earliest `date_start`) and drops any lap whose start is not later than the previous lap's, so duplicated or out-of-order source rows cannot reach the models or break the lap-order test (Chapter 6).
* **`stg_stints` (changed):** drops stints that end before they start or have a negative tyre age.
* **`stg_race_control`:** `TRY_CAST` turns un-castable values into `NULL` instead of crashing, which matters when an empty Parquet file makes DuckDB guess the wrong type. It now also keeps `scope`, `sector` and `driver_number`.
* **`stg_session_result` (changed):** `duration` can be a single number (a race) or a JSON array of qualifying times. It is split into `race_time_seconds` and `q1_seconds`, `q2_seconds`, `q3_seconds`, and `gap_to_leader` into a number or the lapped text.
* **`stg_radio_transcripts` (new):** the staging model for optional radio transcripts. While `enable_radio_transcripts` is `false` (the default) it returns an **empty, correctly typed table**, because pointing a source at a glob with no files is a hard error in DuckDB. That keeps the project building before any transcript file exists.
* **Why `{{ f1_type('int') }}` and `{{ f1_try_cast_fn() }}` appear instead of `INTEGER` and `TRY_CAST`:** they are portable macros (Chapter 10) that produce the right type name and cast function for DuckDB or BigQuery. On DuckDB `'int'` means `INTEGER` (and `INT64` on BigQuery).

#### **Task 5: Build and Verify the Layer**

```bash
dbt run --select staging
```

Expect 19 successes. Then look inside the database with the DuckDB command line (or any tool from Chapter 9):

```sql
SELECT COUNT(*) FROM stg_laps;
SELECT * FROM stg_drivers LIMIT 5;
```

### **Learner Pitfalls & Troubleshooting**

* **The Problem:** `Could not find profile named 'dbt_f1_transformations'`.
* **Why it Happens:** the `profile:` value in `dbt_project.yml` does not match the top-level key in `profiles.yml`, or `profiles.yml` is not where dbt looks.
* **The Solution:** make the two names identical. dbt looks for `profiles.yml` in the folder you run it from first (where the second edition keeps it), then in `~/.dbt/`; or pass `--profiles-dir <folder>`.

* **The Problem:** `IO Error: No files found that match the pattern ...`.
* **Why it Happens:** the glob in `sources.yml` does not match any file: wrong root path, wrong file naming, or Airflow has not produced that endpoint yet.
* **The Solution:** list the folder (`ls airflow/data/raw | head`) and compare the names. The pattern is `<endpoint>_*_extracted.parquet`.

* **The Problem:** the editor shows `UnusedConfigKey` warnings on `sources.yml`.
* **Why it Happens:** `external_location` was placed at the wrong nesting level.
* **The Solution:** nest it under `config:` exactly as shown.

* **The Problem:** staging models contained hard-coded file paths, so moving the data meant editing 19 files.
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

* **The Problem (second edition, hit on the author's machine):** `dbt build` stops with `INTERNAL Error: Failed to bind column reference "session_key" [38.0]: inequal types (INTEGER != BIGINT)`, first in `dim_drivers` and `dim_driver_session`, and everything downstream is skipped (112 passed, 2 errors, 9 skipped).
* **Why it Happens:** a **bug in DuckDB 1.5's query optimizer**. It appeared with three ingredients together: Parquet integers are 64-bit (`BIGINT`), the staging model narrowed them with `CAST(... AS INTEGER)`, and a model kept "the latest row per key" with `ROW_NUMBER() ... WHERE row_num = 1`. The optimizer builds a plan that mixes the two types, and DuckDB's own consistency check rejects it. The models in this repository build on DuckDB 1.5.6 and 2.0.0 (both run for this edition), which is why this can pass on one machine and fail on another. Find your version with `python -c "import duckdb; print(duckdb.__version__)"`.
* **Why it was first misdiagnosed:** the crash was first blamed on the `ASOF JOIN` models, and wrapping the CTEs that feed them in `OFFSET 0` (an optimizer barrier that forces DuckDB to evaluate the casts first) did make the build pass. Reproducing the crash in plain SQL showed it also happens **with no ASOF join at all**: even a bare `SELECT` on `dim_drivers`' query fails on 1.5.0, and a `QUALIFY` rewrite or an `OFFSET 0` barrier avoids it. So the barrier treats a symptom.
* **The Solution:** remove the mismatch where it bites. `dim_drivers` and `dim_driver_session` now `CAST(... AS BIGINT)` their key columns (the SQL is in Chapter 6), so the type the optimizer sees matches Parquet's 64-bit integers. `make ci-test` was run for this edition and passes all 135 build and test nodes on DuckDB 1.5.6 and 2.0.0. `f1_type('int')` still produces `INTEGER` on DuckDB. If you hit a similar `INTERNAL Error` in your own model, `OFFSET 0` on the offending CTE is a reasonable stopgap.

* **The Problem (second edition, hit on the author's machine):** the long `fct_telemetry` build finishes, then every test on it errors with `Not implemented Error: Cannot switch temporary directory after the current one has been used`.
* **Why it Happens:** `temp_directory` was configured under the profile's `settings:`. dbt-duckdb re-applies **every** `settings` entry as `SET key = value` on each new cursor, and each test opens one. During the 85 minute build DuckDB spilled to disk and "used" the temporary directory, after which DuckDB 1.5 refuses to be told to switch it, even to the same path. (Reproduced in plain DuckDB 1.5.0: re-issuing the `SET` after a spill fails, while setting the directory when the connection is opened works.)
* **The Solution:** put it under `config_options`, which is applied once when the connection opens. The shipped `profiles.yml` sets no `temp_directory`, so you only meet this error if you add one under `settings:`. `settings:` is fine for options that can change at any time, such as `memory_limit`.

---


## **Chapter 6: Data Transformation II: Building the Analytical Marts**

### **Chapter Overview**

Staging gave you clean tables. The **marts** turn them into answers: *Who gained the most places? How old were the tyres on each lap? Which pit stops were slowest? Did the undercut work? How likely is a safety car in the next ten laps?* In this chapter you will build the **analytical spine** (`int_lap_context`), then **4 dimensions and about 20 fact tables** on top of it, test them, and generate lineage documentation. The second edition adds the pit-strategy models, the safety-car risk model, a **Monte Carlo strategy simulator** and the radio-message model. The telemetry marts get their own chapter (Chapter 7). **[VERIFIED]** for the original models; **[VERIFIED ON SYNTHETIC DATA]** for everything added in the second edition.

**Concepts introduced in this chapter**

* **`ref()`:** `{{ ref('stg_laps') }}` is how one dbt model depends on another. dbt reads these references to build the dependency graph and to run models in the right order.
* **Window functions:** calculations across related rows without collapsing them (`ROW_NUMBER()`, `LAG()`, `AVG() OVER`). `PARTITION BY` defines the groups; `ORDER BY` defines the order inside each group.
* **Range join:** joining on `BETWEEN` instead of equality, for example matching a lap to the stint whose `lap_start` and `lap_end` contain it.
* **`TRY_CAST`:** like `CAST`, but returns `NULL` instead of failing when a value does not convert (`"+1 LAP"` as a number).
* **Data tests:** assertions dbt runs as SQL, such as `not_null` and `unique`.
* **The analytical spine (`int_lap_context`):** one row per driver per lap, carrying *everything* you want to know about that lap (tyre, pit flags, track status, weather, position, gaps). Every pace and strategy model reads from it, so each question is answered once, in one place.
* **ASOF join (introduced in Chapter 4, used heavily here):** "attach the latest reading at or before this moment". It is how a lap is matched to the weather, position and gap readings that were current when it started.
* **Grain:** the one thing a row represents (here: one driver, one lap). Most bugs in this chapter were a violated grain, and the `unique_combination_of_columns` tests exist to catch exactly that.
* **Common random numbers (Task 7):** when comparing strategies by simulation, every strategy is run against the *same* simulated safety cars and lap-time noise, so a difference between strategies is the strategy, not luck.

### **Prerequisites**

* Chapter 5 complete: all 19 staging views build.

### **Step by Step Procedures**

#### **Task 1: Plan the Five Pillars of Analysis**

Marts are designed around questions, not around source tables. This project groups them into five pillars:

| Pillar | Question | Models |
|---|---|---|
| **Core dimensions** | Who and what? | `dim_drivers`, `dim_driver_session`, `dim_sessions`, `dim_circuits` |
| **Pace and tyres** | How fast, on which rubber? | `fct_lap_performance`, `fct_fastest_laps`, `fct_stints`, `fct_tire_strategy`, `fct_compound_pace_model` |
| **Race dynamics** | Who passed whom, who stopped, who got ahead? | `fct_overtakes`, `fct_driver_positions`, `fct_pit_stops`, `fct_pit_loss_by_circuit`, `fct_undercut_overcut`, `fct_race_intervals`, `fct_lap_gaps`, `fct_race_trace` |
| **Results and standings** | Who won, who leads? | `fct_race_results`, `fct_points_progression`, `fct_championship_standings`, `fct_constructor_standings` |
| **Context and risk** | What was happening, what might? | `fct_weather_impact`, `fct_lap_incidents`, `fct_penalties`, `fct_safety_car_events`, `fct_safety_car_hazard`, `fct_team_radio`, `fct_radio_messages` |

The telemetry marts (`fct_telemetry`, its lap summary, the distance trace, the delta trace and the derived-corner models) are large enough to get their own chapter (Chapter 7).

#### **Task 1b: Build the Lap-Context Layer (new in the second edition)**

The first edition built each fact table straight from staging. That worked, but every model had to rediscover the same facts (which stint is this lap in? was it a safety-car lap? what was the weather?), and some of them guessed differently. The second edition computes those facts **once**, in `models/intermediate/`, and lets every mart read the result.

**Step 1: Rebuild the safety-car and red-flag periods.** Race control publishes *messages*, not periods. `int_track_status_periods` turns "SAFETY CAR DEPLOYED" and "SAFETY CAR IN THIS LAP" into a start and an end: A period ends at the earliest of its own closing message, the start of a **more severe** period (VSC, then SC, then RED: on real data a VSC was upgraded to a Safety Car and no "VSC ending" message ever came), or, when neither exists, a cap (`max_unclosed_vsc_seconds` 600 and `max_unclosed_sc_seconds` 1200); `is_unclosed` marks the capped ones.

```sql
{#
  Safety Car / Virtual Safety Car / Red Flag periods rebuilt from race control messages.

  Method: each status type gets its own ordered event stream. A period starts at a "start" event and ends at the
  next event of the same stream (or at the session end if no closing message exists).
    SC  : starts 'SAFETY CAR DEPLOYED',          ends 'SAFETY CAR IN THIS LAP'
    VSC : starts 'VIRTUAL SAFETY CAR DEPLOYED',  ends 'VIRTUAL SAFETY CAR ENDING'
    RED : starts on a track-wide RED flag,       ends on the next track-wide GREEN/CLEAR flag
  SUPERSEDING: a VSC that is upgraded to a Safety Car (or a red flag), or an SC followed by a red flag, ends when the MORE SEVERE period starts. Real
  data (2024 Shanghai): 'VIRTUAL SAFETY CAR DEPLOYED', then 'SAFETY CAR DEPLOYED' 4 minutes later and no 'VIRTUAL SAFETY CAR ENDING' at all.
  APPROXIMATION: an SC period ends when the "in this lap" message is issued, so the lap in which the message is
  issued is still labelled SC (it is the last lap behind the car). Restart-lap effects are not modelled.
#}
WITH rc AS (
    SELECT * FROM {{ ref('stg_race_control') }}
),

classified AS (
    SELECT
        session_key,
        race_control_timestamp AS ts,
        CASE
            WHEN upper(message) LIKE 'VIRTUAL SAFETY CAR%' THEN 'VSC'
            WHEN upper(message) LIKE 'SAFETY CAR%' THEN 'SC'
            WHEN upper(flag) = 'RED' AND coalesce(upper(scope), 'TRACK') = 'TRACK' THEN 'RED'
            WHEN upper(flag) IN ('GREEN', 'CLEAR') AND coalesce(upper(scope), 'TRACK') = 'TRACK' THEN 'RED'
        END AS track_status,
        CASE
            WHEN upper(message) LIKE '%DEPLOYED%' THEN TRUE
            WHEN upper(flag) = 'RED' THEN TRUE
            ELSE FALSE
        END AS is_start
    FROM rc
    WHERE race_control_timestamp IS NOT NULL
),

streams AS (
    SELECT
        *,
        LEAD(ts) OVER (PARTITION BY session_key, track_status ORDER BY ts) AS next_ts
    FROM classified
    WHERE track_status IS NOT NULL
),

starts AS (
    SELECT session_key, track_status, ts, next_ts,
           CASE track_status WHEN 'RED' THEN 1 WHEN 'SC' THEN 2 WHEN 'VSC' THEN 3 END AS severity_rank
    FROM streams WHERE is_start
),

superseded AS (
    SELECT a.session_key, a.track_status, a.ts, MIN(b.ts) AS superseded_ts
    FROM starts a
    JOIN starts b ON a.session_key = b.session_key AND b.ts > a.ts AND b.severity_rank < a.severity_rank
    GROUP BY a.session_key, a.track_status, a.ts
),

session_bounds AS (
    SELECT session_key, date_end AS session_end FROM {{ ref('stg_sessions') }}
)

SELECT
    s.session_key,
    s.track_status,
    s.severity_rank,
    s.ts AS period_start,
    {# End = the earliest of: its own closing message, the start of a more severe period, or (when neither exists) a cap instead of the session end
       (real data produced a 35-lap VSC). RED flags may be long. #}
    CASE
        WHEN u.superseded_ts IS NOT NULL
         AND u.superseded_ts < (CASE WHEN s.next_ts IS NOT NULL THEN s.next_ts
                                     WHEN s.track_status = 'VSC' THEN {{ f1_ts_add_seconds('s.ts', var('max_unclosed_vsc_seconds', 600)) }}
                                     WHEN s.track_status = 'SC'  THEN {{ f1_ts_add_seconds('s.ts', var('max_unclosed_sc_seconds', 1200)) }}
                                     ELSE COALESCE(b.session_end, s.ts) END)
        THEN u.superseded_ts
        WHEN s.next_ts IS NOT NULL THEN s.next_ts
        WHEN s.track_status = 'VSC' THEN {{ f1_ts_add_seconds('s.ts', var('max_unclosed_vsc_seconds', 600)) }}
        WHEN s.track_status = 'SC'  THEN {{ f1_ts_add_seconds('s.ts', var('max_unclosed_sc_seconds', 1200)) }}
        ELSE COALESCE(b.session_end, s.ts)
    END AS period_end,
    (s.next_ts IS NULL AND u.superseded_ts IS NULL) AS is_unclosed
FROM starts s
LEFT JOIN superseded u ON s.session_key = u.session_key AND s.track_status = u.track_status AND s.ts = u.ts
LEFT JOIN session_bounds b ON s.session_key = b.session_key
```

**Step 2: Build the spine.** `int_lap_context` is the longest model in the project, so read it in the order of its CTEs (the named blocks of the `WITH`):

```sql
{#
  One row per driver per lap, enriched with everything a race engineer needs on one line:
  stint/tyre, pit flags, track status, weather at lap start, position and gaps at lap start/end,
  and pace-normalisation flags (representative lap, fuel-corrected time, deltas).
  Every time-based alignment uses an ASOF join (via the portable f1_asof_join macro): "the latest reading at or before this moment".
#}
WITH laps AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        CAST(lap_number AS INTEGER) AS lap_number,
        date_start,
        lap_duration,
        duration_sector_1,
        duration_sector_2,
        duration_sector_3,
        speed_trap_i1,
        speed_trap_i2,
        speed_trap_finish,
        segments_sector_1,
        segments_sector_2,
        segments_sector_3,
        is_pit_out_lap
    FROM {{ ref('stg_laps') }}
),

calculated_laps AS (
    SELECT
        *,
        COALESCE(
            LEAD(date_start) OVER (PARTITION BY session_key, driver_number ORDER BY lap_number),
            {{ f1_ts_add_seconds('date_start', 'lap_duration') }}
        ) AS lap_end_ts,
        MAX(lap_number) OVER (PARTITION BY session_key) AS session_lap_count
    FROM laps
),

sessions AS (
    SELECT 
        CAST(session_key AS INTEGER) AS session_key, 
        session_type, 
        session_name, 
        year 
    FROM {{ ref('stg_sessions') }}
),

stints AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        CAST(stint_number AS INTEGER) AS stint_number,
        tyre_compound,
        CAST(tyre_age_at_start AS INTEGER) AS tyre_age_at_start,
        CAST(lap_start AS INTEGER) AS lap_start,
        CAST(lap_end AS INTEGER) AS lap_end
    FROM {{ ref('stg_stints') }}
),

pit_laps AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        CAST(lap_number AS INTEGER) AS lap_number,
        MIN(pit_timestamp) AS pit_timestamp,
        MAX(pit_duration) AS pit_lane_time_seconds
    FROM {{ ref('stg_pit') }}
    GROUP BY 1, 2, 3
),

with_stint_and_pit AS (
    SELECT
        l.*,
        s.session_type,
        s.session_name,
        s.year,
        st.stint_number,
        st.tyre_compound,
        st.tyre_age_at_start + (l.lap_number - st.lap_start) AS tyre_age_this_lap,
        (p.lap_number IS NOT NULL) AS is_in_lap,
        p.pit_lane_time_seconds,
        p.pit_timestamp
    FROM calculated_laps l
    LEFT JOIN sessions s ON l.session_key = s.session_key
    LEFT JOIN stints st
        ON l.session_key = st.session_key
        AND l.driver_number = st.driver_number
        AND l.lap_number BETWEEN st.lap_start AND st.lap_end
    LEFT JOIN pit_laps p
        ON l.session_key = p.session_key
        AND l.driver_number = p.driver_number
        AND l.lap_number = p.lap_number
    -- Deduplicate laps that fall exactly on the overlap boundary of two stints
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY l.session_key, l.driver_number, l.lap_number 
        ORDER BY st.stint_number DESC
    ) = 1
),

with_out_lap AS (
    SELECT
        *,
        (is_pit_out_lap OR COALESCE(LAG(is_in_lap) OVER (PARTITION BY session_key, driver_number ORDER BY lap_number), FALSE)) AS is_out_lap
    FROM with_stint_and_pit
),

track_status_periods AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        period_start,
        period_end,
        severity_rank
    FROM {{ ref('int_track_status_periods') }}
),

lap_track_status AS (
    SELECT
        l.session_key, l.driver_number, l.lap_number,
        MIN(p.severity_rank) AS worst_rank
    FROM with_out_lap l
    JOIN track_status_periods p
        ON l.session_key = p.session_key
        AND p.period_start <= l.lap_end_ts
        AND p.period_end >= l.date_start
    GROUP BY 1, 2, 3
),

race_control AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        race_control_timestamp,
        flag
    FROM {{ ref('stg_race_control') }}
),

lap_yellow AS (
    SELECT DISTINCT l.session_key, l.driver_number, l.lap_number
    FROM with_out_lap l
    JOIN race_control rc
        ON l.session_key = rc.session_key
        AND rc.race_control_timestamp >= l.date_start
        AND rc.race_control_timestamp < l.lap_end_ts
    WHERE upper(rc.flag) IN ('YELLOW', 'DOUBLE YELLOW')
),

spine AS (
    SELECT 
        CAST(session_key AS INTEGER) AS session_key, 
        CAST(driver_number AS INTEGER) AS driver_number, 
        CAST(lap_number AS INTEGER) AS lap_number, 
        date_start AS lap_start_ts, 
        lap_end_ts 
    FROM with_out_lap
),

stg_weather_clean AS (
    SELECT 
        CAST(session_key AS INTEGER) AS session_key,
        weather_timestamp,
        air_temperature,
        track_temperature,
        humidity,
        wind_speed,
        rainfall_flag
    FROM {{ ref('stg_weather') }}
),

stg_position_clean AS (
    SELECT 
        CAST(session_key AS INTEGER) AS session_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        position_timestamp,
        position
    FROM {{ ref('stg_position') }}
),

stg_intervals_clean AS (
    SELECT 
        CAST(session_key AS INTEGER) AS session_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        interval_timestamp,
        gap_to_leader_raw,
        interval_raw
    FROM {{ ref('stg_intervals') }}
),

weather_at_start AS (
    {{ f1_asof_join(left='spine', right='stg_weather_clean', on=['session_key'], left_ts='lap_start_ts', right_ts='weather_timestamp',
        left_cols=['session_key', 'driver_number', 'lap_number', 'lap_start_ts'],
        right_cols=['weather_timestamp', 'air_temperature', 'track_temperature', 'humidity', 'wind_speed', 'rainfall_flag']) }}
),

position_at_start AS (
    {{ f1_asof_join(left='spine', right='stg_position_clean', on=['session_key', 'driver_number'], left_ts='lap_start_ts', right_ts='position_timestamp',
        left_cols=['session_key', 'driver_number', 'lap_number', 'lap_start_ts'], right_cols=['position AS position_start']) }}
),

position_at_end AS (
    {{ f1_asof_join(left='spine', right='stg_position_clean', on=['session_key', 'driver_number'], left_ts='lap_end_ts', right_ts='position_timestamp',
        left_cols=['session_key', 'driver_number', 'lap_number', 'lap_end_ts'], right_cols=['position AS position_end']) }}
),

gaps_at_end AS (
    {{ f1_asof_join(left='spine', right='stg_intervals_clean', on=['session_key', 'driver_number'], left_ts='lap_end_ts', right_ts='interval_timestamp',
        left_cols=['session_key', 'driver_number', 'lap_number', 'lap_end_ts'], right_cols=['gap_to_leader_raw', 'interval_raw']) }}
),

enriched AS (
    SELECT
        l.*,
        CASE ts.worst_rank WHEN 1 THEN 'RED' WHEN 2 THEN 'SC' WHEN 3 THEN 'VSC' END AS neutralisation_status,
        (y.lap_number IS NOT NULL) AS has_yellow_flag,
        w.weather_timestamp,
        w.air_temperature,
        w.track_temperature,
        w.humidity,
        w.wind_speed,
        w.rainfall_flag,
        ps.position_start,
        pe.position_end,
        {{ f1_try_cast_fn() }}(g.gap_to_leader_raw AS {{ f1_type('double') }}) AS gap_to_leader_end_seconds,
        {{ f1_try_cast_fn() }}(g.interval_raw AS {{ f1_type('double') }}) AS gap_to_car_ahead_end_seconds,
        COALESCE(g.gap_to_leader_raw LIKE '+%LAP%', FALSE) AS is_lapped_at_lap_end
    FROM with_out_lap l
    LEFT JOIN lap_track_status ts
        ON l.session_key = ts.session_key AND l.driver_number = ts.driver_number AND l.lap_number = ts.lap_number
    LEFT JOIN lap_yellow y
        ON l.session_key = y.session_key AND l.driver_number = y.driver_number AND l.lap_number = y.lap_number
    LEFT JOIN weather_at_start w
        ON l.session_key = w.session_key AND l.driver_number = w.driver_number AND l.lap_number = w.lap_number
    LEFT JOIN position_at_start ps
        ON l.session_key = ps.session_key AND l.driver_number = ps.driver_number AND l.lap_number = ps.lap_number
    LEFT JOIN position_at_end pe
        ON l.session_key = pe.session_key AND l.driver_number = pe.driver_number AND l.lap_number = pe.lap_number
    LEFT JOIN gaps_at_end g
        ON l.session_key = g.session_key AND l.driver_number = g.driver_number AND l.lap_number = g.lap_number
),

flagged AS (
    SELECT
        *,
        COALESCE(neutralisation_status, CASE WHEN has_yellow_flag THEN 'YELLOW' END, 'GREEN') AS track_status,
        (lap_number = 1) AS is_first_lap,
        (lap_duration IS NOT NULL AND lap_duration > 0
            AND NOT is_in_lap AND NOT is_out_lap AND lap_number > 1
            AND neutralisation_status IS NULL AND NOT has_yellow_flag) AS is_clean_candidate
    FROM enriched
),

with_medians AS (
    SELECT
        *,
        {{ f1_median_over('CASE WHEN is_clean_candidate THEN lap_duration END', 'session_key, driver_number') }} AS driver_median_clean_lap_seconds
    FROM flagged
)

SELECT
    session_key, meeting_key, driver_number, lap_number,
    session_type, session_name, year,
    date_start AS lap_start_ts,
    lap_end_ts,
    lap_duration AS lap_time_seconds,
    duration_sector_1, duration_sector_2, duration_sector_3,
    speed_trap_i1, speed_trap_i2, speed_trap_finish,
    stint_number, tyre_compound, tyre_age_this_lap,
    is_first_lap, is_in_lap, is_out_lap, pit_lane_time_seconds,
    track_status, neutralisation_status, has_yellow_flag,
    (track_status = 'GREEN') AS is_green_flag_lap,
    weather_timestamp, air_temperature, track_temperature, humidity, wind_speed,
    (COALESCE(rainfall_flag, 0) > 0) AS is_raining_at_lap_start,
    position_start, position_end,
    gap_to_leader_end_seconds, gap_to_car_ahead_end_seconds, is_lapped_at_lap_end,
    driver_median_clean_lap_seconds,
    (is_clean_candidate AND lap_duration <= driver_median_clean_lap_seconds * {{ var('representative_lap_threshold') }}) AS is_representative_lap,
    CASE
        WHEN session_type IN ('Race', 'Sprint') AND lap_duration IS NOT NULL
        THEN lap_duration - {{ var('fuel_effect_seconds_per_lap') }} * (session_lap_count - lap_number)
    END AS fuel_corrected_lap_time_seconds,
    lap_duration - driver_median_clean_lap_seconds AS delta_to_driver_median_seconds,
    {# Cumulative race time = elapsed time at the END of the lap, measured from the first car to finish lap 1. It is built from lap END timestamps
       (the next lap's start, or start + duration for a final lap), NOT from a running SUM of durations: OpenF1 often has no lap-1 duration/start, and
       SUM silently skipped such laps, putting that driver ~100 s 'ahead' for the whole race; it also froze a retiring car's clock. A lap with no
       end timestamp (a retiring car's unfinished last lap) has no cumulative time. #}
    ({{ f1_epoch_ms('lap_end_ts') }}
       - COALESCE(MIN(CASE WHEN lap_number = 1 THEN {{ f1_epoch_ms('lap_end_ts') }} END) OVER (PARTITION BY session_key),
                  MIN({{ f1_epoch_ms('lap_end_ts') }}) OVER (PARTITION BY session_key))) / 1000.0 AS cumulative_race_time_seconds
FROM with_medians
```

**How it works, CTE by CTE**

1. **`laps`, `calculated_laps`:** cast the keys and add `lap_end_ts` (the next lap's start, or start plus duration for the last lap) so each lap has a time window.
2. **`sessions`, `stints`, `pit_laps`, `with_stint_and_pit`:** match each lap to the stint it belongs to, **keeping exactly one** (`QUALIFY ROW_NUMBER() ... ORDER BY stint_number DESC = 1`: a lap shared by two stints goes to the later one). This is where the overlapping-stint bug was fixed; see the first pitfall at the end of the chapter.
3. **`with_out_lap`:** `is_in_lap` (a stop was logged on this lap) and `is_out_lap` (the lap after, or flagged by the API).
4. **`track_status_periods`, `lap_track_status`, `race_control`, `lap_yellow`:** the worst safety-car/VSC/red period that overlaps the lap, and whether a sector yellow was shown inside it.
5. **`spine`, the `stg_*_clean` CTEs, `weather_at_start`, `position_at_start`, `position_at_end`, `gaps_at_end`, `enriched`:** four **ASOF** lookups, each on a *narrow spine* of just the key columns and the one timestamp, then joined back. Narrow spines keep the joins cheap and, as Chapter 10 shows, make the same SQL work on BigQuery.
6. **`flagged`, `with_medians`:** `is_clean_candidate` (a timed lap that is not first/in/out/yellow/SC/VSC/red), each driver's **median clean lap**, and `is_representative_lap` (a clean lap within `representative_lap_threshold` of that median).
7. **The final `SELECT`:** adds `fuel_corrected_lap_time_seconds` (lap time minus `fuel_effect_seconds_per_lap` times the laps of fuel still on board; Race and Sprint only), `delta_to_driver_median_seconds` and `cumulative_race_time_seconds`. The cumulative time is built from lap **end** timestamps, relative to the first car to finish lap 1, because a running sum of durations skipped any lap with no duration and left that driver about 100 s "ahead" for the whole race; it is `NULL` for a retiring car's unfinished last lap.

> **Two assumptions to know about.** The fuel effect (0.035 seconds per lap) is a commonly quoted order of magnitude, not a measured value: calibrate it per circuit before drawing conclusions. And a Safety Car period ends when "SAFETY CAR IN THIS LAP" is issued, so the lap containing that message is still labelled SC; restart-lap effects are not modelled.

#### **Task 2: Create the Models**

Create the files in **models/marts/core/** (dimensions) and **models/marts/analytics/** (facts). The default from `dbt_project.yml` makes them **tables**. Models marked *new* were added in the second edition; the others exist in the first edition, several with changed SQL.

**`models/marts/core/dim_drivers.sql`**

**Dimension.** One row per driver (their most recent session wins), so the driver list never has duplicates. The key is cast to `BIGINT` explicitly (see the DuckDB 1.5 pitfall in Chapter 5).

```sql
WITH staging_drivers AS (
    SELECT * FROM {{ ref('stg_drivers') }}
),

deduplicated_drivers AS (
    SELECT
        CAST(driver_number AS BIGINT) AS driver_number,
        full_name,
        driver_acronym,
        team_name,
        team_color_hex,
        country_code,
        headshot_url,
        -- This ranks a driver's rows, putting their most recent session as #1
        ROW_NUMBER() OVER (PARTITION BY CAST(driver_number AS BIGINT) ORDER BY session_key DESC) as row_num
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

**`models/marts/core/dim_driver_session.sql`**

**Dimension (new).** The driver-to-team assignment **per session**. `dim_drivers` keeps only the latest team, which is wrong for a driver who changed team mid-season; join facts to this table on `(session_key, driver_number)` instead.

```sql
{# Driver-to-team assignment PER SESSION. dim_drivers only keeps the latest team, which is wrong for historical
   joins after a mid-season move; join facts to this table on (session_key, driver_number) instead. #}
WITH ranked AS (
    SELECT
        CAST(session_key AS BIGINT) AS session_key, 
        meeting_key, 
        CAST(driver_number AS BIGINT) AS driver_number, 
        full_name, 
        driver_acronym,
        team_name, 
        team_color_hex, 
        country_code,
        ROW_NUMBER() OVER (PARTITION BY CAST(session_key AS BIGINT), CAST(driver_number AS BIGINT) ORDER BY team_name) AS row_num
    FROM {{ ref('stg_drivers') }}
)
SELECT session_key, meeting_key, driver_number, full_name, driver_acronym, team_name, team_color_hex, country_code
FROM ranked
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

**`models/marts/core/dim_circuits.sql`**

**Dimension (new).** One row per circuit. The API provides no corner or sector geometry; Chapter 7 *derives* corners from telemetry.

```sql
{# One row per circuit. NOTE: the OpenF1 API exposes no corner / sector geometry, so corner-level analysis
   needs an external reference table (see README, "Known limitations"). #}
SELECT
    circuit_key,
    MAX(circuit_short_name) AS circuit_short_name,
    MAX(location) AS location,
    MAX(country_code) AS country_code,
    MIN(year) AS first_year,
    MAX(year) AS last_year,
    COUNT(*) AS sessions_held,
    COUNT(CASE WHEN session_type = 'Race' THEN 1 END) AS races_held
FROM {{ ref('dim_sessions') }}
WHERE circuit_key IS NOT NULL
GROUP BY circuit_key
```

**`models/marts/analytics/fct_lap_performance.sql`**

Lap-level pace, now read from the spine. Filter `is_representative_lap` for pace comparisons; use `fuel_corrected_lap_time_seconds` to compare laps at different fuel loads.

```sql
{# Lap-level pace table. Use is_representative_lap = TRUE for pace comparisons (excludes first/in/out laps,
   yellow/SC/VSC/red laps and slow outliers). fuel_corrected_* is an ASSUMPTION-driven normalisation
   (see var fuel_effect_seconds_per_lap). #}
SELECT
    session_key, meeting_key, driver_number, lap_number,
    session_type, lap_start_ts,
    lap_time_seconds,
    duration_sector_1, duration_sector_2, duration_sector_3,
    speed_trap_i1, speed_trap_i2, speed_trap_finish,
    stint_number, tyre_compound, tyre_age_this_lap,
    is_first_lap, is_in_lap, is_out_lap,
    track_status, is_green_flag_lap, is_representative_lap,
    fuel_corrected_lap_time_seconds,
    delta_to_driver_median_seconds,
    lap_time_seconds - MIN(CASE WHEN is_representative_lap THEN lap_time_seconds END) OVER (PARTITION BY session_key)
        AS delta_to_session_best_representative_seconds,
    position_start, position_end,
    gap_to_leader_end_seconds, gap_to_car_ahead_end_seconds
FROM {{ ref('int_lap_context') }}
```

**`models/marts/analytics/fct_fastest_laps.sql`**

One row per driver per session: best lap, personal-best sectors, **theoretical best lap** (the sum of the best three sectors), and ranking.

```sql
{# One row per driver per session: best lap, personal-best sectors, theoretical best lap and session ranking. #}
WITH valid AS (
    SELECT * FROM {{ ref('int_lap_context') }}
    WHERE lap_time_seconds IS NOT NULL AND lap_time_seconds > 0
),
best_lap AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_key, driver_number ORDER BY lap_time_seconds ASC, lap_number) AS lap_rank
    FROM valid
),
best_sectors AS (
    SELECT
        session_key, driver_number,
        MIN(duration_sector_1) AS best_sector_1_seconds,
        MIN(duration_sector_2) AS best_sector_2_seconds,
        MIN(duration_sector_3) AS best_sector_3_seconds
    FROM valid
    GROUP BY 1, 2
),
joined AS (
    SELECT
        b.session_key, b.meeting_key, b.driver_number, b.lap_number,
        b.lap_time_seconds AS fastest_lap_time_seconds,
        b.tyre_compound, b.tyre_age_this_lap, b.speed_trap_finish,
        s.best_sector_1_seconds, s.best_sector_2_seconds, s.best_sector_3_seconds,
        s.best_sector_1_seconds + s.best_sector_2_seconds + s.best_sector_3_seconds AS theoretical_best_lap_seconds
    FROM best_lap b
    JOIN best_sectors s ON b.session_key = s.session_key AND b.driver_number = s.driver_number
    WHERE b.lap_rank = 1
)
SELECT
    *,
    fastest_lap_time_seconds - theoretical_best_lap_seconds AS time_left_on_table_seconds,
    RANK() OVER (PARTITION BY session_key ORDER BY fastest_lap_time_seconds) AS session_rank,
    fastest_lap_time_seconds - MIN(fastest_lap_time_seconds) OVER (PARTITION BY session_key) AS gap_to_session_fastest_seconds,
    (fastest_lap_time_seconds = MIN(fastest_lap_time_seconds) OVER (PARTITION BY session_key)) AS is_overall_fastest_lap
FROM joined
```

**`models/marts/analytics/fct_stints.sql`**

**New.** One row per driver stint with pace and **tyre degradation**: the slope of fuel-corrected lap time against tyre age over representative laps (positive means the tyre is losing time).

```sql
{# One row per driver stint with pace and DEGRADATION. degradation = slope of (fuel-corrected) lap time against
   tyre age over representative laps; positive = the tyre is losing time. Needs
   var min_stint_laps_for_degradation representative laps, otherwise NULL (too little evidence). #}
SELECT
    session_key, meeting_key, driver_number, stint_number, tyre_compound,
    MIN(lap_number) AS first_lap,
    MAX(lap_number) AS last_lap,
    COUNT(*) AS stint_length_laps,
    MIN(tyre_age_this_lap) AS tyre_age_at_start,
    MAX(tyre_age_this_lap) AS tyre_age_at_end,
    COUNT(CASE WHEN is_representative_lap THEN 1 END) AS representative_laps,
    AVG(CASE WHEN is_representative_lap THEN lap_time_seconds END) AS avg_representative_lap_seconds,
    MIN(CASE WHEN is_representative_lap THEN lap_time_seconds END) AS best_representative_lap_seconds,
    CASE
        WHEN COUNT(CASE WHEN is_representative_lap THEN 1 END) >= {{ var('min_stint_laps_for_degradation') }}
        THEN {{ f1_regr_slope(
                'CASE WHEN is_representative_lap THEN COALESCE(fuel_corrected_lap_time_seconds, lap_time_seconds) END',
                "CASE WHEN is_representative_lap THEN " ~ f1_cast('tyre_age_this_lap', 'double') ~ " END") }}
    END AS degradation_seconds_per_lap
FROM {{ ref('int_lap_context') }}
WHERE stint_number IS NOT NULL
GROUP BY session_key, meeting_key, driver_number, stint_number, tyre_compound
```

**`models/marts/analytics/fct_tire_strategy.sql`**

One row per driver per session: the whole tyre strategy on one line (`MEDIUM>HARD`), now built from `fct_stints`.

```sql
{# One row per driver per session: the whole tyre strategy on one line, e.g. MEDIUM>HARD. #}
SELECT
    session_key, meeting_key, driver_number,
    STRING_AGG(tyre_compound, '>' ORDER BY stint_number) AS strategy,
    COUNT(*) AS stint_count,
    COUNT(*) - 1 AS pit_stop_count,
    SUM(stint_length_laps) AS total_laps,
    {{ f1_list_agg('stint_length_laps', 'stint_number') }} AS stint_lengths,
    COUNT(DISTINCT CASE WHEN tyre_compound IN ('SOFT', 'MEDIUM', 'HARD') THEN tyre_compound END) >= 2 AS used_two_dry_compounds
FROM {{ ref('fct_stints') }}
GROUP BY session_key, meeting_key, driver_number
```

**`models/marts/analytics/fct_pit_stops.sql`**

One row per pit stop: tyre before and after, **time lost**, positions gained, whether it happened under neutralisation, and a slow-stop flag. Only **Race and Sprint** stops are kept: in practice and qualifying OpenF1 reports every garage visit as a pit stop, which would swamp the circuit pit-loss medians.

```sql
{# One row per pit stop. pit_lane_time_seconds is what OpenF1 reports (pit entry to exit), NOT stationary time.
   time_lost_seconds = (in-lap + out-lap) - 2 x the driver's median clean lap: it is only meaningful under green
   flags, and is_under_neutralisation flags stops made under SC/VSC/red where the real cost is much lower.
   position change across the stop includes any on-track passes made on the out-lap.
   ONLY Race and Sprint sessions are kept: in practice and qualifying OpenF1 reports every garage visit as a pit stop
   (real data: median pit-lane time 340 s in practice vs 23.5 s in races), which would swamp circuit pit-loss medians. #}
WITH pits AS (
    SELECT p.*
    FROM {{ ref('stg_pit') }} p
    JOIN {{ ref('stg_sessions') }} s ON p.session_key = s.session_key
    WHERE p.pit_duration IS NOT NULL AND s.session_type IN ('Race', 'Sprint')
),
ctx AS (
    SELECT * FROM {{ ref('int_lap_context') }}
),
stops AS (
    SELECT
        p.session_key, p.meeting_key, p.driver_number, p.lap_number, p.pit_timestamp,
        p.pit_duration AS pit_lane_time_seconds,
        ROW_NUMBER() OVER (PARTITION BY p.session_key, p.driver_number ORDER BY p.lap_number, p.pit_timestamp) AS stop_number,
        i.tyre_compound AS compound_before, i.tyre_age_this_lap AS tyre_age_before,
        o.tyre_compound AS compound_after,
        i.position_start AS position_before_stop,
        o.position_end AS position_after_stop,
        i.track_status AS in_lap_track_status,
        i.lap_time_seconds AS in_lap_time_seconds,
        o.lap_time_seconds AS out_lap_time_seconds,
        i.driver_median_clean_lap_seconds AS reference_lap_seconds
    FROM pits p
    LEFT JOIN ctx i ON p.session_key = i.session_key AND p.driver_number = i.driver_number AND p.lap_number = i.lap_number
    LEFT JOIN ctx o ON p.session_key = o.session_key AND p.driver_number = o.driver_number AND p.lap_number + 1 = o.lap_number
)
SELECT
    *,
    in_lap_time_seconds + out_lap_time_seconds - 2 * reference_lap_seconds AS time_lost_seconds,
    position_before_stop - position_after_stop AS net_positions_gained,
    COALESCE(in_lap_track_status IN ('SC', 'VSC', 'RED'), FALSE) AS is_under_neutralisation,
    {{ f1_median_over('pit_lane_time_seconds', 'session_key') }} AS session_median_pit_lane_time_seconds,
    pit_lane_time_seconds > {{ f1_median_over('pit_lane_time_seconds', 'session_key') }} + {{ var('slow_stop_margin_seconds') }} AS is_slow_stop,
    RANK() OVER (PARTITION BY session_key ORDER BY pit_lane_time_seconds) AS pit_lane_rank
FROM stops
```

**`models/marts/analytics/fct_overtakes.sql`**

Overtake events with lap, tyre, DRS and **pit-cycle** context (a position change within 90 seconds of a pit stop is probably strategy, not a pass).

```sql
{# Overtake events with context. is_probable_pit_cycle_pass = either driver pitted within 90 s of the pass,
   i.e. the position change is likely strategy, not an on-track pass. gap/tyre context is "latest known before the
   pass" (interval samples can be up to ~30 s old). Each ASOF lookup runs on a narrow spine keyed by ot_id and is
   joined back, which keeps the logic identical (and cheap) on DuckDB and BigQuery. #}
WITH o AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY session_key, overtake_timestamp, overtaking_driver_number, overtaken_driver_number) AS ot_id,
        session_key, meeting_key, overtake_timestamp, overtaking_driver_number, overtaken_driver_number, position
    FROM {{ ref('stg_overtakes') }}
),
overtaker AS (
    SELECT ot_id, session_key, overtaking_driver_number AS driver_number, overtake_timestamp FROM o
),
overtaken AS (
    SELECT ot_id, session_key, overtaken_driver_number AS driver_number, overtake_timestamp FROM o
),
overtaker_ctx AS (
    {{ f1_asof_join(left='overtaker', right=ref('int_lap_context'), on=['session_key', 'driver_number'], left_ts='overtake_timestamp', right_ts='lap_start_ts',
        left_cols=['ot_id', 'session_key', 'driver_number', 'overtake_timestamp'],
        right_cols=['lap_number', 'track_status', 'tyre_compound', 'tyre_age_this_lap']) }}
),
overtaken_ctx AS (
    {{ f1_asof_join(left='overtaken', right=ref('int_lap_context'), on=['session_key', 'driver_number'], left_ts='overtake_timestamp', right_ts='lap_start_ts',
        left_cols=['ot_id', 'session_key', 'driver_number', 'overtake_timestamp'],
        right_cols=['tyre_compound', 'tyre_age_this_lap']) }}
),
gap_ctx AS (
    {{ f1_asof_join(left='overtaker', right=ref('stg_intervals'), on=['session_key', 'driver_number'], left_ts='overtake_timestamp', right_ts='interval_timestamp',
        left_cols=['ot_id', 'session_key', 'driver_number', 'overtake_timestamp'], right_cols=['interval_raw']) }}
)
SELECT
    o.session_key, o.meeting_key, o.overtake_timestamp,
    o.overtaking_driver_number, o.overtaken_driver_number,
    o.position AS post_overtake_position,
    a.lap_number,
    a.track_status,
    a.tyre_compound AS overtaker_tyre_compound,
    a.tyre_age_this_lap AS overtaker_tyre_age,
    d.tyre_compound AS overtaken_tyre_compound,
    d.tyre_age_this_lap AS overtaken_tyre_age,
    a.tyre_age_this_lap - d.tyre_age_this_lap AS tyre_age_delta,
    {{ f1_try_cast_fn() }}(g.interval_raw AS {{ f1_type('double') }}) AS overtaker_gap_ahead_before_pass_seconds,
    COALESCE({{ f1_try_cast_fn() }}(g.interval_raw AS {{ f1_type('double') }}) <= {{ var('drs_range_seconds') }}, FALSE) AS was_in_drs_range_before_pass,
    EXISTS (
        SELECT 1 FROM {{ ref('stg_pit') }} p
        WHERE p.session_key = o.session_key
          AND p.driver_number IN (o.overtaking_driver_number, o.overtaken_driver_number)
          AND ABS({{ f1_seconds_between('o.overtake_timestamp', 'p.pit_timestamp') }}) <= 90
    ) AS is_probable_pit_cycle_pass
FROM o
LEFT JOIN overtaker_ctx a ON o.ot_id = a.ot_id
LEFT JOIN overtaken_ctx d ON o.ot_id = d.ot_id
LEFT JOIN gap_ctx g ON o.ot_id = g.ot_id
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

Radio messages enriched with the driver and team names. The team is taken **per session** from `dim_driver_session`, so a mid-season team change does not rewrite history.

```sql
{{ config(
    materialized = 'table'
) }}

{# Team is resolved PER SESSION via dim_driver_session, so a mid-season team change does not rewrite history. #}
WITH radio_data AS (
    SELECT * FROM {{ ref('stg_team_radio') }}
)

SELECT
    r.session_key,
    r.meeting_key,
    r.driver_number,
    d.full_name AS driver_name,
    d.team_name,
    r.radio_timestamp,
    r.recording_url
FROM radio_data r
LEFT JOIN {{ ref('dim_driver_session') }} d
    ON r.session_key = d.session_key AND r.driver_number = d.driver_number
```

**`models/marts/analytics/fct_weather_impact.sql`**

Each lap with the weather **at lap start** (an ASOF join), plus the session-level averages the first edition used.

```sql
{# Weather at the START of each lap (ASOF join to the latest reading at or before lap start), plus the
   session-level averages kept for backwards compatibility. #}
SELECT
    l.session_key, l.meeting_key, l.driver_number, l.lap_number,
    l.lap_time_seconds,
    l.air_temperature, l.track_temperature, l.humidity, l.wind_speed,
    l.is_raining_at_lap_start,
    l.track_temperature - FIRST_VALUE(l.track_temperature) OVER (PARTITION BY l.session_key, l.driver_number ORDER BY l.lap_number)
        AS track_temperature_change_since_first_lap,
    AVG(l.air_temperature) OVER (PARTITION BY l.session_key) AS avg_air_temperature,
    AVG(l.track_temperature) OVER (PARTITION BY l.session_key) AS avg_track_temperature,
    {{ f1_bool_or('l.is_raining_at_lap_start') }} OVER (PARTITION BY l.session_key) AS rained_during_session,
    l.track_status, l.is_representative_lap, l.delta_to_driver_median_seconds
FROM {{ ref('int_lap_context') }} l
WHERE l.lap_time_seconds IS NOT NULL
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

Lap-level track status and race-control activity, attached by **time window** and ranked by **severity** (the first edition picked the alphabetically largest flag).

```sql
{# Lap-level incident view. Race control messages are attached to laps by TIME WINDOW (lap start to next lap
   start), not by the often-empty lap_number column. primary_flag is ranked by severity, not alphabetically. #}
WITH events AS (
    SELECT
        l.session_key, l.driver_number, l.lap_number,
        rc.category, rc.flag, rc.message,
        CASE upper(rc.flag)
            WHEN 'RED' THEN 1 WHEN 'DOUBLE YELLOW' THEN 2 WHEN 'YELLOW' THEN 3
            WHEN 'BLACK AND WHITE' THEN 4 WHEN 'BLACK' THEN 4 WHEN 'BLUE' THEN 5 ELSE 9
        END AS flag_rank
    FROM {{ ref('int_lap_context') }} l
    JOIN {{ ref('stg_race_control') }} rc
        ON l.session_key = rc.session_key
        AND rc.race_control_timestamp >= l.lap_start_ts
        AND rc.race_control_timestamp < l.lap_end_ts
        AND (rc.driver_number IS NULL OR rc.driver_number = l.driver_number)
),
per_lap AS (
    SELECT
        session_key, driver_number, lap_number,
        STRING_AGG(DISTINCT category, ', ' ORDER BY category) AS incident_categories,
        COUNT(*) AS race_control_message_count,
        MIN(flag_rank) AS worst_flag_rank,
        {{ f1_bool_or("upper(message) LIKE '%PENALTY%'") }} AS has_penalty_message
    FROM events
    GROUP BY 1, 2, 3
)
SELECT
    l.session_key, l.meeting_key, l.driver_number, l.lap_number,
    l.lap_time_seconds,
    l.track_status,
    l.neutralisation_status,
    (l.track_status <> 'GREEN') AS is_affected_by_incident,
    p.incident_categories,
    COALESCE(p.race_control_message_count, 0) AS race_control_message_count,
    CASE p.worst_flag_rank WHEN 1 THEN 'RED' WHEN 2 THEN 'DOUBLE YELLOW' WHEN 3 THEN 'YELLOW'
        WHEN 4 THEN 'BLACK AND WHITE' WHEN 5 THEN 'BLUE' END AS primary_flag,
    COALESCE(p.has_penalty_message, FALSE) AS has_penalty_message
FROM {{ ref('int_lap_context') }} l
LEFT JOIN per_lap p
    ON l.session_key = p.session_key AND l.driver_number = p.driver_number AND l.lap_number = p.lap_number
```

**`models/marts/analytics/fct_penalties.sql`**

**New.** Penalties parsed from race-control wording; anything that does not parse stays `NULL` instead of being guessed. A stewards' "PENALTY SERVED" notice is the same penalty, not a second one: those notices are removed from the rows and their lap is attached to the matching penalty as `served_lap`.

```sql
{# Penalties parsed from race control text. Parsing depends on the FIA message wording, so unparsed fields stay NULL
   instead of guessing.
   The stewards also announce when a penalty has been SERVED ('FIA STEWARDS: PENALTY SERVED - 10 SECOND TIME PENALTY FOR CAR 31 (OCO) - ...'). That is the
   same penalty, not a second one: real data counted it twice (laps 37 and 51). Served notices are removed from the penalty rows and their lap is attached
   to the matching penalty (same session, driver and seconds) as served_lap. #}
WITH msgs AS (
    SELECT * FROM {{ ref('stg_race_control') }} WHERE upper(message) LIKE '%PENALTY%'
),

parsed AS (
    SELECT
        session_key, meeting_key,
        COALESCE(driver_number, {{ f1_try_cast_fn() }}({{ f1_regexp_extract('message', 'CAR (\\d+)') }} AS {{ f1_type('int') }})) AS driver_number,
        race_control_timestamp AS penalty_timestamp,
        lap_number,
        {{ f1_try_cast_fn() }}({{ f1_regexp_extract('message', '(\\d+) SECOND') }} AS {{ f1_type('int') }}) AS penalty_seconds,
        CASE
            WHEN upper(message) LIKE '%DRIVE THROUGH%' THEN 'DRIVE THROUGH'
            WHEN upper(message) LIKE '%STOP/GO%' OR upper(message) LIKE '%STOP AND GO%' THEN 'STOP/GO'
            WHEN upper(message) LIKE '%SECOND%' THEN 'TIME PENALTY'
            ELSE 'OTHER'
        END AS penalty_type,
        {{ f1_regexp_extract('message', ' - (.*)$') }} AS reason,
        message,
        upper(message) LIKE '%SERVED%' AS is_served_notice
    FROM msgs
),

served AS (
    SELECT session_key, driver_number, penalty_seconds, MIN(lap_number) AS served_lap
    FROM parsed WHERE is_served_notice
    GROUP BY session_key, driver_number, penalty_seconds
)

SELECT
    p.session_key, p.meeting_key, p.driver_number, p.penalty_timestamp, p.lap_number, p.penalty_seconds, p.penalty_type, p.reason, p.message,
    s.served_lap
FROM parsed p
LEFT JOIN served s
    ON p.session_key = s.session_key AND p.driver_number = s.driver_number AND p.penalty_seconds = s.penalty_seconds
WHERE NOT p.is_served_notice
```

**`models/marts/analytics/fct_race_results.sql`**

Classification, status, qualifying split times and **championship points** from the **season-aware** `f1_points_system` seed (`season, session_type, position, points`: Race scales 2018 to 2027, Sprint 3-2-1 in 2021 and 8-1 from 2022 on; a season missing from the seed fails `assert_every_season_has_points_scale` instead of scoring 0). OpenF1 keys starting-grid rows to the session that **set** the grid (Qualifying for a Race, Sprint Qualifying or Shootout for a Sprint), so the grid is matched directly or through that session, and `grid_source` says which.

```sql
{# Classification + championship points. Points follow the SEASON-AWARE seed f1_points_system (season, session_type, position),
   so a Sprint in 2021 scores 3-2-1 and a Sprint in 2023 scores 8-1. Seasons missing from the seed score 0 and are caught by
   tests/assert_every_season_has_points_scale.sql. The fastest-lap bonus applies to Race sessions up to
   var fastest_lap_bonus_last_year for drivers finishing in the top 10. DSQ/DNS score 0. #}
WITH results AS (
    SELECT * FROM {{ ref('stg_session_result') }}
),
grid AS (
    SELECT * FROM {{ ref('stg_starting_grid') }}
),
sessions AS (
    SELECT session_key, meeting_key, session_name, session_type, year, date_start AS session_start_time FROM {{ ref('stg_sessions') }}
),
{# OpenF1 can key starting_grid rows to the session that SET the grid (Qualifying for a Race, Sprint Qualifying/Shootout for a Sprint)
   instead of the race itself. grid_session maps each race to that session; the direct match always wins. #}
grid_session AS (
    SELECT session_key, grid_session_key FROM (
        SELECT s.session_key, q.session_key AS grid_session_key,
               ROW_NUMBER() OVER (PARTITION BY s.session_key ORDER BY q.session_start_time DESC) AS rn
        FROM sessions s
        JOIN sessions q ON q.meeting_key = s.meeting_key AND q.session_start_time < s.session_start_time
         AND ((s.session_type = 'Race' AND q.session_name = 'Qualifying')
           OR (s.session_type = 'Sprint' AND q.session_name IN ('Sprint Qualifying', 'Sprint Shootout')))
        WHERE s.session_type IN ('Race', 'Sprint')
    ) m WHERE rn = 1
),
fastest AS (
    SELECT session_key, driver_number FROM {{ ref('fct_fastest_laps') }} WHERE is_overall_fastest_lap
),
calc AS (
    SELECT
        r.session_key, r.meeting_key, r.driver_number,
        s.session_type, s.year, s.session_start_time,
        COALESCE(g1.grid_position, g2.grid_position) AS grid_position,
        CASE WHEN g1.grid_position IS NOT NULL THEN 'starting_grid' WHEN g2.grid_position IS NOT NULL THEN 'starting_grid_via_qualifying' END AS grid_source,
        r.position AS final_position,
        CASE WHEN COALESCE(g1.grid_position, g2.grid_position) IS NOT NULL AND r.position IS NOT NULL
             THEN COALESCE(g1.grid_position, g2.grid_position) - r.position END AS positions_gained_or_lost,
        CASE
            WHEN r.dsq THEN 'DSQ' WHEN r.dns THEN 'DNS' WHEN r.dnf THEN 'DNF'
            WHEN r.gap_to_leader_lapped_text IS NOT NULL THEN 'Lapped'
            ELSE 'Finished'
        END AS classification_status,
        r.number_of_laps AS laps_completed,
        r.race_time_seconds,
        r.gap_to_leader_seconds,
        r.gap_to_leader_lapped_text,
        r.q1_seconds, r.q2_seconds, r.q3_seconds,
        r.duration_raw AS race_duration,
        r.dns AS did_not_start,
        r.dsq AS disqualified,
        (f.driver_number IS NOT NULL) AS set_session_fastest_lap
    FROM results r
    LEFT JOIN grid g1 ON r.session_key = g1.session_key AND r.driver_number = g1.driver_number
    LEFT JOIN grid_session gs ON r.session_key = gs.session_key
    LEFT JOIN grid g2 ON gs.grid_session_key = g2.session_key AND r.driver_number = g2.driver_number
    LEFT JOIN sessions s ON r.session_key = s.session_key
    LEFT JOIN fastest f ON r.session_key = f.session_key AND r.driver_number = f.driver_number
)
SELECT
    c.*,
    (c.final_position = 1) AS is_winner,
    (c.final_position <= 3) AS is_podium,
    CASE WHEN c.classification_status IN ('DSQ', 'DNS') THEN 0 ELSE COALESCE(p.points, 0) END AS position_points,
    CASE WHEN c.session_type = 'Race' AND c.set_session_fastest_lap AND c.final_position <= 10
              AND c.classification_status NOT IN ('DSQ', 'DNS') AND c.year <= {{ var('fastest_lap_bonus_last_year') }}
         THEN 1 ELSE 0 END AS fastest_lap_bonus_points,
    CASE WHEN c.classification_status IN ('DSQ', 'DNS') THEN 0 ELSE COALESCE(p.points, 0) END
      + CASE WHEN c.session_type = 'Race' AND c.set_session_fastest_lap AND c.final_position <= 10
                  AND c.classification_status NOT IN ('DSQ', 'DNS') AND c.year <= {{ var('fastest_lap_bonus_last_year') }}
             THEN 1 ELSE 0 END AS total_points
FROM calc c
LEFT JOIN {{ ref('f1_points_system') }} p
    ON c.year = p.season AND c.session_type = p.session_type AND c.final_position = p.position
```

**`models/marts/analytics/fct_points_progression.sql`**

**New.** Season points after every Race or Sprint, computed from `fct_race_results`.

```sql
{# Season points after every Race/Sprint, computed from fct_race_results (not from the beta standings endpoint),
   so it is complete for every session that has results. #}
WITH scored AS (
    SELECT r.session_key, r.driver_number, r.year, r.session_type, r.total_points, s.session_start_time
    FROM {{ ref('fct_race_results') }} r
    JOIN {{ ref('dim_sessions') }} s ON r.session_key = s.session_key
    WHERE r.session_type IN ('Race', 'Sprint')
),
cum AS (
    SELECT
        *,
        SUM(total_points) OVER (PARTITION BY year, driver_number ORDER BY session_start_time, session_key) AS cumulative_points
    FROM scored
)
SELECT
    session_key, year, driver_number, session_type, session_start_time,
    total_points AS points_scored_in_session,
    cumulative_points,
    MAX(cumulative_points) OVER (PARTITION BY year, session_key) - cumulative_points AS gap_to_points_leader,
    RANK() OVER (PARTITION BY year, session_key ORDER BY cumulative_points DESC) AS championship_position_after_session
FROM cum
```

**`models/marts/analytics/fct_race_intervals.sql`**

Gaps to the leader and the car ahead as numbers, with lapped drivers kept as text, a DRS-range proxy flag and a sample duration for time-weighting.

```sql
{# Time series of gaps. Lapped cars have text gaps ('+1 LAP') which become NULL numbers with the text kept in
   lapped_status. is_in_drs_range is a PROXY: real DRS eligibility is judged at detection points, which the API
   does not expose. sample_duration_seconds lets downstream models time-weight the samples. #}
WITH base AS (
    SELECT
        iv.session_key, iv.meeting_key, iv.driver_number, iv.interval_timestamp,
        {{ f1_try_cast_fn() }}(iv.gap_to_leader_raw AS {{ f1_type('double') }}) AS gap_to_leader_seconds,
        {{ f1_try_cast_fn() }}(iv.interval_raw AS {{ f1_type('double') }}) AS gap_to_car_ahead_seconds,
        CASE WHEN iv.gap_to_leader_raw LIKE '+%LAP%' THEN iv.gap_to_leader_raw END AS lapped_status,
        -- COALESCE: the last sample of a driver has no next sample. DuckDB's LEAST ignores NULLs (giving 60) but BigQuery's
        -- LEAST returns NULL if any argument is NULL, so the cap is made explicit.
        LEAST(60, COALESCE({{ f1_seconds_between('iv.interval_timestamp', 'LEAD(iv.interval_timestamp) OVER (PARTITION BY iv.session_key, iv.driver_number ORDER BY iv.interval_timestamp)') }}, 60))
            AS sample_duration_seconds
    FROM {{ ref('stg_intervals') }} iv
),
with_lap AS (
    {{ f1_asof_join(left='base', right=ref('stg_laps'), on=['session_key', 'driver_number'], left_ts='interval_timestamp', right_ts='date_start',
        left_cols=['session_key', 'meeting_key', 'driver_number', 'interval_timestamp', 'gap_to_leader_seconds',
                   'gap_to_car_ahead_seconds', 'lapped_status', 'sample_duration_seconds'],
        right_cols=['lap_number']) }}
)
SELECT
    b.*,
    COALESCE(b.gap_to_car_ahead_seconds <= {{ var('drs_range_seconds') }}, FALSE) AS is_in_drs_range,
    COALESCE(b.gap_to_car_ahead_seconds <= {{ var('dirty_air_seconds') }}, FALSE) AS is_in_dirty_air
FROM with_lap b
```

**`models/marts/analytics/fct_lap_gaps.sql`**

**New.** Per driver per lap: seconds spent in DRS range and in dirty air, and the gap evolution, time-weighted.

```sql
{# Per driver per lap: how long was the car in DRS range / dirty air, and the gap evolution. Time-weighted. #}
SELECT
    session_key, meeting_key, driver_number, lap_number,
    COUNT(*) AS interval_samples,
    SUM(sample_duration_seconds) AS covered_seconds,
    SUM(CASE WHEN is_in_drs_range THEN sample_duration_seconds END) AS seconds_in_drs_range,
    SUM(CASE WHEN is_in_dirty_air THEN sample_duration_seconds END) AS seconds_in_dirty_air,
    SUM(CASE WHEN is_in_dirty_air THEN sample_duration_seconds END) / NULLIF(SUM(sample_duration_seconds), 0) AS pct_lap_in_dirty_air,
    AVG(gap_to_car_ahead_seconds) AS avg_gap_to_car_ahead_seconds,
    MIN(gap_to_car_ahead_seconds) AS min_gap_to_car_ahead_seconds,
    {{ f1_arg_max('gap_to_leader_seconds', 'interval_timestamp') }} AS gap_to_leader_last_sample_seconds
FROM {{ ref('fct_race_intervals') }}
WHERE lap_number IS NOT NULL
GROUP BY session_key, meeting_key, driver_number, lap_number
```

**`models/marts/analytics/fct_race_trace.sql`**

**New.** The classic race trace: each driver's gap to the leader every lap, from cumulative lap time.

```sql
{# The classic "race trace": for every lap, each driver's gap to the leader computed from cumulative lap times
   (independent of the sampled intervals endpoint), plus position on cumulative time. #}
SELECT
    session_key, meeting_key, driver_number, lap_number,
    lap_time_seconds,
    cumulative_race_time_seconds,
    cumulative_race_time_seconds - MIN(cumulative_race_time_seconds) OVER (PARTITION BY session_key, lap_number)
        AS gap_to_leader_by_cumulative_time_seconds,
    RANK() OVER (PARTITION BY session_key, lap_number ORDER BY cumulative_race_time_seconds) AS position_by_cumulative_time,
    position_end AS reported_position_end,
    tyre_compound, track_status, is_in_lap
FROM {{ ref('int_lap_context') }}
WHERE session_type IN ('Race', 'Sprint') AND cumulative_race_time_seconds IS NOT NULL
```

> **Design note on `fct_weather_impact` (updated).** Weather is sampled about once a minute, while laps are discrete events. The first edition averaged weather **per session** and attached the average to every lap, which cannot say "it was raining on lap 41". The second edition does the time-aligned version it suggested as an exercise: an **ASOF join** attaches the latest reading at or before each lap's start (inside `int_lap_context`), and the old session averages are kept as extra columns for compatibility.

#### **Task 3: Run the Marts**

```bash
dbt build --select +marts      # everything the marts need (staging, intermediate) plus the marts and their tests
dbt build                      # the whole project
```

> **Selection tip.** `+model` means "this model and everything *upstream*", `model+` means "this model and everything *downstream*", `+model+` means both. A common mistake is `dbt build --select int_lap_context+` on a warehouse that has never built `int_track_status_periods`: it selects the spine and what depends on it, **not** what the spine depends on, and fails with `Catalog Error: Table with name int_track_status_periods does not exist`. Use `+int_lap_context+`.

The project now has **19 staging views, 2 intermediate tables, 4 dimensions and about 25 fact tables**. Power BI connects to the mart tables (Chapter 9).

#### **Task 4: Add Tests and Documentation**

Create **models/marts/schema.yml**. Descriptions here become your documentation site, and the `tests:` entries become checks.

```yaml
version: 2

# Grain tests (unique_combination_of_columns) are ERRORS: a duplicated grain silently corrupts every downstream
# aggregate. Plausibility checks live in tests/ and are warnings where real-world data can legitimately surprise.

models:
  - name: dim_drivers
    description: "Latest known identity and team per driver number. For team at a point in time use dim_driver_session."
    columns:
      - name: driver_number
        data_tests: [unique, not_null]
      - name: full_name
        data_tests: [not_null]

  - name: dim_driver_session
    description: "Driver identity and team for each session (keeps mid-season team changes)."
    data_tests:
      - unique_combination_of_columns: {arguments: {combination_of_columns: [session_key, driver_number]}}
    columns:
      - name: session_key
        data_tests:
          - relationships: {arguments: {to: ref('dim_sessions'), field: session_key}}

  - name: dim_sessions
    description: "Every session enriched with its meeting and circuit."
    columns:
      - name: session_key
        data_tests: [unique, not_null]
      - name: session_type
        data_tests:
          - accepted_values:
              arguments:
                values: ['Practice', 'Qualifying', 'Race', 'Sprint', 'Sprint Qualifying', 'Sprint Shootout']
              config: {severity: warn}

  - name: dim_circuits
    description: "One row per circuit. No corner geometry (not provided by OpenF1)."
    columns:
      - name: circuit_key
        data_tests: [unique, not_null]

  - name: fct_lap_performance
    description: "Lap-level pace with tyre, track status and normalisation flags. Filter is_representative_lap for pace work."
    data_tests:
      - unique_combination_of_columns: {arguments: {combination_of_columns: [session_key, driver_number, lap_number]}}
    columns:
      - name: session_key
        data_tests:
          - not_null
          - relationships: {arguments: {to: ref('dim_sessions'), field: session_key}}
      - name: track_status
        data_tests:
          - accepted_values: {arguments: {values: ['GREEN', 'YELLOW', 'VSC', 'SC', 'RED']}}
      - name: tyre_compound
        data_tests:
          - accepted_values:
              arguments: {values: ['SOFT', 'MEDIUM', 'HARD', 'INTERMEDIATE', 'WET', 'UNKNOWN', 'TEST_UNKNOWN']}
              config: {severity: warn}
      - name: fuel_corrected_lap_time_seconds
        description: "ASSUMPTION-DRIVEN: lap time minus var fuel_effect_seconds_per_lap x laps remaining. Race/Sprint only."

  - name: fct_fastest_laps
    description: "Best lap, personal-best sectors, theoretical best lap and ranking per driver per session."
    data_tests:
      - unique_combination_of_columns: {arguments: {combination_of_columns: [session_key, driver_number]}}
    columns:
      - name: fastest_lap_time_seconds
        data_tests: [not_null]
      - name: theoretical_best_lap_seconds
        description: "Sum of the driver's best three sectors (may be NULL if a sector time is missing)."
      - name: is_overall_fastest_lap
        description: "TRUE for the driver holding the session's fastest lap."

  - name: fct_stints
    description: "One row per stint with pace and tyre degradation (seconds lost per lap of tyre age)."
    data_tests:
      - unique_combination_of_columns: {arguments: {combination_of_columns: [session_key, driver_number, stint_number]}}
    columns:
      - name: degradation_seconds_per_lap
        description: "Regression slope over representative laps; NULL when fewer than var min_stint_laps_for_degradation."

  - name: fct_tire_strategy
    description: "One row per driver per session: compound sequence, stop count and stint lengths."
    data_tests:
      - unique_combination_of_columns: {arguments: {combination_of_columns: [session_key, driver_number]}}

  - name: fct_pit_stops
    description: "One row per pit stop with time lost, position change and slow-stop flag."
    data_tests:
      - unique_combination_of_columns: {arguments: {combination_of_columns: [session_key, driver_number, stop_number]}}
    columns:
      - name: pit_lane_time_seconds
        description: "Pit entry to exit as reported by OpenF1 (NOT stationary time)."
        data_tests: [not_null]
      - name: time_lost_seconds
        description: "In-lap + out-lap minus 2x the driver's median clean lap. Not meaningful under SC/VSC (see is_under_neutralisation); NULL for a lap-1 stop."

  - name: fct_penalties
    description: "Penalties parsed from race control messages; unparsed fields are NULL."
    columns:
      - name: session_key
        data_tests: [not_null]

  - name: fct_lap_incidents
    description: "Lap-level track status and race control activity matched by time window."
    data_tests:
      - unique_combination_of_columns: {arguments: {combination_of_columns: [session_key, driver_number, lap_number]}}

  - name: fct_race_intervals
    description: "Gap time series. is_in_drs_range is a proxy (<= var drs_range_seconds), not the official detection."
    data_tests:
      - unique_combination_of_columns: {arguments: {combination_of_columns: [session_key, driver_number, interval_timestamp]}}

  - name: fct_lap_gaps
    description: "Time-weighted DRS-range / dirty-air exposure per driver per lap."
    data_tests:
      - unique_combination_of_columns: {arguments: {combination_of_columns: [session_key, driver_number, lap_number]}}

  - name: fct_weather_impact
    description: "Each lap with the weather at lap start (ASOF join) plus session averages."
    data_tests:
      - unique_combination_of_columns: {arguments: {combination_of_columns: [session_key, driver_number, lap_number]}}

  - name: fct_race_results
    description: "Classification, status, points (current scales) and qualifying split times."
    data_tests:
      - unique_combination_of_columns: {arguments: {combination_of_columns: [session_key, driver_number]}}
    columns:
      - name: classification_status
        data_tests:
          - accepted_values: {arguments: {values: ['Finished', 'Lapped', 'DNF', 'DNS', 'DSQ']}}

  - name: fct_points_progression
    description: "Cumulative season points per driver after each Race/Sprint, from fct_race_results."
    data_tests:
      - unique_combination_of_columns: {arguments: {combination_of_columns: [session_key, driver_number]}}

  - name: fct_overtakes
    description: "Overtakes with lap, tyre, DRS and pit-cycle context."
    columns:
      - name: session_key
        data_tests: [not_null]
      - name: overtaking_driver_number
        data_tests: [not_null]
      - name: overtaken_driver_number
        data_tests: [not_null]

  - name: fct_race_trace
    description: "Gap to leader per lap from cumulative lap time (race trace)."
    data_tests:
      - unique_combination_of_columns: {arguments: {combination_of_columns: [session_key, driver_number, lap_number]}}

  - name: fct_telemetry
    description: >
      Car telemetry with ASOF-joined position and lap number. Incremental by session; a scoped run (var session_keys) replaces those sessions.
      Uniqueness is deliberately not tested here (tens of millions of rows); see assert_telemetry_speed_plausible.
      The not_null tests are scope-aware (not_null_in_scope): a scoped build scans only the sessions it replaced.
    columns:
      - name: session_key
        data_tests: [not_null_in_scope]
      - name: telemetry_timestamp
        data_tests: [not_null_in_scope]

  - name: fct_telemetry_lap_summary
    description: "Per-lap telemetry KPIs (top speed, throttle/brake application, braking zones, DRS, gear usage). Incremental by session."
    data_tests:
      - unique_combination_of_columns: {arguments: {combination_of_columns: [session_key, driver_number, lap_number], scope_column: session_key}}

  - name: fct_telemetry_distance_trace
    description: "Telemetry on a distance axis (var distance_bin_metres) for lap-to-lap comparison. Incremental by session."
    data_tests:
      - unique_combination_of_columns: {arguments: {combination_of_columns: [session_key, driver_number, lap_number, distance_bin], scope_column: session_key}}

  - name: fct_lap_time_delta_trace
    description: "Each driver's best lap vs the session fastest lap along the lap distance. Incremental by session."

  - name: fct_championship_standings
    description: "Driver standings snapshots from the (beta) OpenF1 championship endpoint."
    columns:
      - name: session_key
        data_tests: [not_null]

  - name: fct_constructor_standings
    description: "Constructor standings snapshots from the (beta) OpenF1 championship endpoint."

  - name: fct_driver_positions
    description: "Position change events with the previous position."

  - name: fct_team_radio
    description: "Team radio recording metadata (no transcription)."

  - name: fct_compound_pace_model
    description: "Per driver per compound: fresh-tyre pace, degradation slope and r-squared from clean fuel-corrected laps. Input to the strategy simulator."
    data_tests:
      - unique_combination_of_columns: {arguments: {combination_of_columns: [session_key, driver_number, tyre_compound]}}

  - name: fct_pit_loss_by_circuit
    description: "Typical pit stop cost per circuit, split into green-flag and neutralised stops."
    columns:
      - name: circuit_key
        data_tests: [unique, not_null]

  - name: fct_undercut_overcut
    description: "Pairs of related pit stops where the attacker ran directly behind the defender, with the outcome. See model header for limits."
    data_tests:
      - unique_combination_of_columns:
          arguments: {combination_of_columns: [session_key, attacker_driver_number, attacker_stop_number, defender_driver_number, defender_stop_number]}
    columns:
      - name: strategy_type
        data_tests:
          - accepted_values: {arguments: {values: ['UNDERCUT', 'OVERCUT', 'SAME_LAP']}}

  - name: fct_safety_car_events
    description: "SC / VSC / red periods in Race sessions with the race laps they covered."
    columns:
      - name: track_status
        data_tests:
          - accepted_values: {arguments: {values: ['SC', 'VSC', 'RED']}}
      - name: start_lap
        data_tests: [not_null]

  - name: fct_safety_car_hazard
    description: "Empirical-Bayes probability of an SC/VSC start in the next N laps per circuit. Weak per-circuit evidence: check low_sample."
    data_tests:
      - unique_combination_of_columns: {arguments: {combination_of_columns: [circuit_key, track_status, laps_remaining]}}
    columns:
      - name: p_at_least_one_start
        data_tests: [not_null]

  - name: dim_session_corners
    description: >
      DERIVED corners (detected from reference-lap telemetry, not official turn numbers). One row per detected apex per session. Incremental by session.
    data_tests:
      - unique_combination_of_columns: {arguments: {combination_of_columns: [session_key, corner_number], scope_column: session_key}}
    columns:
      - name: speed_class
        data_tests:
          - accepted_values: {arguments: {values: ['SLOW', 'MEDIUM', 'FAST']}}

  - name: fct_corner_performance
    description: "Entry/apex/exit speed, braking distance and throttle pick-up per driver per lap per detected corner. Incremental by session."
    data_tests:
      - unique_combination_of_columns: {arguments: {combination_of_columns: [session_key, driver_number, lap_number, corner_number], scope_column: session_key}}

  - name: fct_radio_messages
    description: >
      Transcribed team radio with race context and KEYWORD tags (heuristic, not NLP). Empty unless var enable_radio_transcripts is true.
    columns:
      - name: recording_url
        data_tests: [unique]
```

Run the tests:

```bash
dbt test
```

Each test is a query that returns the **bad rows**; zero rows means pass. A **grain test** (`unique_combination_of_columns`) fails when a row is duplicated, and one of those caught the overlapping-stint bug on real data (see the pitfalls below).

> **Documentation drift, a lesson in itself.** An earlier version of this file described `is_overall_fastest_lap` and `is_slow_stop` columns and an ASOF join for weather. **None of those exist in the models.** dbt did not complain because only the *tested* columns are checked, so the docs lied quietly. The version above is corrected to match the SQL. Treat `schema.yml` as code: review it whenever a model changes.

**The singular tests (new in the second edition).** Besides the tests in `schema.yml`, `dbt_f1_transformations/tests/` holds 32 queries written for this project. The custom generic tests live in `tests/generic/` and have **no package dependency** (the dbt package hub is not always reachable): `unique_combination_of_columns`, and, new in the third edition, `not_null_in_scope`.

| Test | What it protects | Severity |
|---|---|---|
| `assert_lap_times_positive` | no zero or negative lap time | error |
| `assert_lap_start_times_increase` | lap *N+1* starts after lap *N* (catches duplicated or mis-numbered laps; `stg_laps` removes such source rows) | error |
| `assert_stint_ranges_valid` | a stint cannot end before it starts or have negative tyre age (`stg_stints` removes such source rows) | error |
| `assert_one_fastest_lap_per_session` | every session with laps has an overall-fastest row | error |
| `assert_race_points_not_inflated` | duplicated result rows would inflate points | error |
| `assert_undercut_outcome_consistent` | the recorded outcome agrees with the signed gap | error |
| `assert_safety_car_probability_valid` | probabilities within 0 and 1 and never falling as the horizon grows | error |
| `assert_telemetry_speed_plausible` | no negative speed or one above 400 km/h | error |
| `assert_pit_lane_time_plausible` | pit-lane times between 10 and 120 s (`fct_pit_stops` is Race and Sprint only); a review of long race stops | warn |
| `assert_pit_stops_only_in_races` | `fct_pit_stops` holds no practice or qualifying garage visits | error |
| `assert_circuit_pit_loss_is_plausible` | each circuit's median green-flag pit-lane time is a race-stop value (10 to 120 s) | error |
| `assert_sprints_are_typed_sprint` | a sprint is typed `Sprint`, not `Race` | error |
| `assert_sprint_points_use_sprint_scale` | sprint points come from the Sprint scale | error |
| `assert_every_season_has_points_scale` | every loaded season has a points scale in the seed | error |
| `assert_race_results_have_grid` | every race has at least one driver with a grid position | warn |
| `assert_race_trace_leader_time_increases` | the leader's cumulative race time rises lap by lap | error on CI, **warn elsewhere** |
| `assert_neutralisation_periods_are_bounded` | an SC/VSC with no closing message never runs past its cap | error |
| `assert_neutralisation_periods_do_not_overlap` | SC/VSC periods in Race and Sprint sessions do not overlap | error on CI, **warn elsewhere** |
| `assert_penalties_not_double_counted` | a "penalty served" notice is not counted as a second penalty | error on CI, **warn elsewhere** |
| `assert_telemetry_samples_within_lap_window` | no telemetry sample is credited to a lap after that lap ended | error |
| `assert_corner_apex_is_a_minimum` | on each session's reference lap, an apex is slower than its entry and exit | warn |
| `assert_unavailable_telemetry_is_masked` | a throttle above 100 ("no data") never reaches the distance trace as a value | error |
| `assert_frozen_telemetry_is_masked` | CI only: a run of identical readings (the synthetic data freezes 3 s of one lap) comes out of the distance trace as NULL, not as driving | error, CI only |
| `assert_corner_direction_needs_angle` | a corner has a direction only if its turn angle was measured | error |
| `assert_out_laps_follow_api_flag` | where the API flags out-laps, a lap is an out-lap exactly when the API says so | error |
| `assert_green_stops_have_green_laps` | a stop counted as green-flag has a green in-lap and out-lap | error |
| `assert_grid_sources_are_not_mixed` | a session's grid comes from the starting-grid feed or from a classification, never a mix | error |
| `assert_push_laps_are_not_in_or_out_laps` | CI only: relies on the shape of the synthetic qualifying session | error, CI only |
| `assert_sprint_grid_falls_back_to_qualifying_classification` | CI only: a Sprint with no starting-grid rows takes its grid from the qualifying classification | error, CI only |
| `assert_positions_in_range`, `assert_race_results_have_laps`, `assert_results_have_driver_dimension` | sanity and reconciliation | warn |

**Why `assert_lap_start_times_increase` and `assert_stint_ranges_valid` are errors.** Both check the *source data*, and real OpenF1 data does contain such rows (the author's first real run reported 59 and 23). Instead of downgrading them to warnings, `stg_laps` and `stg_stints` now remove those rows, so the tests guard the staging layer's cleaning and stay strict everywhere. **Three newer tests are different.** `assert_race_trace_leader_time_increases`, `assert_neutralisation_periods_do_not_overlap` and `assert_penalties_not_double_counted` can fail on real data for reasons that need a human look, so their severity is set from the target name: `config(severity = 'error' if target.name == 'ci' else 'warn')`. `docs/ISSUES_CHECKLIST.md` lists what each real-data warning turned out to be.

**Scope-aware tests (third edition).** The telemetry tables hold hundreds of millions of rows, so a build scoped to one session must not scan all of them for its tests. The heavy tests therefore honour the session scope: the singular telemetry tests (`assert_telemetry_speed_plausible`, `assert_telemetry_samples_within_lap_window`, `assert_unavailable_telemetry_is_masked`, `assert_frozen_telemetry_is_masked`, `assert_corner_apex_is_a_minimum`, `assert_corner_direction_needs_angle`) end with `AND {{ in_scope('session_key') }}`, which is the always-true `1 = 1` when no scope is set, and the generic tests on the heavy models take a `scope_column`:

```yaml
- unique_combination_of_columns: {arguments: {combination_of_columns: [session_key, driver_number, lap_number], scope_column: session_key}}
```

`fct_telemetry` uses the new `not_null_in_scope` instead of the built-in `not_null`. Both generic tests wrap the table in `scoped_test_relation(model, scope_column)` (Chapter 7), so with a scope set only the replaced sessions are checked and with no scope the whole table is checked exactly as before. This is done inside the test macros rather than with a `where:` config because `schema.yml` cannot call project macros. A scoped build can only change the sessions it replaced, so checking only those loses nothing.

**The test suite was mutation-tested.** A test that has never failed proves nothing, so a copy of the synthetic data was deliberately corrupted (a duplicated lap, a 999 km/h telemetry sample, a duplicated grain) and the tests were confirmed to catch each. The fixtures also contain situations that real data produced: garage visits reported as pit stops, a Sprint typed as a Race, a VSC upgraded to a Safety Car and a penalty followed by its served notice.

Generate and browse the documentation site:

```bash
dbt docs generate
dbt docs serve
```

Open the **lineage graph** (the blue circle, bottom right): it shows every source flowing through staging into the marts. You get this for free from `ref()` and `source()`.

#### **Task 5: Pit Strategy: Compound Pace, Pit Loss and Undercut/Overcut**

New in the second edition. Three models turn lap and stop data into strategy questions.

**Step 1: How fast is each tyre, and how quickly does it die?** `fct_compound_pace_model` fits a straight line of fuel-corrected lap time against tyre age for each driver and compound, over representative laps. The **intercept** is the pace on a fresh tyre, the **slope** is degradation in seconds per lap, and **r-squared** says how much a straight line explains. It needs `min_stint_laps_for_degradation` clean laps, otherwise it stays silent rather than report noise.

```sql
{# Per driver per compound: fuel-corrected lap time as a straight line in tyre age
   (intercept = pace on fresh tyres, slope = degradation s/lap, r2 = how well a line explains it).
   Feeds the race strategy simulator. Needs var min_stint_laps_for_degradation clean laps per compound. #}
WITH laps AS (
    SELECT
        session_key, meeting_key, driver_number, tyre_compound,
        CASE WHEN is_representative_lap AND tyre_age_this_lap IS NOT NULL AND COALESCE(fuel_corrected_lap_time_seconds, lap_time_seconds) IS NOT NULL
             THEN COALESCE(fuel_corrected_lap_time_seconds, lap_time_seconds) END AS y,
        CASE WHEN is_representative_lap AND tyre_age_this_lap IS NOT NULL AND COALESCE(fuel_corrected_lap_time_seconds, lap_time_seconds) IS NOT NULL
             THEN {{ f1_cast('tyre_age_this_lap', 'double') }} END AS x
    FROM {{ ref('int_lap_context') }}
    WHERE tyre_compound IS NOT NULL
)
SELECT
    session_key, meeting_key, driver_number, tyre_compound,
    COUNT(y) AS clean_laps,
    {{ f1_regr_intercept('y', 'x') }} AS fresh_tyre_pace_seconds,
    {{ f1_regr_slope('y', 'x') }} AS degradation_seconds_per_lap,
    {{ f1_r2('y', 'x') }} AS r_squared,
    MIN(x) AS min_tyre_age,
    MAX(x) AS max_tyre_age
FROM laps
GROUP BY session_key, meeting_key, driver_number, tyre_compound
HAVING COUNT(y) >= {{ var('min_stint_laps_for_degradation') }}
```

**Step 2: What does a stop cost at this circuit?** `fct_pit_loss_by_circuit` summarises `fct_pit_stops` per circuit, separating green-flag stops from stops under a neutralisation. Under a safety car a stop is cheaper because the field is slow anyway, and `neutralisation_discount_ratio` measures how much cheaper. **Medians are exact:** the model computes them with window functions, because BigQuery has no exact grouped median and `APPROX_QUANTILES` would silently change the numbers (Chapter 10).

```sql
{# Typical cost of a stop at each circuit, split into green-flag and neutralised stops.
   neutralisation_discount_ratio = how much cheaper a stop is under SC/VSC/red (NULL until both kinds are observed). #}
WITH stops AS (
    SELECT p.*, s.circuit_key, s.year
    FROM {{ ref('fct_pit_stops') }} p
    JOIN {{ ref('dim_sessions') }} s ON p.session_key = s.session_key
),
medians AS (
    -- exact medians via window functions (BigQuery has no exact grouped median; APPROX_QUANTILES would only be approximate)
    SELECT
        *,
        {{ f1_median_over('CASE WHEN NOT is_under_neutralisation THEN pit_lane_time_seconds END', 'circuit_key') }} AS med_green_lane,
        {{ f1_median_over('CASE WHEN NOT is_under_neutralisation THEN time_lost_seconds END', 'circuit_key') }} AS med_green_lost,
        {{ f1_median_over('CASE WHEN is_under_neutralisation THEN time_lost_seconds END', 'circuit_key') }} AS med_neutralised_lost
    FROM stops
)
SELECT
    circuit_key,
    COUNT(*) AS stops_observed,
    COUNT(CASE WHEN NOT is_under_neutralisation THEN 1 END) AS green_stops,
    COUNT(CASE WHEN is_under_neutralisation THEN 1 END) AS neutralised_stops,
    MAX(med_green_lane) AS median_green_pit_lane_time_seconds,
    MAX(med_green_lost) AS median_green_time_lost_seconds,
    MAX(med_neutralised_lost) AS median_neutralised_time_lost_seconds,
    1 - MAX(med_neutralised_lost) / NULLIF(MAX(med_green_lost), 0) AS neutralisation_discount_ratio
FROM medians
GROUP BY circuit_key
```

**Step 3: Did the undercut work?** An **undercut** is stopping *earlier* than the car ahead, hoping fresh tyres gain more time than the stop costs. An **overcut** is staying out longer. `fct_undercut_overcut` pairs every two related stops (within `undercut_window_laps`) where one driver ran directly behind the other, and records who stopped first, the gap before and after both stops, and whether the attacker ended up ahead.

```sql
{#
  One row per pair of related pit stops where the ATTACKER was running directly behind the DEFENDER.
    UNDERCUT : attacker stopped first (fresh tyres, hoping to emerge ahead)
    OVERCUT  : attacker stayed out longer (hoping the defender loses time on cold tyres / in traffic)
    SAME_LAP : both stopped on the same lap (not a strategy contest)
  attacker_ahead_after = position by cumulative race time one lap after BOTH stops have completed.
  Limits: pairs only if adjacent before the first stop and within var undercut_max_gap_seconds; gap/position come from
  lap-end cumulative times, so a position swap that happens and reverses inside one lap is invisible. Stops under
  SC/VSC are kept but flagged (any_neutralisation) because the pit-loss maths changes completely there.
#}
WITH stops AS (
    SELECT session_key, driver_number, stop_number, lap_number, tyre_age_before, is_under_neutralisation
    FROM {{ ref('fct_pit_stops') }}
),
trace AS (
    SELECT session_key, driver_number, lap_number,
           position_by_cumulative_time AS pos, cumulative_race_time_seconds AS cum
    FROM {{ ref('fct_race_trace') }}
),
pairs AS (
    SELECT
        a.session_key,
        a.driver_number AS attacker_driver_number, a.stop_number AS attacker_stop_number, a.lap_number AS attacker_stop_lap,
        a.tyre_age_before AS attacker_tyre_age_at_stop,
        d.driver_number AS defender_driver_number, d.stop_number AS defender_stop_number, d.lap_number AS defender_stop_lap,
        d.tyre_age_before AS defender_tyre_age_at_stop,
        (a.is_under_neutralisation OR d.is_under_neutralisation) AS any_neutralisation,
        LEAST(a.lap_number, d.lap_number) - 1 AS lap_before,
        GREATEST(a.lap_number, d.lap_number) + 1 AS lap_after
    FROM stops a
    JOIN stops d
        ON a.session_key = d.session_key
        AND a.driver_number <> d.driver_number
        AND ABS(a.lap_number - d.lap_number) <= {{ var('undercut_window_laps') }}
),
positioned AS (
    SELECT
        p.*,
        ta.pos AS attacker_position_before, td.pos AS defender_position_before,
        ta.cum - td.cum AS gap_before_seconds,
        tae.pos AS attacker_position_after, tde.pos AS defender_position_after,
        tae.cum - tde.cum AS gap_after_seconds
    FROM pairs p
    JOIN trace ta ON p.session_key = ta.session_key AND p.attacker_driver_number = ta.driver_number AND p.lap_before = ta.lap_number
    JOIN trace td ON p.session_key = td.session_key AND p.defender_driver_number = td.driver_number AND p.lap_before = td.lap_number
    LEFT JOIN trace tae ON p.session_key = tae.session_key AND p.attacker_driver_number = tae.driver_number AND p.lap_after = tae.lap_number
    LEFT JOIN trace tde ON p.session_key = tde.session_key AND p.defender_driver_number = tde.driver_number AND p.lap_after = tde.lap_number
)
SELECT
    session_key,
    attacker_driver_number, attacker_stop_number, attacker_stop_lap, attacker_tyre_age_at_stop,
    defender_driver_number, defender_stop_number, defender_stop_lap, defender_tyre_age_at_stop,
    CASE WHEN attacker_stop_lap < defender_stop_lap THEN 'UNDERCUT'
         WHEN attacker_stop_lap > defender_stop_lap THEN 'OVERCUT'
         ELSE 'SAME_LAP' END AS strategy_type,
    attacker_position_before, defender_position_before,
    gap_before_seconds,
    attacker_position_after, defender_position_after,
    gap_after_seconds,
    gap_before_seconds - gap_after_seconds AS gap_swing_seconds,
    attacker_position_after < defender_position_after AS attacker_ahead_after,
    any_neutralisation
FROM positioned
WHERE attacker_position_before = defender_position_before + 1
  AND gap_before_seconds <= {{ var('undercut_max_gap_seconds') }}
```

**Limits to keep in mind**

* Gap and position come from lap-end cumulative times, so a swap that happens and reverses inside a single lap is invisible.
* Stops under a safety car are kept but flagged (`any_neutralisation`), because the pit-loss arithmetic is completely different there.
* `time_lost_seconds` includes any traffic on the out-lap, and `pit_lane_time_seconds` is pit entry to exit as the API reports it, **not** stationary time (the API does not give that).

#### **Task 6: Safety-Car Risk**

New in the second edition. A strategy decision depends on how likely a safety car is, so this model estimates it from history.

**Step 1: List the periods.** `fct_safety_car_events` puts each SC, VSC and red-flag period into a race lap (the highest lap any car had started when it began):

```sql
{# Every SC / VSC / red period in a Race, with the race lap it started and ended on (lap = the highest lap any car had
   started at that moment, i.e. effectively the leader's lap). #}
WITH periods AS (
    SELECT p.session_key, s.circuit_key, s.year, p.track_status, p.period_start, p.period_end
    FROM {{ ref('int_track_status_periods') }} p
    JOIN {{ ref('dim_sessions') }} s ON p.session_key = s.session_key
    WHERE s.session_type = 'Race'
),
laps AS (
    SELECT session_key, lap_number, lap_start_ts FROM {{ ref('int_lap_context') }}
),
race_length AS (
    SELECT session_key, MAX(lap_number) AS race_laps FROM laps GROUP BY session_key
)
SELECT
    p.session_key, p.circuit_key, p.year, p.track_status, p.period_start, p.period_end,
    MAX(CASE WHEN l.lap_start_ts <= p.period_start THEN l.lap_number END) AS start_lap,
    MAX(l.lap_number) AS end_lap,
    MAX(l.lap_number) - MAX(CASE WHEN l.lap_start_ts <= p.period_start THEN l.lap_number END) + 1 AS duration_laps,
    {{ f1_seconds_between('p.period_start', 'p.period_end') }} AS duration_seconds,
    MAX(r.race_laps) AS race_laps
FROM periods p
JOIN laps l ON p.session_key = l.session_key AND l.lap_start_ts <= p.period_end
JOIN race_length r ON p.session_key = r.session_key
GROUP BY p.session_key, p.circuit_key, p.year, p.track_status, p.period_start, p.period_end
```

**Step 2: Turn them into a probability.** `fct_safety_car_hazard` assumes a constant chance *h* that a safety car starts on any given lap, so the chance of at least one in the next *N* laps is `1 - (1 - h)^N`. The estimate of *h* for a circuit is **shrunk toward the all-circuit rate**: `(starts + k x global rate) / (laps + k)`, with `k = sc_prior_laps`. This is **empirical-Bayes shrinkage**, and it matters because OpenF1 history is short, so a circuit with two races and no safety car would otherwise be estimated at exactly zero risk.

```sql
{#
  Probability of at least one SC (or VSC) starting in the next N laps, per circuit.
  Model: constant per-lap start hazard h, so P(at least one in N laps) = 1 - (1 - h)^N.
  h is shrunk toward the all-circuit rate:  h_shrunk = (starts + k * h_global) / (laps + k)  with k = var sc_prior_laps.
  HONEST LIMITS: OpenF1 history is short (a handful of races per circuit), so per-circuit estimates are weak, which is
  what the shrinkage and the low_sample flag are for; the constant-hazard assumption ignores lap-1 incidents,
  weather and late-race effects; red flags are not modelled.
#}
WITH races AS (
    SELECT s.circuit_key, s.session_key, MAX(l.lap_number) AS race_laps
    FROM {{ ref('int_lap_context') }} l
    JOIN {{ ref('dim_sessions') }} s ON l.session_key = s.session_key
    WHERE s.session_type = 'Race'
    GROUP BY s.circuit_key, s.session_key
),
exposure AS (
    SELECT circuit_key, COUNT(*) AS races, SUM(race_laps) AS laps FROM races GROUP BY circuit_key
),
statuses AS (
    SELECT 'SC' AS track_status UNION ALL SELECT 'VSC'
),
events AS (
    SELECT circuit_key, track_status, COUNT(*) AS starts, AVG(duration_laps) AS avg_duration_laps,
           COUNT(DISTINCT session_key) AS races_affected
    FROM {{ ref('fct_safety_car_events') }}
    WHERE track_status IN ('SC', 'VSC')
    GROUP BY circuit_key, track_status
),
circuit_status AS (
    SELECT e.circuit_key, e.races, e.laps, s.track_status,
           COALESCE(v.starts, 0) AS starts, v.avg_duration_laps, COALESCE(v.races_affected, 0) AS races_affected
    FROM exposure e
    CROSS JOIN statuses s
    LEFT JOIN events v ON e.circuit_key = v.circuit_key AND s.track_status = v.track_status
),
global_rate AS (
    SELECT track_status, SUM(starts) * 1.0 / NULLIF(SUM(laps), 0) AS global_hazard_per_lap
    FROM circuit_status GROUP BY track_status
),
horizons AS (
    SELECT 5 AS laps_remaining UNION ALL SELECT 10 UNION ALL SELECT 20 UNION ALL SELECT 30
    UNION ALL SELECT 40 UNION ALL SELECT 50 UNION ALL SELECT 60
),
rates AS (
    SELECT
        c.*, g.global_hazard_per_lap,
        c.starts * 1.0 / NULLIF(c.laps, 0) AS raw_hazard_per_lap,
        (c.starts + {{ var('sc_prior_laps') }} * g.global_hazard_per_lap) / (c.laps + {{ var('sc_prior_laps') }}) AS shrunk_hazard_per_lap
    FROM circuit_status c
    JOIN global_rate g ON c.track_status = g.track_status
)
SELECT
    r.circuit_key, r.track_status, h.laps_remaining,
    r.races AS races_observed, r.laps AS race_laps_observed, r.starts AS starts_observed,
    r.races_affected, r.avg_duration_laps,
    r.raw_hazard_per_lap, r.global_hazard_per_lap, r.shrunk_hazard_per_lap,
    1 - POWER(1 - r.shrunk_hazard_per_lap, h.laps_remaining) AS p_at_least_one_start,
    (r.races < 3) AS low_sample
FROM rates r
CROSS JOIN horizons h
```

> **Be honest about what this is.** Per-circuit estimates rest on a handful of races, so treat `low_sample = true` rows as weak. The constant-hazard assumption ignores lap-1 incidents, weather and late-race effects, and red flags are not modelled. It is a defensible baseline, not a forecast.

#### **Task 7: The Strategy Simulator (Python)**

New in the second edition. SQL answered *what happened*; the simulator answers *what if*: given measured tyre pace, pit loss and safety-car risk, which strategy is expected to finish fastest? It lives in **analytics/f1_strategy/** and reads the three tables above.

**Step 1: The data classes.** A strategy is a sequence of (compound, laps) stints; the race context holds the assumptions.

```python
from dataclasses import dataclass, field
from typing import Dict, Tuple
import numpy as np


@dataclass(frozen=True)
class CompoundModel:
    fresh_pace: float      # fuel-corrected lap time (s) on a new tyre
    degradation: float     # seconds lost per lap of tyre age (>= 0)


@dataclass(frozen=True)
class RaceContext:
    total_laps: int
    fuel_effect: float = 0.035            # s per lap of fuel still on board (ASSUMPTION, see dbt var)
    pit_loss: float = 22.0                # green-flag time lost by a stop (s)
    neutralised_pit_discount: float = 0.5  # fraction of pit_loss saved when the stop is made under SC
    sc_hazard_per_lap: float = 0.0        # chance a Safety Car starts on any lap (when none is out)
    sc_duration_laps: float = 3.0         # mean SC length in laps
    sc_lap_time_factor: float = 1.45      # SC lap time relative to the fastest fresh-tyre pace
    noise_sd: float = 0.25                # lap-to-lap random variation (s), independent per lap
    start_penalty: float = 0.0            # fixed time added to every strategy (grid / lap-1 effects)


@dataclass(frozen=True)
class Strategy:
    stints: Tuple[Tuple[str, int], ...]   # ((compound, laps_in_stint), ...) summing to total_laps

    @property
    def stops(self) -> int:
        return len(self.stints) - 1

    @property
    def pit_laps(self):
        out, lap = [], 0
        for _, n in self.stints[:-1]:
            lap += n
            out.append(lap)               # in-lap number (1-based)
        return out

    def label(self) -> str:
        return " > ".join(f"{c}({n})" for c, n in self.stints)


@dataclass
class SimResult:
    strategies: list
    totals: np.ndarray                    # shape (n_strategies, n_sims), seconds
    sc_mask: np.ndarray                   # shape (n_sims, laps), shared by all strategies (paired comparison)

    def summary(self):
        best = self.totals.argmin(axis=0)
        rows = []
        for i, s in enumerate(self.strategies):
            t = self.totals[i]
            rows.append({
                "strategy": s.label(), "stops": s.stops,
                "mean_s": float(t.mean()), "median_s": float(np.median(t)),
                "p10_s": float(np.percentile(t, 10)), "p90_s": float(np.percentile(t, 90)),
                "p_best": float((best == i).mean()),
            })
        return sorted(rows, key=lambda r: r["mean_s"])
```

**Step 2: Simulate.** Lap time is `fresh pace + degradation x tyre age + fuel effect x laps of fuel left`, plus random noise. A safety car is sampled lap by lap (a new one can only start when none is out), makes laps slow, and makes a stop cheaper. **All strategies are evaluated against the same safety-car draws and the same noise** (common random numbers), so a difference between two strategies is the strategies.

```python
from typing import Dict, Sequence
import numpy as np
from .model import CompoundModel, RaceContext, Strategy, SimResult


def clean_lap_times(ctx: RaceContext, models: Dict[str, CompoundModel], strategy: Strategy) -> np.ndarray:
    """Deterministic (no noise, no SC) lap times for one strategy: fresh pace + degradation x tyre age + fuel effect."""
    if sum(n for _, n in strategy.stints) != ctx.total_laps:
        raise ValueError(f"stints sum to {sum(n for _, n in strategy.stints)} laps, race has {ctx.total_laps}")
    laps = []
    lap_no = 0
    for compound, n in strategy.stints:
        m = models[compound]
        for age in range(n):
            lap_no += 1
            laps.append(m.fresh_pace + m.degradation * age + ctx.fuel_effect * (ctx.total_laps - lap_no))
    return np.asarray(laps, dtype=float)


def sample_sc_mask(rng: np.random.Generator, n_sims: int, laps: int, hazard: float, mean_duration: float) -> np.ndarray:
    """(n_sims, laps) boolean: True where a Safety Car is out. A new SC can only start when none is out."""
    mask = np.zeros((n_sims, laps), dtype=bool)
    if hazard <= 0:
        return mask
    remaining = np.zeros(n_sims, dtype=int)
    for lap in range(laps):
        start = (remaining == 0) & (rng.random(n_sims) < hazard)
        durations = 1 + rng.poisson(max(mean_duration - 1.0, 0.0), size=n_sims)
        remaining = np.where(start, durations, remaining)
        mask[:, lap] = remaining > 0
        remaining = np.maximum(remaining - 1, 0)
    return mask


def evaluate(ctx: RaceContext, models: Dict[str, CompoundModel], strategies: Sequence[Strategy],
             n_sims: int = 4000, seed: int = 0, sc_mask: np.ndarray = None) -> SimResult:
    """Monte Carlo total race time for every strategy. All strategies share the SAME safety-car draws and lap noise
    (common random numbers), so differences between strategies are not sampling luck. Pass sc_mask to force scenarios."""
    rng = np.random.default_rng(seed)
    if sc_mask is None:
        sc_mask = sample_sc_mask(rng, n_sims, ctx.total_laps, ctx.sc_hazard_per_lap, ctx.sc_duration_laps)
    else:
        n_sims = sc_mask.shape[0]
    noise = rng.normal(0.0, ctx.noise_sd, size=(n_sims, ctx.total_laps)) if ctx.noise_sd > 0 else np.zeros((n_sims, ctx.total_laps))
    fastest_fresh = min(m.fresh_pace for m in models.values())
    sc_time = ctx.sc_lap_time_factor * fastest_fresh
    totals = np.empty((len(strategies), n_sims))
    for i, s in enumerate(strategies):
        base = clean_lap_times(ctx, models, s)
        lap_times = np.where(sc_mask, sc_time, base[None, :] + noise)
        total = lap_times.sum(axis=1) + ctx.start_penalty
        for pit_lap in s.pit_laps:
            discount = np.where(sc_mask[:, pit_lap - 1], ctx.neutralised_pit_discount, 0.0)
            total = total + ctx.pit_loss * (1.0 - discount)
        totals[i] = total
    return SimResult(list(strategies), totals, sc_mask)
```

**Step 3: Enumerate and rank strategies.** The optimiser generates every strategy with up to two stops on a grid of pit laps and applies the dry-race rule that two different dry compounds must be used.

```python
import itertools
from typing import Dict, List, Sequence
from .model import CompoundModel, RaceContext, Strategy
from .simulate import evaluate

DRY = {"SOFT", "MEDIUM", "HARD"}


def enumerate_strategies(total_laps: int, compounds: Sequence[str], max_stops: int = 2, pit_step: int = 1,
                         min_stint: int = 5, require_two_dry_compounds: bool = True) -> List[Strategy]:
    """All strategies with 0..max_stops stops, stint lengths >= min_stint, pit laps on a grid of pit_step laps.
    Regulation modelled: a dry race must use at least two different dry compounds."""
    out = []
    for stops in range(0, max_stops + 1):
        stints_n = stops + 1
        cut_range = range(min_stint, total_laps - min_stint + 1, pit_step)
        for cuts in itertools.combinations(cut_range, stops):
            bounds = [0, *cuts, total_laps]
            lengths = [bounds[i + 1] - bounds[i] for i in range(stints_n)]
            if any(l < min_stint for l in lengths):
                continue
            for seq in itertools.product(compounds, repeat=stints_n):
                if require_two_dry_compounds and len({c for c in seq if c in DRY}) < 2:
                    continue
                out.append(Strategy(tuple(zip(seq, lengths))))
    return out


def rank_strategies(ctx: RaceContext, models: Dict[str, CompoundModel], strategies: Sequence[Strategy],
                    n_sims: int = 2000, seed: int = 0, top: int = 10):
    res = evaluate(ctx, models, strategies, n_sims=n_sims, seed=seed)
    return res.summary()[:top]
```

**Step 4: Feed it from the warehouse.** `load_inputs` reads the three tables, takes the driver's own pace where it exists and the median of all drivers otherwise, clips a negative degradation to zero, and **records every value that was a fallback instead of a measurement** so you can see what the answer rests on.

```python
"""Build simulator inputs from the dbt marts. Every value that is a FALLBACK (not measured) is recorded in provenance."""
from statistics import median
from .model import CompoundModel, RaceContext


def load_inputs(con, session_key: int, driver_number: int = None, total_laps: int = None, overrides: dict = None):
    prov, overrides = {}, overrides or {}
    sess = con.execute("SELECT circuit_key FROM dim_sessions WHERE session_key = ?", [session_key]).fetchone()
    if sess is None:
        raise ValueError(f"session {session_key} not found in dim_sessions")
    circuit = sess[0]

    if total_laps is None:
        row = con.execute("SELECT MAX(lap_number) FROM int_lap_context WHERE session_key = ?", [session_key]).fetchone()
        total_laps = int(row[0]) if row and row[0] else None
        prov["total_laps"] = "measured: max lap in session" if total_laps else "MISSING"
    if not total_laps:
        raise ValueError("total_laps could not be determined; pass total_laps explicitly")

    rows = con.execute(
        "SELECT driver_number, tyre_compound, fresh_tyre_pace_seconds, degradation_seconds_per_lap, clean_laps "
        "FROM fct_compound_pace_model WHERE session_key = ? AND fresh_tyre_pace_seconds IS NOT NULL "
        "AND degradation_seconds_per_lap IS NOT NULL", [session_key]).fetchall()
    models, by_compound = {}, {}
    for d, comp, pace, deg, n in rows:
        by_compound.setdefault(comp, []).append((d, pace, deg, n))
    for comp, items in by_compound.items():
        mine = [i for i in items if driver_number is not None and i[0] == driver_number]
        use, how = (mine, f"driver {driver_number}") if mine else (items, f"median of {len(items)} driver(s)")
        pace = median(i[1] for i in use); deg = median(i[2] for i in use)
        if deg < 0:
            prov[f"{comp}.degradation"] = f"clipped from {deg:.4f} to 0 (negative slope = noise or track evolution)"
            deg = 0.0
        models[comp] = CompoundModel(float(pace), float(deg))
        prov.setdefault(f"{comp}.model", f"measured: {how}")

    pl = con.execute("SELECT median_green_time_lost_seconds, median_green_pit_lane_time_seconds, neutralisation_discount_ratio "
                     "FROM fct_pit_loss_by_circuit WHERE circuit_key = ?", [circuit]).fetchone()
    pit_loss = pl[0] if pl and pl[0] is not None else (pl[1] if pl and pl[1] is not None else None)
    prov["pit_loss"] = "measured: circuit median" if pit_loss is not None else "FALLBACK default 22.0 s"
    discount = pl[2] if pl and pl[2] is not None else None
    prov["neutralised_pit_discount"] = "measured" if discount is not None else "FALLBACK default 0.5 (no neutralised stops observed)"

    sc = con.execute("SELECT shrunk_hazard_per_lap, avg_duration_laps, low_sample FROM fct_safety_car_hazard "
                     "WHERE circuit_key = ? AND track_status = 'SC' AND laps_remaining = 5", [circuit]).fetchone()
    hazard = sc[0] if sc and sc[0] is not None else 0.0
    duration = sc[1] if sc and sc[1] is not None else 3.0
    prov["sc_hazard"] = ("measured, LOW SAMPLE (shrunk toward all-circuit rate)" if sc and sc[2] else "measured") if sc else "FALLBACK 0 (no data)"
    prov["sc_duration"] = "measured" if sc and sc[1] is not None else "FALLBACK 3.0 laps"

    ctx = RaceContext(
        total_laps=total_laps,
        pit_loss=float(overrides.get("pit_loss", pit_loss if pit_loss is not None else 22.0)),
        neutralised_pit_discount=float(overrides.get("neutralised_pit_discount", discount if discount is not None else 0.5)),
        sc_hazard_per_lap=float(overrides.get("sc_hazard_per_lap", hazard)),
        sc_duration_laps=float(overrides.get("sc_duration_laps", duration)),
        **{k: v for k, v in overrides.items() if k in ("fuel_effect", "sc_lap_time_factor", "noise_sd", "start_penalty")},
    )
    return ctx, models, prov
```

**Step 5: Run it.**

```bash
PYTHONPATH=analytics python -m f1_strategy.cli --db f1_local_warehouse.duckdb --session <a REAL session key> --driver 1
```

Use a session key from **your own** warehouse (`SELECT session_key, year, location FROM dim_sessions WHERE session_type = 'Race'`). If you pass one that does not exist, the error now lists your most recent race sessions.

**How it was tested.** The tests compare the simulator with answers computed *independently of it*: a total race time worked out by hand, a fuel-effect sum written out longhand, and a **brute-force search** over every one-stop strategy that the optimiser must agree with. They also check properties that must hold whatever the numbers are: more safety-car risk raises expected race time; higher degradation moves the best stop earlier; the same seed gives the same answer.

```python
def test_total_time_matches_hand_calculation():
    ctx = det_ctx(pit_loss=20.0, fuel_effect=0.0)
    s = Strategy((("MEDIUM", 10), ("HARD", 20)))
    expected = sum(89.6 + 0.06 * a for a in range(10)) + sum(90.2 + 0.03 * a for a in range(20)) + 20.0
    got = evaluate(ctx, MODELS, [s], n_sims=3).totals[0]
    assert np.allclose(got, expected)
```

> **What this tells you, and what it does not.** The tests show the simulator computes what it says it computes. They say **nothing** about whether it predicts real races. It ignores traffic, tyre cliffs, track evolution and how rivals react, and it has not been compared with real results. Use it to *compare* strategies under stated assumptions, never to predict an outcome.

#### **Task 8: Team Radio Messages**

New in the second edition, and **opt-in**: it needs `faster-whisper` and outbound HTTPS to the recording host. The audio recordings are listed by the `team_radio` endpoint; this task turns them into text you can search.

**Step 1: Transcribe.** `modules/radio_transcriber.py` downloads each clip, transcribes it with a model running on your own CPU, and writes `radio_transcripts_<session>_extracted.parquet`. Clips already done are skipped, and a clip that fails is recorded with its error instead of failing the session.

```python
"""
Speech-to-text for team radio. OPT-IN and self-hosted (faster-whisper on CPU), in keeping with the rest of the project.

  raw_dir/team_radio_<session>_extracted.parquet        <- written by the extractor (metadata + recording_url)
  raw_dir/radio_transcripts_<session>_extracted.parquet <- written here

The transcripts file deliberately does NOT start with "team_radio_": that prefix is the glob of the team_radio source,
and a transcript file matching it would be mixed into the radio metadata table.

Needs `faster-whisper` (add it to AIRFLOW_EXTRA_PIP) and outbound HTTPS to the recording host. Clips already transcribed
are skipped, so re-runs only process new radio. A clip that fails to download or decode is recorded with its error
instead of failing the session. Transcription quality on noisy radio audio is imperfect and is NOT validated here.
"""
import logging
import os
import tempfile
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

COLUMNS = ["session_key", "meeting_key", "driver_number", "date", "recording_url", "transcript", "language",
           "audio_duration_seconds", "model", "error", "_transcribed_at_utc"]


def default_download(url: str, dest: str) -> str:
    import requests
    with requests.get(url, timeout=60, stream=True) as r:
        r.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in r.iter_content(1 << 16):
                fh.write(chunk)
    return dest


def load_model(model_name: str = "small.en"):
    try:
        from faster_whisper import WhisperModel
    except ImportError as exc:
        raise RuntimeError("faster-whisper is not installed; add it to AIRFLOW_EXTRA_PIP to enable radio transcription") from exc
    return WhisperModel(model_name, device="cpu", compute_type="int8")


def _read(path):
    return pq.read_table(path).to_pylist() if os.path.exists(path) else []


def transcribe_session(session_key, raw_dir: str, model=None, download=None, model_name: str = "small.en", limit: int = None):
    """Transcribe all not-yet-transcribed radio clips of one session. Returns the transcripts file path, or None when
    the session has no radio metadata."""
    src = os.path.join(raw_dir, f"team_radio_{session_key}_extracted.parquet")
    out = os.path.join(raw_dir, f"radio_transcripts_{session_key}_extracted.parquet")
    clips = _read(src)
    if not clips:
        logging.info(f"No team radio metadata for session {session_key}; nothing to transcribe.")
        return None

    done_rows = _read(out)
    done = {r["recording_url"] for r in done_rows if r.get("recording_url")}
    todo = [c for c in clips if c.get("recording_url") and c["recording_url"] not in done]
    if limit:
        todo = todo[:limit]
    if not todo:
        logging.info(f"Session {session_key}: all {len(clips)} clips already transcribed.")
        return out

    model = model or load_model(model_name)
    download = download or default_download
    new_rows = []
    for clip in todo:
        row = {"session_key": clip.get("session_key"), "meeting_key": clip.get("meeting_key"),
               "driver_number": clip.get("driver_number"), "date": clip.get("date"), "recording_url": clip["recording_url"],
               "transcript": None, "language": None, "audio_duration_seconds": None, "model": model_name, "error": None,
               "_transcribed_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        tmp = None
        try:
            fd, tmp = tempfile.mkstemp(suffix=".mp3"); os.close(fd)
            download(clip["recording_url"], tmp)
            segments, info = model.transcribe(tmp, language="en", vad_filter=True)
            row["transcript"] = " ".join(s.text.strip() for s in segments).strip()
            row["language"] = getattr(info, "language", None)
            row["audio_duration_seconds"] = getattr(info, "duration", None)
        except Exception as exc:  # one bad clip must not lose the rest of the session
            logging.warning(f"Transcription failed for {clip['recording_url']}: {exc}")
            row["error"] = str(exc)[:300]
        finally:
            if tmp and os.path.exists(tmp):
                os.remove(tmp)
        new_rows.append(row)

    all_rows = [{k: r.get(k) for k in COLUMNS} for r in done_rows + new_rows]
    tmp_out = out + ".tmp"
    pq.write_table(pa.Table.from_pylist(all_rows), tmp_out, compression="snappy")
    os.replace(tmp_out, out)
    logging.info(f"Session {session_key}: transcribed {len(new_rows)} new clip(s), {sum(1 for r in new_rows if r['error'])} failed.")
    return out
```

> **Why the file is not called `team_radio_transcripts_...`.** The `team_radio` source reads `team_radio_*_extracted.parquet`. A transcript file with that prefix would match the pattern and be silently mixed into the radio *metadata* table. The prefix `radio_transcripts_` cannot collide.

**Step 2: Turn it on.** In the DAG trigger form tick `transcribe_radio` (`faster-whisper` is already in `requirements.txt` and baked into the image by the Dockerfile, so there is nothing to add to `.env`), and tell dbt the files exist:

```bash
dbt build --select +fct_radio_messages --vars '{enable_radio_transcripts: true}'
```

**Step 3: Model it.** `fct_radio_messages` attaches the lap, tyre, track status and position at the moment of each message (an ASOF join) and adds **keyword tags**:

```sql
{#
  Transcribed team radio with race context (lap, tyre, track status, position) and KEYWORD tags.
  The tags are plain substring heuristics on the transcript, NOT a language model: they will miss paraphrases and can
  fire on unrelated words. Treat them as a way to find messages to read, not as ground truth. Transcript text itself
  comes from speech-to-text on noisy radio audio and can be wrong. Empty unless var enable_radio_transcripts is true.
#}
WITH msgs AS (
    SELECT
        session_key, meeting_key, driver_number, radio_timestamp, recording_url,
        transcript, language, audio_duration_seconds, transcription_error,
        LOWER(COALESCE(transcript, '')) AS t
    FROM {{ ref('stg_radio_transcripts') }}
),
with_context AS (
    {{ f1_asof_join(
        left='msgs', right=ref('int_lap_context'),
        on=['session_key', 'driver_number'], left_ts='radio_timestamp', right_ts='lap_start_ts',
        left_cols=['session_key', 'meeting_key', 'driver_number', 'radio_timestamp', 'recording_url', 'transcript',
                   'language', 'audio_duration_seconds', 'transcription_error', 't'],
        right_cols=['lap_number', 'tyre_compound', 'tyre_age_this_lap', 'track_status', 'position_start']
    ) }}
)
SELECT
    c.session_key, c.meeting_key, c.driver_number,
    d.full_name AS driver_name, d.team_name,
    c.radio_timestamp, c.recording_url, c.transcript, c.language, c.audio_duration_seconds, c.transcription_error,
    c.lap_number, c.tyre_compound, c.tyre_age_this_lap, c.track_status, c.position_start,
    (c.t LIKE '%box%' OR c.t LIKE '%pit this lap%' OR c.t LIKE '%pit now%' OR c.t LIKE '%come in%') AS mentions_pit_call,
    (c.t LIKE '%tyre%' OR c.t LIKE '%tire%' OR c.t LIKE '%grain%' OR c.t LIKE '%degrad%' OR c.t LIKE '%blister%') AS mentions_tyres,
    (c.t LIKE '%safety car%' OR c.t LIKE '%virtual%' OR c.t LIKE '%vsc%' OR c.t LIKE '%red flag%' OR c.t LIKE '%yellow%') AS mentions_neutralisation,
    (c.t LIKE '%drs%') AS mentions_drs,
    (c.t LIKE '%penalt%' OR c.t LIKE '%investigat%' OR c.t LIKE '%stewards%') AS mentions_penalty,
    (c.t LIKE '%fuel%' OR c.t LIKE '%lift and coast%' OR c.t LIKE '%engine%' OR c.t LIKE '%battery%' OR c.t LIKE '%deploy%') AS mentions_power_unit
FROM with_context c
LEFT JOIN {{ ref('dim_driver_session') }} d
    ON c.session_key = d.session_key AND c.driver_number = d.driver_number
```

> **Treat the tags as a search aid.** They are plain substring matches on speech-to-text output of noisy radio audio: they miss paraphrases and can fire on unrelated words, and the transcript itself can be wrong. The transcriber's logic is unit-tested with a fake model; **real transcription has not been run**, so the quality on real radio is unknown.

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

* **The Problem (second edition, hit on the author's real data):** `dbt build` reports **13,934 failures** for `unique_combination_of_columns` on `int_lap_context` (`session_key`, `driver_number`, `lap_number`).
* **Why it Happens:** a **fan-out**. OpenF1 can record the pit lap as *both* the last lap of one stint and the first lap of the next, so the join `lap_number BETWEEN lap_start AND lap_end` matches that lap to two stints and returns it twice. The first edition's model silently assumed stints never overlap, and the synthetic test data contained no overlap, so its own tests could not catch it. A duplicated lap is worse than an error: it would have double-counted that lap in every average downstream.
* **The Solution:** keep exactly one stint per lap. `int_lap_context` ranks the matching stints with `QUALIFY ROW_NUMBER() OVER (PARTITION BY session_key, driver_number, lap_number ORDER BY stint_number DESC) = 1`, which keeps the **later** stint, so a shared pit lap is attributed to the stint that *starts* there. Know what that implies: that lap carries the new tyre's compound, so compare `compound_before` with `compound_after` in `fct_pit_stops` on your own data. Neither choice has been checked against real tyre data, and the synthetic fixtures contain no overlapping stint, so the grain test running on real data is what protects this fix.

* **The Problem (second edition, hit on the author's real data):** `assert_pit_lane_time_plausible` reports **13,022 warnings**.
* **Why it Happens:** in practice and qualifying OpenF1 reports every garage visit as a pit stop. On the author's real data the median pit-lane time was 340 s in practice and 260 s in qualifying, against 23.5 s for 2,568 race stops. Those rows flooded the test and the circuit pit-loss medians that the strategy simulator reads.
* **The Solution:** `fct_pit_stops` now keeps **Race and Sprint** stops only. Two new tests guard that (`assert_pit_stops_only_in_races` and `assert_circuit_pit_loss_is_plausible`), and `assert_pit_lane_time_plausible` stays a warn-level review of long race stops (red flags, stuck cars), with its 10 to 120 second window written in the test.

* **The Problem (second edition):** `dbt build --select int_lap_context+` fails with `Catalog Error: Table with name int_track_status_periods does not exist!`.
* **Why it Happens:** `int_lap_context+` selects the model and what is *downstream* of it, not what it depends on. On a warehouse that never built `int_track_status_periods` the spine has nothing to join. (An earlier version of this book's checklist gave exactly that command, which is how it was found.)
* **The Solution:** `dbt build --select +int_lap_context+`, or simply build everything the first time.

* **The Problem (second edition):** the first real-data build stops partway, with two *tests* reported as failures (59 and 23 rows) and every downstream model skipped.
* **Why it Happens:** `assert_lap_start_times_increase` and `assert_stint_ranges_valid` check the **source** data, and real OpenF1 data really does contain such rows. As errors, they block `dbt build` from running anything downstream of them.
* **The Solution:** `stg_laps` removes duplicate and out-of-order laps and `stg_stints` removes impossible stints, so the two tests pass on real data and stay errors. The cleaning sits in staging because, as errors, those rows would otherwise stop `dbt build`.

* **The Problem (second edition):** `python -m f1_strategy.cli --session 9002` says the session was not found, or `Need >= 2 compounds with a fitted pace model`.
* **Why it Happens:** `9002` is a session key from the **synthetic** dataset, and an earlier version of this book told you to use it against your real warehouse. Separately, a real race needs enough clean laps on at least two compounds for the pace model to fit.
* **The Solution:** use a real `session_key` from `dim_sessions`; the error message now lists recent race sessions. If only one compound has enough laps, lower `min_stint_laps_for_degradation` for the experiment.

---


## **Chapter 7: Taming High-Volume Telemetry**

### **Chapter Overview**

Everything so far handles thousands or millions of rows. **Telemetry** (`car_data` and `location`) samples every car several times per second for an entire race: **tens of millions of rows**. Naive SQL that works on laps will exhaust an 8 GB machine. This chapter is the project's best case study in **memory-aware data engineering**, told as the sequence of failures that produced the final design.

* The incident chain and the streaming export: **[VERIFIED]** (the logs show the failures, and later successful exports of about 21 and 43 minutes).
* The incremental ASOF model: **[VERIFIED]** on real data. A full first build over the author's whole dataset **took 1 hour 25 minutes** (single thread, spilling to disk); Task 3 shows how to build only the sessions you need instead.
* Session-scoped incremental builds (third edition: the scope macros, replace-by-session in all six telemetry models, scope-aware tests): **[VERIFIED]**. The equivalence check on the generated dataset passes (replace, idempotency, zero rows, vanished upstream data); a deliberately damaged session on a copy of the author's real warehouse was restored exactly; and one Airflow run in Docker replaced a single session while every other session's row counts stayed identical.
* The distance axis, the delta trace and the derived corners (Tasks 6 to 8, new in the second edition): **[VERIFIED ON SYNTHETIC DATA]**. The delta trace and the two corner models also built on the author's real data in seconds each, but **whether the detected corners are good on real circuits has not been reviewed**.

**Concepts introduced in this chapter**

* **Out-of-memory (OOM):** the query engine needs more RAM than it has.
* **Blocking (pipeline-breaking) operator:** an operation that must see all of its input before it can produce output, such as a hash join's build side or a global `ORDER BY`. Two of them in one query compete for memory.
* **Spill to disk:** DuckDB can write temporary data to a **temp directory** when memory is full, if you give it a place to do so.
* **ASOF join:** match each row to the **closest earlier** row. Cheaper than a many-to-many equality join on high-frequency time series.
* **Incremental model:** after the first build, each run processes only new data.
* **Partitioned Parquet:** one folder per value of chosen columns (`meeting_key=.../session_key=.../driver_number=.../`). Queries that filter on those columns read only the matching folders (**partition pruning**).
* **Macro:** a reusable Jinja function in `macros/`. It can run SQL that is not a model, such as a `COPY` export.
* **Distance axis:** telemetry is recorded against *time*. To compare two laps you need it against *distance along the lap*, so that "300 m into the lap" means the same place for both drivers.
* **Normalisation:** rescaling a measurement to a common reference. Here, every complete lap is rescaled to the same track length, because integrating speed over time drifts a little every lap.
* **Apex:** the slowest point of a corner. The API gives no corner data, so Task 8 *detects* corners as speed minima.
* **Session scope (third edition):** the run tells dbt which sessions changed (`--vars '{session_keys: [9158]}'`) and the telemetry models build **only those sessions**, so a failure loses one small build instead of the whole run. It replaces the first edition's opt-in `telemetry_only_sessions`, which still works as an alias.
* **Replace, never append:** a scoped session is **deleted and re-inserted**. Re-extracting a live session therefore refreshes it with no duplicates and no stale rows, even if some of its raw rows have vanished.
* **Telemetry group:** `stg_car_data`, `stg_location` and the six telemetry models (`fct_telemetry`, `fct_telemetry_distance_trace`, `fct_telemetry_lap_summary`, `fct_lap_time_delta_trace`, `dim_session_corners`, `fct_corner_performance`), tagged `telemetry_group`. Nothing outside the group depends on it.

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
3. **Give the engine somewhere to spill.** DuckDB's default location works; if you set `temp_directory`, make sure that folder exists.
4. **Pick the join that matches the data's shape.** Time series want ASOF.

#### **Task 3: Build the Telemetry Model**

Create **models/marts/analytics/fct_telemetry.sql**.

```sql
{#
  High-frequency car telemetry joined to position and lap number.
  * ASOF joins: "latest location reading at or before this telemetry sample" (the two feeds are not
    timestamp-aligned, so an equality join silently drops almost every row).
  * location_age_ms exposes how stale the matched position is; positions older than 2 s are set to NULL.
  * INCREMENTAL by session. Scoped run (--vars '{session_keys: [9158]}'): those sessions are replaced, never appended. Unscoped run: a
    session already present is skipped. Manual re-load without a scope:
    dbt run-operation delete_sessions_from_incrementals --args '{sessions: [9158]}'  then dbt run.
#}
{{ config(
    materialized='incremental',
    incremental_strategy=var('bq_incremental_strategy', 'insert_overwrite') if target.type == 'bigquery' else 'delete+insert',
    unique_key=(['session_key', 'driver_number', 'telemetry_timestamp'] if var('bq_incremental_strategy', 'insert_overwrite') == 'merge' else none) if target.type == 'bigquery' else 'session_key',
    partition_by=({'field': 'session_key', 'data_type': 'int64',
                   'range': {'start': var('bq_session_key_start'), 'end': var('bq_session_key_end'), 'interval': 1}}
                  if target.type == 'bigquery' else none),
    pre_hook="{{ scoped_pre_delete() }}"
) }}

{# SESSION SCOPE (macros/scope.sql). With --vars '{session_keys: [9158]}' those sessions are REPLACED: stg_car_data / stg_location expose only
   them, the pre_hook deletes them from this table first (so a session whose upstream rows vanished is cleared too), and delete+insert on
   session_key swaps in the fresh rows. With no scope, only sessions this table does not hold yet are loaded. The downstream telemetry marts
   read this table and are scoped the same way. #}
WITH car_data AS (
    SELECT * FROM {{ ref('stg_car_data') }}
    WHERE {{ telemetry_load_filter('session_key') }}
)
, with_location AS (
    {{ f1_asof_join(left='car_data', right=ref('stg_location'), on=['session_key', 'driver_number'], left_ts='telemetry_timestamp', right_ts='location_timestamp',
        left_cols=['session_key', 'meeting_key', 'driver_number', 'telemetry_timestamp', 'rpm', 'speed', 'gear', 'throttle', 'brake', 'drs'],
        right_cols=['location_timestamp', 'x_coordinate', 'y_coordinate', 'z_coordinate']) }}
),
with_lap AS (
    {{ f1_asof_join(left='with_location', right=ref('stg_laps'), on=['session_key', 'driver_number'], left_ts='telemetry_timestamp', right_ts='date_start',
        left_cols=['session_key', 'meeting_key', 'driver_number', 'telemetry_timestamp', 'rpm', 'speed', 'gear', 'throttle', 'brake', 'drs',
                   'location_timestamp', 'x_coordinate', 'y_coordinate', 'z_coordinate'],
        right_cols=['lap_number', 'date_start AS lap_date_start', 'lap_duration']) }}
)

{# A sample belongs to a lap only while that lap is running. The as-of join above finds the latest lap that STARTED before the sample, with
   no upper bound, so cool-down laps after the flag and garage time in practice/qualifying used to be credited to the previous lap
   (real data: the final lap of a race held minutes of slow driving). The window ends at lap_start + lap_duration (+ a small tolerance);
   laps without a duration get var telemetry_max_unknown_lap_seconds. Samples outside any lap keep lap_number NULL. #}
SELECT
    session_key, meeting_key, driver_number, telemetry_timestamp,
    CASE WHEN {{ f1_epoch_ms('telemetry_timestamp') }} - {{ f1_epoch_ms('lap_date_start') }}
              <= 1000 * (COALESCE(lap_duration, {{ var('telemetry_max_unknown_lap_seconds', 200) }}) + {{ var('telemetry_lap_tolerance_seconds', 2) }})
         THEN lap_number END AS lap_number,
    rpm, speed, gear, throttle, brake, drs,
    CASE WHEN {{ f1_epoch_ms('telemetry_timestamp') }} - {{ f1_epoch_ms('location_timestamp') }} <= 2000 THEN x_coordinate END AS x,
    CASE WHEN {{ f1_epoch_ms('telemetry_timestamp') }} - {{ f1_epoch_ms('location_timestamp') }} <= 2000 THEN y_coordinate END AS y,
    CASE WHEN {{ f1_epoch_ms('telemetry_timestamp') }} - {{ f1_epoch_ms('location_timestamp') }} <= 2000 THEN z_coordinate END AS z,
    {{ f1_epoch_ms('telemetry_timestamp') }} - {{ f1_epoch_ms('location_timestamp') }} AS location_age_ms
FROM with_lap
```

**Reading it line by line**

* **The `config` block:** `incremental` means the first run builds the table and later runs only add rows. On DuckDB the strategy is **`delete+insert` on `session_key`** (the first edition used `append`): the rows of any session in the new data are deleted and the new rows inserted, so a session is replaced, not duplicated. BigQuery has no such strategy here, so there it is `insert_overwrite` with a table **partitioned by `session_key`, one session per partition** (Chapter 10). The partition layout is chosen so that overwriting a partition can only ever replace the session being loaded. The `pre_hook`, `{{ scoped_pre_delete() }}`, runs first and deletes the scoped sessions from the table: `delete+insert` alone only replaces sessions that appear in the new rows, so without it a scoped session whose upstream rows had vanished would be left behind as stale data.
* **`f1_asof_join(...)`:** for each car-data row, attach the latest location row for the same session and driver that is **not later** than the reading, then do the same for the lap. It is a macro because DuckDB has a native `ASOF JOIN` and BigQuery does not; Chapter 10 explains how it works on both. The `>=` in the original `ASOF` condition is what makes it "latest at or before".
* **`telemetry_load_filter('session_key')`:** the row filter, from `macros/scope.sql` (below). With a scope set it keeps only the scoped sessions. With no scope, on an incremental run, it expands to `session_key NOT IN (SELECT DISTINCT session_key FROM {{ this }})`, so a session already present is skipped. (The first edition wrote that `NOT IN` inline; it is fine because `session_key` is never `NULL`.) On the first build it keeps everything.
* **`session_keys` (third edition):** an optional list of session keys. When set, only those sessions are loaded, and they are replaced. Unset, every session with car data that is not yet in the table is loaded. See the box below.
* **`location_age_ms`:** how stale the matched position is. A position older than two seconds is set to `NULL` instead of being trusted, because "latest known position" from 30 seconds ago is not a position.
* **Column aliases (`x`, `y`, `z`):** location coordinates arrive from staging as `x_coordinate` and so on.
* **`lap_number`:** the lap each reading belongs to, by an ASOF match to the latest lap start. Everything in Tasks 6 to 8 depends on it. A sample belongs to a lap only **while that lap is running**: from its start to start plus duration plus `telemetry_lap_tolerance_seconds`, and a lap with no duration gets `telemetry_max_unknown_lap_seconds`. Samples outside any lap keep a `NULL` lap number. Without that upper bound, cool-down laps and garage time were credited to the previous lap (the final lap of a race held minutes of slow driving, which produced 150 s delta traces and 93 km/h "top speeds"). The test `assert_telemetry_samples_within_lap_window` guards it.

**Build only the sessions you need.** On the author's real data the first full build took **1 hour 25 minutes**. A job that long that fails at minute 84 loses everything, so you tell dbt which sessions to build:

```bash
dbt build --select tag:telemetry_group --vars '{session_keys: [9158]}'
```

Only those sessions are built, and every telemetry mart downstream (lap summary, distance trace, corners, delta trace) follows automatically and is scoped the same way. A scoped session is **replaced**: run the command again and you get the same rows, not duplicates. Run it with other session keys to add or refresh more. Airflow passes the key for you after each extraction (Chapter 8).

Build it without a scope:

```bash
dbt run --select fct_telemetry
```

With no scope the model behaves like the first edition: it loads every session that is not in the table yet and **skips the ones that are**. That has a consequence worth knowing: after you change the telemetry logic, a plain run does **not** refresh sessions already loaded. Re-run the sessions you care about with `session_keys`, or rebuild everything with `--full-refresh` (never together: see the pitfalls).

**The session scope in detail (third edition).** Create **macros/scope.sql**. It is the single place that knows what the scope is:

```jinja
{#
  SESSION SCOPE. Airflow tells dbt which sessions changed; dbt never guesses.

    dbt build --vars '{session_keys: [9158, 9159]}'

  * var `session_keys` unset or empty  -> no scope: everything behaves as before (manual runs, CI, dbt_scope=full). The
                                          telemetry tables then load only the sessions they do not have yet.
  * var `session_keys` set             -> the scoped staging views (stg_car_data, stg_location) expose only those sessions,
                                          and every model in the telemetry group REPLACES those sessions (never appends).
  * `telemetry_only_sessions` is the old name of the var and still works as an alias.

  RULE: only stg_car_data and stg_location are scoped views. A `table` model must never read them, because a table rebuilt from a
  scoped view silently loses every other session. ci/check_scoped_views.py enforces this on the manifest.
#}

{# Validated, sorted, de-duplicated list of integer session keys. Anything that is not a positive integer is an error:
   the keys are pasted into SQL, so they are never coerced. `raw` none -> []. #}
{% macro validate_session_keys(raw) %}
  {%- set keys = [] -%}
  {%- if raw is not none -%}
    {%- if raw is string or raw is mapping or raw is not iterable -%}
      {{ exceptions.raise_compiler_error("Session keys must be a list of integers, e.g. [9158, 9159]. Got: " ~ raw) }}
    {%- endif -%}
    {%- for k in raw -%}
      {%- if not modules.re.fullmatch('[0-9]+', k | string) or (k | int) <= 0 -%}
        {{ exceptions.raise_compiler_error("Invalid session key: " ~ k ~ ". Session keys are positive integers.") }}
      {%- endif -%}
      {%- do keys.append(k | int) -%}
    {%- endfor -%}
  {%- endif -%}
  {{ return(keys | unique | sort | list) }}
{% endmacro %}

{# The scope of this run: var session_keys, else the legacy alias telemetry_only_sessions, else [] (no scope). #}
{% macro scope_session_keys() %}
  {%- set raw = var('session_keys', none) -%}
  {%- if raw is none or (raw is not string and raw is not mapping and raw is iterable and raw | length == 0) -%}
    {%- set raw = var('telemetry_only_sessions', none) -%}
  {%- endif -%}
  {{ return(validate_session_keys(raw)) }}
{% endmacro %}

{% macro is_scoped() %}{{ return(scope_session_keys() | length > 0) }}{% endmacro %}

{# `col IN (...)` when a scope is set, otherwise the always-true `1 = 1`. Safe to AND into any WHERE. #}
{% macro in_scope(col) -%}
  {%- set keys = scope_session_keys() -%}
  {%- if keys | length > 0 -%}{{ col }} IN ({{ keys | join(', ') }}){%- else -%}1 = 1{%- endif -%}
{%- endmacro %}

{#
  The row filter of every telemetry-group model that reads PERSISTED or scoped telemetry:
    scope set                     -> only the scoped sessions (the model REPLACES them: see scoped_pre_delete)
    no scope, incremental run     -> only sessions this table does not hold yet
    no scope, first build/refresh -> everything
  --full-refresh together with a scope would rebuild the table from the scoped sessions ONLY and drop all the others: refused.
#}
{% macro telemetry_load_filter(col='session_key') -%}
  {%- if execute and flags.FULL_REFRESH and is_scoped() -%}
    {{ exceptions.raise_compiler_error("--full-refresh together with session_keys would rebuild the telemetry tables from the scoped sessions only and silently drop every other session. Unset session_keys for a full refresh, or drop --full-refresh for a scoped run.") }}
  {%- endif -%}
  {%- if is_scoped() -%}{{ in_scope(col) }}
  {%- elif is_incremental() -%}{{ col }} NOT IN (SELECT DISTINCT session_key FROM {{ this }})
  {%- else -%}1 = 1{%- endif -%}
{%- endmacro %}

{# pre_hook for the incremental telemetry-group models. delete+insert already replaces sessions present in the new rows; this also
   clears a scoped session whose upstream rows have vanished, which delete+insert alone would leave behind as stale data.
   Empty (no statement) unless the table exists and a scope is set. #}
{% macro scoped_pre_delete() -%}
  {%- if is_incremental() and is_scoped() -%}DELETE FROM {{ this }} WHERE {{ in_scope('session_key') }}{%- endif -%}
{%- endmacro %}

{# For generic tests on heavy tables: the relation to test, restricted to the scoped sessions when a scope is set. #}
{% macro scoped_test_relation(model, scope_column) -%}
  {%- if scope_column and is_scoped() -%}(SELECT * FROM {{ model }} WHERE {{ in_scope(scope_column) }}) f1_scoped
  {%- else -%}{{ model }}{%- endif -%}
{%- endmacro %}
```

* **`scope_session_keys()` and `validate_session_keys()`:** read `session_keys` (falling back to `telemetry_only_sessions`) and accept only a **list of positive integers**. The keys are pasted into SQL, so anything else, such as `"9158; DROP TABLE x"`, `9158.5` or `true`, is a compile error, never coerced.
* **`in_scope(col)`:** returns `col IN (9158, 9159)`, or the always-true `1 = 1` with no scope, so it can be added to any `WHERE`. It is what the two staging views end with.
* **`telemetry_load_filter(col)`:** the filter of the incremental models, with the three behaviours above. It also **refuses `--full-refresh` together with a scope**: that would rebuild the table from the scoped sessions only and silently drop every other session.
* **`scoped_pre_delete()`:** the `pre_hook`; empty unless the table already exists and a scope is set.
* **`scoped_test_relation()`:** used by the scope-aware generic tests (Chapter 6).

> **The rule that keeps this safe.** Only `stg_car_data` and `stg_location` are scoped *views*, and **no `table` model may read them**: a table rebuilt from a scoped view silently loses every other session. The six telemetry models are incremental, so they persist all sessions. `ci/check_scoped_views.py` reads dbt's manifest and fails if a table reads scoped data (Chapter 11).

#### **Task 4: The Export Macro**

Create **macros/export_fct_telemetry.sql**. It writes the table out as a partitioned Parquet data lake, only when you ask it to.

```jinja
{#
  Streams fct_telemetry to a Hive-partitioned Parquet lake (meeting_key/session_key/driver_number).
  Opt-in and scoped, so a normal `dbt run` never triggers a long export:
    dbt run-operation export_fct_telemetry --vars '{export_data: true}'                       # everything
    dbt run-operation export_fct_telemetry --vars '{export_data: true, target_session_key: 9158}'
    dbt run-operation export_fct_telemetry --vars '{export_data: true, target_meeting_key: 1219}'
  Output folder: var telemetry_export_path (env F1_EXPORT_PATH). The partition folders are created by COPY, but
  the PARENT of that path must already exist (COPY will not create missing parents):  mkdir -p output
  Memory notes (see handbook Chapter 7): no global ORDER BY, preserve_insertion_order=false, capped open writers.
#}
{% macro export_fct_telemetry() %}
  {% if execute %}
    {% if not var('export_data', false) %}
      {% do log("export_fct_telemetry skipped: pass --vars '{export_data: true}' to run it.", info=True) %}
    {% else %}
      {% set path = var('telemetry_export_path') %}
      {% set filters = [] %}
      {% if var('target_session_key', none) is not none %}{% do filters.append("session_key = " ~ (var('target_session_key') | int)) %}{% endif %}
      {% if var('target_meeting_key', none) is not none %}{% do filters.append("meeting_key = " ~ (var('target_meeting_key') | int)) %}{% endif %}
      {% set where_clause = ("WHERE " ~ (filters | join(" AND "))) if filters else "" %}

      {% do run_query("SET preserve_insertion_order = false;") %}
      {% do run_query("SET partitioned_write_max_open_files = 10;") %}
      {% set export_query %}
        COPY (SELECT * FROM {{ ref('fct_telemetry') }} {{ where_clause }})
        TO '{{ path }}'
        (FORMAT PARQUET, PARTITION_BY (meeting_key, session_key, driver_number), OVERWRITE_OR_IGNORE TRUE);
      {% endset %}
      {% do log("Starting Parquet export to " ~ path ~ " " ~ where_clause, info=True) %}
      {% do run_query(export_query) %}
      {% do log("Export finished.", info=True) %}
    {% endif %}
  {% endif %}
{% endmacro %}
```

**What each section does**

1. **`{% if execute %}`:** dbt *parses* macros before running. Without this guard, the export would fire during parsing.
2. **`var('export_data', false)`:** a **safety switch**. Nothing is exported unless you pass `export_data: true`.
3. **Scoping variables:** `target_session_key` or `target_meeting_key` limit the export to new data (incremental export). With neither, it exports everything.
4. **DuckDB settings:** `preserve_insertion_order = false` lets DuckDB stream without remembering row order (a large memory saver); `partitioned_write_max_open_files = 10` caps simultaneously open partition files.
5. **`COPY (...) TO ... PARTITION_BY (meeting_key, session_key, driver_number) OVERWRITE_OR_IGNORE TRUE`:** writes the folder tree, replacing files that already exist.
6. **`var('telemetry_export_path')` (new):** the output folder is a variable (environment variable `F1_EXPORT_PATH`, default `output/telemetry_data`) instead of a hard-coded `F:/` path, so the same macro runs on your laptop and in the Airflow container. `COPY` creates the partition folders but **not** the parent of that path: the parent must already exist.

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

**Re-loading a session whose raw files changed.** An incremental model skips any session already in the table, so if you re-extract a session (the first copy was taken while it was live, say) the table will not notice. Without a scope that is true, and the macro below removes sessions by hand so the next `dbt run` rebuilds them. (With the third edition's `session_keys` you rarely need it: a scoped run replaces the session by itself.) The macro now finds **every incremental model tagged `telemetry_group`** (six tables) through the dbt graph instead of a hard-coded pair:

```jinja
{# Remove sessions from EVERY incremental model in the telemetry group (fct_telemetry, fct_telemetry_distance_trace,
   fct_telemetry_lap_summary, dim_session_corners, fct_corner_performance, fct_lap_time_delta_trace), found through the dbt tag.
   For manual use: a scoped run (--vars '{session_keys: [...]}') already replaces sessions by itself.
     dbt run-operation delete_sessions_from_incrementals --args '{sessions: [9158, 9159]}'
   Optional: --args '{sessions: [9158], tag: some_other_tag}' to target another group of incrementals. #}
{% macro delete_sessions_from_incrementals(sessions, tag='telemetry_group') %}
  {% if execute %}
    {% set keys = validate_session_keys(sessions) %}
    {% if keys | length > 0 %}
      {% set ids = keys | join(', ') %}
      {% for node in graph.nodes.values() | selectattr('resource_type', 'equalto', 'model') %}
        {% if tag in node.tags and node.config.materialized == 'incremental' %}
          {% set rel = adapter.get_relation(database=node.database, schema=node.schema, identifier=node.alias) %}
          {% if rel is not none %}
            {% do run_query("DELETE FROM " ~ rel ~ " WHERE session_key IN (" ~ ids ~ ")") %}
            {% do log("Deleted sessions " ~ ids ~ " from " ~ node.name, info=True) %}
          {% endif %}
        {% endif %}
      {% endfor %}
      {# run-operation does not auto-commit DML: without this the DELETE is silently rolled back #}
      {% do adapter.commit() %}
    {% endif %}
  {% endif %}
{% endmacro %}
```

```bash
dbt run-operation delete_sessions_from_incrementals --args '{sessions: [9158]}'
dbt run --select tag:telemetry_group
```

> **The `adapter.commit()` line is there because of a bug.** The first version of this macro logged "Deleted sessions ..." and the row count never changed: `run-operation` does **not** auto-commit DML, so the `DELETE` was rolled back silently. A log line saying "deleted" is not evidence of deletion; the test that found this counted the rows before and after.

#### **Task 5: Safe Memory and Temp Settings**

The shipped `profiles.yml` (Chapter 5) sets no `temp_directory`, so DuckDB spills to its default location. If you want it on a specific disk, set it under `config_options` so that it is applied once when the connection opens (see the Chapter 5 pitfall for why that matters), and create the folder so spilling can work:

```bash
mkdir -p .duckdb_tmp
```

If you need to cap memory (for example on a shared machine), DuckDB accepts `memory_limit` in `settings`:

```yaml
      settings:
        memory_limit: "8GB"            # fine under `settings`: it can change at any time
      config_options:
        temp_directory: "F:/DaTech/f1_data_project/duckdb_tmp"   # must stay under config_options
```

#### **Task 6: Put Telemetry on a Distance Axis (new in the second edition)**

Telemetry is recorded against time, but an engineer asks "where on the lap does driver A lose time to driver B?". For that you need speed, throttle and brake **against distance**. `fct_telemetry_distance_trace` builds that: it integrates speed into distance, then averages the readings into bins of `distance_bin_metres` (10 m by default).

```sql
{#
  Telemetry resampled onto a DISTANCE axis (var distance_bin_metres) so laps can be compared point by point.
  Distance is INTEGRATED from speed (sum of speed x dt), which drifts a little every lap. Because the circuit length is
  constant, every COMPLETE lap (telemetry covers >= 98% of the lap time) is rescaled so that its total distance equals the
  session's typical lap length (the median of complete-lap totals). Incomplete laps (cut-off in/out laps, red-flagged
  laps) keep their raw distance and are marked is_complete_lap = FALSE; do not compare them point by point.
  Laps through the pit lane follow a different path but are still rescaled; filter on is_representative_lap upstream.
  Incremental by session, like fct_telemetry: a scoped run (var session_keys) replaces those sessions, an unscoped run loads only missing ones.

  Unavailable readings are masked, not dropped (the distance axis still needs every sample): a throttle above 100 means "no data" (the value 104
  appears in the raw feed), and a run of var stale_telemetry_min_samples or more consecutive samples with the same non-zero speed and gear is a
  frozen reading, not driving (on real data a qualifying lap started with 3 s of identical 168 km/h, gear 4, throttle 104). The channels of those
  samples are NULL in the output, so a chart shows a gap instead of a flat line and a minimum speed is not taken from them.
#}
{{ config(
    materialized='incremental',
    incremental_strategy=var('bq_incremental_strategy', 'insert_overwrite') if target.type == 'bigquery' else 'delete+insert',
    unique_key=(['session_key', 'driver_number', 'lap_number', 'distance_bin'] if var('bq_incremental_strategy', 'insert_overwrite') == 'merge' else none) if target.type == 'bigquery' else 'session_key',
    partition_by=({'field': 'session_key', 'data_type': 'int64',
                   'range': {'start': var('bq_session_key_start'), 'end': var('bq_session_key_end'), 'interval': 1}}
                  if target.type == 'bigquery' else none),
    pre_hook="{{ scoped_pre_delete() }}"
) }}

WITH base AS (
    SELECT t.*, l.lap_duration
    FROM {{ ref('fct_telemetry') }} t
    LEFT JOIN {{ ref('stg_laps') }} l
        ON t.session_key = l.session_key AND t.driver_number = l.driver_number AND t.lap_number = l.lap_number
    WHERE t.lap_number IS NOT NULL
      AND {{ telemetry_load_filter('t.session_key') }}
),
changes AS (
    SELECT
        *,
        CASE WHEN speed = LAG(speed) OVER w AND gear = LAG(gear) OVER w THEN 0 ELSE 1 END AS is_change
    FROM base
    WINDOW w AS (PARTITION BY session_key, driver_number, lap_number ORDER BY telemetry_timestamp)
),
runs AS (
    SELECT
        *,
        SUM(is_change) OVER (PARTITION BY session_key, driver_number, lap_number ORDER BY telemetry_timestamp ROWS UNBOUNDED PRECEDING) AS run_id
    FROM changes
),
flagged AS (
    SELECT *, (run_len >= {{ var('stale_telemetry_min_samples') }} AND speed > 0) AS is_stale
    FROM (SELECT *, COUNT(*) OVER (PARTITION BY session_key, driver_number, lap_number, run_id) AS run_len FROM runs) r
),
stepped AS (
    SELECT
        *,
        COALESCE({{ f1_seconds_between('LAG(telemetry_timestamp) OVER w', 'telemetry_timestamp') }}, 0) AS dt_seconds,
        -- window MIN/MAX run over integer microseconds, not TIMESTAMPs (some engines mishandle windowed MIN/MAX of TIMESTAMP)
        MIN({{ f1_epoch_us('telemetry_timestamp') }}) OVER (PARTITION BY session_key, driver_number, lap_number) AS lap_first_us,
        MAX({{ f1_epoch_us('telemetry_timestamp') }}) OVER (PARTITION BY session_key, driver_number, lap_number) AS lap_last_us
    FROM flagged
    WINDOW w AS (PARTITION BY session_key, driver_number, lap_number ORDER BY telemetry_timestamp)
),
integrated AS (
    SELECT
        *,
        SUM(speed / 3.6 * dt_seconds) OVER (
            PARTITION BY session_key, driver_number, lap_number ORDER BY telemetry_timestamp ROWS UNBOUNDED PRECEDING
        ) AS lap_distance_raw_m,
        ({{ f1_epoch_us('telemetry_timestamp') }} - lap_first_us) / 1000000.0 AS lap_elapsed_seconds
    FROM stepped
),
lap_totals AS (
    SELECT
        session_key, driver_number, lap_number,
        MAX(lap_distance_raw_m) AS lap_total_raw_m,
        MAX(lap_duration) AS lap_duration,
        (MAX(lap_last_us) - MIN(lap_first_us)) / 1000000.0 AS covered_seconds
    FROM integrated
    GROUP BY session_key, driver_number, lap_number
),
lap_flags AS (
    SELECT *, COALESCE(covered_seconds >= 0.98 * lap_duration, FALSE) AS is_complete_lap
    FROM lap_totals
),
session_length AS (
    SELECT DISTINCT
        session_key,
        {{ f1_median_over('CASE WHEN is_complete_lap THEN lap_total_raw_m END', 'session_key') }} AS track_length_m
    FROM lap_flags
),
normalised AS (
    SELECT
        i.*,
        f.is_complete_lap,
        s.track_length_m,
        ROUND(
            CASE WHEN f.is_complete_lap AND f.lap_total_raw_m > 0 AND s.track_length_m IS NOT NULL
                 THEN i.lap_distance_raw_m * s.track_length_m / f.lap_total_raw_m
                 ELSE i.lap_distance_raw_m END, 6) AS lap_distance_m
    FROM integrated i
    JOIN lap_flags f ON i.session_key = f.session_key AND i.driver_number = f.driver_number AND i.lap_number = f.lap_number
    JOIN session_length s ON i.session_key = s.session_key
),
binned AS (
    SELECT *, {{ f1_cast('FLOOR(lap_distance_m / ' ~ var('distance_bin_metres') ~ ')', 'int') }} AS distance_bin
    FROM normalised
)
SELECT
    session_key, meeting_key, driver_number, lap_number,
    distance_bin,
    distance_bin * {{ var('distance_bin_metres') }} AS distance_m,
    MIN(lap_elapsed_seconds) AS elapsed_seconds,
    AVG(CASE WHEN NOT is_stale THEN speed END) AS avg_speed_kph,
    MIN(CASE WHEN NOT is_stale THEN speed END) AS min_speed_kph,
    AVG(CASE WHEN NOT is_stale AND throttle <= 100 THEN throttle END) AS avg_throttle,
    MAX(CASE WHEN NOT is_stale THEN brake END) AS max_brake,
    {{ f1_cast('ROUND(AVG(CASE WHEN NOT is_stale THEN gear END))', 'int') }} AS gear,
    AVG(x) AS x, AVG(y) AS y,
    MIN(is_complete_lap) AS is_complete_lap,
    MIN(track_length_m) AS track_length_m
FROM binned
GROUP BY session_key, meeting_key, driver_number, lap_number, distance_bin
```

**Why it is written this way**

* **Distance is integrated from speed** (the sum of speed times elapsed time), so it **drifts** a little every lap. If you compare two laps raw, their corners sit at slightly different distances and every corner-by-corner comparison is quietly wrong.
* **Normalisation:** the model finds the session's typical lap length (the median total distance of the *complete* laps, those where telemetry covers at least 98% of the lap time) and rescales every complete lap to it. Incomplete laps (cut-off in/out laps) keep their raw distance and are marked `is_complete_lap = false`; do not compare those point by point. This step was found necessary by testing: without it, a corner's measured apex speed on the synthetic data averaged 142 km/h when the true value was 90, because the corners of slower laps had slid along the lap.
* **Window functions over integer microseconds, not timestamps:** the first and last sample times are taken with `epoch_us(...)` (an integer). Some engines mishandle windowed `MIN`/`MAX` over a `TIMESTAMP` column (the BigQuery emulator returned a wrong value), and integers are the same everywhere.
* **The bin is computed in its own CTE.** Selecting an expression of `lap_distance_m` while grouping by an *alias* of a different expression is accepted by DuckDB and rejected by BigQuery. It is a genuine portability bug found when the model was ported.
* **Unavailable and frozen readings are masked, not dropped (third edition's code, which also brings in earlier repository fixes).** The distance axis needs every sample, so bad readings stay as rows but their channels become `NULL`. A throttle above 100 means "no data" (the raw feed reports 104 for it), and a run of `stale_telemetry_min_samples` or more consecutive samples with the same non-zero speed **and** gear is a frozen reading, not driving: on the author's real data a qualifying lap started with 3 s of identical 168 km/h, gear 4, throttle 104. The `changes`, `runs` and `flagged` CTEs find those runs, and the averages use `CASE WHEN NOT is_stale ...`, so a chart shows a gap instead of a flat line and a minimum speed is never taken from them. Two tests guard this: `assert_unavailable_telemetry_is_masked` and, on the generated data only, `assert_frozen_telemetry_is_masked` (Chapter 6).
* **Incremental, scoped by session:** like `fct_telemetry`, it is a `delete+insert` model on `session_key`, filtered by `telemetry_load_filter('t.session_key')` and cleaned by the same `pre_hook`. A scoped run reads only the scoped sessions' telemetry, so the normalisation above (the session's median lap length) is computed from that session alone, which is exactly what it needs.
* **`ROUND` before the floor:** a distance sum that lands exactly on a bin edge could flip to the neighbouring bin depending on floating-point rounding order. Rounding to six decimals first makes a full build and an incremental refill produce identical bins.

#### **Task 7: Per-Lap Telemetry KPIs and the Delta Trace**

**Step 1: A one-line summary of every lap.** `fct_telemetry_lap_summary` reduces each lap to the numbers an engineer looks at first: top and minimum speed, how much of the lap was full throttle or braking, the number of **braking zones** (a zone starts when the brake goes from off to on), DRS-open share and gear usage.

```sql
{# Per driver per lap telemetry KPIs: top/min speed, throttle/brake application, gear usage, DRS and braking zones.
   INCREMENTAL by session (was a table that rescanned every telemetry row on each build): a scoped run replaces those sessions, an unscoped run
   loads only sessions not yet here. #}
{{ config(
    materialized='incremental',
    incremental_strategy=var('bq_incremental_strategy', 'insert_overwrite') if target.type == 'bigquery' else 'delete+insert',
    unique_key=(['session_key', 'driver_number', 'lap_number'] if var('bq_incremental_strategy', 'insert_overwrite') == 'merge' else none) if target.type == 'bigquery' else 'session_key',
    partition_by=({'field': 'session_key', 'data_type': 'int64',
                   'range': {'start': var('bq_session_key_start'), 'end': var('bq_session_key_end'), 'interval': 1}}
                  if target.type == 'bigquery' else none),
    pre_hook="{{ scoped_pre_delete() }}"
) }}

WITH samples AS (
    SELECT
        *,
        LAG(brake) OVER (PARTITION BY session_key, driver_number, lap_number ORDER BY telemetry_timestamp) AS prev_brake
    FROM {{ ref('fct_telemetry') }}
    WHERE lap_number IS NOT NULL
      AND {{ telemetry_load_filter('session_key') }}
)
SELECT
    session_key, meeting_key, driver_number, lap_number,
    COUNT(*) AS telemetry_samples,
    MAX(speed) AS top_speed_kph,
    MIN(speed) AS min_speed_kph,
    AVG(speed) AS avg_speed_kph,
    -- a throttle above 100 (the raw feed's 104) means 'no data': it must not count as full throttle
    AVG(CASE WHEN throttle <= 100 THEN {{ f1_cast('(throttle >= 99)', 'int') }} END) AS full_throttle_fraction,
    AVG({{ f1_cast('(brake > 0)', 'int') }}) AS braking_fraction,
    -- a braking zone starts when brake goes from released (0) to applied (>0)
    COUNT(CASE WHEN brake > 0 AND COALESCE(prev_brake, 0) = 0 THEN 1 END) AS braking_zone_count,
    AVG({{ f1_cast('(drs IN (10, 12, 14))', 'int') }}) AS drs_open_fraction,
    MAX(gear) AS max_gear,
    AVG({{ f1_cast('(gear <= 3)', 'int') }}) AS low_gear_fraction,
    AVG({{ f1_cast('(gear >= 7)', 'int') }}) AS high_gear_fraction
FROM samples
GROUP BY session_key, meeting_key, driver_number, lap_number
```

**Step 2: Where does a driver lose time to the fastest lap?** `fct_lap_time_delta_trace` takes each driver's best lap and the session's fastest lap and subtracts their elapsed times at every distance bin. A positive `time_delta_seconds` means the driver is behind the reference at that point.

```sql
{# Each driver's best lap vs the session's fastest lap, point by point along the lap (the engineer's "delta trace").
   time_delta_seconds > 0 means the driver is behind the reference at that distance. Only drivers with extracted
   telemetry appear (telemetry is the heaviest endpoint and may be extracted for fewer drivers/sessions).
   INCREMENTAL by session: a scoped run replaces those sessions, an unscoped run loads only sessions not yet here. #}
{{ config(
    materialized='incremental',
    incremental_strategy=var('bq_incremental_strategy', 'insert_overwrite') if target.type == 'bigquery' else 'delete+insert',
    unique_key=(['session_key', 'driver_number', 'lap_number', 'distance_bin'] if var('bq_incremental_strategy', 'insert_overwrite') == 'merge' else none) if target.type == 'bigquery' else 'session_key',
    partition_by=({'field': 'session_key', 'data_type': 'int64',
                   'range': {'start': var('bq_session_key_start'), 'end': var('bq_session_key_end'), 'interval': 1}}
                  if target.type == 'bigquery' else none),
    pre_hook="{{ scoped_pre_delete() }}"
) }}

WITH best AS (
    SELECT session_key, driver_number, lap_number, is_overall_fastest_lap FROM {{ ref('fct_fastest_laps') }}
),
reference AS (
    SELECT session_key, driver_number AS ref_driver_number, lap_number AS ref_lap_number FROM best WHERE is_overall_fastest_lap
)
SELECT
    d.session_key, d.driver_number, d.lap_number, r.ref_driver_number,
    d.distance_bin, d.distance_m,
    d.elapsed_seconds - ref.elapsed_seconds AS time_delta_seconds,
    d.avg_speed_kph - ref.avg_speed_kph AS speed_delta_kph
FROM {{ ref('fct_telemetry_distance_trace') }} d
JOIN best b ON d.session_key = b.session_key AND d.driver_number = b.driver_number AND d.lap_number = b.lap_number
JOIN reference r ON d.session_key = r.session_key
JOIN {{ ref('fct_telemetry_distance_trace') }} ref
    ON ref.session_key = r.session_key AND ref.driver_number = r.ref_driver_number
    AND ref.lap_number = r.ref_lap_number AND ref.distance_bin = d.distance_bin
WHERE {{ telemetry_load_filter('d.session_key') }}
```

> **Both models are incremental by session (third edition).** `fct_telemetry_lap_summary` used to be a table that rescanned every telemetry row on each build. Both now read the persisted telemetry (and the distance trace) filtered by `telemetry_load_filter`: a scoped run replaces those sessions, an unscoped run loads only sessions not yet in the table. `fct_telemetry_lap_summary` also stopped counting a throttle above 100 ("no data") as full throttle.

> **It only covers drivers with telemetry.** Telemetry is the heaviest endpoint and may be extracted for fewer drivers or sessions than laps, and this trace appears only where a driver's best lap has telemetry.

#### **Task 8: Detect Corners and Measure Apex Speeds (new in the second edition)**

The API publishes **no corner data**: no turn numbers, names or positions. So corners are **detected** from the telemetry of a reference lap.

**Step 1: Detect the corners.** For each session, `dim_session_corners` takes the fastest lap that has telemetry, smooths its speed along the distance axis, and calls a point an **apex** when it is the lowest speed within `corner_min_separation_bins` *and* at least `corner_min_speed_drop_kph` below the highest speed nearby. Apexes are numbered in lap order. It also estimates the turn angle and direction from the x/y path, and classifies the corner as slow, medium or fast.

```sql
{#
  DERIVED corner segmentation. OpenF1 publishes no corner data, so corners are DETECTED from telemetry:
    1. take each session's fastest lap that has telemetry (the reference lap) on the distance axis
    2. smooth speed, then call a bin an apex when it is the lowest speed within var corner_min_separation_bins
       AND at least var corner_min_speed_drop_kph below the highest speed within var corner_prominence_window_bins
    3. number the apexes in lap order; entry/exit points sit var corner_entry/exit_distance_m either side
  corner_number is therefore "the Nth detected braking/slowing zone", NOT the official turn number, and it can differ
  between sessions (a chicane may be one corner in one session and two in another). Apexes within
  var corner_min_separation_bins of the lap start/end are not detected (their detection windows would be cut off). turn_direction uses the raw x/y
  plane convention and may be mirrored relative to the real circuit. Speed class uses the reference apex speed.
  INCREMENTAL by session: a scoped run replaces those sessions' corners, an unscoped run adds only sessions not yet here. Corners are
  derived from the session's fastest lap, which is rebuilt on every run, so re-run a session with session_keys after its laps change.
#}
{{ config(
    materialized='incremental',
    incremental_strategy=var('bq_incremental_strategy', 'insert_overwrite') if target.type == 'bigquery' else 'delete+insert',
    unique_key=(['session_key', 'corner_number'] if var('bq_incremental_strategy', 'insert_overwrite') == 'merge' else none) if target.type == 'bigquery' else 'session_key',
    partition_by=({'field': 'session_key', 'data_type': 'int64',
                   'range': {'start': var('bq_session_key_start'), 'end': var('bq_session_key_end'), 'interval': 1}}
                  if target.type == 'bigquery' else none),
    pre_hook="{{ scoped_pre_delete() }}"
) }}

WITH ref AS (
    SELECT session_key, driver_number, lap_number
    FROM (
        SELECT t.session_key, t.driver_number, t.lap_number,
               ROW_NUMBER() OVER (PARTITION BY t.session_key ORDER BY f.fastest_lap_time_seconds) AS rn
        FROM (SELECT DISTINCT session_key, driver_number, lap_number FROM {{ ref('fct_telemetry_distance_trace') }}
              WHERE {{ telemetry_load_filter('session_key') }}) t
        JOIN {{ ref('fct_fastest_laps') }} f
            ON t.session_key = f.session_key AND t.driver_number = f.driver_number AND t.lap_number = f.lap_number
    ) ranked
    WHERE rn = 1
),
trace AS (
    SELECT d.session_key, d.driver_number AS ref_driver_number, d.lap_number AS ref_lap_number,
           d.distance_bin, d.distance_m, d.avg_speed_kph, d.x, d.y
    FROM {{ ref('fct_telemetry_distance_trace') }} d
    JOIN ref r ON d.session_key = r.session_key AND d.driver_number = r.driver_number AND d.lap_number = r.lap_number
),
smoothed AS (
    SELECT *,
        AVG(avg_speed_kph) OVER (PARTITION BY session_key ORDER BY distance_bin
            ROWS BETWEEN {{ var('corner_smooth_bins') }} PRECEDING AND {{ var('corner_smooth_bins') }} FOLLOWING) AS speed_smooth
    FROM trace
),
windows AS (
    SELECT *,
        MIN(speed_smooth) OVER (PARTITION BY session_key ORDER BY distance_bin
            ROWS BETWEEN {{ var('corner_min_separation_bins') }} PRECEDING AND {{ var('corner_min_separation_bins') }} FOLLOWING) AS local_min,
        MAX(speed_smooth) OVER (PARTITION BY session_key ORDER BY distance_bin
            ROWS BETWEEN {{ var('corner_prominence_window_bins') }} PRECEDING AND {{ var('corner_prominence_window_bins') }} FOLLOWING) AS local_max
    FROM smoothed
),
candidates AS (
    SELECT *,
        (speed_smooth = local_min AND local_max - speed_smooth >= {{ var('corner_min_speed_drop_kph') }}
         -- windows are truncated at the lap edges, which makes the first/last bins look like minima: require a full window
         AND distance_bin >= MIN(distance_bin) OVER (PARTITION BY session_key) + {{ var('corner_min_separation_bins') }}
         AND distance_bin <= MAX(distance_bin) OVER (PARTITION BY session_key) - {{ var('corner_min_separation_bins') }}) AS is_candidate
    FROM windows
),
apexes AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY session_key ORDER BY distance_bin) AS corner_number
    FROM (
        SELECT *, COALESCE(LAG(is_candidate) OVER (PARTITION BY session_key ORDER BY distance_bin), FALSE) AS prev_candidate
        FROM candidates
    ) c
    WHERE is_candidate AND NOT prev_candidate     -- first bin of a flat minimum only
),
bounds AS (
    SELECT a.*,
        a.distance_bin - {{ (var('corner_entry_distance_m') / var('distance_bin_metres')) | int }} AS entry_bin,
        a.distance_bin + {{ (var('corner_exit_distance_m') / var('distance_bin_metres')) | int }} AS exit_bin
    FROM apexes a
),
-- The distance axis is sparse where the car is fast: car data arrives about 4 times a second, so at 300 km/h a 10 m bin often has no row at all.
-- An exact-bin join for the entry and exit points therefore found nothing on fast corners and the turn angle was NULL. Use the NEAREST bin that has a
-- position, within var corner_geometry_search_bins.
entry_pts AS (
    SELECT session_key, corner_number, x AS entry_x, y AS entry_y
    FROM (
        SELECT b.session_key, b.corner_number, e.x, e.y,
               ROW_NUMBER() OVER (PARTITION BY b.session_key, b.corner_number ORDER BY ABS(e.distance_bin - b.entry_bin)) AS rn
        FROM bounds b
        JOIN trace e ON b.session_key = e.session_key AND ABS(e.distance_bin - b.entry_bin) <= {{ var('corner_geometry_search_bins') }}
        WHERE e.x IS NOT NULL AND e.y IS NOT NULL
    ) ranked WHERE rn = 1
),
exit_pts AS (
    SELECT session_key, corner_number, x AS exit_x, y AS exit_y
    FROM (
        SELECT b.session_key, b.corner_number, e.x, e.y,
               ROW_NUMBER() OVER (PARTITION BY b.session_key, b.corner_number ORDER BY ABS(e.distance_bin - b.exit_bin)) AS rn
        FROM bounds b
        JOIN trace e ON b.session_key = e.session_key AND ABS(e.distance_bin - b.exit_bin) <= {{ var('corner_geometry_search_bins') }}
        WHERE e.x IS NOT NULL AND e.y IS NOT NULL
    ) ranked WHERE rn = 1
),
geom AS (
    SELECT b.*, en.entry_x, en.entry_y, ex.exit_x, ex.exit_y
    FROM bounds b
    LEFT JOIN entry_pts en ON b.session_key = en.session_key AND b.corner_number = en.corner_number
    LEFT JOIN exit_pts ex ON b.session_key = ex.session_key AND b.corner_number = ex.corner_number
),
turns AS (
    -- radians to degrees by hand: BigQuery has no DEGREES function
    SELECT *,
        ATAN2(y - entry_y, x - entry_x) * 180.0 / ACOS(-1) AS heading_in,
        ATAN2(exit_y - y, exit_x - x) * 180.0 / ACOS(-1) AS heading_out
    FROM geom
),
angled AS (
    SELECT *, heading_out - heading_in AS raw_turn
    FROM turns
)
SELECT
    session_key,
    corner_number,
    distance_bin AS apex_bin,
    distance_m AS apex_distance_m,
    entry_bin, exit_bin,
    avg_speed_kph AS reference_apex_speed_kph,
    CASE WHEN raw_turn > 180 THEN raw_turn - 360 WHEN raw_turn < -180 THEN raw_turn + 360 ELSE raw_turn END AS turn_angle_deg,
    -- NULL when the angle could not be measured: this used to fall through to 'RIGHT', which invented a direction for every corner without an angle
    CASE WHEN raw_turn IS NULL THEN NULL
         WHEN (CASE WHEN raw_turn > 180 THEN raw_turn - 360 WHEN raw_turn < -180 THEN raw_turn + 360 ELSE raw_turn END) > 0
         THEN 'LEFT' ELSE 'RIGHT' END AS turn_direction,
    CASE WHEN avg_speed_kph < {{ var('corner_slow_kph') }} THEN 'SLOW'
         WHEN avg_speed_kph >= {{ var('corner_fast_kph') }} THEN 'FAST' ELSE 'MEDIUM' END AS speed_class,
    x AS apex_x, y AS apex_y,
    ref_driver_number, ref_lap_number
FROM angled
```

**Step 2: Measure every driver through every corner.** `fct_corner_performance` finds, for every driver and lap, the **entry speed, apex speed and exit speed**, the distance from the first brake application to the apex, and the distance from the apex to full throttle.

```sql
{# Per driver per lap per DETECTED corner (see dim_session_corners for what "corner" means here).
   INCREMENTAL by session: a scoped run replaces those sessions, an unscoped run loads only sessions not yet here. #}
{{ config(
    materialized='incremental',
    incremental_strategy=var('bq_incremental_strategy', 'insert_overwrite') if target.type == 'bigquery' else 'delete+insert',
    unique_key=(['session_key', 'driver_number', 'lap_number', 'corner_number'] if var('bq_incremental_strategy', 'insert_overwrite') == 'merge' else none) if target.type == 'bigquery' else 'session_key',
    partition_by=({'field': 'session_key', 'data_type': 'int64',
                   'range': {'start': var('bq_session_key_start'), 'end': var('bq_session_key_end'), 'interval': 1}}
                  if target.type == 'bigquery' else none),
    pre_hook="{{ scoped_pre_delete() }}"
) }}

WITH j AS (
    SELECT
        t.session_key, t.driver_number, t.lap_number, c.corner_number,
        t.distance_bin, t.distance_m, t.avg_speed_kph, t.min_speed_kph, t.avg_throttle, t.max_brake, t.elapsed_seconds,
        t.is_complete_lap,
        c.entry_bin, c.apex_bin, c.exit_bin, c.apex_distance_m
    FROM {{ ref('fct_telemetry_distance_trace') }} t
    JOIN {{ ref('dim_session_corners') }} c
        ON t.session_key = c.session_key
        AND t.distance_bin BETWEEN c.entry_bin - {{ (var('corner_brake_lookback_m') / var('distance_bin_metres')) | int }} AND c.exit_bin + 2
    WHERE {{ telemetry_load_filter('t.session_key') }}
),
agg AS (
    SELECT
        session_key, driver_number, lap_number, corner_number,
        MIN(apex_distance_m) AS apex_distance_m,
        -- entry and exit use the bins within 2 of the entry/exit point: on a fast section an exact bin often has no sample (see dim_session_corners)
        AVG(CASE WHEN distance_bin BETWEEN entry_bin - 2 AND entry_bin + 2 THEN avg_speed_kph END) AS entry_speed_kph,
        MIN(CASE WHEN distance_bin BETWEEN apex_bin - 3 AND apex_bin + 3 THEN min_speed_kph END) AS apex_speed_kph,
        AVG(CASE WHEN distance_bin BETWEEN exit_bin - 2 AND exit_bin + 2 THEN avg_speed_kph END) AS exit_speed_kph,
        MIN(CASE WHEN max_brake > 0 AND distance_bin <= apex_bin THEN distance_m END) AS brake_start_distance_m,
        MIN(CASE WHEN avg_throttle >= 99 AND distance_bin > apex_bin THEN distance_m END) AS full_throttle_distance_m,
        MIN(CASE WHEN distance_bin BETWEEN exit_bin - 2 AND exit_bin + 2 THEN elapsed_seconds END)
            - MIN(CASE WHEN distance_bin BETWEEN entry_bin - 2 AND entry_bin + 2 THEN elapsed_seconds END) AS corner_time_seconds,
        MIN(is_complete_lap) AS is_complete_lap
    FROM j
    GROUP BY session_key, driver_number, lap_number, corner_number
)
SELECT
    a.*,
    a.apex_distance_m - a.brake_start_distance_m AS brake_to_apex_distance_m,
    a.full_throttle_distance_m - a.apex_distance_m AS apex_to_full_throttle_distance_m,
    a.apex_speed_kph - MAX(CASE WHEN a.is_complete_lap THEN a.apex_speed_kph END) OVER (PARTITION BY a.session_key, a.corner_number) AS apex_speed_delta_to_session_best_kph,
    l.is_representative_lap
FROM agg a
LEFT JOIN {{ ref('int_lap_context') }} l
    ON a.session_key = l.session_key AND a.driver_number = l.driver_number AND a.lap_number = l.lap_number
```

> **Corner geometry and direction (the repository's fixes, now in the book).** The distance axis is sparse where the car is fast: car data arrives about four times a second, so at 300 km/h a 10 m bin often has no row at all. An exact-bin join for a corner's entry and exit points therefore found nothing on fast corners and the turn angle was `NULL`. `entry_pts` and `exit_pts` now take the **nearest bin that has a position**, within `corner_geometry_search_bins`. A corner whose angle still cannot be measured now has a `NULL` direction: the first version reported `RIGHT` for every corner without an angle, inventing a direction (`assert_corner_direction_needs_angle` guards this). In `fct_corner_performance`, entry, exit and corner time use the bins **within two of** the entry and exit points for the same sparseness reason, and `is_complete_lap` limits the session-best comparison to complete laps.

> **Both corner models are incremental by session (third edition)**, like the rest of the telemetry group. A corner is derived from the session's fastest lap, which is rebuilt on every run, so re-run a session with `session_keys` after its laps change.

> **What "corner" means here.** `corner_number` is *the Nth detected slowing zone*, **not** the official turn number, and it can differ between sessions (a chicane may be one corner in one session and two in another). `turn_direction` uses the raw x/y convention and may be mirrored. Apexes within `corner_min_separation_bins` of the lap start or end are not detected, because their detection windows would be cut off. The thresholds were tuned on synthetic laps, so expect to retune them on real circuits.

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
* **Why it Happens:** with no scope, an incremental model skips sessions already present (`session_key NOT IN (SELECT ... FROM this)`), so you see nothing new unless the session is genuinely new.
* **The Solution:** add the new session's staging data first, or use `--full-refresh` when the logic changes.

* **The Problem:** the export prints "Skipping export".
* **Why it Happens:** the safety switch `export_data` was not set, or was passed with `--args` instead of `--vars`.
* **The Solution:** pass `--vars '{export_data: true}'` and confirm the log line "Starting ... Parquet export".

* **The Problem (second edition, hit on the author's real data):** the first full build of `fct_telemetry` runs for **1 hour 25 minutes**, and the tests that follow it error out.
* **Why it Happens:** it joins every car-data reading to the latest location and lap in one go, spilling heavily to disk on a single thread, and if `temp_directory` is configured under `settings` the spill then triggers the error described in Chapter 5. The model is correct; the job is simply too big for one transaction.
* **The Solution:** build only the sessions you need with `session_keys` (Task 3), and keep any `temp_directory` under `config_options`.

* **The Problem (second edition, found by the tests):** corner apex speeds vary wildly from lap to lap even though every lap uses the same corners.
* **Why it Happens:** distance integrated from speed drifts, so a slower lap's corners land at different distances and the corner window misses the true minimum.
* **The Solution:** normalise each complete lap to the session's typical lap length (Task 6). Before this fix the synthetic data measured 142 km/h average apex speed against a true 90; afterwards every lap and driver measured exactly 90.

* **The Problem (second edition, found by the tests):** a corner appears at distance 0 with an apex speed of about 200 km/h and no turn angle.
* **Why it Happens:** at the start of the lap the smoothing and prominence windows are cut off on one side, so the first bin looks like a local minimum.
* **The Solution:** require a full window on both sides, so no apex is detected within `corner_min_separation_bins` of either end of the lap.

* **The Problem (second edition, hit on the author's real data):** `assert_corner_apex_is_a_minimum` warns **once**.
* **Why it Happens:** the test checks that, on each session's *reference* lap, an apex is slower than its entry and exit points. It can warn for several reasons: corners packed so closely that one corner's entry or exit point lies inside its neighbour, a telemetry dropout, a lock-up or slide on the reference lap, or a detection threshold that needs retuning. A single warning cannot tell them apart. Note also that the test looks at one row per session and corner on the *reference lap*, not at every corner measurement, so the denominator for "how rare is one warning" is the number of session-corner pairs, much smaller than the number of measured corners.
* **The Solution:** the test is a **warning**, not an error, for exactly that reason. Inspect the session's corner and its neighbours in `dim_session_corners` and `fct_corner_performance` before retuning a threshold. `docs/ISSUES_CHECKLIST.md` (item D11) records that the detector finds speed minima and braking zones rather than official turns.

* **The Problem (third edition, hit while verifying it):** a telemetry test fails (`assert_frozen_telemetry_is_masked` returned 24 rows) although the model code is right, and the three models downstream of it are skipped.
* **Why it Happens:** the database file already held telemetry from an **older** build, and an unscoped incremental run keeps sessions it already has. The raw fixtures had not been regenerated either, because `make` was not installed on that machine: the line printed `make: command not found`, but the next lines of the pasted block still ran against the old files. The old rows were stale, not wrong code. After the data and database were regenerated the test passed on both the original and the changed code, which is how the two causes were told apart.
* **The Solution:** delete the database (`rm -rf .ci_raw` on the generated dataset), regenerate the data, rebuild. For a real warehouse, re-run the affected sessions with `session_keys` instead. Lesson: when a test fails after you changed incremental logic, rebuild from clean state *before* debugging the code, and compare against the original commit in a separate checkout (`git worktree`) rather than guessing.

* **The Problem (third edition):** `Compilation Error ... --full-refresh together with session_keys would rebuild the telemetry tables from the scoped sessions only`.
* **Why it Happens:** `--full-refresh` drops and rebuilds an incremental table. With a scope set it would be rebuilt from the scoped sessions alone and every other session would disappear. `telemetry_load_filter` refuses the combination on purpose.
* **The Solution:** use one or the other. To refresh a session, drop `--full-refresh` and pass `session_keys`; to rebuild everything, unset the scope. Airflow's `dbt_full_refresh` parameter never sets both (Chapter 8).

* **The Problem (third edition):** a scoped run for a session **deletes** its rows and loads none.
* **Why it Happens:** the session's raw car data or location files no longer exist (deleted, or the extraction returned nothing), so the pre-delete cleared the old rows and there was nothing to insert. That is the replace-by-session rule working: the table now matches the raw data. The equivalence check in Chapter 11 tests this case on purpose.
* **The Solution:** restore or re-extract the raw files and run the session again. Keep a copy of the warehouse before the first scoped runs against real data.

* **The Problem (second edition, found by the tests):** after `delete_sessions_from_incrementals` the telemetry row count is unchanged.
* **Why it Happens:** `dbt run-operation` does not auto-commit, so the `DELETE` was silently rolled back.
* **The Solution:** `{% do adapter.commit() %}` at the end of the macro (Task 4).

---

## **Chapter 8: Closing the Loop: Running dbt From Airflow**

### **Chapter Overview**

So far you trigger Airflow, then separately trigger dbt by hand. In this chapter the DAG calls dbt itself, once extraction has finished, and (third edition) tells dbt **which sessions changed**, so each run builds only its own sessions. **[VERIFIED]** on the author's real data in Docker: a `latest` run extracted 17 Parquet files (17 written, 1 empty, the `starting_grid` feed for that session), logged `mode=scoped keys=[11420]`, ran a single `dbt build --target container ... --vars '{"enable_radio_transcripts":true,"session_keys":[11420]}'` (140 nodes: 137 passed, 3 pre-existing data-quality warnings, 0 errors), and left every other session's row count in the six telemetry tables identical to a backup taken beforehand. The earlier second-edition statement that nothing had run in Docker no longer applies. The multi-chunk plan, `dbt_scope=full` and the full-refresh guard are covered by unit tests only, not by a live run.

> **What changed from the first edition.** In the first edition this chapter was a to-do list ("add these mounts, write a container profile, change the paths"). All of that is now **in the repository**: the compose file mounts the dbt project, `profiles.yml` has a `container` target, and the paths that were hard-coded to `F:/` are environment variables. The task is also simpler: it runs `dbt build`, and the telemetry **export** is no longer scoped by the DAG; run it separately with the `run-operation` from Chapter 7.
>
> **What changed in the third edition.** The task used to run one `dbt build` over the whole project after any extraction. It now builds a **scope** from what the run actually wrote, plans the dbt commands as argument lists, validates the session keys, logs the exact command, and waits for transcription. The first edition's pitch, "dbt decides how to transform", is unchanged: Airflow only decides *which sessions*.

**Concepts introduced in this chapter**

* **Orchestration boundary:** Airflow decides *when and what*, dbt decides *how to transform*. Airflow should call dbt, not reimplement it.
* **Scope:** the list of sessions a run changed, passed to dbt as the variable `session_keys`. The run tells dbt what changed; dbt never guesses.
* **Argument list instead of a shell string:** the runner hands `subprocess.run` a **list** (`["dbt", "build", "--target", ...]`) with `shell=False`. There is no shell to inject into, so no quoting is needed. This replaces the first edition's `shlex.quote`.
* **Chunking:** a run over many sessions builds the telemetry group a few sessions at a time (`dbt_chunk_size`, default 10), so one failure loses one chunk.
* **Trigger rule (`none_failed`):** by default a task is skipped if any upstream task was skipped. The dbt task must still run when transcription is switched off and its mapped tasks are all skipped, so it uses `TriggerRule.NONE_FAILED`: it runs unless an upstream task *failed*.
* **Task dependency (`>>`):** the dbt task waits for `verify_and_load` **and** for the radio transcription task.

### **Step by Step Procedures**

#### **Task 1: Check That dbt Is Available Inside the Airflow Container**

The task runs `dbt` **inside the Airflow container**, so the container needs dbt, the project, and paths that exist in Linux. The second edition's Dockerfile and compose file (Chapter 2) already provide all three; this task is a checklist of *why* each piece is there, so you can debug it.

1. **dbt is installed.** The Dockerfile installs `requirements.txt` (`pyarrow`, `faster-whisper` and so on) and `dbt-duckdb` into the image when it is built. That needs the **Python 3.11 image tag**; see the Chapter 2 pitfall.
2. **The dbt project is mounted** at `/opt/airflow/dbt/f1_data_project` (`../dbt_f1_transformations` in the compose file), and `DBT_PROJECT_DIR` and `DBT_PROFILES_DIR` point there. dbt finds `profiles.yml` in the project folder.
3. **The paths resolve in Linux.** The `container` target in `profiles.yml` and the `F1_*` environment variables in the compose file point the raw Parquet, the DuckDB file and the export folder at `/opt/airflow/data/...`, which is the mounted `airflow/data` folder. The `F:/...` paths from your laptop do not exist inside the container, which is why they are environment variables with defaults rather than constants.

> **Why this matters.** A wrong path or a missing install is still the most likely reason the dbt task fails on its first real run. That is an inference from the files, not an error recorded in a log.

#### **Task 2: The `run_dbt_build` Task (rewritten in the third edition)**

The task is part of the DAG you pasted in Chapter 3 (it is skipped unless you tick `run_dbt_build` in the trigger form). It is now a thin layer over two small, Airflow-free modules that you create in the next two steps, so the DAG file must be able to import **modules/dbt_scope.py**: create Steps 2 and 3 before you let the scheduler parse the Chapter 3 DAG. Here is the task on its own:

```python
    # ==========================================
    # OPTIONAL: RUN DBT AFTER EXTRACTION (scoped to what this run changed)
    # ==========================================
    # Runs after transcription too (radio transcript files feed fct_radio_messages). The trigger rule lets it run when
    # the mapped transcription tasks were SKIPPED (transcribe_radio=false); a failed transcription still blocks it.
    @task(trigger_rule=TriggerRule.NONE_FAILED)
    def run_dbt_build(load_result, selected_session_keys, **context):
        from modules.dbt_runner import DbtRunError, run_dbt_plan

        p = context["params"]
        if not p["run_dbt_build"]:
            raise AirflowSkipException("run_dbt_build is off for this run.")
        try:
            scope = build_dbt_scope(
                load_result["results"], selected_session_keys,
                dbt_scope=p["dbt_scope"], dbt_full_refresh=bool(p["dbt_full_refresh"]),
                chunk_size=int(p["dbt_chunk_size"]), allow_large_backfill=bool(p["allow_large_backfill"]),
                non_telemetry_changed=bool(p["transcribe_radio"]),   # new transcript files change fct_radio_messages
            )
            if scope["mode"] == "none":
                raise AirflowSkipException(f"dbt skipped: {scope['reason']}.")
            steps = plan_dbt_commands(
                scope, target=DBT_TARGET, profiles_dir=DBT_PROFILES_DIR,
                radio_transcripts=radio_transcripts_exist(RAW_DIR), chunk_size=int(p["dbt_chunk_size"]),
                dbt_executable=os.environ.get("DBT_EXECUTABLE", "dbt"),
            )
            summary = run_dbt_plan(steps, cwd=DBT_PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR, scope=scope)
        except ScopeError as exc:
            raise AirflowFailException(str(exc)) from exc
        except DbtRunError as exc:
            raise AirflowFailException(str(exc)) from exc
        return summary
```

**What to notice**

* **Opt-in and safe to skip:** `AirflowSkipException` marks the task *skipped* (pink), not failed, when the parameter is off or when the scope says there is nothing to do, so a normal extraction run never touches your warehouse.
* **The scope comes from the results, not from the file list:** `load_result["results"]` is the list of `{endpoint, session_key, path, status}` dictionaries that `verify_and_load` built (Chapter 3). Only `written` results count.
* **Errors become task failures:** `ScopeError` (an invalid scope or an unsafe request) and `DbtRunError` (a non-zero dbt exit) both raise `AirflowFailException`, so the task turns red with a clear message and the full dbt output above it.
* **`dbt build`, not `dbt run`:** the build runs the tests too, in dependency order, and **skips everything downstream of a failed test**. This is deliberate. It is also why the source-data tests are warnings on real data (Chapter 6): an error there would stop the whole pipeline.
* **The trigger rule and the extra dependency:** `radio_task >> dbt_task` makes dbt wait for transcription, and `TriggerRule.NONE_FAILED` lets it run when the mapped transcription tasks were skipped.
* **Separation:** the task still runs the same dbt command you could run by hand, and it **logs that exact command**, so you can reproduce a failure in a terminal.

**Step 2: The scope builder.** Create **airflow/dags/modules/dbt_scope.py**. It imports nothing from Airflow or dbt, so it is unit tested like the other helpers (`airflow/tests/test_dbt_scope.py`):

```python
"""Airflow-free helpers: turn extraction results into a dbt *scope*, and a scope into dbt commands.

Design rules (see docs/SESSION_SCOPED_BUILDS.md):
  1. The run tells dbt what changed. dbt never guesses: the scope is passed as the `session_keys` var.
  2. An unset scope behaves as before (manual runs, CI and dbt_scope=full).
  3. A scoped session is replaced, never appended (enforced by the dbt models, not here).
  4. No `table` model may read scoped data (enforced by dbt/ci/check_scoped_views.py).

Nothing here imports Airflow or touches dbt, so it is unit tested like the other helpers.
"""
import glob
import json
import os
import re

TELEMETRY_TAG = "telemetry_group"      # dbt tag on stg_car_data, stg_location and every model downstream of them
DEFAULT_CHUNK_SIZE = 10
SCOPE_MODES = ("none", "scoped", "full", "full_refresh")
DBT_SCOPE_CHOICES = ("auto", "full")

_DIGITS = re.compile(r"[0-9]+")


class ScopeError(ValueError):
    """The requested scope is invalid or unsafe. The DAG turns this into a failed task."""


# ---------------------------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------------------------
def validate_session_keys(keys) -> list:
    """Return sorted, de-duplicated positive ints. Anything else is rejected, never coerced:
    the keys end up inside SQL (`session_key IN (...)`), so '9158; DROP' or 9158.5 must not get through."""
    if keys is None or isinstance(keys, (str, bytes, dict)):
        raise ScopeError(f"session keys must be a list of integers, got {type(keys).__name__}")
    out = set()
    for k in keys:
        if isinstance(k, bool):
            raise ScopeError(f"invalid session key {k!r}")
        if isinstance(k, int):
            value = k
        elif isinstance(k, str) and _DIGITS.fullmatch(k.strip()):
            value = int(k.strip())
        else:
            raise ScopeError(f"invalid session key {k!r}: session keys are integers")
        if value <= 0:
            raise ScopeError(f"invalid session key {k!r}: must be positive")
        out.add(value)
    return sorted(out)


def validate_chunk_size(size) -> int:
    if isinstance(size, bool) or not isinstance(size, int) or size < 1:
        raise ScopeError(f"dbt_chunk_size must be an integer >= 1, got {size!r}")
    return size


def chunk_keys(keys, size) -> list:
    keys = list(keys)
    size = validate_chunk_size(size)
    return [keys[i:i + size] for i in range(0, len(keys), size)]


def flatten_results(*groups) -> list:
    """Flatten Airflow's mapped-task outputs into one list of extractor result dicts.

    The DAG passes lists of mapped-task outputs; Airflow resolves each mapped output to a LAZY SEQUENCE, which is not a list or
    tuple, so this recurses into anything iterable (except dicts and strings). None entries (skipped mapped instances) are dropped.
    Any other leaf is an error, never silently ignored: dropping results hides exactly the data the dbt scope is built from.
    """
    flat = []

    def walk(item):
        if item is None:
            return
        if isinstance(item, dict):
            flat.append(item)
        elif isinstance(item, (str, bytes)) or not hasattr(item, "__iter__"):
            raise TypeError(f"unexpected extractor result of type {type(item).__name__}: {item!r}; expected a result dict")
        else:
            for sub in item:
                walk(sub)

    for group in groups:
        walk(group)
    return flat


# ---------------------------------------------------------------------------------------------
# scope builder
# ---------------------------------------------------------------------------------------------
def build_dbt_scope(results, sessions=None, *, dbt_scope="auto", dbt_full_refresh=False,
                    chunk_size=DEFAULT_CHUNK_SIZE, allow_large_backfill=False, non_telemetry_changed=False) -> dict:
    """Decide what dbt should build.

    results   : list of extractor results {endpoint, session_key, path, status} (None / non-dicts are ignored)
    sessions  : session keys this run selected (optional). When given, only these can end up in the scope.
    returns   : {mode, keys, endpoints, chunks, reason}
        mode 'none'         nothing was written: skip dbt
        mode 'scoped'       `keys` = sessions with at least one file actually written; `chunks` = those keys in chunks
        mode 'full'         dbt_scope=full: whole graph, scope unset (telemetry loads only the sessions it lacks)
        mode 'full_refresh' deliberate rebuild of everything; refused unless allow_large_backfill is set
    non_telemetry_changed: something outside the extractor results changed (e.g. radio transcripts were produced). When
    nothing was written but this is true, the non-telemetry models still rebuild (scoped mode with no session keys).
    """
    if dbt_scope not in DBT_SCOPE_CHOICES:
        raise ScopeError(f"dbt_scope must be one of {DBT_SCOPE_CHOICES}, got {dbt_scope!r}")
    chunk_size = validate_chunk_size(chunk_size)

    written = [r for r in (results or []) if isinstance(r, dict) and r.get("status") == "written"]
    endpoints = sorted({r["endpoint"] for r in written if r.get("endpoint")})

    if dbt_full_refresh:
        if not allow_large_backfill:
            raise ScopeError("dbt_full_refresh rebuilds every model, including the 200M+ row telemetry tables. "
                             "Tick allow_large_backfill to run it deliberately.")
        return {"mode": "full_refresh", "keys": [], "endpoints": endpoints, "chunks": [],
                "reason": "dbt_full_refresh=true"}

    if dbt_scope == "full":
        return {"mode": "full", "keys": [], "endpoints": endpoints, "chunks": [],
                "reason": "dbt_scope=full: whole graph, telemetry loads only missing sessions"}

    if not written and non_telemetry_changed:
        return {"mode": "scoped", "keys": [], "endpoints": [], "chunks": [],
                "reason": "no extractor file was written, but other inputs changed (radio transcripts)"}

    if not written:
        return {"mode": "none", "keys": [], "endpoints": [], "chunks": [],
                "reason": "no file was written in this run, so there is nothing to transform"}

    wanted = None if sessions is None else set(validate_session_keys(sessions))
    keys = set()
    for r in written:
        sk = r.get("session_key")
        if sk == "global" or sk is None:
            continue                      # global files (meetings, drivers, sessions) are not session scoped
        sk = validate_session_keys([sk])[0]
        if wanted is None or sk in wanted:
            keys.add(sk)
    keys = sorted(keys)
    return {"mode": "scoped", "keys": keys, "endpoints": endpoints, "chunks": chunk_keys(keys, chunk_size),
            "reason": f"{len(keys)} session(s) with written files"
                      if keys else "only global files were written; no session telemetry to load"}


# ---------------------------------------------------------------------------------------------
# dbt command planning
# ---------------------------------------------------------------------------------------------
def radio_transcripts_exist(raw_dir: str) -> bool:
    """enable_radio_transcripts must be true only when the files exist: a missing glob is a hard error in DuckDB."""
    return bool(glob.glob(os.path.join(raw_dir, "radio_transcripts_*_extracted.parquet")))


def _vars_arg(variables: dict) -> list:
    # JSON is valid YAML, which is what dbt parses --vars as. Compact separators: no shell, so no quoting needed.
    return ["--vars", json.dumps(variables, separators=(",", ":"), sort_keys=True)] if variables else []


def plan_dbt_commands(scope: dict, *, target: str, profiles_dir: str, radio_transcripts: bool = False,
                      chunk_size: int = DEFAULT_CHUNK_SIZE, dbt_executable: str = "dbt") -> list:
    """Return the ordered dbt invocations as [{label, argv, session_keys}]. argv is a list: it is never joined into a shell string.

    scoped, one chunk of sessions : ONE whole-graph build with session_keys set. Telemetry group replaces those
                                    sessions; every other model does its normal cheap full rebuild.
    scoped, many chunks           : first the non-telemetry models once (the telemetry group reads fct_fastest_laps,
                                    int_lap_context and stg_laps, so they must be fresh first), then the telemetry
                                    group once per chunk of sessions.
    scoped, no session keys       : non-telemetry models only (only global files changed).
    """
    mode = scope.get("mode")
    if mode not in SCOPE_MODES:
        raise ScopeError(f"unknown scope mode {mode!r}")
    if mode == "none":
        return []

    base = [dbt_executable, "build", "--target", target, "--profiles-dir", profiles_dir]
    common = {"enable_radio_transcripts": True} if radio_transcripts else {}
    tag = f"tag:{TELEMETRY_TAG}"

    def step(label, extra, variables=None, keys=None):
        return {"label": label, "argv": base + extra + _vars_arg({**common, **(variables or {})}),
                "session_keys": keys or []}

    if mode == "full":
        return [step("whole graph (scope unset)", [])]
    if mode == "full_refresh":
        return [step("FULL REFRESH of the whole graph", ["--full-refresh"])]

    keys = validate_session_keys(scope.get("keys", []))
    if not keys:
        return [step("non-telemetry models (no session scope)", ["--exclude", tag])]
    chunks = chunk_keys(keys, validate_chunk_size(chunk_size))
    if len(chunks) == 1:
        return [step(f"whole graph, scoped to {len(keys)} session(s)", [], {"session_keys": keys}, keys)]
    steps = [step("non-telemetry models", ["--exclude", tag])]
    for i, chunk in enumerate(chunks, 1):
        steps.append(step(f"telemetry group, chunk {i}/{len(chunks)} ({len(chunk)} sessions)",
                          ["--select", tag], {"session_keys": chunk}, chunk))
    return steps
```

**Reading it**

* **`validate_session_keys`:** accepts only positive integers (or digit strings) and returns them sorted and de-duplicated. The keys end up inside SQL (`session_key IN (...)`), so `"9158; DROP"`, `9158.5`, `true` or `0` are **rejected, never coerced**. The dbt macro validates again (Chapter 7); two checks at two boundaries.
* **`flatten_results`:** turns the mapped-task outputs into one list of result dictionaries. It recurses into any iterable because Airflow hands over lazy sequences (see the Chapter 3 pitfall), drops `None`, and raises on anything else.
* **`build_dbt_scope`:** the decision. `written` results give the scope; `skipped` and `empty` ones do not. Global files (`session_key` is `global`) are not sessions. The table below is the whole behaviour.
* **`plan_dbt_commands`:** turns a scope into the ordered dbt commands. It sets `enable_radio_transcripts` only if transcript files exist (`radio_transcripts_exist`), because a missing glob is a hard error in DuckDB.

| Run | What dbt does |
|---|---|
| `latest`, one key, or no more sessions than `dbt_chunk_size` | **one** whole-graph build with `session_keys` set: the telemetry group replaces those sessions, every other model does its normal cheap full rebuild |
| more sessions than one chunk | the non-telemetry models **once** (`--exclude tag:telemetry_group`), then the telemetry group once per chunk (`--select tag:telemetry_group --vars {session_keys: [...]}`) |
| `skip_existing` | the scope is only the sessions with files actually **written** |
| only global files written | the non-telemetry models only |
| nothing written, `transcribe_radio` off | dbt is skipped |
| `dbt_scope=full` | the whole graph with **no scope** (the telemetry tables load only sessions they lack); ignores the extraction results |
| `dbt_full_refresh=true` | `dbt build --full-refresh` of everything; refused unless `allow_large_backfill` is also ticked |

> **Why the non-telemetry models run first in a multi-chunk run.** The corner and delta-trace models read `fct_fastest_laps`, `int_lap_context` and `stg_laps`. If those were rebuilt *after* the telemetry chunks, the chunks would be built from stale laps. Nothing outside the telemetry group depends on it, so the order costs nothing.

**Step 3: The runner.** Create **airflow/dags/modules/dbt_runner.py**:

```python
"""Runs the dbt commands planned by modules.dbt_scope. Airflow-free, so it can be tested with a fake `runner`.

Safety properties:
  * every command is an argument list handed to subprocess.run, never a shell string (shell=False)
  * the exact command and scope are logged before each step runs
  * a failing step stops the run and raises DbtRunError with the tail of dbt's output
"""
import logging
import os
import shlex
import subprocess
import time


class DbtRunError(RuntimeError):
    """A dbt invocation exited non-zero. The DAG turns this into a failed task."""


def run_dbt_plan(steps, *, cwd: str, profiles_dir: str, scope: dict = None, runner=subprocess.run, log=logging) -> dict:
    """Run each planned step in order; return a summary dict (also safe to return from an Airflow task)."""
    env = {**os.environ, "DBT_PROFILES_DIR": profiles_dir}
    scope = scope or {}
    log.info("dbt scope: mode=%s keys=%s endpoints=%s (%s)", scope.get("mode"), scope.get("keys"),
             scope.get("endpoints"), scope.get("reason"))
    summary = {"mode": scope.get("mode"), "session_keys": scope.get("keys", []), "steps": []}
    for i, step in enumerate(steps, 1):
        argv = step["argv"]
        log.info("dbt step %d/%d [%s]: %s (cwd=%s)", i, len(steps), step["label"], shlex.join(argv), cwd)
        started = time.monotonic()
        result = runner(argv, capture_output=True, text=True, cwd=cwd, env=env, shell=False)
        seconds = round(time.monotonic() - started, 1)
        log.info(result.stdout or "")      # the whole dbt output: per-model timings are needed to see what is slow
        summary["steps"].append({"label": step["label"], "command": shlex.join(argv),
                                 "returncode": result.returncode, "seconds": seconds})
        if result.returncode != 0:
            log.error((result.stderr or "")[-3000:])
            raise DbtRunError(f"dbt step {i}/{len(steps)} [{step['label']}] failed with exit code {result.returncode}. "
                              f"Command: {shlex.join(argv)}. See the task log above.")
    return summary
```

* **No shell:** every step is an argument list passed with `shell=False`.
* **Logs the scope and the exact command** before each step, then dbt's **entire** output (the first version kept only the last 6,000 characters, which cut the log mid-line and hid the per-model timings).
* **Stops at the first failing step** and raises `DbtRunError` naming the command. It returns a summary (mode, session keys, each step's command, exit code and seconds) that Airflow stores as the task's return value.

**The three parameters.** In the trigger form: `dbt_scope` (`auto`, the default, or `full`), `dbt_full_refresh` (off) and `dbt_chunk_size` (default 10, minimum 1). They only matter when `run_dbt_build` is on.

**What the log should show.** For a `latest` run that wrote session 11420 the task log contains a line like `dbt scope: mode=scoped keys=[11420] endpoints=[...] (1 session(s) with written files)`, then `dbt step 1/1 [whole graph, scoped to 1 session(s)]: dbt build --target container --profiles-dir ... --vars '{"enable_radio_transcripts":true,"session_keys":[11420]}'`, then dbt's output, then `Marking task as SUCCESS`. If you see `keys=[]` after an extraction that wrote files, see the Chapter 3 pitfall.

#### **Task 3: Where Cosmos Fits (Optional)**

`airflow/dags/dbt_cosmos_dag.py` now exists. It uses **Astronomer Cosmos**, a library that renders each dbt model as its own Airflow task, so a failed model or test shows up in the Airflow graph instead of as one opaque "dbt build". It is **[CODE FINAL, RUN NOT CONFIRMED]**: it needs `astronomer-cosmos` in the image (add it to `AIRFLOW_EXTRA_PIP`), and it excludes the heavy incremental telemetry models so they are not rebuilt on every run. It does **not** pass the session scope (passing it through Cosmos is a deferred item), so it only ever behaves like an unscoped run: the incremental models there load only the sessions they lack. If Cosmos is not installed the file defines nothing and Airflow parses it cleanly, so it is safe to leave in place. Treat it as the next learning step once the plain `run_dbt_build` task works.

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

* **The Problem:** the dbt task is skipped every time.
* **Why it Happens:** the opt-in parameter defaults to off, and the task also skips when the scope says nothing changed: no file was written (for example `skip_existing` found every file already on disk) and `transcribe_radio` is off.
* **The Solution:** tick **run_dbt_build** in *Trigger DAG w/ config*, and check the `verify_and_load` log line (`N Parquet files ready (X written, Y skipped, Z empty)`): `X` must be above zero. To rebuild without extracting, use `dbt_scope=full`.

* **The Problem (third edition):** the task fails at once with `dbt_full_refresh rebuilds every model ... Tick allow_large_backfill`.
* **Why it Happens:** a full refresh rebuilds the 200M-row telemetry tables, so it is behind the same guard as a large backfill.
* **The Solution:** tick `allow_large_backfill` only if you mean it, and leave `dbt_scope` at `auto`; the refresh never sets a session scope.

* **The Problem (third edition):** a scoped run takes about 20 minutes inside Docker although only one session changed.
* **Why it Happens:** the telemetry steps are a few minutes of that; the rest is the non-telemetry build, and every staging view has to open the Parquet files through the Docker bind mount on Windows. On the author's machine the same build took about 20 minutes in the container against about 4.5 on the host, roughly four times slower: staging views took 8 to 18 seconds each, and the heaviest model, `int_lap_context`, between 2 and 4 minutes. It is file access, not model logic, and every scoped run also re-scans all raw car data and location files, because the session filter cannot skip files.
* **The Solution:** read the per-model timings now present in the task log (or `target/run_results.json`) before optimising. Moving the raw files and warehouse onto a Docker volume is the cheapest next step; converting the heaviest non-telemetry models to incremental is the next one.

* **The Problem (third edition, Windows):** `docker compose exec ... grep ... /opt/airflow/dags/modules/dbt_scope.py` fails with `C:/Program Files/Git/opt/airflow/...: No such file or directory`.
* **Why it Happens:** Git Bash rewrites any argument that starts with `/` into a Windows path before Docker sees it.
* **The Solution:** prefix the command with `MSYS_NO_PATHCONV=1` (or start the path with `//`). This is the quickest way to prove the container sees your edited DAG module.

---

## **Chapter 9: Serving the Data: SQL Tools, Power BI, Dashboards and the Semantic Layer**

### **Chapter Overview**

A warehouse nobody can query is a hobby. This chapter connects tools to the DuckDB file: a terminal UI (**Harlequin**), a GUI (**DBeaver**), and **Power BI** over **ODBC**. It also contains the best data-quality incident of the project. **[VERIFIED]** (all 16 mart tables loaded into Power BI).

**Concepts introduced in this chapter**

* **ODBC:** a standard driver interface that lets BI tools talk to databases.
* **Single-writer rule:** DuckDB lets one process **write** at a time. Open the file read-only for analysis whenever you can.
* **Schema drift:** the shape or type of data changing between files.
* **Materialising a view as a table:** storing the result so BI tools read ready values instead of recomputing a heavy query.
* **Semantic layer:** one governed definition of each business metric ("average representative lap time"), so every tool that asks for it gets the same number.
* **Headless test:** running an app's pages in a test with no browser, to check they render without errors.

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
2. In Power BI: **Get Data > ODBC**, choose the DSN, and select the mart tables you want. The first-edition build had 16 (2 dimensions and 14 facts); the current project has 34 mart models (4 in `core/`, 30 in `analytics/`).
3. In the relationship view, link facts to dimensions on `session_key` and `driver_number` (the star schema from Chapter 4).

For laptops with limited RAM, reduce parallel loading in Power BI's Data Load options (for example **parallel loading = 1**) and consider setting a DuckDB `temp_directory` (Chapter 5) so large queries can spill.

#### **Task 4: The `+2 LAPS` Incident (Schema Drift in Practice)**

This is the full story, in order, because every step teaches something.

1. **Symptom.** Power BI refresh fails loading interval data.
2. **First diagnosis.** Interval gaps are numbers like `1.2` but lapped cars have text like `+2 LAPS`. Comparing text to a number fails.
3. **First fix (necessary, not sufficient).** Staging keeps `interval` and `gap_to_leader` as **text**; the mart converts with `TRY_CAST`, keeps the text in `lapped_status`, and the model is materialised as a **table**.
4. **The error came back**, now at model creation: `Conversion Error: Could not convert string '+2 LAPS' to DOUBLE`.
5. **Real root cause.** The problem was never in the models. `read_parquet` on a glob infers each column's type from the **first file**. If that file stores gaps as numbers (DOUBLE) and a later file has strings, the later file cannot be read. The Airflow sanitiser fixes mixing **inside** one file but not **between** files.
6. **Real fix.** `union_by_name=true` on the source (Chapter 5), which reconciles types across files. In the second edition **every** source uses it, not only the three that had already caused trouble.
7. **Verification.** `dbt run --select stg_intervals+` then refresh Power BI.

**What it teaches**

* A fix at the wrong layer can look right and fail later. Read the error's *location*.
* Prevent the class of error where it begins: **schema contracts at the source**.
* Keep raw text columns raw, convert in the mart with `TRY_CAST`, and keep the original text for analysis.

#### **Task 5: Dashboards with Streamlit (new in the second edition)**

Power BI is the right tool for a polished report. For *exploring* race data, an engineer wants something that runs next to the warehouse, in Python, with no licence and no ODBC driver. **Streamlit** fits: a page is a Python function, and charts are **Plotly** figures. The app lives in **dashboards/**.

```bash
pip install -r dashboards/requirements.txt
F1_DUCKDB_PATH=/path/to/f1_local_warehouse.duckdb streamlit run dashboards/app.py
```

Navigation is organised by **role** and **race phase**. You pick a season, a Grand Prix, a session and a focus and comparison driver once, in the sidebar, and every page reads that shared context.

| Role | Pre-race | During race (replay) | Post-race |
|---|---|---|---|
| Race Engineer | baseline telemetry, circuit map, tyres, weather, setup gaps | per-lap telemetry summary, rule-based alerts, radio | telemetry debrief, corners, stints, incidents |
| Race Analyst | grid, standings, circuit history, competitor pace | timing tower, position and gap evolution, pit stops, overtakes | classification, pace, strategy, overtakes, championship |
| Race Data Scientist | finishing-position base rate, hazard, degradation models, simulator | stint projection, anomaly screen, hazard | prediction error, residuals, counterfactual simulator |

A tenth page, **Data coverage**, shows what the warehouse holds and flags data-health problems.

**The rules the pages follow**

* **The warehouse is opened read-only.** DuckDB allows one writer: if dbt is running you get a clear message, not a hang.
* **Historical data is never shown as live.** The "During race" pages are replays driven by a lap slider, and say so.
* **Pre-race pages use only information from before the race.** The strategy simulator is calibrated from an earlier session and carries a leakage warning, because circuit pit loss and safety-car hazard in the marts include every race in the warehouse.
* **Every panel is tagged** measured, derived, estimated, predicted, simulated, historical or manual, and data the warehouse does not hold is shown as "not available".

**The design, in three layers**

1. **`data.py` only talks to the database.** One function per question, each returning a DataFrame. The connection is opened **read-only**, and a lock held by dbt becomes a clear message instead of a hang:

```python
def connect(path: str = None):
    """Open the warehouse READ-ONLY. DuckDB allows one writer: if dbt is running you get a clear error, not a hang."""
    path = path or DEFAULT_DB
    if not os.path.exists(path):
        raise FileNotFoundError(f"Warehouse not found at {path}. Set F1_DUCKDB_PATH or build it with dbt first.")
    try:
        return duckdb.connect(path, read_only=True)
    except duckdb.IOException as exc:
        raise RuntimeError("The warehouse is locked by another process (probably a dbt run). Retry when it finishes.") from exc
```

`q(con, sql)` runs a query and returns a DataFrame with floats rounded to six decimals, which removes display noise such as `1e-13`.

2. **`charts/` only draws.** Each function takes a DataFrame and returns a Plotly figure, with no Streamlit and no database, so it can be tested on its own. The package has one module per theme (`circuit`, `performance`, `predictions`, `strategy`, `telemetry`). Here is one, from `charts/strategy.py`:

```python
def stint_gantt_fig(df: pd.DataFrame, pits: pd.DataFrame = None):
    if df.empty:
        return empty("No stint data")
    d = df.assign(length=df["last_lap"] - df["first_lap"] + 1)
    fig = go.Figure()
    for comp, g in d.groupby("tyre_compound"):
        fig.add_bar(y=g["driver"], x=g["length"], base=g["first_lap"] - 1, orientation="h", name=str(comp),
                    marker_color=COMPOUND_COLOURS.get(comp, "#888888"),
                    hovertext=[f"stint {s}: laps {a}-{b}" for s, a, b in zip(g["stint_number"], g["first_lap"], g["last_lap"])])
    if pits is not None and not pits.empty:
        fig.add_scatter(x=pits["lap_number"], y=pits["driver"], mode="markers", name="pit stop", marker={"symbol": "line-ns", "size": 18, "color": "#111111", "line": {"width": 3, "color": "#111111"}})
    fig.update_layout(barmode="overlay", xaxis_title="Lap", yaxis={"autorange": "reversed"})
    return fig
```

3. **`workspaces/` and `app.py` only wire them together.** Each page is a `render(con)` function in `workspaces/<role>/<phase>.py`, built from reusable pieces in `components/` (KPIs, timing tower, simulator, filters). `app.py` gives every Streamlit run a **fresh DuckDB cursor**, because one shared connection raised "connection is busy" when reruns overlapped, and registers the pages with `st.navigation`:

```python
def run_con():
    """A fresh cursor per script run: one shared DuckDB connection raises 'connection is busy' when reruns overlap."""
    return con.cursor()


def page(module, title, path, **kw):
    return st.Page(lambda: module.render(run_con()), title=title, url_path=path, **kw)
```

A page that cannot show something says why instead of drawing an empty chart. The helpers live in `workspaces/_common.py`:

```python
def need_context(con):
    plot.reset()
    ctx = state.current(con)
    if ctx is None:
        st.info("No sessions in the warehouse yet. Run the pipeline and `dbt build` first.")
    return ctx
```

**How it is tested.** `dashboards/tests/` (104 test cases in two files) runs every workspace page headlessly with Streamlit's `AppTest` against the warehouse built from the synthetic data. Each page must render without an exception; the pages must survive a session with no laps, no telemetry, or a driver who did not start; replay pages must follow the lap slider; pre-race pages must not see the race itself; and the data layer is checked against known fixture answers (season-aware points, sprint typing, served penalties, one row per incident, and so on). A missing warehouse produces a message, not a stack trace.

> **What the tests do not cover.** A test can prove a page renders without raising an error; it cannot tell you whether the layout is readable or the colours are sensible. `docs/CHANGES.md` records fixes made after looking at the pages on real data (for example, Plotly silently switches to WebGL above about 1,000 rows, where lines can fail to draw, so the charts are forced to SVG). Open the app and judge it yourself.

#### **Task 6: A Semantic Layer with MetricFlow (new in the second edition)**

A dashboard, a notebook and a Power BI report can each compute "average lap time" slightly differently (all laps? clean laps only? fuel-corrected?). A **semantic layer** defines each metric **once**, in YAML, and every tool asks for it by name. **MetricFlow** is dbt's implementation, and it runs against DuckDB.

**Step 1: A calendar.** MetricFlow needs a daily calendar to group by time:

```sql
{{ config(materialized='table') }}
{# Daily calendar required by MetricFlow for time-based metrics. #}
SELECT CAST(date_day AS DATE) AS date_day
FROM ({{ dbt.date_spine('day', "CAST('2023-01-01' AS DATE)", "CAST('2032-01-01' AS DATE)") }}) AS spine
```

**Step 2: Describe the tables.** A *semantic model* says which columns are **entities** (the keys that let tables join), **dimensions** (what you group by) and **measures** (what you add up or average). Seven tables are described (including `driver_sessions`, the team a driver drove for *at the time of each session*, next to `drivers`, which holds only the latest team), and they share the `session` and `driver` entities, which is what lets you group a lap metric by a driver's name:

```yaml
version: 2

# Governed business definitions. Query with MetricFlow, e.g.:
#   mf query --metrics avg_representative_lap_time_seconds --group-by driver__full_name,lap__tyre_compound
# Joins use shared entities: every fact carries `session` and `driver` foreign entities that resolve to the dimension models.

semantic_models:
  - name: sessions
    description: "One row per session (practice, qualifying, sprint, race) with its meeting and circuit."
    model: ref('dim_sessions')
    defaults: {agg_time_dimension: session_start_time}
    entities:
      - {name: session, type: primary, expr: session_key}
    dimensions:
      - {name: session_start_time, type: time, type_params: {time_granularity: day}}
      - {name: session_type, type: categorical}
      - {name: session_name, type: categorical}
      - {name: season, type: categorical, expr: year}
      - {name: location, type: categorical}
      - {name: circuit_short_name, type: categorical}
      - {name: country_code, type: categorical}
    measures:
      - {name: session_count, expr: "1", agg: sum}

  - name: drivers
    description: "Latest known identity and team per driver number. For historical analysis use `driver_sessions` (team at the time of the session)."
    model: ref('dim_drivers')
    entities:
      - {name: driver, type: primary, expr: driver_number}
    dimensions:
      - {name: full_name, type: categorical}
      - {name: driver_acronym, type: categorical}
      - {name: team_name, type: categorical}

  - name: driver_sessions
    description: "Driver identity and team AS OF each session (keeps mid-season team changes). Prefer this over `drivers` for any historical analysis."
    model: ref('dim_driver_session')
    entities:
      - {name: driver_session, type: primary, expr: "CONCAT(CAST(session_key AS VARCHAR), '-', CAST(driver_number AS VARCHAR))"}
      - {name: session, type: foreign, expr: session_key}
      - {name: driver, type: foreign, expr: driver_number}
    dimensions:
      - {name: session_driver_name, type: categorical, expr: full_name}
      - {name: session_driver_acronym, type: categorical, expr: driver_acronym}
      - {name: session_team_name, type: categorical, expr: team_name}

  - name: laps
    description: "One row per driver per lap with tyre, track status and pace flags."
    model: ref('fct_lap_performance')
    defaults: {agg_time_dimension: lap_start_ts}
    entities:
      - {name: lap, type: primary, expr: "CONCAT(CAST(session_key AS VARCHAR), '-', CAST(driver_number AS VARCHAR), '-', CAST(lap_number AS VARCHAR))"}
      - {name: session, type: foreign, expr: session_key}
      - {name: driver, type: foreign, expr: driver_number}
    dimensions:
      - {name: lap_start_ts, type: time, type_params: {time_granularity: day}}
      - {name: tyre_compound, type: categorical}
      - {name: track_status, type: categorical}
      - {name: is_representative_lap, type: categorical}
      - {name: stint_number, type: categorical}
    measures:
      - {name: lap_count, expr: "1", agg: sum}
      - {name: lap_time_sum, expr: lap_time_seconds, agg: sum}
      - {name: lap_time_avg, expr: lap_time_seconds, agg: average}
      - {name: lap_time_best, expr: lap_time_seconds, agg: min}
      - {name: fuel_corrected_lap_time_avg, expr: fuel_corrected_lap_time_seconds, agg: average}
      - {name: speed_trap_max, expr: speed_trap_finish, agg: max}

  - name: pit_stops
    description: "One row per pit stop."
    model: ref('fct_pit_stops')
    defaults: {agg_time_dimension: pit_timestamp}
    entities:
      - {name: pit_stop, type: primary, expr: "CONCAT(CAST(session_key AS VARCHAR), '-', CAST(driver_number AS VARCHAR), '-', CAST(stop_number AS VARCHAR))"}
      - {name: session, type: foreign, expr: session_key}
      - {name: driver, type: foreign, expr: driver_number}
    dimensions:
      - {name: pit_timestamp, type: time, type_params: {time_granularity: day}}
      - {name: compound_after, type: categorical}
      - {name: is_under_neutralisation, type: categorical}
    measures:
      - {name: stop_count, expr: "1", agg: sum}
      - {name: pit_lane_time_avg, expr: pit_lane_time_seconds, agg: average}
      - {name: time_lost_avg, expr: time_lost_seconds, agg: average}
      - {name: slow_stops, expr: "CASE WHEN is_slow_stop THEN 1 ELSE 0 END", agg: sum}

  - name: race_results
    description: "One row per driver per classified session."
    model: ref('fct_race_results')
    defaults: {agg_time_dimension: session_start_time}
    entities:
      - {name: result, type: primary, expr: "CONCAT(CAST(session_key AS VARCHAR), '-', CAST(driver_number AS VARCHAR))"}
      - {name: session, type: foreign, expr: session_key}
      - {name: driver, type: foreign, expr: driver_number}
    dimensions:
      - {name: session_start_time, type: time, type_params: {time_granularity: day}}
      - {name: result_session_type, type: categorical, expr: session_type}
      - {name: classification_status, type: categorical}
    measures:
      - {name: points, expr: total_points, agg: sum}
      - {name: wins, expr: "CASE WHEN is_winner THEN 1 ELSE 0 END", agg: sum}
      - {name: podiums, expr: "CASE WHEN is_podium THEN 1 ELSE 0 END", agg: sum}
      - {name: dnfs, expr: "CASE WHEN classification_status = 'DNF' THEN 1 ELSE 0 END", agg: sum}
      - {name: starts, expr: "1", agg: sum}
      - {name: places_gained, expr: positions_gained_or_lost, agg: sum}

  - name: overtakes
    description: "One row per overtake event."
    model: ref('fct_overtakes')
    defaults: {agg_time_dimension: overtake_timestamp}
    entities:
      - {name: overtake, type: primary, expr: "CONCAT(CAST(session_key AS VARCHAR), '-', CAST(overtaking_driver_number AS VARCHAR), '-', CAST(overtaken_driver_number AS VARCHAR), '-', CAST(overtake_timestamp AS VARCHAR))"}
      - {name: session, type: foreign, expr: session_key}
      - {name: driver, type: foreign, expr: overtaking_driver_number}
    dimensions:
      - {name: overtake_timestamp, type: time, type_params: {time_granularity: day}}
      - {name: is_probable_pit_cycle_pass, type: categorical}
      - {name: overtake_track_status, type: categorical, expr: track_status}
    measures:
      - {name: overtake_events, expr: "1", agg: sum}
      - {name: on_track_overtakes, expr: "CASE WHEN NOT is_probable_pit_cycle_pass THEN 1 ELSE 0 END", agg: sum}
      - {name: drs_assisted_overtakes, expr: "CASE WHEN was_in_drs_range_before_pass THEN 1 ELSE 0 END", agg: sum}
```

**Step 3: Define the metrics.** A metric is a measure plus an optional filter, or a ratio of two metrics. Note that `avg_lap_time_seconds` and `avg_representative_lap_time_seconds` are *different, named, described metrics*: the difference is now a decision recorded in one file.

```yaml
version: 2

metrics:
  # ---- pace ----
  - name: laps_driven
    label: Laps driven
    type: simple
    type_params: {measure: lap_count}
  - name: avg_lap_time_seconds
    label: Average lap time (all laps)
    description: "Includes in/out/safety-car laps. For pace comparisons use avg_representative_lap_time_seconds."
    type: simple
    type_params: {measure: lap_time_avg}
  - name: avg_representative_lap_time_seconds
    label: Average representative lap time
    description: "Clean laps only (no first/in/out/yellow/SC/VSC/red, within the representative-lap threshold)."
    type: simple
    type_params: {measure: lap_time_avg}
    filter: "{{ Dimension('lap__is_representative_lap') }} = true"
  - name: avg_fuel_corrected_representative_lap_time_seconds
    label: Average fuel-corrected representative lap time
    description: "Fuel correction rests on the var fuel_effect_seconds_per_lap assumption."
    type: simple
    type_params: {measure: fuel_corrected_lap_time_avg}
    filter: "{{ Dimension('lap__is_representative_lap') }} = true"
  - name: best_lap_time_seconds
    label: Best lap time
    type: simple
    type_params: {measure: lap_time_best}
  - name: top_speed_trap_kph
    label: Top speed-trap reading (km/h)
    type: simple
    type_params: {measure: speed_trap_max}
  # ---- pit stops ----
  - name: pit_stops
    label: Pit stops
    type: simple
    type_params: {measure: stop_count}
  - name: avg_pit_lane_time_seconds
    label: Average pit lane time (s)
    description: "Pit entry to exit as reported by OpenF1; NOT stationary time."
    type: simple
    type_params: {measure: pit_lane_time_avg}
  - name: avg_green_flag_time_lost_seconds
    label: Average time lost per green-flag stop (s)
    type: simple
    type_params: {measure: time_lost_avg}
    filter: "{{ Dimension('pit_stop__is_under_neutralisation') }} = false"
  - name: slow_stop_rate
    label: Share of stops flagged slow
    type: ratio
    type_params: {numerator: slow_stops_total, denominator: pit_stops}
  - name: slow_stops_total
    label: Slow stops
    type: simple
    type_params: {measure: slow_stops}
  # ---- results ----
  - name: championship_points
    label: Points scored
    type: simple
    type_params: {measure: points}
  - name: race_wins
    label: Wins
    type: simple
    type_params: {measure: wins}
  - name: podium_finishes
    label: Podiums
    type: simple
    type_params: {measure: podiums}
  - name: retirements
    label: DNFs
    type: simple
    type_params: {measure: dnfs}
  - name: race_starts
    label: Classified entries
    type: simple
    type_params: {measure: starts}
  - name: points_per_start
    label: Points per start
    type: ratio
    type_params: {numerator: championship_points, denominator: race_starts}
  - name: net_places_gained
    label: Net places gained vs grid
    type: simple
    type_params: {measure: places_gained}
  # ---- overtaking ----
  - name: overtakes
    label: Overtakes (all)
    type: simple
    type_params: {measure: overtake_events}
  - name: on_track_overtakes
    label: On-track overtakes (excl. probable pit-cycle)
    type: simple
    type_params: {measure: on_track_overtakes}
  - name: drs_assisted_overtake_share
    label: Share of overtakes made from DRS range
    type: ratio
    type_params: {numerator: drs_assisted_total, denominator: overtakes}
  - name: drs_assisted_total
    label: DRS-assisted overtakes
    type: simple
    type_params: {measure: drs_assisted_overtakes}
```

**Step 4: Validate and query.**

```bash
pip install dbt-metricflow
cd dbt_f1_transformations
dbt build --select metricflow_time_spine
mf validate-configs
mf query --metrics avg_representative_lap_time_seconds --group-by driver__full_name
mf query --metrics championship_points,race_wins --group-by driver__full_name
```

`make semantic-validate` runs the validation and then `ci/semantic_check.py`, which executes seven metrics through MetricFlow and compares **each result with an independent, hand-written SQL query**. Validation proves the YAML is well-formed; the cross-check proves the metrics mean what their names say, including the two **ratio** metrics and a metric grouped through the `session` entity.

> **MetricFlow and the single writer.** `mf query` opens the DuckDB file for writing, so it cannot run while Harlequin, DBeaver or another process holds it. The cross-check script therefore runs all its MetricFlow queries *first* and opens the file for its own SQL afterwards.

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

* **The Problem (second edition, hit on the author's machine):** `make dashboard` crashes with a long traceback ending in a `CatalogException` about a missing table (for example `dim_driver_session` or `fct_radio_messages`).
* **Why it Happens:** the warehouse had not been fully built, most likely because `dbt build` had stopped at the DuckDB 1.5 error described in Chapter 5, so the tables the page needs never existed. The dashboard was working correctly and the warehouse was incomplete.
* **The Solution:** fix the build first. The pages now catch a missing table and say which one and what to run (`dbt build`, and `--vars '{enable_radio_transcripts: true}'` for the radio page) instead of showing a traceback.

* **The Problem (second edition, found by the tests):** Streamlit warns that `use_container_width` will be removed after 2025-12-31.
* **Why it Happens:** Streamlit replaced that argument with `width="stretch"`. The date has passed, so code still using the old argument is on borrowed time.
* **The Solution:** use `width="stretch"` everywhere and require Streamlit 1.50 or newer (`dashboards/requirements.txt`).

* **The Problem (second edition, found by validation):** MetricFlow warns `Invalid name year - names cannot match reserved time granularity keywords`.
* **Why it Happens:** `year`, `month`, `day` and the like are time-granularity keywords, and MetricFlow reserves them as names. A dimension called `year` clashes.
* **The Solution:** name it differently and point at the column with `expr`: `{name: season, type: categorical, expr: year}`.

* **The Problem (second edition):** `mf query` fails with `Could not set lock on file ...duckdb: Conflicting lock is held`.
* **Why it Happens:** DuckDB allows one writer, and another tool (or your own script) has the file open.
* **The Solution:** close the other connection, or order your own scripts so every `mf` call finishes before you open the file.

---


## **Chapter 10: Infrastructure as Code (OpenTofu and GCP)**

### **Chapter Overview**

Clicking around a cloud console builds infrastructure nobody can reproduce. **Infrastructure as Code (IaC)** describes servers and storage in text files you can version, review and rebuild. In this chapter you write the OpenTofu configuration for the project's **cloud data lake** on Google Cloud Storage.

**Status.** `tofu init` and `tofu plan` are **[VERIFIED]** in the build logs. The logs end before `tofu apply` is confirmed. The second edition adds BigQuery datasets to the configuration (**[CODE FINAL, RUN NOT CONFIRMED]**: no `tofu` binary was available to validate it) and, in Tasks 4 to 7, a **BigQuery port of the whole dbt project**, **[VERIFIED ON SYNTHETIC DATA]** on a BigQuery *emulator* (every table identical to its DuckDB twin) but never run on real BigQuery. Cloud Composer is still **[SCAFFOLDED]**.

**Concepts introduced in this chapter**

* **OpenTofu:** the open-source fork of Terraform. It reads `.tf` files written in **HCL** and makes the cloud match them. It uses the same provider registry, which is why the source below still says `hashicorp/google`.
* **Provider:** the plugin that talks to one platform (Google Cloud here).
* **Resource:** one thing to create (a bucket).
* **Variable and `terraform.tfvars`:** inputs to the configuration, and the file that supplies them.
* **Plan versus apply:** `plan` previews changes and changes nothing; `apply` makes them.
* **State:** a record of what OpenTofu has created. Never commit it.
* **Dialect:** the flavour of SQL an engine speaks. DuckDB and BigQuery both speak "SQL" but differ in many small ways; making a project run on both is called making it **portable**.
* **Emulator:** a local program that imitates a cloud service closely enough to test against, without an account or a bill. It is a substitute, not the real thing.

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
  # We will leave this without a default so you can pass it in securely
}

variable "gcp_region" {
  description = "The GCP region to deploy resources to"
  type        = string
  default     = "us-central1" # Feel free to change this to a region closer to you
}

variable "gcp_zone" {
  description = "The specific zone within the region"
  type        = string
  default     = "us-central1-a"
}

variable "bigquery_location" {
  description = "Location for the BigQuery datasets (multi-region 'US'/'EU' or a region)"
  type        = string
  default     = "US"
}
```

In **opentofu/main.tf** (the bucket from the first edition, plus three **BigQuery datasets** for the cloud warehouse in Tasks 4 to 7):

```hcl
resource "google_storage_bucket" "data_lake" {
  # Bucket names must be globally unique across all of Google Cloud
  # We append your project ID to ensure it doesn't clash with anyone else
  name          = "falcon-fleet-f1-data-lake-${var.gcp_project_id}"
  location      = var.gcp_region
  
  # This allows OpenTofu to delete the bucket later even if it has files inside it
  # (Great for dev environments, but you'd turn this off in production!)
  force_destroy = true 

  # Best practice for modern GCP buckets
  uniform_bucket_level_access = true
}

# Let's output the bucket name so OpenTofu tells us what it created
output "data_lake_bucket_name" {
  value       = google_storage_bucket.data_lake.name
  description = "The name of the F1 data lake bucket"
}

# ---------------------------------------------------------------------------
# BigQuery datasets for a future cloud warehouse (raw landing, staging, marts).
# NOT yet applied, and the dbt models are not BigQuery-portable as-is (they use DuckDB-only SQL such as
# ASOF JOIN, read_parquet and json_extract_string). See README "Cloud roadmap".
# ---------------------------------------------------------------------------
resource "google_bigquery_dataset" "layers" {
  for_each   = toset(["f1_raw", "f1_staging", "f1_marts"])
  dataset_id = each.key
  location   = var.bigquery_location

  # Safe default for a learning project; set to false before storing anything you cannot rebuild.
  delete_contents_on_destroy = true

  labels = {
    project = "f1-data-project"
    layer   = replace(each.key, "f1_", "")
  }
}

output "bigquery_datasets" {
  value       = [for d in google_bigquery_dataset.layers : d.dataset_id]
  description = "BigQuery datasets created for the cloud warehouse"
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

#### **Task 4: Load the Raw Data into BigQuery (new in the second edition)**

The first edition listed "BigQuery as a cloud warehouse" as a planned step. It is now built, starting with getting the raw Parquet into BigQuery tables. DuckDB reads many Parquet files as one table and reconciles their types with `union_by_name`. **A BigQuery column has exactly one type**, so the `+1 LAP`-versus-number drift from Chapter 9 comes back, and it must be reconciled *before* loading. `scripts/load_raw_to_bigquery.py` does that:

```python
#!/usr/bin/env python3
"""
Load the extractor's raw Parquet files into BigQuery, one table per endpoint (dataset f1_raw by default).

  python scripts/load_raw_to_bigquery.py --raw-dir airflow/data/raw --project my-project [--dataset f1_raw]
  python scripts/load_raw_to_bigquery.py ... --emulator http://localhost:9050     # local BigQuery emulator, no credentials

WHY THIS EXISTS: DuckDB reads many Parquet files with union_by_name and reconciles types. A BigQuery table has ONE type per
column, so cross-file drift (a number in one session's file, a '+1 LAP' string in another) must be reconciled BEFORE loading.
Rules: identical types stay; int + float -> FLOAT64; null-only columns adopt the other files' type; anything else -> STRING.
Each endpoint is fully re-loaded (WRITE_TRUNCATE), so re-running is idempotent. Files under _manifests/ are ignored.
"""
import argparse
import glob
import io
import os
import re
import sys
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq

FILE_RE = re.compile(r"^(?P<endpoint>.+)_(?P<session>\d+|global)_extracted\.parquet$")


def unify_types(types):
    """Pick one Arrow type from the types a column has across files."""
    real = [t for t in types if not pa.types.is_null(t)]
    if not real:
        return pa.string()
    if all(t == real[0] for t in real):
        return real[0]
    if all(pa.types.is_integer(t) or pa.types.is_floating(t) for t in real):
        return pa.float64()
    return pa.string()


def unified_schema(tables):
    cols = defaultdict(list)
    order = []
    for t in tables:
        for f in t.schema:
            if f.name not in cols:
                order.append(f.name)
            cols[f.name].append(f.type)
    return pa.schema([(n, unify_types(cols[n])) for n in order])


def conform(table: pa.Table, schema: pa.Schema) -> pa.Table:
    arrays = []
    for f in schema:
        if f.name not in table.column_names:
            arrays.append(pa.nulls(table.num_rows, f.type))
            continue
        col = table[f.name]
        if col.type == f.type:
            arrays.append(col)
        elif pa.types.is_string(f.type):
            arrays.append(pa.array([None if v is None else str(v) for v in col.to_pylist()], type=pa.string()))
        else:
            arrays.append(col.cast(f.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def combine_endpoint(files):
    tables = [pq.read_table(f) for f in files]
    schema = unified_schema(tables)
    return pa.concat_tables([conform(t, schema) for t in tables])


def group_files(raw_dir):
    groups = defaultdict(list)
    for f in sorted(glob.glob(os.path.join(raw_dir, "*_extracted.parquet"))):
        m = FILE_RE.match(os.path.basename(f))
        if m:
            groups[m.group("endpoint")].append(f)
    return groups


def make_client(project, emulator=None):
    from google.cloud import bigquery
    if emulator:
        from google.api_core.client_options import ClientOptions
        from google.auth.credentials import AnonymousCredentials
        return bigquery.Client(project=project, credentials=AnonymousCredentials(), client_options=ClientOptions(api_endpoint=emulator))
    return bigquery.Client(project=project)


BQ_TYPES = {pa.types.is_integer: "INT64", pa.types.is_floating: "FLOAT64", pa.types.is_boolean: "BOOL"}


def bq_type(t):
    for pred, name in BQ_TYPES.items():
        if pred(t):
            return name
    return "STRING"


def load(client, dataset, endpoint, table: pa.Table, fmt="parquet"):
    from google.cloud import bigquery
    ref = f"{client.project}.{dataset}.{endpoint}"
    if fmt == "parquet":
        buf = io.BytesIO(); pq.write_table(table, buf, compression="snappy"); buf.seek(0)
        job = client.load_table_from_file(buf, ref, job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET, write_disposition="WRITE_TRUNCATE"))
    else:  # newline-delimited JSON with an explicit schema (for backends without Parquet load support)
        import json
        schema = [bigquery.SchemaField(f.name, bq_type(f.type)) for f in table.schema]
        buf = io.BytesIO("\n".join(json.dumps(r, default=str) for r in table.to_pylist()).encode())
        job = client.load_table_from_file(buf, ref, job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, schema=schema, write_disposition="WRITE_TRUNCATE"))
    job.result()
    return client.get_table(ref).num_rows


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw-dir", required=True); ap.add_argument("--project", required=True)
    ap.add_argument("--dataset", default="f1_raw"); ap.add_argument("--emulator", help="e.g. http://localhost:9050")
    ap.add_argument("--format", choices=["parquet", "json"], default="parquet"); ap.add_argument("--location", default="US")
    a = ap.parse_args(argv)
    from google.cloud import bigquery
    client = make_client(a.project, a.emulator)
    from google.api_core.exceptions import NotFound
    try:
        client.get_dataset(f"{a.project}.{a.dataset}")
    except NotFound:
        client.create_dataset(bigquery.Dataset(f"{a.project}.{a.dataset}"))
    groups = group_files(a.raw_dir)
    if not groups:
        sys.exit(f"No *_extracted.parquet files under {a.raw_dir}")
    for endpoint, files in sorted(groups.items()):
        table = combine_endpoint(files)
        print(f"{endpoint:24s} {len(files):3d} file(s) -> {load(client, a.dataset, endpoint, table, a.format):>8,} rows")


if __name__ == "__main__":
    main()
```

```bash
python scripts/load_raw_to_bigquery.py --raw-dir airflow/data/raw --project my-gcp-project
```

**The rules for reconciling types:** identical types stay as they are; an integer in one file and a float in another becomes `FLOAT64`; a column that is entirely empty in some files takes the type from the others; **anything else becomes `STRING`**, which loses no data (a `+1 LAP` stays `+1 LAP`) and which the dbt staging models already cast with `SAFE_CAST`. Each endpoint is reloaded in full (`WRITE_TRUNCATE`), so re-running is idempotent. The file-name pattern keeps multi-word endpoints intact (`session_result`, not `session`) and ignores `_manifests/`.

#### **Task 5: Make the SQL Portable (new in the second edition)**

The models used DuckDB-only syntax in many places. Rather than keep two copies of every model, the project calls **macros** that write the right SQL for the current engine. They live in `macros/portable.sql`, and the dialect is read from `target.type`:

```jinja
{#
  Cross-database SQL helpers. DuckDB is the default dialect (`default__`); BigQuery overrides are `bigquery__`.
  Models call these instead of writing DuckDB-only syntax directly (TRY_CAST, epoch(), REGR_SLOPE, MEDIAN, ASOF JOIN,
  FILTER (WHERE ...), LIST(), json_extract_string, regexp_extract, BOOL_OR, ARG_MAX).
  Dialect is `target.type`, overridable with --vars '{f1_dialect: bigquery}' for compile-only checks.
#}
{% macro f1_dialect() %}{{ return(var('f1_dialect', target.type)) }}{% endmacro %}
{% macro f1_is_bq() %}{{ return(f1_dialect() == 'bigquery') }}{% endmacro %}

{# ---- types & casts ---- #}
{% macro f1_type(dtype) %}
  {%- set bq = {'int': 'INT64', 'double': 'FLOAT64', 'string': 'STRING', 'timestamp': 'TIMESTAMP', 'bool': 'BOOL'} -%}
  {%- set dd = {'int': 'INTEGER', 'double': 'DOUBLE', 'string': 'VARCHAR', 'timestamp': 'TIMESTAMP', 'bool': 'BOOLEAN'} -%}
  {{- bq[dtype] if f1_is_bq() else dd[dtype] -}}
{% endmacro %}
{% macro f1_try_cast_fn() %}{{ 'SAFE_CAST' if f1_is_bq() else 'TRY_CAST' }}{% endmacro %}
{% macro f1_cast(expr, dtype) %}CAST({{ expr }} AS {{ f1_type(dtype) }}){% endmacro %}
{% macro f1_try_cast(expr, dtype) %}{{ 'SAFE_CAST' if f1_is_bq() else 'TRY_CAST' }}({{ expr }} AS {{ f1_type(dtype) }}){% endmacro %}

{# ---- time ---- #}
{% macro f1_seconds_between(start_ts, end_ts) %}
  {%- if f1_is_bq() -%}TIMESTAMP_DIFF({{ end_ts }}, {{ start_ts }}, MICROSECOND) / 1000000.0
  {%- else -%}epoch({{ end_ts }} - {{ start_ts }}){%- endif -%}
{% endmacro %}
{% macro f1_ts_add_seconds(ts, secs) %}
  {%- if f1_is_bq() -%}TIMESTAMP_ADD({{ ts }}, INTERVAL CAST(ROUND(({{ secs }}) * 1000000) AS INT64) MICROSECOND)
  {%- else -%}({{ ts }} + ({{ secs }}) * INTERVAL 1 SECOND){%- endif -%}
{% endmacro %}
{% macro f1_epoch_us(ts) %}{{ 'UNIX_MICROS' if f1_is_bq() else 'epoch_us' }}({{ ts }}){% endmacro %}
{% macro f1_epoch_ms(ts) %}{{ 'UNIX_MILLIS' if f1_is_bq() else 'epoch_ms' }}({{ ts }}){% endmacro %}

{# ---- aggregates / windows ---- #}
{% macro f1_median_over(expr, partition_by) %}
  {%- if f1_is_bq() -%}PERCENTILE_CONT({{ expr }}, 0.5) OVER (PARTITION BY {{ partition_by }})
  {%- else -%}MEDIAN({{ expr }}) OVER (PARTITION BY {{ partition_by }}){%- endif -%}
{% endmacro %}
{# y, x must already be NULL-masked identically by the caller (CASE WHEN keep THEN value END for both). #}
{% macro f1_regr_slope(y, x) %}
  {%- if f1_is_bq() -%}SAFE_DIVIDE(COVAR_POP({{ y }}, {{ x }}), VAR_POP({{ x }})){%- else -%}REGR_SLOPE({{ y }}, {{ x }}){%- endif -%}
{% endmacro %}
{% macro f1_regr_intercept(y, x) %}
  {%- if f1_is_bq() -%}(AVG({{ y }}) - SAFE_DIVIDE(COVAR_POP({{ y }}, {{ x }}), VAR_POP({{ x }})) * AVG({{ x }}))
  {%- else -%}REGR_INTERCEPT({{ y }}, {{ x }}){%- endif -%}
{% endmacro %}
{% macro f1_r2(y, x) %}POWER(CORR({{ y }}, {{ x }}), 2){% endmacro %}
{% macro f1_list_agg(expr, order_by) %}{{ 'ARRAY_AGG' if f1_is_bq() else 'LIST' }}({{ expr }} ORDER BY {{ order_by }}){% endmacro %}
{% macro f1_bool_or(expr) %}{{ 'LOGICAL_OR' if f1_is_bq() else 'BOOL_OR' }}({{ expr }}){% endmacro %}
{% macro f1_arg_max(value, by) %}
  {%- if f1_is_bq() -%}ARRAY_AGG({{ value }} ORDER BY {{ by }} DESC LIMIT 1)[OFFSET(0)]
  {%- else -%}ARG_MAX({{ value }}, {{ by }}){%- endif -%}
{% endmacro %}
{% macro f1_json_scalar(expr, path) %}
  {%- if f1_is_bq() -%}JSON_EXTRACT_SCALAR({{ expr }}, '{{ path }}'){%- else -%}json_extract_string({{ expr }}, '{{ path }}'){%- endif -%}
{% endmacro %}
{# pattern is a plain regex with ONE capture group; returns that group (or NULL). #}
{% macro f1_regexp_extract(expr, pattern) %}
  {%- if f1_is_bq() -%}REGEXP_EXTRACT({{ expr }}, r'{{ pattern }}')
  {%- else -%}NULLIF(regexp_extract({{ expr }}, '{{ pattern }}', 1), ''){%- endif -%}
{% endmacro %}

{#
  ASOF LEFT JOIN: attach to every `left` row the latest `right` row with the same keys and right_ts <= left_ts.
    left / right : relation or CTE names
    on           : equality key column names (must exist in BOTH, and be in left_cols)
    left_cols    : left columns to carry through (include the keys)
    right_cols   : right columns to attach, either 'col' or 'col AS alias'
  DuckDB uses its native ASOF JOIN. BigQuery has none, so the same result is built in LINEAR time by merging both
  sides onto one timeline, carrying the latest right-side timestamp forward with a running MAX over integer microseconds,
  then joining back to the right table on (keys, that timestamp). This keeps legitimately-NULL right values intact and works
  on engines without LAST_VALUE ... IGNORE NULLS. Right rows win ties (right_ts = left_ts). Right rows are de-duplicated
  per (keys, right_ts) first, so duplicate readings cannot multiply left rows.
#}
{% macro f1_asof_join(left, right, on, left_ts, right_ts, left_cols, right_cols) %}
  {%- set rc = [] -%}
  {%- for c in right_cols -%}
    {%- set parts = c.split(' AS ') if ' AS ' in c else [c, c] -%}
    {%- do rc.append({'src': parts[0] | trim, 'alias': parts[1] | trim}) -%}
  {%- endfor -%}
  {%- if f1_is_bq() -%}
    SELECT
      {% for c in left_cols %}m.{{ c }},{% endfor %}
      {% for r in rc %}r.{{ r.src }} AS {{ r.alias }}{{ ',' if not loop.last }}{% endfor %}
    FROM (
      SELECT
        {% for c in left_cols %}{{ c }},{% endfor %} _side,
        MAX(CASE WHEN _side = 1 THEN _ts END) OVER (
          PARTITION BY {{ on | join(', ') }} ORDER BY _ts, _side DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS _last_ts
      FROM (
        SELECT 0 AS _side, {% for c in left_cols %}{{ c }},{% endfor %} UNIX_MICROS({{ left_ts }}) AS _ts
        FROM {{ left }}
        UNION ALL
        SELECT 1 AS _side, {% for c in left_cols %}{{ c if c in on else 'NULL' }} AS {{ c }},{% endfor %} UNIX_MICROS({{ right_ts }}) AS _ts
        FROM {{ right }}
        WHERE {{ right_ts }} IS NOT NULL
      )
    ) m
    LEFT JOIN (
      SELECT * EXCEPT (_rn) FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY {{ on | join(', ') }}, {{ right_ts }} ORDER BY {{ right_ts }}) AS _rn
        FROM {{ right }}
      ) WHERE _rn = 1
    ) r
      ON {% for k in on %}r.{{ k }} = m.{{ k }} AND {% endfor %}UNIX_MICROS(r.{{ right_ts }}) = m._last_ts
    WHERE m._side = 0
  {%- else -%}
    SELECT
      {% for c in left_cols %}l.{{ c }},{% endfor %}
      {% for r in rc %}r.{{ r.src }} AS {{ r.alias }}{{ ',' if not loop.last }}{% endfor %}
    FROM {{ left }} l
    ASOF LEFT JOIN {{ right }} r
      ON {% for k in on %}l.{{ k }} = r.{{ k }} AND {% endfor %}l.{{ left_ts }} >= r.{{ right_ts }}
  {%- endif -%}
{% endmacro %}
```

**What had to change, and why** (each item was found by running the project on BigQuery and comparing with DuckDB):

| DuckDB-only construct | Problem on BigQuery | Replacement |
|---|---|---|
| `AGG(x) FILTER (WHERE c)` | not supported | `AGG(CASE WHEN c THEN x END)`, which means the same everywhere |
| `TRY_CAST`, `INTEGER`, `DOUBLE`, `VARCHAR` | different names | `f1_try_cast_fn()`, `f1_type('int')` and friends |
| `epoch(a - b)`, `ts + n * INTERVAL 1 SECOND` | different time arithmetic | `f1_seconds_between`, `f1_ts_add_seconds` |
| `MEDIAN(x)` (grouped) | no exact grouped median; `APPROX_QUANTILES` is only approximate | a window `PERCENTILE_CONT`, then `MAX` of it |
| `REGR_SLOPE`, `REGR_INTERCEPT` | not available | covariance divided by variance |
| `LIST(...)`, `BOOL_OR`, `ARG_MAX` | other names | `ARRAY_AGG`, `LOGICAL_OR`, an ordered `ARRAY_AGG` |
| `json_extract_string`, `regexp_extract` | other names and argument shapes | `f1_json_scalar`, `f1_regexp_extract` |
| `DEGREES(x)` | **the function does not exist** | `x * 180 / ACOS(-1)` |
| `LEAST(60, NULL)` | DuckDB returns **60**, BigQuery returns **NULL** | `LEAST(60, COALESCE(x, 60))` |
| `STRING_AGG(DISTINCT x, ', ')` | the order is unspecified, so engines differ | `... ORDER BY x` |
| `SELECT <expr of a>, ... GROUP BY <alias of another expr>` | DuckDB accepts it, BigQuery rejects it | compute the bin in its own CTE |
| `"interval"` | `interval` is a reserved word and `"..."` is a *string* in BigQuery | `adapter.quote('interval')` |
| `ASOF JOIN` | does not exist | see below |
| `BIGINT` casts of join keys | (DuckDB 1.5's optimizer crash, Chapter 5) | explicit `CAST(... AS BIGINT)` in `dim_drivers` and `dim_driver_session`; `f1_type('int')` is `INTEGER` on DuckDB and `INT64` on BigQuery |
| `append` incremental strategy | not offered | `insert_overwrite`, partitioned (Task 7) |

**The ASOF join without `ASOF JOIN`.** "Attach the latest right-hand row at or before each left-hand time" can be done in linear time without a native operator: put left and right rows on **one timeline**, sort by time, carry the **latest right-hand timestamp** forward with a running `MAX` over integer microseconds, then join back to the right table on *(keys, that timestamp)*. Right rows sort before left rows at equal times, so a reading at exactly the same instant counts. A legitimately `NULL` right-hand value survives, because the join returns the whole right row and not a "last non-null value". This formulation was the **second attempt**: the first used `LAST_VALUE(... IGNORE NULLS)`, which real BigQuery supports but which the emulator silently ignored (it returned `NULL` for everything, see the pitfall below). The shipped version works on both, and right-hand rows are de-duplicated per `(keys, timestamp)` first so a duplicated reading cannot multiply left rows.

**On DuckDB the macro just writes the native `ASOF LEFT JOIN`.** Nothing about the DuckDB build changed: after the port, the 56 DuckDB tables hashed identically to their values before the port (apart from two intended, explained changes).

#### **Task 6: Test It Without a Cloud Account (new in the second edition)**

A portability claim you cannot test is a hope. The **goccy/bigquery-emulator** speaks enough of BigQuery's protocol for the real `dbt-bigquery` adapter to run against it, with no account and no bill. `dbt_f1_transformations/ci/bigquery_emulator/run_tests.sh` does the whole experiment:

1. generate the synthetic dataset (one lap of telemetry per driver; the emulator is slow on large joins);
2. build it on **DuckDB** as the reference;
3. start the emulator, load the same raw data with `load_raw_to_bigquery.py`;
4. run `dbt build` (models **and tests**) on BigQuery;
5. **compare every table with the DuckDB version, row for row**.

```bash
BQ_EMULATOR_BIN=/path/to/bigquery-emulator BQ_PYTHON=/path/to/python make bq-emulator-test
```

The result at the end of this edition: **121 models and tests passed on BigQuery, and all 56 tables were identical to DuckDB.** (That run predates the later model changes in `docs/CHANGES.md`; it has not been repeated on the current 135 nodes.) Step 5 is the important one. Passing SQL only shows the queries *run*; the first full comparison found **17 of 56 tables different**, with six downstream tables empty, and every difference traced to a real portability defect (the pitfalls below).

> **What this does and does not prove.** The emulator implements a subset of BigQuery's SQL, so a pass is strong evidence, not proof, of identical behaviour on real BigQuery. It cannot test incremental **re-runs**: dbt's `MERGE` sources are subqueries and the emulator only accepts a bare table there. Nothing here tests BigQuery cost or performance, the semantic layer on BigQuery, or the `metricflow_time_spine` model. The comparison was re-run on the final code, including the `BIGINT` change and the stint-overlap handling from Chapters 5 and 6: 121 nodes pass on BigQuery and 56 of 56 tables match DuckDB.

**Emulator quirks you will meet** (all handled in the test harness, none in the project): it hangs on Parquet and CSV *loads* (the harness loads JSON and pre-loads the points seed), it labels DDL statements as `SELECT` with no destination table, it returns an error for `CREATE SCHEMA IF NOT EXISTS` on an existing dataset, it does not implement the routines endpoint, and it ignores `IGNORE NULLS` and mishandles windowed `MIN`/`MAX` over `TIMESTAMP` columns.

#### **Task 7: Build on Real BigQuery (untested)**

Everything above runs without a Google account. This task is the **same steps against your own project**, and it is the part nobody has run:

```bash
gcloud auth application-default login
export F1_BQ_PROJECT=my-gcp-project
python scripts/load_raw_to_bigquery.py --raw-dir airflow/data/raw --project $F1_BQ_PROJECT
cd dbt_f1_transformations
dbt deps && dbt seed --target bigquery
dbt build --target bigquery --exclude fct_telemetry+      # build everything except telemetry first
```

Then telemetry, one session at a time (Chapter 7's `session_keys`), watching the bill.

**What is different on BigQuery.** The telemetry tables are **partitioned with exactly one session per partition**, so the `insert_overwrite` strategy can only ever replace the session being loaded. If one partition held several sessions, loading a new one would silently overwrite its neighbours. The partition range is `bq_session_key_start` to `bq_session_key_end` (9000 to 13000 by default, at most 4000 partitions): **raise the upper bound before the season counter passes it**, because sessions outside the range land in a catch-all partition.

**The other cloud pieces, and where they stand.** `modules/gcs_loader.py` (used by the `upload_to_gcs` DAG parameter) uploads raw Parquet to your bucket and skips objects whose size already matches; it is unit-tested with a fake hook, never against real storage. `scripts/deploy_to_composer.sh` and the `deploy_to_composer` workflow exist and have not been run against a real Composer environment. **No Composer environment is created by this project.**

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

* **The Problem (second edition, found by comparing with DuckDB):** every column that came from an ASOF join (weather, positions, gaps, lap numbers on telemetry) is `NULL` on BigQuery, and six downstream tables are empty.
* **Why it Happens:** the first BigQuery version of the macro carried the latest right-hand row forward with `LAST_VALUE(... IGNORE NULLS)` over a struct. The emulator silently ignores `IGNORE NULLS`, so each row saw only itself. (Real BigQuery supports the clause, so the original may well have worked there; it simply could not be *proved* here.) The telemetry model's `lap_number` was `NULL` everywhere, and the models that filter on it produced nothing.
* **The Solution:** a formulation that needs no `IGNORE NULLS` (Task 5). It was first tested on a tiny example (five left-hand rows, three right-hand rows) that included the awkward cases: no earlier reading, a tie, a `NULL` right-hand value, two keys. **Test a new join pattern on a tiny example before trusting it on a table.**

* **The Problem (second edition, found by the comparison):** a duration column has no `NULL`s on DuckDB and eight on BigQuery.
* **Why it Happens:** `LEAST(60, x)` returns `60` on DuckDB when `x` is `NULL`, because DuckDB's `LEAST` ignores `NULL`s. BigQuery's `LEAST` returns `NULL` if *any* argument is `NULL`. The last sample of each driver has no "next sample", so its duration was `NULL` there.
* **The Solution:** make the cap explicit: `LEAST(60, COALESCE(x, 60))`. This kind of difference never raises an error. Only comparing the *data* finds it.

* **The Problem (second edition):** a model fails with `Function not found: DEGREES`, or `... neither grouped nor aggregated`.
* **Why it Happens:** BigQuery has no `DEGREES()`, and it is stricter than DuckDB about selecting an expression of a column while grouping only by an alias of a different expression.
* **The Solution:** convert by hand (`x * 180 / ACOS(-1)`) and compute the grouped expression in its own CTE.

* **The Problem (second edition):** `dbt` prints `failed to add job: job <id> is already created` and nothing else.
* **Why it Happens:** the real error happened first. The Google client then **retried** the failed statement under the same job id, and the retry's error replaced the real one.
* **The Solution:** run the failing SQL directly with retries switched off (`client.query(sql, retry=None, job_retry=None)`). The real message appears at once. The same trick explained several "hangs" in the emulator.

* **The Problem (second edition, not testable here):** the first run of an incremental telemetry model works, and a re-run may fail on BigQuery.
* **Why it Happens:** dbt's `insert_overwrite` and `merge` generate a `MERGE` whose source is a subquery, which the emulator cannot run. This path is **untested**.
* **The Solution:** none yet. Run a re-run against real BigQuery before relying on it, and use `dbt run-operation delete_sessions_from_incrementals` (Chapter 7, verified on BigQuery) if you need to reload a session.

---

## **Chapter 11: Professional Workflow: Git, CI/CD and Testing Everything**

### **Chapter Overview**

Version control is how a project survives mistakes. This chapter covers the **branch-based workflow** used in the build **[VERIFIED]**, the CI/CD workflows (which exist in the second edition, **[CODE FINAL, RUN NOT CONFIRMED]** on GitHub Actions itself), and, new in this edition, **one command that tests everything locally**.

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

#### **Task 2: The CI/CD Workflows**

The first edition had two empty files and a reference sketch. The second edition has two real workflows in `.github/workflows/`, plus `scripts/deploy_to_composer.sh`.

* **Continuous Integration (CI):** on every pull request, run automatic checks.
* **Continuous Deployment (CD):** on a manual trigger, ship the code to the environment.

**`ci.yml`** is the one that matters day to day. It runs, in order, every Python test (including the helpers' own tests), a compile check of the DAG code, a synthetic-data build of the whole dbt project (models and tests), the **truncation guard**, the **scoped-build equivalence check** (both new in the third edition, Task 4), and the dashboard tests:

```yaml
name: ci
on:
  pull_request:
  push:
    branches: [main]
jobs:
  dbt-and-python:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install dbt-duckdb pyarrow pytest numpy duckdb streamlit plotly
      - name: Python unit tests (extractor, manifest, dbt scope and runner, GCS loader, radio, BigQuery loader)
        run: python -m pytest -q airflow/tests
      - name: Truncation-guard unit tests
        run: python -m pytest -q dbt_f1_transformations/ci/tests
      - name: Strategy simulator tests
        run: python -m pytest -q analytics/tests
      - name: Compile DAG and plugin code
        run: python -m compileall -q airflow/dags airflow/plugins
      - name: Build a synthetic OpenF1 dataset
        run: python dbt_f1_transformations/ci/generate_fixtures.py "$RUNNER_TEMP/f1_raw"
      - name: dbt deps
        working-directory: dbt_f1_transformations
        run: dbt deps --profiles-dir . --target ci
      - name: dbt build (all models and all tests) on the synthetic data
        working-directory: dbt_f1_transformations
        env:
          F1_RAW_DATA_PATH: ${{ runner.temp }}/f1_raw
          F1_DUCKDB_PATH: ${{ runner.temp }}/f1_ci.duckdb
        run: |
          dbt seed --profiles-dir . --target ci
          dbt build --profiles-dir . --target ci --vars '{enable_radio_transcripts: true, min_stint_laps_for_degradation: 3}'
      - name: Truncation guard (no table model may read scoped data; the telemetry_group tag matches the graph)
        working-directory: dbt_f1_transformations
        run: python ci/check_scoped_views.py target/manifest.json
      - name: Scoped-build equivalence (replace, idempotency, zero rows, vanished upstream) on copies of the built warehouse
        working-directory: dbt_f1_transformations
        env:
          F1_RAW_DATA_PATH: ${{ runner.temp }}/f1_raw
          F1_DUCKDB_PATH: ${{ runner.temp }}/f1_ci.duckdb
        run: python ci/scoped_equivalence_check.py --full-db "$F1_DUCKDB_PATH" --raw-dir "$F1_RAW_DATA_PATH" --session 9002 --target ci
      - name: Dashboard tests (every page renders against the built warehouse)
        env:
          F1_DUCKDB_PATH: ${{ runner.temp }}/f1_ci.duckdb
        run: python -m pytest -q dashboards/tests

  # Same project built on BigQuery (emulator) and compared table-by-table with DuckDB. Not yet run on GitHub Actions
  # (only locally), so it is advisory until it has proven stable there.
  bigquery-emulator:
    runs-on: ubuntu-latest
    continue-on-error: true
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install dbt-duckdb dbt-bigquery google-cloud-bigquery pyarrow duckdb
      - name: Install the BigQuery emulator
        run: |
          curl -sL -o emu.deb https://github.com/goccy/bigquery-emulator/releases/download/v0.8.1/bigquery-emulator_0.8.1-1_amd64.deb
          sudo dpkg -i emu.deb
      - name: Build on BigQuery and compare with DuckDB
        env:
          BQ_EMULATOR_BIN: /usr/bin/bigquery-emulator
          BQ_PYTHON: python
        run: bash dbt_f1_transformations/ci/bigquery_emulator/run_tests.sh
```

**Read it like an engineer**

* **One job, whatever DuckDB is current.** The workflow installs the latest `duckdb`. DuckDB 1.5 once crashed on an `INTEGER`/`BIGINT` mismatch (Chapter 5), so if you want an engine regression to fail a pull request instead of your laptop, add a matrix of DuckDB versions. `make ci-test` was run by hand for the second edition on DuckDB 1.5.6 and 2.0.0. For the third edition the full fixtures build (`dbt build --target ci`: 143 nodes, 142 passed, 1 warning, which also appears on the commit before the change) and the equivalence check were run by hand on DuckDB 2.0.0 with dbt-core 1.11.7 and dbt-duckdb 1.10.1; the workflow file itself has not been confirmed on GitHub.
* **The truncation guard and the equivalence check run after the full build**, on the warehouse it just produced. The guard only reads dbt's `manifest.json`; the equivalence check works on **copies** of the built database and never modifies it.
* **No real data and no secrets.** The dataset is generated on the spot (`generate_fixtures.py`), so CI needs no API access and no credentials.
* **`bigquery-emulator` is `continue-on-error: true`.** It downloads the emulator and runs the equivalence test from Chapter 10. It stays advisory until it has proven stable on GitHub's runners.

**`deploy_to_composer.yml`** is **manual-only** (`workflow_dispatch`) with a dry-run default, because no Composer environment exists yet. It authenticates with a service-account key stored in **GitHub Actions secrets**, never in the repository, and calls `deploy_to_composer.sh`, which syncs `dags/` and `plugins/` to the Composer bucket **without deleting** anything remote. It has never been run against a real environment.

#### **Task 3: Run Every Test Locally (new in the second edition)**

Everything CI does, you can do on your own machine in about a minute, before you push. This is the first thing to run after cloning, and the last thing to run before a pull request:

```bash
pip install dbt-duckdb pyarrow pytest numpy duckdb streamlit plotly
cd dbt_f1_transformations && dbt deps && cd ..       # once
make ci-test
```

`make ci-test` generates a synthetic OpenF1-shaped dataset (`ci/generate_fixtures.py`), then runs:

| Step | Expected |
|---|---|
| `python -m pytest -q airflow/tests` | extractor (including the `written` / `skipped` / `empty` results), manifest, dbt scope builder, dbt runner, GCS loader, radio transcriber, BigQuery loader tests pass |
| `python -m pytest -q dbt_f1_transformations/ci/tests` | the truncation guard's own tests pass (third edition) |
| `python -m pytest -q analytics/tests` | strategy simulator tests pass |
| `dbt seed` then `dbt build --target ci` | every model and test passes (143 nodes; the one expected warning is `assert_race_results_have_laps`) |
| `make scoped-check` | the truncation guard passes and `scoped_equivalence_check.py` reports `ALL CHECKS PASSED` (third edition) |
| `python -m pytest -q dashboards/tests` | every dashboard page renders (104 tests) |

Other targets: `make dashboard` (the Streamlit app), `make strategy SESSION=<a real key>`, `make semantic-validate` (MetricFlow, Chapter 9) and `make bq-emulator-test` (Chapter 10). If `make` is not installed (common in Git Bash on Windows), the commands in the `Makefile` can be typed directly; just remember that DuckDB needs `F:/...` style paths, not `/f/...`. **Do not skip the first step quietly:** the fixtures are produced by `python dbt_f1_transformations/ci/generate_fixtures.py .ci_raw/raw`, and if that step is missed the build runs on whatever data and database file are already there (see the third-edition pitfall below). When in doubt, `rm -rf .ci_raw` first.

**Why synthetic data is worth the trouble.** The dataset is small and deterministic, and it deliberately contains the situations the code must handle: a safety car, a sector yellow, rain starting, a penalty message, an in-lap and out-lap, an **overlapping stint**, string-typed gap columns in one session and numeric in another, and car-data and location timestamps that do not line up. When a test fails you know what you broke.

**What a green run does and does not tell you.** It proves the code is consistent and that the tests catch the faults that were injected on purpose. It does **not** prove the models are right on real OpenF1 data (the real-data runs found four defects that the synthetic data could not, all described in Chapters 5 to 7) or that the corner thresholds, safety-car estimates and strategy simulator are right for real races. Passing here is the *start* of trusting a change, not the end.

**Run your real data in this order** the first time, so that a failure points at one thing:

```bash
dbt seed
dbt build --select staging                              # fix any "column not found" first
dbt build --select +int_lap_context+ --exclude fct_telemetry+
dbt run --select fct_telemetry --vars '{session_keys: [<session_key>]}'     # add sessions as needed
dbt build --select fct_telemetry+
```

#### **Task 4: Prove the Scoped Builds Are Safe (third edition)**

A scoped build that is *almost* right is worse than none, because the damage is silent: another session's rows quietly disappear. Two scripts in **dbt_f1_transformations/ci/** check the two things that matter. Both run in CI after the full build and in `make scoped-check`.

**The truncation guard.** Rule 4 of the design says no `table` model may read scoped data. **ci/check_scoped_views.py** reads `target/manifest.json` (produced by any dbt command that parses the project) and fails if a model that uses the scope is a table, if a table reads a scoped view directly or through other views, or if the `telemetry_group` tag has drifted away from the graph (a model that reads the group but is not tagged would be built at the wrong time by Airflow):

```python
"""Truncation guard (rule 4 of the session-scoped build design).

A `table` model rebuilt from a SCOPED view silently loses every other session. This reads target/manifest.json and fails when:
  1. a model whose SQL uses the scope (in_scope, scope_session_keys, telemetry_load_filter) is materialized as a table, or
  2. a `table` model reads a scoped VIEW or ephemeral model (directly, or through views in between). Reading a scoped INCREMENTAL
     model is fine: it is persisted and holds every session, or
  3. the telemetry_group tag is out of step with the graph: a model that reads a telemetry_group model, or a scoped model, is not tagged.

  python ci/check_scoped_views.py [path/to/manifest.json]      (default: target/manifest.json; run `dbt parse` first)
Exit code 1 on any violation.
"""
import json
import sys

SCOPE_MARKERS = ("in_scope(", "scope_session_keys(", "telemetry_load_filter(")
GROUP_TAG = "telemetry_group"


def _models(manifest):
    return {uid: n for uid, n in manifest["nodes"].items() if n.get("resource_type") == "model"}


def _materialized(node):
    return (node.get("config") or {}).get("materialized")


def _is_scoped(node):
    code = node.get("raw_code") or node.get("raw_sql") or ""
    return any(marker in code for marker in SCOPE_MARKERS)


def check(manifest) -> list:
    models = _models(manifest)
    scoped = {uid for uid, n in models.items() if _is_scoped(n)}
    # models that expose only the scoped sessions to whoever reads them (incremental models persist ALL sessions, so they do not)
    exposing = {uid for uid in scoped if _materialized(models[uid]) != "incremental"}
    violations = []

    for uid in sorted(scoped):
        if _materialized(models[uid]) == "table":
            violations.append(f"{models[uid]['name']}: reads the session scope but is materialized as a TABLE")

    def reaches_scoped(uid, seen):
        """Does `uid` read a scoped model, directly or through non-persisting (view / ephemeral) models?"""
        for parent in (models[uid].get("depends_on") or {}).get("nodes", []):
            if parent in exposing:
                return parent
            if parent in models and parent not in seen and _materialized(models[parent]) in ("view", "ephemeral"):
                seen.add(parent)
                hit = reaches_scoped(parent, seen)
                if hit:
                    return hit
        return None

    for uid, node in sorted(models.items()):
        if _materialized(node) == "table":
            hit = reaches_scoped(uid, {uid})
            if hit:
                violations.append(f"{node['name']}: TABLE reads scoped model {models[hit]['name']} "
                                  f"(it would silently lose every session outside the scope)")

    tagged = {uid for uid, n in models.items() if GROUP_TAG in (n.get("tags") or [])}
    for uid, node in sorted(models.items()):
        parents = set((node.get("depends_on") or {}).get("nodes", []))
        if parents & (scoped | tagged) and uid not in tagged:
            violations.append(f"{node['name']}: reads a scoped or {GROUP_TAG} model but is not tagged {GROUP_TAG} "
                              f"(add it to dbt_project.yml so Airflow builds it with the group)")
    for uid in sorted(scoped - tagged):
        violations.append(f"{models[uid]['name']}: is scoped but not tagged {GROUP_TAG}")
    return violations


def main(argv):
    path = argv[1] if len(argv) > 1 else "target/manifest.json"
    with open(path) as fh:
        manifest = json.load(fh)
    problems = check(manifest)
    models = _models(manifest)
    print(f"checked {len(models)} models; scoped: {sorted(n['name'] for n in models.values() if _is_scoped(n))}")
    for p in problems:
        print("VIOLATION:", p)
    print("OK: no table model reads scoped data" if not problems else f"{len(problems)} violation(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
```

Its tests, in **ci/tests/test_check_scoped_views.py**, feed it small hand-made manifests: the intended design passes, and each kind of violation is caught. Run it with `python ci/check_scoped_views.py target/manifest.json` from `dbt_f1_transformations/`.

**The equivalence check.** **ci/scoped_equivalence_check.py** takes a warehouse that was built **in full** and proves, on copies, that rebuilding one session through the scoped path gives the same result as the full build:

| Scenario | What it proves |
|---|---|
| 1. Damage one session (change speeds, delete lap-summary and corner rows, add a duplicate), then run the **whole-graph** scoped build | that session equals the full build again, and every other session is unchanged, including a sentinel row tampered with in another session |
| 2. The same scoped build again | idempotent: nothing changes |
| 3. Damage the session, then run only `--select tag:telemetry_group` | the per-chunk build used by multi-session runs gives the same result |
| 4. A scope for a session that exists nowhere | a no-op |
| 5. Remove the session's car-data and location files from a copy of the raw folder, then run a scoped build | the session's old rows are **cleared** from all six tables (the pre-delete), while other sessions are untouched |

```bash
cd dbt_f1_transformations
python ci/scoped_equivalence_check.py --full-db "$F1_DUCKDB_PATH" --raw-dir "$F1_RAW_DATA_PATH" --session 9002 --target ci
```

The full script, which needs `dbt` and the `duckdb` Python package on your path:

```python
"""Equivalence check for session-scoped incremental builds (DuckDB).

Proves, on a copy of a warehouse that was built IN FULL, that rebuilding one session through the scoped path:
  * replaces that session exactly as a fresh full build computed it (no stale rows, no duplicates),
  * leaves every other session byte-for-byte untouched (including rows we deliberately tampered with as sentinels),
  * is idempotent (a second identical run changes nothing),
  * is a no-op for a session that has no data anywhere, and
  * CLEARS a session whose upstream rows vanished (the pre-delete), instead of leaving stale rows behind.

  cd dbt_f1_transformations
  F1_RAW_DATA_PATH=<fixtures dir> python ci/scoped_equivalence_check.py --full-db <built.duckdb> [--session 9002] [--target ci]

The input warehouse is only ever copied, never modified. Needs `dbt` (dbt-duckdb) on PATH and the `duckdb` Python package.
Exit code 1 on any failure.
"""
import argparse
import collections
import datetime
import decimal
import json
import os
import shutil
import subprocess
import sys
import tempfile

import duckdb

GROUP_TABLES = ["fct_telemetry", "fct_telemetry_distance_trace", "fct_telemetry_lap_summary",
                "fct_lap_time_delta_trace", "dim_session_corners", "fct_corner_performance"]
CI_VARS = {"enable_radio_transcripts": True, "min_stint_laps_for_degradation": 3}
FAILURES = []


def norm(v):
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, (float, decimal.Decimal)):
        return round(float(v), 5)
    if isinstance(v, datetime.datetime):
        return (v.astimezone(datetime.timezone.utc).replace(tzinfo=None) if v.tzinfo else v).isoformat(timespec="milliseconds")
    if isinstance(v, (list, tuple)):
        return str([norm(x) for x in v])
    return v


def snapshot(db):
    """{table: {session_key: Counter(rows)}} for every telemetry-group table. Rows are unordered multisets."""
    con = duckdb.connect(db, read_only=True)
    try:
        out = {}
        for t in GROUP_TABLES:
            cur = con.execute(f'SELECT * FROM "{t}"')
            cols = [d[0] for d in cur.description]
            k = cols.index("session_key")
            per = collections.defaultdict(collections.Counter)
            for row in cur.fetchall():
                per[row[k]][tuple(norm(x) for x in row)] += 1
            out[t] = per
        return out
    finally:
        con.close()


def execute(db, statements):
    con = duckdb.connect(db)
    try:
        for s in statements:
            con.execute(s)
    finally:
        con.close()


def check(ok, message):
    print(("  ok    " if ok else "  FAIL  ") + message)
    if not ok:
        FAILURES.append(message)


def same_rows(a, b):
    return a == b      # Counter equality: same rows, same multiplicities


EXCLUDE = None     # set from --exclude: dbt node selection to leave out of every build (e.g. a known failing test)


def run_dbt(args, db, raw_dir, target, variables):
    if EXCLUDE:
        args = args + ["--exclude", EXCLUDE]
    env = {**os.environ, "F1_DUCKDB_PATH": db, "F1_RAW_DATA_PATH": raw_dir}
    cmd = ["dbt", "build", "--profiles-dir", ".", "--target", target] + args + ["--vars", json.dumps(variables)]
    print("  $", " ".join(cmd))
    res = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if res.returncode != 0:
        print(res.stdout[-4000:])
        print(res.stderr[-2000:])
    check(res.returncode == 0, f"dbt exited 0 ({' '.join(args) or 'whole graph'})")
    return res.returncode == 0


def compare(after, expected_for_k, before, k, what):
    """Session k must equal expected_for_k; every other session must equal `before`."""
    for t in GROUP_TABLES:
        check(same_rows(after[t].get(k, collections.Counter()), expected_for_k[t].get(k, collections.Counter())),
              f"{what}: {t} session {k} matches the full build")
        others_after = {s: c for s, c in after[t].items() if s != k}
        others_before = {s: c for s, c in before[t].items() if s != k}
        check(others_after == others_before, f"{what}: {t} other sessions untouched")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--full-db", required=True, help="warehouse built in full (copied, never modified)")
    ap.add_argument("--raw-dir", default=os.environ.get("F1_RAW_DATA_PATH"), help="raw Parquet dir the full build read")
    ap.add_argument("--session", type=int, default=9002, help="session to rebuild through the scoped path")
    ap.add_argument("--target", default="ci")
    ap.add_argument("--exclude", default=None, help="dbt --exclude passed to every build, e.g. a test that already fails on the baseline")
    a = ap.parse_args()
    global EXCLUDE
    EXCLUDE = a.exclude
    if not a.raw_dir:
        sys.exit("--raw-dir (or F1_RAW_DATA_PATH) is required")
    k = a.session
    work_dir = tempfile.mkdtemp(prefix="f1_scoped_eq_")
    try:
        full = snapshot(a.full_db)
        have = {s for t in GROUP_TABLES for s in full[t]}
        if k not in have or len(have) < 2:
            sys.exit(f"need telemetry for session {k} and at least one other session in {a.full_db}; found {sorted(have)}")
        sentinel = sorted(have - {k})[0]
        print(f"session under test: {k}; sentinel (must stay untouched): {sentinel}")

        def fresh_copy(name):
            p = os.path.join(work_dir, name)
            shutil.copy(a.full_db, p)
            return p

        def tamper(db):
            """Damage session k in every way a stale/partial load could, and mark a sentinel row in another session."""
            execute(db, [
                f"UPDATE fct_telemetry SET speed = speed + 7 WHERE session_key = {k}",
                f"DELETE FROM fct_telemetry_distance_trace WHERE session_key = {k} AND distance_bin % 2 = 0",
                f"DELETE FROM fct_telemetry_lap_summary WHERE session_key = {k}",
                f"INSERT INTO fct_corner_performance SELECT * FROM fct_corner_performance WHERE session_key = {k} LIMIT 1",
                f"DELETE FROM dim_session_corners WHERE session_key = {k}",
                f"UPDATE fct_telemetry SET speed = speed + 1 WHERE session_key = {sentinel}",
                f"UPDATE fct_lap_time_delta_trace SET speed_delta_kph = 123.0 WHERE session_key = {sentinel}",
            ])

        print("\n[1] damaged session, whole-graph scoped build (what Airflow runs for one session)")
        db1 = fresh_copy("one.duckdb")
        tamper(db1)
        before = snapshot(db1)
        check(not same_rows(before["fct_telemetry"][k], full["fct_telemetry"][k]), "the damage is visible before the run (test is meaningful)")
        if run_dbt([], db1, a.raw_dir, a.target, {**CI_VARS, "session_keys": [k]}):
            after = snapshot(db1)
            compare(after, full, before, k, "scoped replace")
            print("\n[2] idempotency: the same scoped build again")
            if run_dbt([], db1, a.raw_dir, a.target, {**CI_VARS, "session_keys": [k]}):
                again = snapshot(db1)
                check(again == after, "second identical run changes nothing")

        print("\n[3] damaged session, telemetry-group-only build (what Airflow runs per chunk)")
        db2 = fresh_copy("chunk.duckdb")
        tamper(db2)
        before2 = snapshot(db2)
        if run_dbt(["--select", "tag:telemetry_group"], db2, a.raw_dir, a.target, {**CI_VARS, "session_keys": [k]}):
            compare(snapshot(db2), full, before2, k, "chunk replace")

        print("\n[4] zero rows: a scope with no data anywhere is a no-op")
        db3 = fresh_copy("empty.duckdb")
        before3 = snapshot(db3)
        if run_dbt(["--select", "tag:telemetry_group"], db3, a.raw_dir, a.target, {**CI_VARS, "session_keys": [987654321]}):
            check(snapshot(db3) == before3, "nothing changed for an unknown session")

        print(f"\n[5] upstream vanished: session {k} has no telemetry files any more, so its old rows must be cleared")
        db4 = fresh_copy("vanished.duckdb")
        raw_copy = os.path.join(work_dir, "raw_without_telemetry")
        shutil.copytree(a.raw_dir, raw_copy)
        for endpoint in ("car_data", "location"):
            path = os.path.join(raw_copy, f"{endpoint}_{k}_extracted.parquet")
            if os.path.exists(path):
                os.remove(path)
        before4 = snapshot(db4)
        if run_dbt(["--select", "tag:telemetry_group"], db4, raw_copy, a.target, {**CI_VARS, "session_keys": [k]}):
            after4 = snapshot(db4)
            for t in GROUP_TABLES:
                check(sum(after4[t].get(k, collections.Counter()).values()) == 0, f"vanished upstream: {t} holds no stale rows for {k}")
                check({s: c for s, c in after4[t].items() if s != k} == {s: c for s, c in before4[t].items() if s != k},
                      f"vanished upstream: {t} other sessions untouched")

        print("\n" + ("ALL CHECKS PASSED" if not FAILURES else f"{len(FAILURES)} CHECK(S) FAILED"))
        for f in FAILURES:
            print("  -", f)
        return 1 if FAILURES else 0
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
```

* **Why it works on copies:** each scenario copies the database file, so the warehouse you pass in is never modified, and every comparison is of unordered row multisets (floats rounded to five decimals).
* **`--exclude`:** passes a dbt `--exclude` to every build, for a test you already know fails on the baseline. Use it to keep checking your own change around an unrelated failure, then fix the real cause.
* **What it does not prove:** that the *full* build is right on real data (Chapters 5 to 7 cover that), or BigQuery, whose emulator cannot re-run incremental models.

### **Learner Pitfalls & Troubleshooting**

* **The Problem:** `fatal: Unable to create '.git/index.lock': File exists`.
* **Why it Happens:** a previous Git process crashed or is still running and left a lock file.
* **The Solution:** make sure no Git process or editor integration is running, then delete `.git/index.lock`.

* **The Problem:** a push is rejected because a file is too large.
* **Why it Happens:** a Parquet or `.duckdb` file was committed.
* **The Solution:** remove it from history or tracking and rely on `.gitignore`; data is re-creatable from the pipeline.

* **The Problem (third edition, Windows):** `bash: make: command not found`, and the next command still seems to work.
* **Why it Happens:** Git Bash has no `make`. The failed line is only one of several in the pasted block, so the later ones ran against whatever fixtures and database were already on disk (the error line scrolls past in a long paste), which is how a stale database produced a test failure that was not a code bug (Chapter 7 pitfall).
* **The Solution:** run the steps by hand (the generator command above), delete `.ci_raw` first, and read the output of *every* line of a pasted block.

* **The Problem (third edition):** `dbt found 1 package(s) specified in packages.yml, but only 0 package(s) installed` in a fresh `git worktree`.
* **Why it Happens:** a new checkout has no `dbt_packages/` folder. Comparing against the original commit is the fastest way to tell a regression from an existing failure, but the second checkout needs its own `dbt deps`.
* **The Solution:** `git worktree add ../f1_baseline HEAD`, then `cd` there, `dbt deps --profiles-dir .`, and build with a different `F1_DUCKDB_PATH`. Remove it afterwards with `git worktree remove --force`.

* **The Problem (second edition):** `make ci-test` stops immediately with `dbt expects 2 package(s) based on packages specified in packages.yml, but found only 0 package(s) installed`.
* **Why it Happens:** `packages.yml` lists `codegen` (and its dependency `dbt_utils`), and they have not been downloaded into `dbt_packages/` yet.
* **The Solution:** run `dbt deps` once from `dbt_f1_transformations/` (the GitHub workflow does this for you). No model uses these packages, so if the package hub is unreachable you can simply move `packages.yml` and `package-lock.yml` aside while testing.

* **The Problem (second edition, hit on the author's machine):** `make ci-test` reports `PASS=112 ... ERROR=2 ... SKIP=9`, with `INTERNAL Error: ... inequal types (INTEGER != BIGINT)`.
* **Why it Happens:** the DuckDB 1.5 optimizer bug described in Chapter 5. The project passed on the machine it was written on and failed on a different DuckDB version, which is exactly the situation a matrix of DuckDB versions in CI would catch.
* **The Solution:** `dim_drivers` and `dim_driver_session` cast their keys to `BIGINT` (Chapter 5). Check your engine with `python -c "import duckdb; print(duckdb.__version__)"`.

---

## **Chapter 12: Operations and Troubleshooting Playbook**

### **Daily Start-Up and Shut-Down**

```bash
# Start (airflow/.env must exist: copy airflow/.env.example the first time)
cd airflow && docker compose up -d
source venv/Scripts/activate        # from the project root, if not already active

# Stop
cd airflow && docker compose down
```

Before running dbt, **close** SQL tools that hold the DuckDB file.

### **The Full Run Order**

1. Trigger the Airflow DAG with `latest` (or a specific `session_key`). For a backfill, bound it with `min_session_key` and consider `skip_existing`.
2. Confirm Parquet files in `airflow/data/raw/` and the run's manifest in `airflow/data/raw/_manifests/`.
3. `dbt build` (staging, intermediate, marts and their tests), or let Airflow do it: tick `run_dbt_build` and it builds only the sessions the run wrote (Chapter 8). Warnings about plausibility are signals to read, not necessarily bugs.
4. For telemetry on a large dataset: `dbt run --select fct_telemetry --vars '{session_keys: [<session_key>]}'` for the sessions you need, then `dbt build --select fct_telemetry+`. Then, if you want the partitioned Parquet lake, the export with `--vars`.
5. Look at the results: Power BI, Harlequin, the Streamlit dashboards (`make dashboard`), or ask MetricFlow (`mf query ...`).
6. When you have something to decide: `make strategy SESSION=<a real session key>`.
7. Before a pull request: `make ci-test`.

### **Deep Clean**

When dbt behaves strangely after many edits, clear generated artefacts, then rebuild:

```bash
cd dbt_f1_transformations
dbt clean          # removes target/ and dbt_packages/
dbt deps           # required again after `dbt clean`: packages.yml lists codegen
dbt build
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
| `required variable ... is missing a value` on `docker compose up` | `.env` not created | 2 |
| Run aborts: `sessions would be extracted (guard max_sessions=...)` | Fan-out guard | 3 |
| `INTERNAL Error: ... inequal types (INTEGER != BIGINT)` | DuckDB 1.5 optimizer bug with a key cast to `INTEGER` on one side | 5, 11 |
| `Cannot switch temporary directory after the current one has been used` | `temp_directory` under `settings` instead of `config_options` (the shipped profile sets none) | 5 |
| Thousands of failures on `unique_combination_of_columns` for `int_lap_context` | Overlapping stints fan out the lap join | 6 |
| `assert_pit_lane_time_plausible` floods with warnings | Test not scoped to green-flag Race/Sprint stops | 6 |
| `Catalog Error: ... int_track_status_periods does not exist` | `--select model+` skips upstream; use `+model+` | 6 |
| Strategy CLI: `session ... not found` | Used a synthetic session key on real data | 6 |
| First `fct_telemetry` build runs for over an hour | One giant spill; build only the sessions you need | 7 |
| Corner apex speeds differ lap to lap; a corner at distance 0 | Distance drift not normalised; truncated detection window | 7 |
| `delete_sessions_from_incrementals` logs "Deleted" but rows remain | `run-operation` does not auto-commit | 7 |
| Dashboard `CatalogException` | A model was never built | 9 |
| `mf query`: `Could not set lock on file` | DuckDB single writer | 9 |
| BigQuery: ASOF-derived columns all `NULL` | `LAST_VALUE ... IGNORE NULLS` not honoured; use running `MAX` + join | 10 |
| BigQuery: `NULL` where DuckDB gives a value | `LEAST(x, NULL)` is `NULL` on BigQuery | 10 |
| `failed to add job: job ... is already created` | A retry hid the real first error; run with retries off | 10 |
| `dbt expects 2 package(s) ...` | Run `dbt deps` once | 11 |
| `verify_and_load` returns `'results': []` and `run_dbt_build` logs `keys=[]`, though files were written | the results were flattened as plain lists, but Airflow passes lazy sequences | 3, 8 |
| `Invalid session key: ...` at compile time | `session_keys` must be a list of positive integers | 7, 8 |
| `--full-refresh together with session_keys ...` compile error | refused on purpose: it would drop every other session | 7 |
| `dbt_full_refresh rebuilds every model ... allow_large_backfill` | a full refresh is behind the large-backfill guard | 8 |
| A telemetry test fails after changing incremental logic, and the models downstream of it are skipped | the incremental filter kept stale rows from an older build; rebuild from clean state or re-run the sessions with `session_keys` | 7, 11 |
| A scoped run deletes a session's rows and loads none | its raw files are gone; the pre-delete made the table match | 7 |
| `docker compose exec ... /opt/...: No such file` in Git Bash | Git Bash rewrote the path; use `MSYS_NO_PATHCONV=1` | 8 |
| Scoped run takes about 20 minutes in Docker | slow file access through the bind mount, plus a re-scan of the raw files | 8 |

---

## **Appendix A: Final Directory Tree**

```text
f1_data_project/
├── .github/workflows/
│   ├── ci.yml                                      tests, truncation guard and equivalence check on every pull request
│   └── deploy_to_composer.yml                      manual only                           [CODE FINAL, RUN NOT CONFIRMED]
├── .gitignore
├── Makefile                                        make ci-test, scoped-check, dashboard, strategy, semantic-validate, bq-emulator-test
├── README.md
├── analytics/
│   ├── f1_strategy/                                Monte Carlo strategy simulator (model, simulate, optimize, calibrate, cli)
│   ├── requirements.txt
│   └── tests/
├── airflow/
│   ├── .env.example                                copy to .env (git-ignored) before `docker compose up`
│   ├── Dockerfile                                  Airflow 2.8.1 on Python 3.11 + requirements + dbt-duckdb + Whisper model
│   ├── docker-compose.yaml
│   ├── requirements.txt
│   ├── dags/
│   │   ├── extract_openf1_dag.py                   extraction + opt-in dbt / GCS / radio tasks
│   │   ├── dbt_cosmos_dag.py                       optional Cosmos DAG                   [CODE FINAL, RUN NOT CONFIRMED]
│   │   └── modules/
│   │       ├── universal_extractor.py              (event_extractor.py is the Chapter 2 first version)
│   │       ├── pipeline_utils.py                   manifest (with extraction statuses) + failure notification
│   │       ├── dbt_scope.py                        run results -> dbt scope -> dbt commands (no Airflow import)
│   │       ├── dbt_runner.py                       runs the planned dbt commands: argument lists, no shell
│   │       ├── radio_transcriber.py                opt-in speech-to-text                 [CODE FINAL, RUN NOT CONFIRMED]
│   │       └── gcs_loader.py                       upload to GCS                         [CODE FINAL, RUN NOT CONFIRMED]
│   ├── plugins/
│   │   ├── hooks/openf1_hook.py
│   │   └── slack_alerts.py                         Slack failure callback used by the DAG
│   ├── tests/                                      extractor, manifest, dbt scope, dbt runner, GCS, radio, BigQuery loader tests
│   └── data/raw/                                   Parquet files from Airflow (and _manifests/)
├── dashboards/
│   ├── app.py                                      streamlit run dashboards/app.py
│   ├── f1_dash/                                    data.py, charts/, components/, workspaces/<role>/<phase>.py, insights.py, state.py, provenance.py, theme.py
│   └── tests/
├── dbt_f1_transformations/
│   ├── dbt_project.yml                             materialisations, the telemetry_group tags, the session_keys var and every tunable assumption
│   ├── profiles.yml                                dev / container / ci / bigquery / bigquery_emulator targets
│   ├── packages.yml
│   ├── seeds/f1_points_system.csv                  season-aware Race and Sprint points scales
│   ├── macros/
│   │   ├── portable.sql                            cross-database macros (DuckDB and BigQuery)
│   │   ├── export_fct_telemetry.sql
│   │   ├── scope.sql                               the session scope: in_scope, telemetry_load_filter, scoped_pre_delete
│   │   ├── delete_sessions_from_incrementals.sql   every incremental model tagged telemetry_group
│   │   └── emulator_overrides.sql                  test-only
│   ├── models/
│   │   ├── metricflow_time_spine.sql
│   │   ├── staging/            sources.yml + 19 stg_*.sql
│   │   ├── intermediate/       int_track_status_periods.sql, int_lap_context.sql
│   │   ├── marts/
│   │   │   ├── core/           dim_drivers, dim_driver_session, dim_sessions, dim_circuits
│   │   │   ├── analytics/      fct_*.sql, dim_session_corners.sql
│   │   │   └── schema.yml
│   │   └── semantic/           semantic_models.yml, metrics.yml, time_spine.yml
│   ├── tests/                  singular tests; generic/unique_combination_of_columns.sql, not_null_in_scope.sql
│   └── ci/
│       ├── generate_fixtures.py                    the synthetic OpenF1 dataset
│       ├── check_scoped_views.py                   truncation guard: no table may read scoped data
│       ├── scoped_equivalence_check.py             scoped rebuild == full build, on copies
│       ├── tests/                                  tests for the guard
│       ├── semantic_check.py                       MetricFlow results vs hand-written SQL
│       ├── bq_emulator_runner.py                   runs dbt-bigquery against the emulator
│       └── bigquery_emulator/                      run_tests.sh, prepare.py, compare_duckdb_bigquery.py
├── docs/                   ANALYTICS_GUIDE.md, CHANGES.md, ISSUES_CHECKLIST.md, SESSION_SCOPED_BUILDS.md, example_queries.sql, architecture.mmd
├── opentofu/               main.tf (GCS bucket + BigQuery datasets), provider.tf, variables.tf, terraform.tfvars
├── output/telemetry_data/  (partitioned Parquet lake)
├── scripts/
│   ├── load_raw_to_bigquery.py
│   └── deploy_to_composer.sh                       [CODE FINAL, RUN NOT CONFIRMED]
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

**A note on derived files.** `radio_transcripts_<session>_extracted.parquet` is *not* an OpenF1 endpoint. It is produced by `radio_transcriber.py` from the `team_radio` recordings (Chapter 6), and its name is chosen so the `team_radio_*` source glob cannot pick it up.

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
* **Apex:** the slowest point of a corner. Derived from telemetry here, since the API publishes no corner data.
* **BigQuery emulator:** a local program that imitates BigQuery well enough to run the real `dbt-bigquery` adapter against, with no account.
* **Common random numbers:** evaluating every simulated strategy against the *same* random draws, so differences are the strategies and not luck.
* **Distance axis:** telemetry expressed against distance along the lap instead of time, so two laps can be compared point by point.
* **Empirical-Bayes shrinkage:** pulling a noisy per-group estimate toward the overall estimate, by an amount that depends on how little data the group has.
* **Fan-out (join):** a join that returns a row more than once, usually because the right side matches several times. Not to be confused with *fan-out* of mapped Airflow tasks.
* **Grain:** the single thing one row represents (here, one driver in one lap). A violated grain duplicates or drops data silently.
* **Hazard (per-lap):** the chance that an event, such as a safety car, starts on any given lap.
* **Monte Carlo simulation:** estimating an outcome by running many randomised trials and looking at the distribution.
* **Mutation testing:** deliberately breaking the data to confirm that the tests notice.
* **Normalisation:** rescaling measurements to a common reference, such as rescaling every lap to the same track length.
* **Optimizer barrier:** a construct such as `OFFSET 0` that stops a query optimizer from rewriting across it.
* **Portable SQL:** SQL written (here through macros) so one project runs on more than one engine.
* **Representative lap:** a clean, comparable lap: not the first, an in-lap or out-lap, nor under yellow, safety car or red flag, and not a slow outlier.
* **Opt-in session build:** telemetry built only for chosen sessions, so a failure loses one small build, not the whole run. Since the third edition it is driven by the `session_keys` scope (`telemetry_only_sessions` is the old name).
* **Equivalence check:** rebuilding one session through the scoped path and comparing every table with a fresh full build; it also checks that other sessions are untouched.
* **Replace-by-session:** delete a session's rows and insert the new ones, so a re-run refreshes the session with no duplicates and no stale rows.
* **Scope (dbt):** the list of sessions a run changed, passed as the variable `session_keys`. With no scope the project behaves as before.
* **Structured extraction result:** `{endpoint, session_key, path, status}` with status `written`, `skipped` or `empty`; the input of the dbt scope.
* **Telemetry group:** the two scoped staging views and the six incremental telemetry models, tagged `telemetry_group`.
* **Trigger rule (Airflow):** the condition under which a task runs given the state of its upstream tasks; `none_failed` runs unless one failed.
* **Truncation guard:** the check that no `table` model reads a scoped view, because it would silently lose every session outside the scope.
* **Semantic layer:** one governed, named definition of each business metric that every tool asks for.
* **Spine (analytical):** the model that carries every fact about a unit (here, `int_lap_context`, one row per driver per lap) for all downstream models to read.
* **Undercut / overcut:** stopping earlier than a rival to gain on fresh tyres, or staying out longer to gain when the rival's stop is slower.
