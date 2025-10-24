# Health Data Snowflake Dashboard

A Python project that loads public health data into Snowflake, maintains a secure schema with masked views, and visualizes the data with a Streamlit dashboard.

---

## Features

* Idempotent Snowflake setup (`setup.sql`) with database, schema, table, roles, and views
* Automatic CSV data loading (`data.csv`) into Snowflake
* Masked view for sensitive data columns
* Streamlit dashboard for interactive visualization
* Auto-checks and installs required Python packages on first run

---

## Requirements

* Python 3.13+
* Snowflake account with proper credentials
* `data.csv` file (sample health data)

---

## Setup

1. **Clone the repository**

```bash
git clone <repo_url>
cd healthdata
```

2. **Create and activate a virtual environment**

```bash
python3 -m venv venv
source venv/bin/activate
```

3. **Set up environment variables**

Copy `.env.example` to `.env` and fill in your Snowflake credentials:

```bash
cp .env.example .env
```

`.env` file should contain:

```
SNOWFLAKE_USER=<your_username>
SNOWFLAKE_PASSWORD=<your_password>
SNOWFLAKE_ACCOUNT=<your_account>
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=HEALTH_GOV_DB
SNOWFLAKE_SCHEMA=CDC_SCHEMA
```

4. **Run the project**

```bash
python main.py
```

> This will automatically:
>
> * Install missing Python packages
> * Connect to Snowflake
> * Run `setup.sql`
> * Load `data.csv`
> * Launch the Streamlit dashboard

---

## Snowflake Structure

* **Database:** `HEALTH_GOV_DB`
* **Schema:** `CDC_SCHEMA`
* **Table:** `CDI_DATA`
* **Views:**

  * `CDI_DATA_FULL` – full data
  * `CDI_DATA_MASKED` – masked sensitive columns
* **Roles:** `DATA_STEWARD`, `ANALYST`, `PUBLIC_USER`

---

## Adding New Data

* Replace or update `data.csv`
* Run `python main.py` again – it will load new rows automatically.

---

## Dependencies

* `pandas`
* `python-dotenv`
* `snowflake-connector-python[pandas]`
* `streamlit`
* `plotly`

All dependencies are automatically checked and installed on first run.

---

## Folder Structure

```
healthdata/
├── main.py
├── dashboard.py
├── setup.sql
├── data.csv
├── .env.example
├── .gitignore
└── README.md
```

---

## License

MIT License
