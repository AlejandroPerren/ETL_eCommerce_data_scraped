# ETL Pipeline for eCommerce Data

This project implements a robust and scalable ETL (Extract, Transform, Load) pipeline for processing scraped eCommerce data from multiple sources. The pipeline is designed to be reproducible, reliable, and ready for orchestration with tools like Apache Airflow.

## Table of Contents

- [Problem Context](#problem-context)
- [Pipeline Design](#pipeline-design)
- [ETL Steps](#etl-steps)
  - [Extract](#extract)
  - [Transform](#transform)
  - [Data Quality Validations](#data-quality-validations)
  - [Load](#load)
- [Configuration and Environment](#configuration-and-environment)
- [Main Pipeline Flow](#main-pipeline-flow)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Running the ETL](#running-the-etl)

## Problem Context

### Initial State of the Data

The data comes from three different scraped sources of an eCommerce platform:

- `products_1`
- `products_2`
- `productsclassified`

The raw data had several issues:

- **Denormalized:** Data was not structured in a relational way.
- **Poorly Formatted Strings:** Inconsistent and messy string values.
- **Lists as Text:** Lists were stored as strings (e.g., `"['Red', 'Blue']"`).
- **Inconsistent Null Values:** Null or empty values were represented in various ways (e.g., `""`, `"none"`, `None`, `"[]"`).
- **No Quality Control:** The data lacked any form of quality assurance.

### Desired Outcome

The goal was to build a realistic ETL pipeline that is:

- **Reproducible:** The pipeline should produce the same results every time it runs with the same input.
- **Reliable:** The pipeline should be robust and handle errors gracefully.
- **Scalable:** The pipeline should be able to handle a growing volume of data.
- **Airflow-Ready:** The pipeline should be designed for easy integration with Apache Airflow.

## Pipeline Design

Before writing any code, we designed the pipeline architecture based on a layered data approach, similar to what is used in modern data architectures like Lakehouse or Data Warehouse.

### Data Layers

| Layer   | Objective                                     |
| ------- | --------------------------------------------- |
| STAGING | Raw data with minimal cleaning.               |
| CORE    | Structured data with clean keys and formats.  |
| GOLD    | Data ready for analysis and consumption.      |

This layered approach helps to separate concerns, improve data quality, and facilitate debugging and re-execution of the pipeline.

## ETL Steps

### Extract

- **What we did:**
  - Read the datasets using the Pandas library.
  - Followed the ETL best practice of not performing transformations in the extract step.
  - Created defensive copies of the data to avoid mutating the original sources.
- **Why we did it:**
  - **Separation of Concerns:** Each step in the ETL process has a clear and distinct responsibility.
  - **Easier Debugging:** Isolating the extraction logic makes it easier to identify and fix issues.
  - **Re-executability:** A clean extraction step allows the pipeline to be re-run from the beginning without side effects.

### Transform

This is the most critical part of the pipeline, where the data is cleaned, normalized, and structured.

#### Structural Cleaning

- **Problem:** Columns with lists stored as strings (e.g., `"['Red', 'Blue']"`).
- **Solution:** Used `ast.literal_eval()` to safely convert these strings into actual Python lists, avoiding the security risks of using `eval()`.

#### Normalization of Null Values

- **Problem:** Inconsistent representation of empty values (e.g., `""`, `"none"`, `None`, `"[]"`).
- **Solution:** Implemented a `clean_empty()` function to standardize all empty values to `NULL`, ensuring consistency and preventing errors in SQL joins and validations.

#### Text Normalization

- **Problem:** Mixed case, leading/trailing spaces, and inconsistent string formatting.
- **Solution:** Applied `.lower()` and `.strip()` methods to clean and standardize text fields, leading to consistent searches, stable joins, and reduced false cardinality.

#### Logical Separation by Layer

We created separate DataFrames for each layer and data source:

- `df_staging_productos_1`
- `df_core_productos_2`
- `df_gold_products_classified`

Each DataFrame has a clear role, is validated differently, and is loaded into its corresponding schema in the database.

### Data Quality Validations

- **What we validated:**
  - **Mandatory Keys:** Ensured that primary keys (e.g., `itemid`) were not null.
  - **Logical Uniqueness:** Verified the uniqueness of keys in the CORE layer.
  - **Critical Fields:** Checked that important fields did not contain null values.
- **Why we did it:**
  - **Fail Fast:** The pipeline stops immediately if the data quality checks fail, preventing a "garbage in, garbage out" scenario.
  - **Reliability:** Data quality validations are essential for a trustworthy pipeline.
  - This approach mimics the functionality of tools like Great Expectations or dbt tests, but implemented in Python.

### Load

- **Database Design:**
  - Used PostgreSQL running in a Docker container.
  - Created separate schemas for each data layer: `staging`, `core`, and `gold`.
  - Defined a clear primary key for the tables: `PRIMARY KEY (itemid)`.
- **Handling Duplicates:**
  - **Problem:** Encountered `duplicate key value violates unique constraint` errors, highlighting that validating uniqueness in Pandas is not enough to guarantee uniqueness in the database.
  - **Design Decision:** Implemented a **Full Refresh** strategy using `if_exists="replace"` when loading data into the database.
- **Why we chose Full Refresh:**
  - The entire dataset is processed in each run.
  - The project does not require historical data.
  - This approach results in a simple, stable, and idempotent pipeline that can be run multiple times with the same outcome.

## Configuration and Environment

- **What we did:**
  - Used Docker and `docker-compose` to create a reproducible environment.
  - Managed environment variables for database connection settings: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`.
- **Why we did it:**
  - **Consistency:** The same code runs in both local development and production environments.
  - **Airflow-Ready:** The configuration can be easily consumed by Apache Airflow.
  - **12-Factor App Compliance:** Follows the best practices for building modern, scalable applications.

## Main Pipeline Flow

The final pipeline follows a logical and easy-to-orchestrate flow:

```
extract
   ↓
transform
   ↓
validate
   ↓
load
```

This design makes the pipeline easy to test, debug, and orchestrate with tools like Apache Airflow.

## Project Structure

```
.
├── dags
├── data
│   ├── productsclassified.csv
│   ├── productsfull.csv
│   └── productsfull2.csv
├── docker-compose.yml
├── Dockerfile
├── README.md
├── requirements.txt
├── sql
│   ├── core_tables.sql
│   ├── gold_tables.sql
│   ├── schemas.sql
│   └── staging_tables.sql
└── src
    ├── extract.py
    ├── load.py
    ├── main.py
    └── transform.py
```

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [Python 3.8+](https://www.python.org/downloads/)

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/ETL_eCommerce_data_scraped.git
    cd ETL_eCommerce_data_scraped
    ```
2.  **Create a virtual environment and install dependencies:**
    ```bash
    python -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

### Running the ETL

1.  **Start the PostgreSQL database:**
    ```bash
    docker-compose up -d
    ```
2.  **Run the ETL pipeline:**
    ```bash
    python src/main.py
    ```
3.  **Stop the database:**
    ```bash
    docker-compose down
    ```
