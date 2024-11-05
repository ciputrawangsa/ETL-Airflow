# Vehicle ETL Pipeline with Apache Airflow

## Project Overview

This project implements an **ETL (Extract, Transform, Load)** process using **Apache Airflow** to process Electric Vehicle (EV) data. The data is sourced from the Washington State Open Data API, which provides information about electric vehicles in Washington state. The pipeline extracts, transforms, and loads the data into a PostgreSQL database.

### Tasks Performed:
1. **Extract**: Fetches raw EV data from the Washington State Open Data API.
2. **Transform**: Processes the raw data by filtering out hidden columns, adding a `Production` column to classify vehicles as "Last 5 Years" or "More Than 5 Years" based on their model year.
3. **Load**: Loads the transformed data into three PostgreSQL tables:
   - `all_vehicles`: Contains all vehicles with additional vehicle attributes.
   - `recent_vehicles`: Contains vehicles from the last 5 years.
   - `old_vehicles`: Contains vehicles older than 5 years.

## Data Source

The data is sourced from the **Washington State Open Data API**:

- **URL**: [https://data.wa.gov/api/views/f6w7-q2d2/rows.json?accessType=DOWNLOAD](https://data.wa.gov/api/views/f6w7-q2d2/rows.json?accessType=DOWNLOAD)

## Pipeline Details

### DAG Configuration
- **Schedule**: The DAG is configured to run **daily** (`0 0 * * *`).
- **Tasks**:
    - **Create Tables**: Creates three PostgreSQL tables: `all_vehicles`, `recent_vehicles`, and `old_vehicles`.
    - **Extract Data**: Downloads raw EV data from the Washington State Open Data API.
    - **Transform Data**: Processes the raw data by classifying vehicles as "Recent" or "Old" based on their `Model Year`.
    - **Load Data**: Loads the transformed data into the PostgreSQL database.

### PostgreSQL Database

The data is loaded into a PostgreSQL database with the following tables:
1. **all_vehicles**: Contains all vehicle records with various attributes.
2. **recent_vehicles**: Contains vehicles from the last 5 years.
3. **old_vehicles**: Contains vehicles older than 5 years.

## Data Transformation

### Model Year Classification
- The `Model Year` field is used to classify the vehicles into:
  - **Last 5 Years**: Vehicles that are within 5 years from the current year.
  - **More Than 5 Years**: Vehicles that are older than 5 years.
  - **Unknown**: For rows where `Model Year` is missing or invalid.

### Data Filtering
- The data is processed to remove "hidden" columns that are not relevant to the analysis, ensuring that only necessary information is kept.

## Code Walkthrough

The ETL pipeline consists of the following functions:

1. **`create_tables()`**:
   - Creates the three necessary tables in the PostgreSQL database: `all_vehicles`, `recent_vehicles`, and `old_vehicles`.
   
2. **`fetch_ev_data(link)`**:
   - Fetches the raw EV data from the API and processes it into a Pandas DataFrame, filtering out any "hidden" columns.

3. **`extract_data(ti)`**:
   - Extracts the raw EV data from the API and pushes it to Airflow's XCom in JSON format for downstream tasks.

4. **`transform_data(ti)`**:
   - Transforms the raw data by:
     - Converting `Model Year` into numeric values.
     - Classifying vehicles into "Recent" or "Old" based on `Model Year`.
     - Splitting the data into three subsets: `all_vehicles`, `recent_vehicles`, and `old_vehicles`.
   
5. **`load_data(ti)`**:
   - Loads the transformed data from XCom into the PostgreSQL database.

## Running the DAG

This pipeline is set up to run in Apache Airflow. Once the DAG is triggered (either manually or according to the schedule), the following steps will occur:

1. **Create Tables**: Ensures that the required tables are created in PostgreSQL.
2. **Extract Data**: Downloads raw EV data from the provided API.
3. **Transform Data**: Processes the raw data by adding a `Production` column and splits it into three datasets.
4. **Load Data**: Loads the data into the respective PostgreSQL tables.

## Prerequisites

1. **Apache Airflow**: Install Apache Airflow with the necessary dependencies for PostgreSQL and Python operators.
2. **PostgreSQL Database**: Set up a PostgreSQL database where the data will be loaded.
3. **Python Packages**:
   - `requests`: To fetch data from the API.
   - `pandas`: To process the data.
   - `sqlalchemy`: To interact with PostgreSQL.

## Example Usage

1. Update the `POSTGRES_CONN` string with your database credentials.
2. Create a new Airflow DAG file and add this script.
3. Run the DAG using Airflow's UI or CLI.
4. Monitor the progress and check the data in your PostgreSQL database.
