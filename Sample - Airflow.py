import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import requests

default_args = {
    'owner': 'ciputra wangsa',
    'start_date': datetime(2024, 11, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

# PostgreSQL connection
POSTGRES_CONN = "postgresql://postgres:postgres@host:5432/airflow_data"

def create_tables():
    """Create tables in PostgreSQL"""
    engine = create_engine(POSTGRES_CONN)
    
    # create tables
    create_all_vehicles = """
    CREATE TABLE IF NOT EXISTS all_vehicles (
        VIN VARCHAR(10) PRIMARY KEY,
        County VARCHAR(100),
        City VARCHAR(100),
        State VARCHAR(50),
        Postal_Code VARCHAR(10),
        Model_Year INTEGER,
        Make VARCHAR(100),
        Model VARCHAR(100),
        Electric_Vehicle_Type VARCHAR(100),
        CAFV_Eligibility VARCHAR(100),
        Electric_Range VARCHAR(50),
        Base_MSRP VARCHAR(50),
        Legislative_District VARCHAR(50),
        DOL_Vehicle_ID VARCHAR(100),
        Vehicle_Location VARCHAR(255),
        Electric_Utility VARCHAR(100),
        Census_Tract VARCHAR(100),
        Counties VARCHAR(100),
        Congressional_Districts VARCHAR(100),
        Legislative_District_Boundary VARCHAR(255),
        Production VARCHAR(20)
    );

    CREATE TABLE IF NOT EXISTS recent_vehicles (
        VIN VARCHAR(10) PRIMARY KEY,
        County VARCHAR(100),
        City VARCHAR(100),
        State VARCHAR(50),
        Model_Year INTEGER,
        Make VARCHAR(100),
        Model VARCHAR(100),
        Production VARCHAR(20)
    );

    CREATE TABLE IF NOT EXISTS old_vehicles (
        VIN VARCHAR(10) PRIMARY KEY,
        County VARCHAR(100),
        City VARCHAR(100),
        State VARCHAR(50),
        Model_Year INTEGER,
        Make VARCHAR(100),
        Model VARCHAR(100),
        Production VARCHAR(20)
    );
    """
    
    with engine.connect() as conn:
        conn.execute(create_all_vehicles)

def fetch_ev_data(link):
    """Fetch the EV data and filter out hidden columns directly into a DataFrame"""
    # Fetch data
    url = link
    response = requests.get(url)
    data = response.json()
    
    # filter columns with 'hidden' flag
    visible_columns = [
        col['name'] 
        for col in data['meta']['view']['columns'] 
        if 'flags' not in col or 'hidden' not in col['flags']
    ]
    
    visible_column_indices = [
        i for i, col in enumerate(data['meta']['view']['columns'])
        if col['name'] in visible_columns
    ]
    
    filtered_rows = [
        [row[i] for i in visible_column_indices]
        for row in data['data']
    ]
    
    df = pd.DataFrame(filtered_rows, columns=visible_columns)
    
    return df

def extract_data(ti=None):
    """Extract data from source"""
    url = "https://data.wa.gov/api/views/f6w7-q2d2/rows.json?accessType=DOWNLOAD"
    df = fetch_ev_data(url)
    ti.xcom_push(key='extracted_data', value=df.to_json())

def transform_data(ti=None):
    """Transform data"""
    json_data = ti.xcom_pull(key='extracted_data', task_ids='extract_data')
    df = pd.read_json(json_data)

    current_year = datetime.now().year
    df['Model Year'] = pd.to_numeric(df['Model Year'], errors='coerce')
    df['Production'] = df['Model Year'].apply(
        lambda x: 'Last 5 Years'
        if pd.notnull(x) and x >= (current_year - 4)
        else 'More Than 5 Years' if pd.notnull(x)
        else 'Unknown'
    )
    
    # Split data
    recent_mask = df['Production'] == 'Last 5 Years'
    old_mask = df['Production'] == 'More Than 5 Years'
    
    selected_columns = ['VIN', 'County', 'City', 'State', 'Model Year', 'Make', 'Model', 'Production']
    
    # Store all DataFrames in XCom
    ti.xcom_push(key='all_vehicles', value=df.to_json())
    ti.xcom_push(
        key='recent_vehicles', 
        value=df[recent_mask][selected_columns].to_json()
    )
    ti.xcom_push(
        key='old_vehicles', 
        value=df[old_mask][selected_columns].to_json()
    )

def load_data(ti=None):
    """Load data to PostgreSQL"""
    # Get data from XCom
    all_vehicles = pd.read_json(ti.xcom_pull(key='all_vehicles', task_ids='transform_data'))
    recent_vehicles = pd.read_json(ti.xcom_pull(key='recent_vehicles', task_ids='transform_data'))
    old_vehicles = pd.read_json(ti.xcom_pull(key='old_vehicles', task_ids='transform_data'))
    
    # Load to PostgreSQL
    engine = create_engine(POSTGRES_CONN)
    
    # Load all data
    all_vehicles.to_sql(
        'all_vehicles',
        engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    # Load recent vehicle data
    recent_vehicles.to_sql(
        'recent_vehicles',
        engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    # Load old vehicle data
    old_vehicles.to_sql(
        'old_vehicles',
        engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )

with DAG(
    "vehicle_etl_dag",
    schedule_interval='0 0 * * *',
    default_args=default_args,
    catchup=False
) as dag:
    
    create_tables_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables
    )
    
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )
    
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )
    
    # Set task dependencies
    create_tables_task >> extract_task >> transform_task >> load_task
