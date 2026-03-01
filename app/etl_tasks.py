import logging
import os
import re

import pandas as pd
import pymongo
import uuid_utils as uuid
from airflow.models import Variable
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL

from config import POSTGRES_CONFIG, MYSQL_CONFIG, MONGO_CONFIG, LOCAL_FILE_PATH


def extract_postgres(**kwargs):
    try:
        source_table = Variable.get('postgres_source_table', default_var='source_table')
        postgres_url = URL.create(
            drivername='postgresql+psycopg2',
            username=POSTGRES_CONFIG['user'],
            password=POSTGRES_CONFIG['password'],
            host=POSTGRES_CONFIG['host'],
            port=POSTGRES_CONFIG['port'],
            database=POSTGRES_CONFIG['database']
        )
        engine = create_engine(postgres_url)

        query = f"SELECT * FROM {source_table};"
        df = pd.read_sql_query(query, engine)

        # Logging
        rows_extracted = len(df)
        logging.info(f"Data extracted from PostgreSQL. Rows extracted: {rows_extracted}")
        logging.info(f"PostgreSQL DataFrame (first 6 rows):\n{df.head(6)}")

        execution_date = kwargs['execution_date'].strftime('%Y%m%dT%H%M%S')
        file_path = f'/tmp/postgres_data_{execution_date}.csv'
        df.to_csv(file_path, index=False)
        return file_path
    except Exception as e:
        logging.error(f"Error extracting data from PostgreSQL: {e}")
        return None


def extract_mongodb(**kwargs):
    try:
        collection_name = Variable.get('mongodb_source_collection', default_var='source_collection')
        mongo_uri = f"mongodb://{MONGO_CONFIG['user']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}"
        client = pymongo.MongoClient(mongo_uri)
        db = client[MONGO_CONFIG['database']]
        collection = db[collection_name]
        data = list(collection.find({}, {'_id': 0}))
        df = pd.DataFrame(data)
        client.close()

        # Logging
        rows_extracted = len(df)
        logging.info(f"Data extracted from MongoDB. Rows extracted: {rows_extracted}")
        logging.info(f"MongoDB DataFrame (first 6 rows):\n{df.head(6)}")

        execution_date = kwargs['execution_date'].strftime('%Y%m%dT%H%M%S')
        file_path = f'/tmp/mongodb_data_{execution_date}.csv'
        df.to_csv(file_path, index=False)
        return file_path
    except Exception as e:
        logging.error(f"Error extracting data from MongoDB: {e}")
        return None


def extract_local_file(**kwargs):
    try:
        if LOCAL_FILE_PATH.endswith('.csv'):
            df = pd.read_csv(LOCAL_FILE_PATH)
        elif LOCAL_FILE_PATH.endswith('.json'):
            df = pd.read_json(LOCAL_FILE_PATH, lines=True)
        else:
            logging.error("Unsupported file format: " + LOCAL_FILE_PATH)
            return None

        # Logging
        rows_extracted = len(df)
        logging.info(f"Data extracted from local file. Rows extracted: {rows_extracted}")
        logging.info(f"Local file DataFrame (first 6 rows):\n{df.head(6)}")

        execution_date = kwargs['execution_date'].strftime('%Y%m%dT%H%M%S')
        file_path = f'/tmp/local_file_data_{execution_date}.csv'
        df.to_csv(file_path, index=False)
        return file_path
    except Exception as e:
        logging.error(f"Error reading local file: {e}")
        return None


def transform_data(extract_task_ids, **kwargs):
    ti = kwargs['ti']
    file_paths = []
    for task_id in extract_task_ids:
        file_path = ti.xcom_pull(task_ids=task_id)
        if file_path:
            file_paths.append(file_path)

    if not file_paths:
        logging.warning("No data files to transform.")
        return None

    data_frames = []
    try:
        for file_path in file_paths:
            df = pd.read_csv(file_path)
            data_frames.append(df)

        df_combined = pd.concat(data_frames, ignore_index=True)
        logging.info(f"Combined DataFrame before transformations. Rows: {len(df_combined)}")
        logging.info(f"Combined DataFrame (first 6 rows):\n{df_combined.head(6)}")

        # 1. Deduplication by composite key
        df_combined.drop_duplicates(subset=['Name', 'Manager', 'StartDate'], inplace=True)

        # 2. Trim spaces in Name
        df_combined['Name'] = df_combined['Name'].astype(str).str.strip()

        # 3. Validate Name (English letters first, then alphanumeric/spaces/symbols)
        name_pattern = re.compile(r"^[a-zA-Z][a-zA-Z0-9\s.,?%!-]*$")

        def validate_name(row):
            name = str(row['Name'])
            if pd.isna(row['Name']) or name.lower() == 'nan' or not name_pattern.match(name):
                source_id = str(row.get('id', 'Unknown'))
                logging.warning(f"Invalid Project Name found for SourceId {source_id}: '{name}'")
                return f"[INVALID NAME] FOR: {source_id}"
            return name

        df_combined['Name'] = df_combined.apply(validate_name, axis=1)

        # Drop the old source 'id' column now that we used it for the warning
        if 'id' in df_combined.columns:
            df_combined.drop(columns=['id'], inplace=True)

        # 4. Filter rule: Drop if NO Manager AND NO Status AND StartDate is NOT in the future
        # Convert StartDate to datetime for comparison
        df_combined['StartDate'] = pd.to_datetime(df_combined['StartDate'], errors='coerce')
        current_date = pd.Timestamp.now()

        drop_condition = (
                df_combined['Manager'].isna() &
                df_combined['Status'].isna() &
                ((df_combined['StartDate'] <= current_date) | df_combined['StartDate'].isna())
        )
        dropped_count = drop_condition.sum()
        df_combined = df_combined[~drop_condition]
        logging.info(f"Dropped {dropped_count} rows based on Manager/Status/StartDate rule.")

        # 5. Parse Cost and Benefit to positive float
        for col in ['Cost', 'Benefit']:
            # Remove any non-numeric characters except dot, then convert to float and take absolute
            df_combined[col] = pd.to_numeric(df_combined[col].astype(str).str.replace(r'[^\d.]', '', regex=True),
                                             errors='coerce').abs()

        # 6. Generate UUIDv7 for ProjectKey
        df_combined['ProjectKey'] = [str(uuid.uuid7()) for _ in range(len(df_combined))]

        logging.info(f"Data transformed successfully. Rows remaining: {len(df_combined)}")
        logging.info(f"Transformed DataFrame (first 6 rows):\n{df_combined.head(6)}")

        execution_date = kwargs['execution_date'].strftime('%Y%m%dT%H%M%S')
        transformed_file_path = f'/tmp/transformed_data_{execution_date}.csv'
        df_combined.to_csv(transformed_file_path, index=False)
        return transformed_file_path

    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        raise e
    finally:
        for file_path in file_paths:
            if os.path.exists(file_path):
                os.remove(file_path)


def load_to_mysql(**kwargs):
    ti = kwargs['ti']
    transformed_file_path = ti.xcom_pull(task_ids='transform_data')
    if not transformed_file_path:
        logging.warning("No transformed data file to load into MySQL.")
        return

    try:
        df = pd.read_csv(transformed_file_path)
        # Convert dates back to datetime objects after reading from CSV
        df['StartDate'] = pd.to_datetime(df['StartDate'], errors='coerce')
        df['EndDate'] = pd.to_datetime(df['EndDate'], errors='coerce')

        mysql_url = URL.create(
            drivername='mysql+mysqlconnector',
            username=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            host=MYSQL_CONFIG['host'],
            port=MYSQL_CONFIG['port'],
            database=MYSQL_CONFIG['database']
        )
        engine = create_engine(mysql_url)

        # --- DYNAMIC LOOKUP LOGIC ---
        def sync_lookup_table(table_name, column_name, df_column):
            """Reads lookup table, inserts missing values, and returns an id mapping dictionary."""
            # Get existing values
            existing_df = pd.read_sql(f"SELECT Id, Name FROM {table_name}", engine)
            existing_map = dict(zip(existing_df['Name'], existing_df['Id']))

            # Find new values in our data that are not in the database
            unique_values = df[df_column].dropna().unique()
            missing_values = [val for val in unique_values if val not in existing_map]

            # Insert missing values
            if missing_values:
                logging.info(f"Inserting {len(missing_values)} new records into {table_name}.")
                new_records_df = pd.DataFrame({'Name': missing_values})
                new_records_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

                # Re-fetch the updated mapping
                existing_df = pd.read_sql(f"SELECT Id, Name FROM {table_name}", engine)
                existing_map = dict(zip(existing_df['Name'], existing_df['Id']))

            return existing_map

        # Map Managers
        manager_map = sync_lookup_table('Managers', 'Name', 'Manager')
        df['ManagerId'] = df['Manager'].map(manager_map)

        # Map Fields
        field_map = sync_lookup_table('Fields', 'Name', 'Field')
        df['FieldId'] = df['Field'].map(field_map)

        # Map Statuses and fill missing with 'Unknown'
        status_map = sync_lookup_table('Statuses', 'Name', 'Status')
        df['StatusId'] = df['Status'].map(status_map)
        unknown_status_id = status_map.get('Unknown')
        df['StatusId'] = df['StatusId'].fillna(unknown_status_id)

        # --- FINAL PREPARATION ---
        # Select and rename columns to match the target MySQL table
        final_columns = ['ProjectKey', 'Name', 'Description', 'StartDate', 'EndDate',
                         'ManagerId', 'FieldId', 'StatusId', 'Cost', 'Benefit']

        df_final = df[final_columns].copy()

        # NOT NULL Check before insertion to avoid DB errors
        not_null_cols = ['ProjectKey', 'Name', 'StartDate', 'StatusId']
        valid_rows = df_final.dropna(subset=not_null_cols)
        dropped_nulls = len(df_final) - len(valid_rows)
        if dropped_nulls > 0:
            logging.warning(f"Dropped {dropped_nulls} rows due to NULLs in mandatory columns {not_null_cols}.")

        df_final = valid_rows

        logging.info(f"Final DataFrame ready for MySQL. Rows: {len(df_final)}")
        logging.info(f"Final DataFrame (first 6 rows):\n{df_final.head(6)}")

        # Insert data
        target_table = Variable.get('target_table', default_var='final_table')
        df_final.to_sql(name=target_table, con=engine, if_exists='append', index=False)
        logging.info("Data successfully loaded into MySQL.")

    except Exception as e:
        logging.error(f"Error loading data into MySQL: {e}")
        raise e
    finally:
        if os.path.exists(transformed_file_path):
            os.remove(transformed_file_path)