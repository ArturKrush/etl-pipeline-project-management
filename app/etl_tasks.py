import logging
import os
import re

import pandas as pd
import pymongo
from airflow.models import Variable
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import uuid_utils as uuid

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

        logging.info(f"Data extracted from PostgreSQL. Rows: {len(df)}")
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

        logging.info(f"Data extracted from MongoDB. Rows: {len(df)}")
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

        logging.info(f"Data extracted from local file. Rows: {len(df)}")
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
        logging.info(f"Combined DataFrame created. Total rows: {len(df_combined)}")

        # 1. Trim spaces in text columns immediately
        for col in ['Name', 'Description', 'Field']:
            if col in df_combined.columns:
                df_combined[col] = df_combined[col].astype(str).str.strip()
                df_combined[col] = df_combined[col].replace({'nan': pd.NA, '': pd.NA})

        # 2. Drop duplicates by specific subset
        duplicates = df_combined[df_combined.duplicated(subset=['Name', 'Manager', 'StartDate'], keep='first')]
        if not duplicates.empty:
            logging.info(f"Dropped {len(duplicates)} duplicated rows. Dropped rows:\n{duplicates.to_string()}")
        df_combined.drop_duplicates(subset=['Name', 'Manager', 'StartDate'], inplace=True)

        # 3. New Filter Rule: Tag invalid names first, then filter
        name_pattern = re.compile(r"^[a-zA-Z][a-zA-Z0-9\s.,?%!-]*$")

        # Крок 3.1: Визначаємо, які імена валідні, а які ні
        is_valid_name = df_combined['Name'].notna() & df_combined['Name'].astype(str).str.match(name_pattern)
        invalid_mask = ~is_valid_name

        # Крок 3.2: Логуємо та тегуємо невалідні імена ДО фільтрації
        for idx, row in df_combined[invalid_mask].iterrows():
            source_id = str(row.get('id', 'Unknown'))
            orig_name = str(row['Name']) if pd.notna(row['Name']) else "EMPTY"
            logging.warning(f"Invalid Project Name found for SourceId {source_id}: '{orig_name}'")

        # Векторизована заміна імен (працює миттєво замість apply)
        df_combined.loc[invalid_mask, 'Name'] = "[INVALID NAME] FOR: " + df_combined['id'].astype(str).fillna('Unknown')

        # Крок 3.3: Створюємо маски для інших умов
        has_desc = df_combined['Description'].notna()
        has_field = df_combined['Field'].notna()

        # Застосовуємо правило: ((Valid Name OR Has Desc) AND Has Field)
        keep_mask_1 = (is_valid_name | has_desc) & has_field

        # Крок 3.4: Відкидаємо рядки та виводимо їх у лог
        dropped_rule_1 = df_combined[~keep_mask_1]
        if not dropped_rule_1.empty:
            logging.info(
                f"Dropped {len(dropped_rule_1)} rows failing (Name/Desc + Field) rule. Dropped rows:\n{dropped_rule_1.to_string()}")

        # Залишаємо лише валідні рядки
        df_combined = df_combined[keep_mask_1].copy()

        # Drop old source 'id' column as we already used it for logging
        if 'id' in df_combined.columns:
            df_combined.drop(columns=['id'], inplace=True)

            # 4. Old Filter rule: Drop if NO Manager AND NO Status AND StartDate is NOT in the future
            # Додано utc=True для безпечного змішування дат з Mongo та CSV
            df_combined['StartDate'] = pd.to_datetime(
                df_combined['StartDate'], format='mixed', errors='coerce', utc=True).dt.tz_localize(None)
            current_date = pd.Timestamp.now()

        drop_mask_2 = (
                df_combined['Manager'].isna() &
                df_combined['Status'].isna() &
                ((df_combined['StartDate'] <= current_date) | df_combined['StartDate'].isna())
        )

        dropped_rule_2 = df_combined[drop_mask_2]
        if not dropped_rule_2.empty:
            logging.info(
                f"Dropped {len(dropped_rule_2)} rows failing Manager/Status/StartDate rule. Dropped rows:\n{dropped_rule_2.to_string()}")

        df_combined = df_combined[~drop_mask_2].copy()

        # 5. Parse Cost and Benefit to positive float
        for col in ['Cost', 'Benefit']:
            if col in df_combined.columns:
                df_combined[col] = pd.to_numeric(df_combined[col].astype(str).str.replace(r'[^\d.]', '', regex=True),
                                                 errors='coerce').abs()

        # 6. Generate UUIDv7 for ProjectKey
        df_combined['ProjectKey'] = [str(uuid.uuid7()) for _ in range(len(df_combined))]

        logging.info(f"Data transformed successfully. Final rows: {len(df_combined)}")
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
        # Додано utc=True
        df['StartDate'] = pd.to_datetime(
            df['StartDate'], format='mixed', errors='coerce', utc=True).dt.tz_localize(None)
        df['EndDate'] = pd.to_datetime(
            df['EndDate'], format='mixed', errors='coerce', utc=True).dt.tz_localize(None)

        mysql_url = URL.create(
            drivername='mysql+mysqlconnector',
            username=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            host=MYSQL_CONFIG['host'],
            port=MYSQL_CONFIG['port'],
            database=MYSQL_CONFIG['database']
        )
        engine = create_engine(mysql_url)
        target_table = Variable.get('target_table', default_var='final_table')

        # --- DYNAMIC LOOKUP LOGIC ---
        def sync_lookup_table(table_name, df_column):
            """Reads lookup table, inserts missing values, and returns an id mapping dictionary."""
            existing_df = pd.read_sql(f"SELECT Id, Name FROM {table_name}", engine)
            existing_map = dict(zip(existing_df['Name'], existing_df['Id']))

            unique_values = df[df_column].dropna().unique()
            missing_values = [val for val in unique_values if val not in existing_map]

            if missing_values:
                logging.info(f"Inserting {len(missing_values)} new records into {table_name}.")
                new_records_df = pd.DataFrame({'Name': missing_values})
                new_records_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

                existing_df = pd.read_sql(f"SELECT Id, Name FROM {table_name}", engine)
                existing_map = dict(zip(existing_df['Name'], existing_df['Id']))

            return existing_map

        manager_map = sync_lookup_table('Managers', 'Manager')
        df['ManagerId'] = df['Manager'].map(manager_map)

        # Повернуто вашу чисту логіку без зайвого 'Unknown'
        field_map = sync_lookup_table('Fields', 'Field')
        df['FieldId'] = df['Field'].map(field_map)

        status_map = sync_lookup_table('Statuses', 'Status')
        df['StatusId'] = df['Status'].map(status_map)
        df['StatusId'] = df['StatusId'].fillna(status_map.get('Unknown'))

        # --- FINAL PREPARATION ---
        final_columns = ['ProjectKey', 'Name', 'Description', 'StartDate', 'EndDate',
                         'ManagerId', 'FieldId', 'StatusId', 'Cost', 'Benefit']
        df_final = df[final_columns].copy()

        not_null_cols = ['ProjectKey', 'FieldId', 'StatusId', 'StartDate']

        # --- ДОДАНО/ЗМІНЕНО: Додано логування перед dropna, щоб точно знати, якщо якісь рядки відсіються ---
        before_drop_count = len(df_final)
        df_final = df_final.dropna(subset=not_null_cols)
        after_drop_count = len(df_final)
        if before_drop_count != after_drop_count:
            logging.warning(
                f"Dropped {before_drop_count - after_drop_count} rows due to missing NOT NULL values in DB mapping (Columns: {not_null_cols}).")

        # --- SIMPLIFIED DATABASE DEDUPLICATION ---
        try:
            query = f"SELECT Name, ManagerId, StartDate FROM {target_table}"
            db_existing_df = pd.read_sql(query, engine)

            if not db_existing_df.empty:
                # Helper function to create a text key for comparison
                def make_key(row):
                    name = str(row['Name']) if pd.notna(row['Name']) else ""
                    return f"{name}|{row['ManagerId']}|{row['StartDate']}"

                # Create a set of keys that already exist in the database
                existing_keys = set(db_existing_df.apply(make_key, axis=1))

                # Apply the same key to our current DataFrame
                df_final['temp_key'] = df_final.apply(make_key, axis=1)

                # Find and log rows that we are going to skip
                duplicates_db = df_final[df_final['temp_key'].isin(existing_keys)]
                if not duplicates_db.empty:
                    logging.info(f"Filtered out {len(duplicates_db)} rows already existing in MySQL.")

                # Keep only rows whose keys are NOT in existing_keys
                df_final = df_final[~df_final['temp_key'].isin(existing_keys)].copy()
                df_final.drop(columns=['temp_key'], inplace=True)
            else:
                logging.info("Target table is empty. Skipping DB deduplication.")
        except Exception as e:
            logging.warning(f"Could not perform DB deduplication (table might not exist yet): {e}")

        if df_final.empty:
            logging.info("No new data to insert into MySQL.")
            return

        logging.info(f"Final DataFrame ready for MySQL. Rows: {len(df_final)}")
        logging.info(f"Final DataFrame (first 6 rows):\n{df_final.head(6)}")

        # Insert data
        df_final.to_sql(name=target_table, con=engine, if_exists='append', index=False)
        logging.info("Data successfully loaded into MySQL.")

    except Exception as e:
        logging.error(f"Error loading data into MySQL: {e}")
        raise e
    finally:
        if os.path.exists(transformed_file_path):
            os.remove(transformed_file_path)