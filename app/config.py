import os

# PostgreSQL Configuration
POSTGRES_CONFIG = {
    'host': os.environ.get('POSTGRES_HOST', 'postgres'),
    'port': os.environ.get('POSTGRES_PORT', 5432),
    'database': os.environ.get('POSTGRES_DB', 'postgres_db'),
    'user': os.environ.get('POSTGRES_USER', 'postgres_user'),
    'password': os.environ.get('POSTGRES_PASSWORD', 'postgres_password'),
}

# MySQL Configuration
MYSQL_CONFIG = {
    'host': os.environ.get('MYSQL_HOST', 'mysql'),
    'port': os.environ.get('MYSQL_PORT', 3306),
    'database': os.environ.get('MYSQL_DATABASE', 'mysql_db'),
    'user': os.environ.get('MYSQL_USER', 'mysql_user'),
    'password': os.environ.get('MYSQL_PASSWORD', 'mysql_password'),
}

# MongoDB Configuration
MONGO_CONFIG = {
    'host': os.environ.get('MONGO_HOST', 'mongodb'),
    'port': os.environ.get('MONGO_PORT', 27017),
    'database': os.environ.get('MONGO_INITDB_DATABASE', 'mongo_db'),
    'user': os.environ.get('MONGO_INITDB_ROOT_USERNAME', 'mongo_user'),
    'password': os.environ.get('MONGO_INITDB_ROOT_PASSWORD', 'mongo_password'),
}

# Local File Path
LOCAL_FILE_PATH = os.environ.get('LOCAL_FILE_PATH', '/opt/airflow/data/input_file.csv')