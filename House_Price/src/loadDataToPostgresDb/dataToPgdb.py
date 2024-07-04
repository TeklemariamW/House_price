import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

def load_data(connection_engine):
    # Get the directory path of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the relative path to the file
    file_path = os.path.join(current_dir, "../Data/housing_price_dataset.csv")
    df = pd.read_csv(file_path)

    df.to_sql("house_price", connection_engine, if_exists='append', index=False)

if __name__ == '__main__':
    try:
        # Load environment variables from .env file
        load_dotenv()
        # Retrieve database connection details from environment variables
        db_username = os.getenv("DB_USERNAME")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")

        url = f"postgresql+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"

        sql_engine = create_engine(url)

        load_data(connection_engine=sql_engine)

    except Exception as ex:
        print(ex)