from flask import Flask, jsonify
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number, monotonically_increasing_id

# Initialize Flask application
app = Flask(__name__)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("HousePrice") \
    .config("spark.sql.warehouse.dir", "/warehouse/tablespace/external/hive") \
    .enableHiveSupport() \
    .getOrCreate()

# PostgreSQL connection properties
postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
postgres_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver"
}

# Read data from PostgreSQL into a DataFrame
postgres_table_name = "house_price"
df_postgres = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)
df_postgres.show(3)
# Add a unique row number column
df_postgres = df_postgres.withColumn("row_number", monotonically_increasing_id())

# Split the dataset into the first 40,000 records and the last 10,000 records
df_first_40000 = df_postgres.filter(df_postgres.row_number <= 40000)
df_last_10000 = df_postgres.filter((df_postgres.row_number > 40000) & (df_postgres.row_number <= 50000))
df_last_10000.show(3)

# Drop the row_number column if you no longer need it
df_first_40000 = df_first_40000.drop("row_number")
df_last_10000 = df_last_10000.drop("row_number")

# Define a route to serve the last 10,000 records as JSON
@app.route('/api/last_10000', methods=['GET'])
def get_last_10000():
    data = df_last_10000.toPandas().to_dict(orient='records')  # Convert PySpark DataFrame to Pandas DataFrame and then to dictionary
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
