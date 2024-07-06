from flask import Flask, jsonify
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, upper, concat, length, current_date, sum

# Initialize SparkSession with Hive support
# Replace with your Hive warehouse directory

# Create spark session with hive enabled
'''
spark = SparkSession.builder \
        .appName("HousePrice") \
        .config("spark.sql.warehouse.dir", "/warehouse/tablespace/external/hive") \
        .enableHiveSupport() \
        .getOrCreate()
'''
spark = SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()

app = Flask(__name__)

# Load your CSV data into a pandas DataFrame
#df = pd.read_csv('employeeT.csv')
#df = pd.read_csv('/tmp/catbd125/Tekle/student.csv')

postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
postgres_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver"
}

 # read data from postgres table into dataframe :
postgres_table_name = "house_price"
df_postgres = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)
df_postgres.printSchema()
df_postgres.show(3)


# Split the dataset into the first 40,000 records and the last 10,000 records
df_first_40000 = df_postgres.limit(40000)
df_last_10000 = df_postgres.limit(10000).subtract(df_first_40000)

# Hive database and table names
#hive_database_name = "tekle"
#hive_table_name = "house_price"

# Read Hive table
#df = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))
#df.show(5)

# Optionally, you can process or clean your data here if needed

# Define a route to serve your data as JSON
@app.route('/api/data', methods=['GET'])
def get_data():
    return jsonify(df.to_dict(orient='records'))

if __name__ == '__main__':
    #app.run(debug=True)
    app.run(host='0.0.0.0', port=5000)
