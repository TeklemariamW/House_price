from flask import Flask, jsonify
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, upper, concat, length, current_date, sum

# Initialize SparkSession with Hive support
# Replace with your Hive warehouse directory

# Create spark session with hive enabled
spark = SparkSession.builder \
        .appName("HousePrice") \
        .config("spark.sql.warehouse.dir", "/warehouse/tablespace/external/hive") \
        .enableHiveSupport() \
        .getOrCreate()

app = Flask(__name__)

# Load your CSV data into a pandas DataFrame
#df = pd.read_csv('employeeT.csv')
#df = pd.read_csv('/tmp/catbd125/Tekle/student.csv')

# Hive database and table names
hive_database_name = "tekle"
hive_table_name = "house_price"

# Read Hive table
df = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))
df.show(5)



# Optionally, you can process or clean your data here if needed

# Define a route to serve your data as JSON
@app.route('/api/data', methods=['GET'])
def get_data():
    return jsonify(df.to_dict(orient='records'))

if __name__ == '__main__':
    #app.run(debug=True)
    app.run(host='0.0.0.0', port=5000)
