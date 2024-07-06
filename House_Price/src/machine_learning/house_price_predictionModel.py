from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

# Initialize Spark Session
spark = SparkSession.builder.appName("EducationLevelPrediction").getOrCreate()

# Load the saved model from the specified path
model = PipelineModel.load("/tmp/catbd125/Tekle/educatioModel")

# Load new data into a DataFrame
new_data = spark.read.csv("insurance.csv", inferSchema=True, header=True)

# Make predictions on the new data using the loaded model
predictions = model.transform(new_data)

# Show the predictions
predictions.select("features", "prediction").show()

# Stop the Spark session
spark.stop()