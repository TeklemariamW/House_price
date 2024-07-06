from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

appName= "HousePrice"
master= "local"

spark = SparkSession.builder.master(master).appName(appName).enableHiveSupport().getOrCreate()
''''
spark = SparkSession.builder.master(master)\
                    .appName(appName)\
                    .config("spark.executor.memory", "8g")\
                    .config("spark.driver.memory", "4g")\
                    .getOrCreate()
'''


query = "SELECT * FROM tekle.house_price LIMIT 5000"
df = spark.sql(query)

df.show(3)

df.printSchema()
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline

from pyspark.ml.feature import StringIndexer

education_indexer = StringIndexer(inputCol="Neighborhood", outputCol="Neighborhood_tranform")
transformed_df = education_indexer.fit(df).transform(df)

transformed_df=transformed_df.drop("Neighborhood")
transformed_df.show(3)

transformed_df=transformed_df.select("SquareFeet","Bedrooms","Bathrooms","YearBuilt","Neighborhood_tranform", "Price")
transformed_df.show(3)


from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

transformed_df = transformed_df.withColumn("SquareFeet", col("SquareFeet").cast(DoubleType()))
transformed_df = transformed_df.withColumn("Bedrooms", col("Bedrooms").cast(DoubleType()))
transformed_df = transformed_df.withColumn("Bathrooms", col("Bathrooms").cast(DoubleType()))
transformed_df = transformed_df.withColumn("YearBuilt", col("YearBuilt").cast(DoubleType()))
transformed_df = transformed_df.withColumn("Neighborhood_tranform", col("Neighborhood_tranform").cast(DoubleType()))
transformed_df = transformed_df.withColumn("Price", col("Price").cast(DoubleType()))


from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
#train_data, test_data = transformed_df.randomSplit([0.7, 0.3], seed=42)
train_data, test_data = transformed_df.randomSplit([0.6, 0.4], seed=42)
input_cols = transformed_df.columns[:-1]  # Assuming the last column is the target column
target_col = transformed_df.columns[-1]
#input_cols.show(3)
print(input_cols[:])
print(target_col)

from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
train_data = assembler.transform(train_data)
test_data = assembler.transform(test_data)
indexer = StringIndexer(inputCol=target_col, outputCol="label")

train_data = indexer.fit(train_data).transform(train_data)
test_data = indexer.fit(test_data).transform(test_data)

classifier = RandomForestClassifier(featuresCol="features", labelCol="label")

from pyspark.ml.tuning import ParamGridBuilder
param_grid = ParamGridBuilder() \
    .addGrid(classifier.maxDepth, [5, 10, 15]) \
    .addGrid(classifier.numTrees, [20, 50, 100]) \
    .build()

evaluator = BinaryClassificationEvaluator(labelCol="label")

from pyspark.ml.tuning import CrossValidator
cross_validator = CrossValidator(estimator=classifier,
                                estimatorParamMaps=param_grid,
                                evaluator=evaluator,
                                numFolds=5)  # Adjust the number of folds as desired

cv_model = cross_validator.fit(train_data)

predictions = cv_model.transform(test_data)
accuracy = evaluator.evaluate(predictions)

print("Accuracy:", accuracy)
# Save the best model to the mounted volume
# if path is not spacified, model save under /user/ec2-user
#cv_model.bestModel.save("/tmp/catbd125/Tekle/educatioModel")
cv_model.bestModel.save("/tmp/catbd125/Tekle/housePrice")