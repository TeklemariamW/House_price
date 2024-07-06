from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

appName= "Bank marketing"
master= "local"

spark = SparkSession.builder.master(master).appName(appName).enableHiveSupport().getOrCreate()
query = "SELECT * FROM tekle.bank_marketing"
df = spark.sql(query)
#df = df.withColumn("id", monotonically_increasing_id())

# Drop the row with ID 0
#df = df.filter(df.id != 0).drop('id')
df.show(3)
df=df.drop("name").drop("job_upper","id")
df.show(3)
df.printSchema()
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline

from pyspark.ml.feature import StringIndexer

marital_indexer = StringIndexer(inputCol="marital", outputCol="marital_tranform")
transformed_df = marital_indexer.fit(df).transform(df)
job_indexer = StringIndexer(inputCol="job", outputCol="job_tranform")
transformed_df = job_indexer.fit(transformed_df).transform(transformed_df)
education_indexer = StringIndexer(inputCol="education", outputCol="education_tranform")
transformed_df = education_indexer.fit(transformed_df).transform(transformed_df)
housing_indexer = StringIndexer(inputCol="housing", outputCol="housing_tranform")
transformed_df = housing_indexer.fit(transformed_df).transform(transformed_df)
loan_indexer = StringIndexer(inputCol="loan", outputCol="loan_tranform")
transformed_df = loan_indexer.fit(transformed_df).transform(transformed_df)
contact_indexer = StringIndexer(inputCol="contact", outputCol="contact_tranform")
transformed_df = contact_indexer.fit(transformed_df).transform(transformed_df)
month_indexer = StringIndexer(inputCol="month", outputCol="month_tranform")
transformed_df = month_indexer.fit(transformed_df).transform(transformed_df)
transformed_df=transformed_df.drop("marital","job","education","housing","loan","contact","month")
transformed_df.show(3)
#transformed_df=transformed_df.select("age","job_tranform","marital_tranform","education_tranform","housing_tranform","loan_tranform","contact_tranform","balance","day","month_tranform","duration","campaign","pdays","previous")
transformed_df=transformed_df.select("age","job_tranform","marital_tranform","housing_tranform","loan_tranform","contact_tranform","balance","day","month_tranform","duration","campaign","pdays","previous","education_tranform")
#day|month|duration|campaign|pdays
transformed_df.show(3)
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
transformed_df = transformed_df.withColumn("age", col("age").cast(DoubleType()))
transformed_df = transformed_df.withColumn("job_tranform", col("job_tranform").cast(DoubleType()))
transformed_df = transformed_df.withColumn("marital_tranform", col("marital_tranform").cast(DoubleType()))
transformed_df = transformed_df.withColumn("education_tranform", col("education_tranform").cast(DoubleType()))
transformed_df = transformed_df.withColumn("housing_tranform", col("housing_tranform").cast(DoubleType()))
transformed_df = transformed_df.withColumn("loan_tranform", col("loan_tranform").cast(DoubleType()))
transformed_df = transformed_df.withColumn("contact_tranform", col("contact_tranform").cast(DoubleType()))
transformed_df = transformed_df.withColumn("balance", col("balance").cast(DoubleType()))
transformed_df = transformed_df.withColumn("day", col("day").cast(DoubleType()))
transformed_df = transformed_df.withColumn("month_tranform", col("month_tranform").cast(DoubleType()))
transformed_df = transformed_df.withColumn("duration", col("duration").cast(DoubleType()))
transformed_df = transformed_df.withColumn("campaign", col("campaign").cast(DoubleType()))
transformed_df = transformed_df.withColumn("pdays", col("pdays").cast(DoubleType()))
transformed_df = transformed_df.withColumn("previous", col("previous").cast(DoubleType()))
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
cv_model.bestModel.save("/tmp/catbd125/Tekle/educatioModel")
#cv_model.bestModel.write().overwrite().save("/tmp/catbd125/Tekle/educatioModel")
#cv_model.bestModel.write().save("/tmp/catbd125/Tekle/newEducatioModel")

#spark-submit --master yarn <file name>