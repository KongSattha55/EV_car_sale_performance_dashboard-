import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.functions import vector_to_array

os.environ["HADOOP_HOME"] = os.path.abspath("hadoop")
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=final_project;encrypt=true;trustServerCertificate=true"
jdbc_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
db_user = "sa"
db_password = "YourStrongPassword123!"

def main():
    spark = SparkSession.builder \
        .appName("LoanDefaultPrediction-Pipeline") \
        .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Load data from SQL Server
    print("Loading data from SQL Server...")
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", jdbc_driver) \
        .option("dbtable", "loans") \
        .option("user", db_user) \
        .option("password", db_password) \
        .load()

    # 1. Groupby Aggregations & Database Export
    print("Aggregating by Grade...")
    by_grade = df.groupBy("grade") \
        .agg(
            F.count("*").alias("loan_count"),
            F.round(F.avg("int_rate"), 2).alias("avg_int_rate"),
            F.round(F.avg("loan_amnt"), 2).alias("avg_loan_amnt"),
            F.round(F.avg("default"), 4).alias("default_rate")
        ) \
        .orderBy("grade")

    print("Writing processed results back to SQL Server (processed_results)...")
    by_grade.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", jdbc_driver) \
        .option("dbtable", "processed_results") \
        .option("user", db_user) \
        .option("password", db_password) \
        .mode("overwrite") \
        .save()

    # 2. Preprocessing and Feature Engineering
    print("Engineering features...")
    processed_df = df.withColumn("fico_average", (F.col("fico_range_low") + F.col("fico_range_high")) / 2.0) \
        .withColumn("loan_to_income", F.col("loan_amnt") / (F.col("annual_inc") + 1.0)) \
        .withColumn("installment_to_income", F.col("installment") / ((F.col("annual_inc") / 12.0) + 1.0)) \
        .withColumn("revol_to_income", F.col("revol_bal") / (F.col("annual_inc") + 1.0)) \
        .withColumn("open_to_total_acc", F.col("open_acc") / (F.col("total_acc") + 1.0)) \
        .withColumn("grade_val", F.ascii(F.substring(F.col("sub_grade"), 1, 1)) - 65) \
        .withColumn("sub_grade_num", F.col("grade_val") * 5 + F.substring(F.col("sub_grade"), 2, 1).cast("int"))

    numeric_features = [
        "loan_amnt", "funded_amnt", "term", "int_rate", "installment",
        "annual_inc", "dti", "fico_average", "sub_grade_num",
        "open_acc", "pub_rec", "revol_bal", "revol_util", "total_acc", "mort_acc",
        "loan_to_income", "installment_to_income", "revol_to_income", "open_to_total_acc",
        "emp_length"
    ]
    categorical_cols = ["home_ownership", "purpose", "verification_status"]

    for col_name in categorical_cols:
        indexer = StringIndexer(inputCol=col_name, outputCol=f"{col_name}_idx", handleInvalid="keep")
        processed_df = indexer.fit(processed_df).transform(processed_df)

    ohe_inputs = [f"{c}_idx" for c in categorical_cols]
    ohe_outputs = [f"{c}_ohe" for c in categorical_cols]
    encoder = OneHotEncoder(inputCols=ohe_inputs, outputCols=ohe_outputs)
    processed_df = encoder.fit(processed_df).transform(processed_df)

    # Scale numeric features
    numeric_assembler = VectorAssembler(inputCols=numeric_features, outputCol="numeric_features")
    processed_df = numeric_assembler.transform(processed_df)
    
    scaler = StandardScaler(inputCol="numeric_features", outputCol="scaled_numeric_features", withStd=True, withMean=True)
    scaler_model = scaler.fit(processed_df)
    processed_df = scaler_model.transform(processed_df)

    feature_cols = ["scaled_numeric_features"] + ohe_outputs
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    ml_df = assembler.transform(processed_df)

    # 3. Model Training
    print("Training Logistic Regression Model...")
    train_df, test_df = ml_df.randomSplit([0.8, 0.2], seed=42)

    lr = LogisticRegression(
        featuresCol="features",
        labelCol="default",
        regParam=0.01,
        elasticNetParam=0.5,
        maxIter=100,
        threshold=0.25
    )
    model = lr.fit(train_df)

    # 4. Generate Predictions & Export
    print("Generating predictions for the entire dataset...")
    full_predictions = model.transform(ml_df)
    
    # Select only original columns plus predictions to filter out Spark Vector struct types
    final_predictions_df = full_predictions.withColumn(
        "predicted_default", F.col("prediction").cast("int")
    ).withColumn(
        "default_probability", F.round(vector_to_array(F.col("probability"))[1], 4)
    ).select(df.columns + ["predicted_default", "default_probability"])

    print("Writing predictions table back to SQL Server (loans_predictions)...")
    final_predictions_df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", jdbc_driver) \
        .option("dbtable", "loans_predictions") \
        .option("user", db_user) \
        .option("password", db_password) \
        .mode("overwrite") \
        .save()

    print("Success! Table loans_predictions has been created.")
    spark.stop()

if __name__ == "__main__":
    main()
