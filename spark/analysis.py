"""
PySpark analysis of the `loans` collection in MongoDB Atlas.

Runs:
  1. Summary statistics on numeric columns
  2. Groupby aggregations (by grade, purpose, home_ownership)
  3. Correlation analysis between key numeric columns and `default`
  4. (Bonus) Logistic regression to predict `default`
  5. Export a consolidated `processed_results` table for the Windows/Power BI team
"""

import os

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import functions as F

from connect_mongodb import MONGODB_COLLECTION, MONGODB_DB, get_spark

OUTPUT_DIR = "outputs"

NUMERIC_COLS = [
    "loan_amnt", "funded_amnt", "term", "int_rate", "installment",
    "annual_inc", "dti", "fico_range_low", "fico_range_high",
    "open_acc", "pub_rec", "revol_bal", "revol_util", "total_acc", "mort_acc",
]
CATEGORICAL_COLS = ["grade", "home_ownership", "purpose", "verification_status"]


def load_data(spark):
    df = (
        spark.read.format("mongodb")
        .option("database", MONGODB_DB)
        .option("collection", MONGODB_COLLECTION)
        .load()
    )
    return df.drop("_id")


def summary_statistics(df):
    print("\n=== Analysis 1: Summary Statistics ===")
    summary = df.select(NUMERIC_COLS + ["default"]).describe()
    summary.show()
    summary.toPandas().to_csv(f"{OUTPUT_DIR}/summary_statistics.csv", index=False)


def aggregations(df):
    print("\n=== Analysis 2: Aggregations ===")

    by_grade = (
        df.groupBy("grade")
        .agg(
            F.count("*").alias("loan_count"),
            F.round(F.avg("int_rate"), 2).alias("avg_int_rate"),
            F.round(F.avg("loan_amnt"), 2).alias("avg_loan_amnt"),
            F.round(F.avg("default"), 4).alias("default_rate"),
        )
        .orderBy("grade")
    )
    by_grade.show()
    by_grade.toPandas().to_csv(f"{OUTPUT_DIR}/aggregation_by_grade.csv", index=False)

    by_purpose = (
        df.groupBy("purpose")
        .agg(
            F.count("*").alias("loan_count"),
            F.round(F.sum("loan_amnt"), 2).alias("total_loan_amnt"),
            F.round(F.avg("default"), 4).alias("default_rate"),
        )
        .orderBy(F.desc("loan_count"))
    )
    by_purpose.show()
    by_purpose.toPandas().to_csv(f"{OUTPUT_DIR}/aggregation_by_purpose.csv", index=False)

    by_home_ownership = (
        df.groupBy("home_ownership")
        .agg(
            F.count("*").alias("loan_count"),
            F.round(F.avg("annual_inc"), 2).alias("avg_annual_inc"),
            F.round(F.avg("default"), 4).alias("default_rate"),
        )
        .orderBy(F.desc("loan_count"))
    )
    by_home_ownership.show()
    by_home_ownership.toPandas().to_csv(
        f"{OUTPUT_DIR}/aggregation_by_home_ownership.csv", index=False
    )

    return by_grade


def correlation_analysis(df):
    print("\n=== Analysis 3: Correlation Analysis ===")
    pairs = [
        ("int_rate", "fico_range_low"),
        ("int_rate", "dti"),
        ("int_rate", "default"),
        ("loan_amnt", "annual_inc"),
        ("dti", "default"),
        ("annual_inc", "default"),
    ]
    rows = [(a, b, df.stat.corr(a, b)) for a, b in pairs]
    for a, b, corr in rows:
        print(f"corr({a}, {b}) = {corr:.4f}")

    spark = df.sparkSession
    corr_df = spark.createDataFrame(rows, ["column_a", "column_b", "correlation"])
    corr_df.toPandas().to_csv(f"{OUTPUT_DIR}/correlation_matrix.csv", index=False)


def train_logistic_regression(df):
    print("\n=== Analysis 4 (Bonus): Logistic Regression for `default` ===")

    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in CATEGORICAL_COLS
    ]
    feature_cols = NUMERIC_COLS + [f"{c}_idx" for c in CATEGORICAL_COLS]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    pipeline_df = df
    for indexer in indexers:
        pipeline_df = indexer.fit(pipeline_df).transform(pipeline_df)
    pipeline_df = assembler.transform(pipeline_df).select("features", "default")

    train_df, test_df = pipeline_df.randomSplit([0.8, 0.2], seed=42)

    lr = LogisticRegression(featuresCol="features", labelCol="default", maxIter=50)
    model = lr.fit(train_df)
    predictions = model.transform(test_df)

    auc = BinaryClassificationEvaluator(labelCol="default").evaluate(predictions)
    accuracy = MulticlassClassificationEvaluator(
        labelCol="default", metricName="accuracy"
    ).evaluate(predictions)

    print(f"Test AUC: {auc:.4f}")
    print(f"Test Accuracy: {accuracy:.4f}")

    with open(f"{OUTPUT_DIR}/model_metrics.txt", "w") as f:
        f.write("Logistic Regression - predicting `default`\n")
        f.write(f"Test AUC: {auc:.4f}\n")
        f.write(f"Test Accuracy: {accuracy:.4f}\n")


def export_processed_results(by_grade, df):
    print("\n=== Step 5: Export Processed Results ===")

    by_grade.toPandas().to_csv(f"{OUTPUT_DIR}/processed_results.csv", index=False)
    print(f"Wrote {OUTPUT_DIR}/processed_results.csv")

    spark = df.sparkSession
    results_collection = os.environ.get("MONGODB_RESULTS_COLLECTION", "processed_results")
    (
        by_grade.write.format("mongodb")
        .option("database", MONGODB_DB)
        .option("collection", results_collection)
        .mode("overwrite")
        .save()
    )
    print(f"Wrote results to MongoDB collection: {MONGODB_DB}.{results_collection}")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    spark = get_spark()
    df = load_data(spark)
    df.cache()

    summary_statistics(df)
    by_grade = aggregations(df)
    correlation_analysis(df)
    train_logistic_regression(df)
    export_processed_results(by_grade, df)

    spark.stop()


if __name__ == "__main__":
    main()
