import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------
# Bronze Cleaning Functions
# -----------------------------


def cast_dtypes(df):
    """
    Cast columns to correct data types for Bronze layer.

    Args:
        df: Input dataframe with raw schema.

    Returns:
        : Dataframe with amount cast to Decimal(12,2)
        and txn_ts cast to Timestamp.
    """
    cast_df = df.withColumn(
        "amount", F.col("amount").cast(DecimalType(12, 2))
    ).withColumn("txn_ts", F.to_timestamp("txn_ts"))
    return cast_df


def normalise_strings(df):
    """Normalise string columns in the DataFrame by trimming whitespace and converting to uppercase.

    Args:
        df: Input DataFrame with string columns to normalize.

    Returns:
        DataFrame with normalised string columns.
    """

    string_cols = [
        field.name for field in df.schema.fields if field.dataType == "string"
    ]

    for col in string_cols:
        df = df.withColumn(col, F.upper(F.trim(F.col(col))))

    return df


def deduplicate_df(df):
    """
    Deduplicate the dataframe based on txn_id, keeping the most recent txn_ts.

    Args:
        df: Input dataframe with possible duplicates.

    Returns:
        : Deduplicated dataframe.
    """

    window = Window.partitionBy("txn_id").orderBy(F.col("txn_ts").desc())

    deduped_df = (
        df.withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    return deduped_df


def derive_txn_date(df):
    """
    Derive txn_date column from txn_ts.

    Args:
        df: Input dataframe with txn_ts column.

    Returns:
        : Dataframe with derived txn_date column.
    """
    txn_date_df = df.withColumn("txn_date", F.to_date("txn_ts"))
    return txn_date_df


def apply_cleaning_functions(df, functions):
    """
    Apply a set of cleaning functions to a dataframe.

    Args:
        df (pyspark.sql.DataFrame): Input dataframe to be cleaned.
        functions (set): Set of cleaning functions to apply.

    Returns:
        pyspark.sql.DataFrame: Cleaned dataframe.
    """
    for func in functions:
        df = func(df)
    return df


CLEANING_FUNCTIONS = {
    cast_dtypes,
    normalise_strings,
    deduplicate_df,
    derive_txn_date,
}

# -----------------------------
# Main
# -----------------------------
raw_txn_df = glueContext.create_dynamic_frame.from_catalog(
    database="payments_db", table_name="raw_transactions"
).toDF()

bronze_df = apply_cleaning_functions(raw_txn_df, CLEANING_FUNCTIONS)

(
    bronze_df.write.mode("append")
    .partitionBy("txn_date")
    .parquet("s3://payments-lake-jordanpacho/bronze/transactions_parquet/")
)

# -----------------------------
# Commit job
# -----------------------------
job.commit()
