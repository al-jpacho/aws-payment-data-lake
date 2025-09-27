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
# Silver Validation Functions
# -----------------------------

def validate_amount(df):
    """
    Validates the 'amount' column in a DataFrame.
    This function filters the DataFrame to include only rows where the 'amount' 
    is greater than 0 and is not null.
    Args:
        df (DataFrame): The input DataFrame containing a column named 'amount'.
    Returns:
        DataFrame: A filtered DataFrame containing only valid rows based on the 
        'amount' column criteria.
    """

    valid_df = df.where((F.col("amount") > 0) & F.col("amount").isNotNull())

    return valid_df

def validate_currency_types(df):
    """
    Validate the currency types in the given DataFrame.
    This function filters the DataFrame to include only rows where the 
    'currency' column contains one of the valid currency codes: 
    'USD', 'EUR', 'GBP', 'JPY', 'AUD', or 'CAD'.
    Args:
        df (DataFrame): The input DataFrame containing a 'currency' column.
    Returns:
        DataFrame: A DataFrame containing only the rows with valid currency types.
    """

    valid_df = df.where(F.col("currency").isin("USD", "EUR", "GBP", "JPY", "AUD", "CAD"))

    return valid_df

def validate_txn_statuses(df): 
    """
    Validate transaction statuses in a DataFrame.
    This function filters the input DataFrame to include only rows 
    where the 'status' column contains valid transaction statuses.
    Args:
        df (DataFrame): The input DataFrame containing transaction data.
    Returns:
        DataFrame: A DataFrame containing only the rows with valid statuses.
    """

    valid_df = df.where(F.col("status").isin(
        ["AUTHORISED", "SETTLED", "REFUNDED", "CHARGEBACK", "DECLINED", "PENDING", "SUCCESS", "FAILED"]
        ))

    return valid_df

def curate_status(df):
    """
    Map raw transaction statuses into curated categories 
    for downstream analytics, while preserving raw status.

    Args:
        df (DataFrame): Input DataFrame with a 'status' column.

    Returns:
        DataFrame: DataFrame with an additional 'status_curated' column.
    """
    mapping_expr = (
        F.when(F.col("status") == "AUTHORISED", "PENDING")
         .when(F.col("status") == "SETTLED", "SUCCESS")
         .when(F.col("status") == "REFUNDED", "REFUNDED")
         .when(F.col("status") == "CHARGEBACK", "FAILED")
         .when(F.col("status") == "DECLINED", "FAILED")
         .when(F.col("status") == "PENDING", "PENDING")
         .when(F.col("status") == "SUCCESS", "SUCCESS")
         .when(F.col("status") == "FAILED", "FAILED")
         .otherwise("UNKNOWN")
    )

    return df.withColumn("status_curated", mapping_expr)

def validate_bronze_df(df):
    """
    Apply all validation functions to the input DataFrame and curates the statuses. 
    This function filters the DataFrame based on amount, currency types, and transaction statuses.
    
    Args:
        df (DataFrame): The input DataFrame containing transaction data.
    
    Returns:
        DataFrame: A DataFrame that has been filtered based on the validation criteria.
    """
    df = validate_amount(df)  
    df = validate_currency_types(df) 
    df = validate_txn_statuses(df) 
    df = curate_status(df)
    return df  

# -----------------------------
# Main
# -----------------------------

bronze_df = glueContext.create_dynamic_frame.from_catalog(
    database="payments_db",
    table_name="bronze_transactions"
).toDF()

silver_df = validate_bronze_df(bronze_df)

(
    silver_df
    .write
    .mode("append")
    .partitionBy("txn_date")
    .parquet("s3://payments-lake-jordanpacho/silver/transactions_parquet/")
)


# -----------------------------
# Commit job
# -----------------------------
job.commit()