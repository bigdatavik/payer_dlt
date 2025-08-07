# Databricks notebook source
# DBTITLE 1,Bronze Layer Table Definitions and Parameters
import dlt
from pyspark.sql.functions import (
    trim, col, upper, lower, regexp_replace, round as spark_round, countDistinct,
    sum as spark_sum, max as spark_max, min as spark_min
)

# -------------------
# PARAMETERS
# -------------------
catalog_name = spark.conf.get("conf.catalog_name")
bronze_schema_name = spark.conf.get("conf.bronze_schema_name")

# Fully qualified table names for bronze tables
BRONZE_TABLES = {
    "members_raw": f"{catalog_name}.{bronze_schema_name}.members_raw",
    "claims_raw": f"{catalog_name}.{bronze_schema_name}.claims_raw",
    "providers_raw": f"{catalog_name}.{bronze_schema_name}.providers_raw",
    "diagnoses_raw": f"{catalog_name}.{bronze_schema_name}.diagnoses_raw",
    "procedures_raw": f"{catalog_name}.{bronze_schema_name}.procedures_raw",
}

# COMMAND ----------

# DBTITLE 1,Silver Layer Data Transformation Logic
# -------------------
# SILVER LAYER
# -------------------

@dlt.table(name="members")
def payor_silver_members():
    df = dlt.read_stream(BRONZE_TABLES["members_raw"])
    return (
        df.filter(col("member_id").isNotNull())
        .selectExpr(
            "cast(member_id as string) as member_id",
            "trim(first_name) as first_name",
            "trim(last_name) as last_name",
            "cast(birth_date as date) as birth_date",
            "gender", "plan_id",
            "cast(effective_date as date) as effective_date"
        ).distinct()
    )

@dlt.table(name="claims")
def payor_silver_claims():
    df = dlt.read_stream(BRONZE_TABLES["claims_raw"])
    return (
        df.filter((col("claim_id").isNotNull()) & (col("total_charge") > 0))
        .select(
            "claim_id", "member_id", "provider_id",
            col("claim_date").cast("date").alias("claim_date"),
            spark_round("total_charge", 2).alias("total_charge"),
            lower(col("claim_status")).alias("claim_status")
        ).distinct()
    )

@dlt.table(name="providers")
def payor_silver_providers():
    df = dlt.read_stream(BRONZE_TABLES["providers_raw"])
    return (
        df.filter(col("provider_id").isNotNull())
        .select("provider_id", "npi", "provider_name", "specialty",
                "address", "city", "state").distinct()
    )

@dlt.table(name="diagnoses")
def payor_silver_diagnoses():
    df = dlt.read_stream(BRONZE_TABLES["diagnoses_raw"])
    return (
        df.filter(col("claim_id").isNotNull() & col("diagnosis_code").isNotNull())
        .select("claim_id", upper(col("diagnosis_code")).alias("diagnosis_code"),
                trim(col("diagnosis_desc")).alias("diagnosis_desc")
        ).distinct()
    )

# @dlt.table(name="procedures")
# def payor_silver_procedures():
#     df = dlt.read_stream(BRONZE_TABLES["procedures_raw"])
#     return (
#         df.filter((col("claim_id").isNotNull()) & (col("procedure_code").isNotNull()) & (col("amount") > 0))
#         .select("claim_id", upper(col("procedure_code")).alias("procedure_code"),
#                 "procedure_desc", "amount"
#         ).distinct()
#     )

@dlt.table(name="procedures")
def payor_silver_procedures():
    df = dlt.read_stream(BRONZE_TABLES["procedures_raw"])
    return (
        df.filter((col("claim_id").isNotNull()) & (col("procedure_code").isNotNull()))
        .select(
            "claim_id",
            upper(col("procedure_code")).alias("procedure_code"),
            "procedure_desc",
            regexp_replace(col("amount"), "\\$", "").cast("DOUBLE").alias("amount")
        ).distinct()
    )
