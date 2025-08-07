# Databricks notebook source
import dlt
from pyspark.sql.functions import (
    trim, col, upper, lower, round as spark_round, countDistinct,
    sum as spark_sum, max as spark_max, min as spark_min
)

catalog_name = spark.conf.get("conf.catalog_name")
bronze_schema_name = spark.conf.get("conf.bronze_schema_name")

BRONZE_PATHS = {
    "members": f"/Volumes/{catalog_name}/{bronze_schema_name}/payor/files/members/",
    "claims":  f"/Volumes/{catalog_name}/{bronze_schema_name}/payor/files/claims/",
    "providers": f"/Volumes/{catalog_name}/{bronze_schema_name}/payor/files/providers/",
    "diagnoses": f"/Volumes/{catalog_name}/{bronze_schema_name}/payor/files/diagnosis/",
    "procedures": f"/Volumes/{catalog_name}/{bronze_schema_name}/payor/files/procedures/",
}

FORMATS = {"members": "csv", "claims": "csv", "providers": "csv", "diagnoses": "csv", "procedures": "csv"}

SCHEMAS = {
    "members": "member_id INTEGER, first_name STRING, last_name STRING, birth_date DATE, gender STRING, plan_id STRING, effective_date DATE",
    "claims": "claim_id STRING, member_id STRING, provider_id STRING, claim_date DATE, total_charge DOUBLE, claim_status STRING",
    "providers": "provider_id STRING, npi STRING, provider_name STRING, specialty STRING, address STRING, city STRING, state STRING",
    "diagnoses": "claim_id STRING, diagnosis_code STRING, diagnosis_desc STRING",
    "procedures": "claim_id STRING, procedure_code STRING, procedure_desc STRING, amount STRING"
}

# -------------------
# BRONZE LAYER
# -------------------

@dlt.table(name='members_raw')
def payor_bronze_members_raw():
    return (
        spark.readStream
        .format(FORMATS["members"])
        .option("header", True)
        .schema(SCHEMAS["members"])
        .load(BRONZE_PATHS["members"])
    )

@dlt.table(name='claims_raw')
def payor_bronze_claims_raw():
    return (
        spark.readStream
        .format(FORMATS["claims"])
        .option("header", True)
        .schema(SCHEMAS["claims"])
        .load(BRONZE_PATHS["claims"])
    )

@dlt.table(name='providers_raw')
def payor_bronze_providers_raw():
    return (
        spark.readStream
        .format(FORMATS["providers"])
        .option("header", True)
        .schema(SCHEMAS["providers"])
        .load(BRONZE_PATHS["providers"])
    )

@dlt.table(name='diagnoses_raw')
def payor_bronze_diagnoses_raw():
    return (
        spark.readStream
        .format(FORMATS["diagnoses"])
        .option("header", True)
        .schema(SCHEMAS["diagnoses"])
        .load(BRONZE_PATHS["diagnoses"])
    )

@dlt.table(name='procedures_raw')
def payor_bronze_procedures_raw():
    return (
        spark.readStream
        .format(FORMATS["procedures"])
        .option("header", True)
        .schema(SCHEMAS["procedures"])
        .load(BRONZE_PATHS["procedures"])
    )

