# Databricks notebook source
# DBTITLE 1,Bronze Layer Table Definitions and Parameters
import dlt
from pyspark.sql.functions import (
    trim, col, upper, regexp_replace, lower, round as spark_round, countDistinct,
    sum as spark_sum, max as spark_max, min as spark_min
)

# -------------------
# PARAMETERS
# -------------------
catalog_name = spark.conf.get("conf.catalog_name")
silver_schema_name = spark.conf.get("conf.silver_schema_name")

# Fully qualified table names for silver tables
SILVER_TABLES = {
    "members": f"{catalog_name}.{silver_schema_name}.members",
    "claims": f"{catalog_name}.{silver_schema_name}.claims",
    "providers": f"{catalog_name}.{silver_schema_name}.providers",
    "diagnoses": f"{catalog_name}.{silver_schema_name}.diagnoses",
    "procedures": f"{catalog_name}.{silver_schema_name}.procedures",
}

# COMMAND ----------

# DBTITLE 1,Silver Layer Data Transformation Logic
# -------------------
# GOLD LAYER
# -------------------

@dlt.table(name="claims_enriched")
def claims_enriched1():
    claims = dlt.read(SILVER_TABLES["claims"])
    members = dlt.read(SILVER_TABLES["members"])
    providers = dlt.read(SILVER_TABLES["providers"])
    diagnoses = dlt.read(SILVER_TABLES["diagnoses"])
    
    # Change 'on' clause as needed depending on your data model. 
    # Here assuming: one diagnosis per claim (if multiple, see note below).

    return (
        claims
        .join(members, "member_id", "left")
        .join(providers, "provider_id", "left")
        .join(diagnoses, "claim_id", "left")
        .select(
            "claim_id", "claim_date", "total_charge", "claim_status",
            "member_id", "first_name", "last_name", "gender", "plan_id",
            "provider_id", "provider_name", "specialty", "city", "state",
            "diagnosis_code", "diagnosis_desc"  # Fields from diagnoses table
        )
    )


@dlt.table(name="member_claim_summary")
def payor_gold_member_claim_summary():
    claims = dlt.read(SILVER_TABLES["claims"])
    return (
        claims.groupBy("member_id")
              .agg(
                  countDistinct("claim_id").alias("total_claims"),
                  spark_sum("total_charge").alias("sum_claims"),
                  spark_max("total_charge").alias("max_claim"),
                  spark_min("total_charge").alias("min_claim")
              )
    )

@dlt.table(name="diagnosis_frequency")
def payor_gold_diagnosis_frequency():
    diagnoses = dlt.read(SILVER_TABLES["diagnoses"])
    return (diagnoses.groupBy("diagnosis_code")
                    .agg(countDistinct("claim_id").alias("claim_count"))
    )

@dlt.table(name="procedure_agg")
def payor_gold_procedure_agg():
    procedures = dlt.read(SILVER_TABLES["procedures"])
    return (procedures.groupBy("procedure_code")
                     .agg(
                         countDistinct("claim_id").alias("claim_count"),
                         spark_sum("amount").alias("total_amount")
                     )
    )


