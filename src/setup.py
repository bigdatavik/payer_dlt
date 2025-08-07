# Databricks notebook source
dbutils.widgets.text("catalog", "quickstart_catalog_vkm_external", "Catalog")
dbutils.widgets.text("bronze_db", "payor_bronze", "Bronze DB")
dbutils.widgets.text("silver_db", "payor_silver", "Silver DB")
dbutils.widgets.text("gold_db", "payor_gold", "Gold DB")

catalog = dbutils.widgets.get("catalog")
bronze_db = dbutils.widgets.get("bronze_db")
silver_db = dbutils.widgets.get("silver_db")
gold_db = dbutils.widgets.get("gold_db")

path = f"/Volumes/{catalog}/{bronze_db}/payor/files/"

# COMMAND ----------

print(f"Catalog: {catalog}")
print(f"Bronze DB: {bronze_db}")
print(f"Silver DB: {silver_db}")
print(f"Silver DB: {silver_db}")
print(f"Gold DB: {gold_db}")
print(f"Path: {path}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {bronze_db}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_db}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {gold_db}")

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {bronze_db}.payor")

# COMMAND ----------

# Create the volume and folders
dbutils.fs.mkdirs(f"/Volumes/{catalog}/{bronze_db}/payor/files/claims")
dbutils.fs.mkdirs(f"/Volumes/{catalog}/{bronze_db}/payor/files/diagnosis")
dbutils.fs.mkdirs(f"/Volumes/{catalog}/{bronze_db}/payor/files/procedures")
dbutils.fs.mkdirs(f"/Volumes/{catalog}/{bronze_db}/payor/files/members")
dbutils.fs.mkdirs(f"/Volumes/{catalog}/{bronze_db}/payor/files/providers")
dbutils.fs.mkdirs(f"/Volumes/{catalog}/{bronze_db}/payor/downloads")

# COMMAND ----------

import requests
import zipfile
import io
import os
import shutil

# Define the URL of the ZIP file
url = "https://github.com/bigdatavik/notebookassets/blob/6ca9ed60c60e37b6e98d90cb7817746e7880e170/common/Payor_Archive.zip?raw=true"

# Download the ZIP file
response = requests.get(url)
zip_file = zipfile.ZipFile(io.BytesIO(response.content))

# Define the base path
base_path = f"/Volumes/{catalog}/{bronze_db}/payor/downloads" 

# Extract the ZIP file to the base path
zip_file.extractall(base_path)

# Define the paths
paths = {
    "claims.csv": f"{base_path}/claims",
    "diagnoses.csv": f"{base_path}/diagnosis",
    "procedures.csv": f"{base_path}/procedures",
    "member.csv": f"{base_path}/members",
    "providers.csv": f"{base_path}/providers"
}

# Create the destination directories if they do not exist
for dest_path in paths.values():
    os.makedirs(dest_path, exist_ok=True)

# Move the files to the respective directories
for file_name, dest_path in paths.items():
    source_file = f"{base_path}/{file_name}"
    if os.path.exists(source_file):
        os.rename(source_file, f"{dest_path}/{file_name}")



# COMMAND ----------

# Copy the files to the specified directories and print the paths
shutil.copy(f"{base_path}/claims/claims.csv", f"/Volumes/{catalog}/{bronze_db}/payor/files/claims/claims.csv")
print(f"Copied to /Volumes/{catalog}/{bronze_db}/payor/files/claims/claims.csv")

shutil.copy(f"{base_path}/diagnosis/diagnoses.csv", f"/Volumes/{catalog}/{bronze_db}/payor/files/diagnosis/diagnosis.csv")
print(f"Copied to /Volumes/{catalog}/{bronze_db}/payor/files/diagnosis/diagnosis.csv")

shutil.copy(f"{base_path}/procedures/procedures.csv", f"/Volumes/{catalog}/{bronze_db}/payor/files/procedures/procedures.csv")
print(f"Copied to /Volumes/{catalog}/{bronze_db}/payor/files/procedures/procedures.csv")

shutil.copy(f"{base_path}/members/member.csv", f"/Volumes/{catalog}/{bronze_db}/payor/files/members/members.csv")
print(f"Copied to /Volumes/{catalog}/{bronze_db}/payor/files/members/members.csv")

shutil.copy(f"{base_path}/providers/providers.csv", f"/Volumes/{catalog}/{bronze_db}/payor/files/providers/providers.csv")
print(f"Copied to /Volumes/{catalog}/{bronze_db}/payor/files/providers/providers.csv")
