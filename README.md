# Payer Medallion Lakehouse Training Project with Lakeflow Declarative Pipelines 🚀

This repo uses Databricks Lakeflow Declarative Pipelines for end-to-end, production-grade ETL with minimal operational overhead. The project is perfect for data engineers, analysts, and healthcare professionals looking to ramp up on both modern lakehouse technology and payer-specific analytics.

## Setting up & running

> [!IMPORTANT]
This bundle uses Serverless compute, so make sure that it's enabled for your workspace (works on [Databricks Free Edition](https://www.databricks.com/blog/introducing-databricks-free-edition) as well). If it's not, then you need to adjust parameters of the job and DLT pipelines!

You can install the project two ways:

1. Using Databricks Assset Bundles (DABs) inside the Databricks Workspace (recommended):
1. Using DABs from the command line of your computer

### Setting it up using DABs in workspace

1. Create a [Git Folder](https://docs.databricks.com/aws/en/repos/) inside your Databricks workspace by cloning this repository.

2. Open the `payer_dlt/databricks.yaml` inside create Git Folder.

3. Adjust the following parameters inside the `databricks.yaml` (create necessary objects before use):

 - `catalog_name` - the name of the existing UC Catalog used in configuration.
 - `bronze_schema_name` - the name of an existing UC Schema to put raw data.
 - `silver_schema_name` - the name of an existing UC Schema to put tables with transformed data.
 - `gold_schema_name` - the name of an existing UC Schema to put tables with reporting data.

4. Click **Deploy** button in the **Deployments** tab on the left - this will create necessary jobs and pipelines

5. Click **Run** button next to the `DLT Payer Demo: Setup` job.

6. Click **Start pipeline** for DLT pipelines to process data and run detections (in the following order):

 - `DLT Payer Demo: Ingest Bronze data`
 - `DLT Payer Demo: Ingest Silver data`
 - `DLT Payer Demo: Ingest Gold data`

### Setting it up using DABs locally

1. Install the latest version of [Databricks CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html).

2. Authenticate to your Databricks workspace, if you have not done so already:

```sh
databricks configure
```

3. Set environment variable `DATABRICKS_CONFIG_PROFILE` to the name of Databricks CLI profile you configured, and configure necessary variables in the `dev` profile of `databricks.yml` file.  You need to specify the following (create necessary objects before use):

 - `catalog_name` - the name of the existing UC Catalog used in configuration.
 - `bronze_schema_name` - the name of an existing UC Schema to put raw data.
 - `silver_schema_name` - the name of an existing UC Schema to put tables with transformed data.
 - `gold_schema_name` - the name of an existing UC Schema to put tables with reporting data.

4. To deploy a development copy of this project, type:

```sh
databricks bundle deploy
```

5. Run a job to set up the normalized tables and download sample log files:

```sh
databricks bundle run dlt_payer_demo_setup
```

6. Run DLT pipelines to ingest data in bronze, silver and gold tiers:

```sh
databricks bundle run ingest_payer_bronze_data
databricks bundle run ingest_payer_bronze_data_silver
databricks bundle run ingest_payer_silver_data_gold
```

