"""
Jira Data Pipeline Orchestrator: Bronze -> Silver -> Gold.

This script centralizes the execution of all ETL process stages:
1. Bronze: Downloads raw data from Azure Blob Storage.
2. Silver: Cleans, normalizes, and converts JSON data types.
3. Gold: Applies business rules (SLA), fetches holidays, and generates final reports.

The pipeline depends on a .env file configured with Azure credentials 
and an internet connection for the holidays API lookup.
"""

from src.bronze.ingest_bronze import download_jira_data
from src.silver.transform_silver import process_bronze_to_silver
from src.gold.build_gold import process_silver_to_gold

###########################################
# Bronze layer execution
###########################################

download_jira_data()

###########################################
# Silver layer execution
###########################################

df_silver = process_bronze_to_silver()

###########################################
# Gold layer execution
###########################################

process_silver_to_gold(df_silver)