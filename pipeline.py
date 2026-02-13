# import
from src.bronze.ingest_bronze import download_jira_data
from src.silver.transform_silver import process_bronze_to_silver
from src.sla_calculation import get_holidays
import numpy as np

# somente debug, pode remover antes de entregar o projeto
import pandas as pd
import os

###########################################
# camada bronze
###########################################

# baixa o arquivo json do blob para a camada bronze (traduzir para o ingles)
#blob_name = download_jira_data()

###########################################
# camada silver
###########################################

# cria camada silver
#df_silver = process_bronze_to_silver(blob_name)

###########################################
# camada gold
###########################################

df = pd.read_parquet(os.path.join("data", "silver", "silver_issues.parquet"))
year_created_at = df['created_at'].dt.year.dropna().astype(int).unique()
year_resolved_at = df['resolved_at'].dt.year.dropna().astype(int).unique()

year_list = np.union1d(year_created_at, year_resolved_at)

print(get_holidays(year_list))

