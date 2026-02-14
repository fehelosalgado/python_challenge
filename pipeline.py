# import
from src.bronze.ingest_bronze import download_jira_data
from src.silver.transform_silver import process_bronze_to_silver
from src.gold.build_gold import process_silver_to_gold

###########################################
# camada bronze
###########################################

# baixa o arquivo json do blob para a camada bronze (traduzir para o ingles)
blob_name = download_jira_data()

###########################################
# camada silver
###########################################

# cria camada silver
df_silver = process_bronze_to_silver("bronze_issues.json")

###########################################
# camada gold
###########################################

# le parquet silver
#df_silver = pd.read_parquet(os.path.join("data", "silver", "silver_issues.parquet"))

df_gold = process_silver_to_gold(df_silver)

