import os
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

def download_jira_data():
    """
    Realiza o download de dados brutos do Jira armazenados no Azure Blob Storage.

    A função carrega credenciais de um arquivo de configuração local (.env), 
    autentica-se no Azure via Service Principal e extrai um arquivo JSON específico 
    de um container. O conteúdo é baixado como um fluxo de dados binários e 
    salvo fisicamente no diretório da camada 'Bronze'.

    Args:
        Não recebe argumentos diretos. Depende das variáveis de ambiente:
        - ACCOUNT_URL: URL da conta de armazenamento do Azure.
        - CONTAINER_NAME: Nome do container onde o blob está localizado.
        - BLOB_NAME: Nome exato do arquivo .json na nuvem.
        - AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET: Credenciais de acesso.

    Returns:
        None: A função salva o arquivo 'bronze_issues.json' no disco e 
            exibe mensagens de status no console.
    """

    print("Iniciando processamento da camada bronze...")

    # carrega variáveis do arquivo .env para a memória local
    load_dotenv()
    
    # credencias para autenticação no Azure
    # utiliza automaticamente as variáveis AZURE_TENANT_ID, AZURE_CLIENT_ID e AZURE_CLIENT_SECRET existentes no arquivo .env, já carregadas na memória por load_dotenv()
    credential_env = DefaultAzureCredential()

    # endereço da conta onde está o arquivo a ser baixado da nuvem
    account_url_env = os.getenv("ACCOUNT_URL") # ACCOUNT_URL: variável existente no arquivo .env

    # container onde está o arquivo
    container_env = os.getenv("CONTAINER_NAME") # CONTAINER_NAME: variável existente no arquivo .env

    # nome do arquivo, com extensão
    blob_env = os.getenv("BLOB_NAME") # BLOB_NAME: variável existente no arquivo .env

    # conexão com a nuvem    
    blob_service_client = BlobServiceClient(
        account_url=account_url_env,
        credential=credential_env
    )

    # aponta para o arquivo que vou baixar da nuvem
    blob_client = blob_service_client.get_blob_client(
        container=container_env,
        blob=blob_env
    )

    print(f"    Iniciando download: {blob_env}...")
    
    # cria conexão através de um stream objeto
    download_stream = blob_client.download_blob()
    
    # faz o download do arquivo na nuvem para a memória local
    data = download_stream.readall()
    
    # caminho do arquivo local a receber o conteúdo do arquivo da nuvem
    output_path = os.path.join("data", "bronze", "bronze_issues.json")
    
    # escreve o binário do arquivo da nuvem para dentro do arquivo físico local
    with open(output_path, "wb") as file:
        file.write(data)
        
    print(f"    Arquivo salvo em: {output_path}")
    print("Camada bronze finalizada com sucesso!")