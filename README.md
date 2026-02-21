
---

## Arquitetura do pipeline

A estrutura segue a Arquitetura Medalhão (Medallion Architecture), que é o padrão da indústria para organizar e transformar dados à medida que avançam pelas etapas bronze, silver e gold.

- **Camada BRONZE (Ingestão / Raw)**  
    - **Dado (`data/bronze/bronze_issues.json`):** Contém os dados brutos, exatamente como vieram da fonte.  
    - **Script (`src/bronze/ingest_bronze.py`):** Serve apenas para buscar o dado da nuvem Azure e salvá-lo sem nenhuma alteração.

- **Camada SILVER (Limpeza / Padronização)**  

    - **Dado (`data/silver/silver_issues.parquet`):** Aqui o dado já está em formato Parquet, que é binário, mais leve e rápido de ler.  
    - **Script (`src/silver/transform_silver.py`):** Faz a limpeza: renomeia colunas, remove duplicatas e garante que as colunas de data (created_at, resolved_at) sejam do tipo datetime.

- **Camada GOLD (Agregação / Negócio)**  

    - **Dado (`data/gold/`):** Contém uma tabela (gold_sla_issues.csv) e dois relatórios (gold_sla_by_analyst.csv e gold_sla_by_issue_type.csv), todos em formato CSV.  
    - **Script (`src/gold/build_gold.py`):** Aplica as regras de negócio, como o cálculo de SLA, e gera os 3 relatórios solicitados.

- **Script de Apoio**  

    - **`src/sla_calculation.py`:** Onde se encontram as funções utilizadas para cálculo do SLA na camada gold.  

- **Script de Orquestração**  

    - **`pipeline.py`:** Importa e executa os scripts de cada camada na ordem correta: Bronze(Ingestão) → Silver(Transformação) → Gold(Geração dos Relatórios).

---

## Lógica de cálculo do SLA

Calcula a diferença, em horas, entre duas datas (datetime), considerando dias úteis e desconsiderando sábados, domingos e feriados nacionais captados pela API: https://brasilapi.com.br/api/feriados/v1/{year}. O cálculo baseia-se 
em dias úteis de 24 horas. Se as datas estiverem no mesmo dia, calcula a diferença direta entre horário de abertura e horário de encerramento do chamado.
Para períodos maiores, soma as horas proporcionais do primeiro e último dia aos dias intermediários completos: Qtde de horas do 1º dia (contando do horário de abertura até meio-noite) + Qtde de dias intermediários * 24 + Qtde de horas do último dia (contando da meio-noite até o horário de encerramento).

---

## Dicionário de Dados

### Tabela Final – SLA por Chamado: `gold_sla_issues.csv`

**Descrição:** Tabela detalhada por chamado, contendo todos os indicadores de performance calculados.

**Localização:** `data/gold/`

**Formato:** CSV (Separador: `;`)

| Campo | Descrição | Tipo |
|-|-|-|
| issue_id           | ID do chamado                             | String |
| issue_type         | Tipo do chamado                           | String |
| priority           | Prioridade                                | String |
| analyst            | Analista responsável                      | String |
| created_at         | Data de abertura                          | DateTime |
| resolved_at        | Data de resolução                         | DateTime |
| resolution_hours   | Tempo de resolução em horas úteis         | Int |
| sla_expected_hours | SLA esperado (em horas)                   | Int |
| is_sla_met         | Indicador de SLA atendido ou não atendido | Boolean |

### SLA Médio por Analista: `gold_sla_by_analyst.csv`

**Descrição:** Relatório agregado para avaliar a produtividade e eficiência por analista.

**Localização:** `data/gold/`

**Agrupamento:** `analyst`

| Campo | Descrição | Tipo |
|-|-|-|
| analyst            | Analista                                  | String
| issue_quantity     | Quantidade de chamados                    | Int |
| sla_mean_hours     | SLA médio (em horas)                      | Float |

### SLA Médio por Tipo de Chamado: `gold_sla_by_issue_type.csv`

**Descrição:** Relatório agregado para identificar quais tipos de chamados consomem mais tempo.

**Localização:** `data/gold/`

**Agrupamento:** `issue_type`

| Campo | Descrição | Tipo |
|-|-|-|
| issue_type         | Tipo do chamado                           | String |
| issue_quantity     | Quantidade de chamados                    | Int |
| sla_mean_hours     | SLA médio (em horas)                      | Float |