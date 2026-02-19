
---

## Dicionário de Dados

### Tabela Final – SLA por Chamado: `gold_sla_issues.csv`

**Descrição:** Tabela detalhada por chamado, contendo todos os indicadores de performance calculados.

**Localização:** `data/gold/`

**Formato:** CSV (Separador: `;`)

| Campo | Descrição |
|--------------------|-------------------------------------------|
| issue_id           | ID do chamado                             |
| issue_type         | Tipo do chamado                           |
| priority           | Prioridade                                |
| analyst            | Analista responsável                      |
| created_at         | Data de abertura                          |
| resolved_at        | Data de resolução                         |
| resolution_hours   | Tempo de resolução em horas úteis         |
| sla_expected_hours | SLA esperado (em horas)                   |
| is_sla_met         | Indicador de SLA atendido ou não atendido |

### SLA Médio por Analista: `gold_sla_by_analyst.csv`

**Descrição:** Relatório agregado para avaliar a produtividade e eficiência por analista.

**Localização:** `data/gold/`

**Agrupamento:** `analyst`

| Campo | Descrição |
|--------------------|-------------------------------------------|
| analyst            | Analista                                  |
| issue_quantity     | Quantidade de chamados                    |
| sla_mean_hours     | SLA médio (em horas)                      |

### SLA Médio por Tipo de Chamado: `gold_sla_by_issue_type.csv`

**Descrição:** Relatório agregado para identificar quais tipos de chamados consomem mais tempo.

**Localização:** `data/gold/`

**Agrupamento:** `issue_type`

| Campo | Descrição |
|--------------------|-------------------------------------------|
| issue_type         | Tipo do chamado                           |
| issue_quantity     | Quantidade de chamados                    |
| sla_mean_hours     | SLA médio (em horas)                      |
