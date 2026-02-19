'''
Script com as funções necessárias para implementar a lógica de cálculo de SLA
'''
import pandas as pd
import requests

def get_holidays(year_list):
    """
    Busca feriados nacionais brasileiros via API pública para múltiplos anos.

    Args:
        year_list (list[int]): Lista de anos (ex: [2023, 2024]) para os quais 
            deseja-se obter os feriados.

    Returns:
        list[str]: Uma lista de strings contendo as datas dos feriados 
            no formato 'YYYY-MM-DD'.
    """

    # lista para receber todos feriados
    all_holidays = []

    # para cada ano da lista fornecida (year_list)
    for year in year_list:
        # pegue o status da requisição
        response = requests.get(f"https://brasilapi.com.br/api/feriados/v1/{year}")
        # se status = 200 (OK), continue
        if response.status_code == 200:
            # monte uma lista com o valor de date de cada elemento da lista retornada pela requisição
            holidays = [f['date'] for f in response.json()]
            # adicione a lista de feriados de cada ano na lista base (all_holidays)
            all_holidays.extend(holidays)
    # retorna a lista de todos os feriados
    return all_holidays

def calculate_working_hours(start_date, end_date, holidays_issues):    
    """
    Calcula a diferença em horas entre duas datas considerando apenas dias úteis e feriados.

    A função desconsidera fins de semana e os feriados fornecidos. O cálculo baseia-se
    em dias úteis de 24 horas. Se as datas estiverem no mesmo dia, calcula a diferença direta.
    Para períodos maiores, soma as horas proporcionais do primeiro e último dia aos dias 
    intermediários completos.

    Args:
        start_date (pd.Timestamp): Data e hora de início (abertura do chamado).
        end_date (pd.Timestamp): Data e hora de fim (resolução do chamado).
        holidays_issues (list[str]): Lista de datas de feriados no formato 'YYYY-MM-DD'.

    Returns:
        int: Total de horas úteis arredondado para o inteiro mais próximo ou None se alguma das datas for nula.
    """

    # retorna None se alguma das datas for nula
    if pd.isna(start_date) or pd.isna(end_date):
        return None

    # retorna uma lista de datas entre data de abertura e data de fechamento - dias úteis -, desconsiderando sábado, domingo e 
    # os feriados fornecidos
    days = pd.bdate_range(start=start_date, end=end_date, freq='C', holidays=holidays_issues)

    # se abriu e fechou no mesmo dia útil
    if len(days) <= 1:
        # diferença entre hora inicial e final
        diff = end_date - start_date
        # converta a diferença em segundos (total_seconds()), depois em horas (1h tem 3600s) e garanta que resultado não seja 
        # negativo (caso hora final seja anterior a hora inicial)
        return max(0, diff.total_seconds() / 3600)

    # Cálculo: (Horas do 1º dia) + (Dias intermediários * 24h) + (Horas do último dia)

    # quantidade de horas dos dias intermediários, desconsiderando os dias de abertura e de encerramento do chamado (-2)
    # consideramos 24h úteis por dia de semana
    total_hours = (len(days) - 2) * 24

    # quantidade de horas do dia de abertura, até meia-noite
    first_day_end = start_date.replace(hour=23, minute=59, second=59)
    total_hours += (first_day_end - start_date).total_seconds() / 3600

    # quantidade de horas do dia de encerramento, desde a meia-noite
    last_day_start = end_date.replace(hour=0, minute=0, second=0)
    total_hours += (end_date - last_day_start).total_seconds() / 3600

    # retorna total de horas úteis
    return int(round(total_hours, 0))

def define_expected_sla(priority):
    """
    Retorna o SLA em horas conforme a regra de negócio baseada na prioridade.

    A função utiliza um mapeamento fixo onde cada nível de prioridade (High, Medium, Low)
    possui um limite de horas definido para a resolução do chamado. Caso a prioridade 
    não seja encontrada no mapeamento, a função retorna None.

    Args:
        priority (str): O nível de prioridade do chamado (ex: 'High', 'Medium', 'Low').

    Returns:
        int: O limite de horas de SLA correspondente ou None se a prioridade for inválida.
    """
    regras = {
        'High': 24,
        'Medium': 72,
        'Low': 120
    }
    return regras.get(priority)

def verify_sla_status(spent_hours, expected_hours):
    """
    Indica se o SLA foi atendido ou violado comparando as horas gastas com o limite esperado.

    A função realiza uma comparação lógica simples: se as horas reais gastas forem menores 
    ou iguais ao limite definido para aquela prioridade, o SLA é considerado 'Atendido' 
    (True). Caso contrário, é considerado 'Violado' (False).

    Args:
        spent_hours (float | int): O total de horas úteis calculadas para a resolução.
        expected_hours (int): O limite de horas de SLA esperado para o chamado.

    Returns:
        bool: True se o SLA foi cumprido, False se foi estourado. 
            Retorna None se o valor de spent_hours for nulo.
    """

    if spent_hours is None:
        return None
    return True if spent_hours <= expected_hours else False