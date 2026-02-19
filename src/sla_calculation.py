"""
Script containing the necessary functions to implement SLA calculation logic.
"""
import pandas as pd
import requests

def get_holidays(year_list):
    """
    Fetches Brazilian national holidays via public API for multiple years.

    Args:
        year_list (list[int]): List of years (e.g., [2023, 2024]) for which 
            holidays should be retrieved.

    Returns:
        list[str]: A list of strings containing holiday dates in 
            'YYYY-MM-DD' format.
    """

    # List to store all holidays
    all_holidays = []

    # For each year in the provided list (year_list)
    for year in year_list:
        # Get request status
        response = requests.get(f"https://brasilapi.com.br/api/feriados/v1/{year}")
        # If status = 200 (OK), proceed
        if response.status_code == 200:
            # Build a list with the 'date' value from each element returned by the request
            holidays = [f['date'] for f in response.json()]
            # Add each year's holiday list to the base list (all_holidays)
            all_holidays.extend(holidays)
    # Returns the list of all holidays
    return all_holidays

def calculate_working_hours(start_date, end_date, holidays_issues):    
    """
    Calculates the difference in hours between two dates considering only business days and holidays.

    The function ignores weekends and the provided holidays. The calculation is based 
    on 24-hour business days. If dates are on the same day, it calculates the direct difference.
    For longer periods, it adds proportional hours from the first and last day to the 
    full intermediate days.

    Args:
        start_date (pd.Timestamp): Start date and time (ticket opening).
        end_date (pd.Timestamp): End date and time (ticket resolution).
        holidays_issues (list[str]): List of holiday dates in 'YYYY-MM-DD' format.

    Returns:
        int: Total working hours rounded to the nearest integer, or None if any date is null.
    """

    # Returns None if any of the dates are null
    if pd.isna(start_date) or pd.isna(end_date):
        return None

    # Returns a list of dates between opening and closing dates - business days -, 
    # ignoring Saturdays, Sundays, and provided holidays
    days = pd.bdate_range(start=start_date, end=end_date, freq='C', holidays=holidays_issues)

    # If opened and closed on the same business day
    if len(days) <= 1:
        # Difference between start and end time
        diff = end_date - start_date
        # Convert difference to seconds (total_seconds()), then to hours (1h = 3600s) 
        # and ensure result is not negative (in case end time is before start time)
        return max(0, diff.total_seconds() / 3600)

    # Calculation: (1st day hours) + (Intermediate days * 24h) + (Last day hours)

    # Amount of hours for intermediate days, excluding opening and closing days (-2)
    # We consider 24 working hours per weekday
    total_hours = (len(days) - 2) * 24

    # Amount of hours for the opening day, until midnight
    first_day_end = start_date.replace(hour=23, minute=59, second=59)
    total_hours += (first_day_end - start_date).total_seconds() / 3600

    # Amount of hours for the closing day, since midnight
    last_day_start = end_date.replace(hour=0, minute=0, second=0)
    total_hours += (end_date - last_day_start).total_seconds() / 3600

    # Returns total working hours
    return int(round(total_hours, 0))

def define_expected_sla(priority):
    """
    Returns the SLA in hours according to the business rule based on priority.

    The function uses a fixed mapping where each priority level (High, Medium, Low)
    has a defined hour limit for ticket resolution. If the priority is 
    not found in the mapping, the function returns None.

    Args:
        priority (str): The ticket priority level (e.g., 'High', 'Medium', 'Low').

    Returns:
        int: Corresponding SLA hour limit or None if priority is invalid.
    """

    regras = {
        'High': 24,
        'Medium': 72,
        'Low': 120
    }
    return regras.get(priority)

def verify_sla_status(spent_hours, expected_hours):
    """
    Indicates whether the SLA was met or violated by comparing spent hours with the expected limit.

    The function performs a simple logical comparison: if the actual hours spent are less 
    than or equal to the limit defined for that priority, the SLA is considered 'Met' 
    (True). Otherwise, it is considered 'Violated' (False).

    Args:
        spent_hours (float | int): Total working hours calculated for resolution.
        expected_hours (int): Expected SLA hour limit for the ticket.

    Returns:
        bool: True if SLA was met, False if breached. 
            Returns None if spent_hours value is null.
    """

    if spent_hours is None:
        return None
    return True if spent_hours <= expected_hours else False