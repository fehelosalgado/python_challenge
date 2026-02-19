import pandas as pd
import os
import numpy as np
from src.sla_calculation import get_holidays, calculate_working_hours, define_expected_sla, verify_sla_status

def process_silver_to_gold(df_silver):
    """
    Transforms Silver layer data into business indicators in the Gold layer.

    The function filters only finished tickets, identifies the involved years to 
    fetch national holidays via API, and calculates the actual SLA in working hours. 
    Additionally, it assigns the expected SLA by priority and verifies goal compliance. 
    Finally, it generates and exports three CSV files: the detailed ticket-by-ticket 
    base and two aggregated reports (by analyst and by issue type).

    Args:
        df_silver (pd.DataFrame): DataFrame from the Silver layer, already cleaned 
            and with date columns properly typed.

    Returns:
        None: The function performs physical saving of multiple CSV files in the 
            'data/gold' directory and outputs results through local files.
    """
    
    print("Starting gold layer processing...")

    # Creates a new dataframe (.copy()), filtering only finished tickets (Done or Resolved)
    df_gold = df_silver[df_silver['status'].isin(['Done', 'Resolved'])].copy()

    # Holiday list creation
    # List of distinct years from the 'created_at' field
    year_created_at = df_gold['created_at'].dt.year.dropna().astype(int).unique()
    # List of distinct years from the 'resolved_at' field
    year_resolved_at = df_gold['resolved_at'].dt.year.dropna().astype(int).unique()
    # Joins all distinct years, combining the two lists above
    year_list = np.union1d(year_created_at, year_resolved_at)
    
    print("    Fetching holidays...")
    # Holiday list
    holidays = get_holidays(year_list)

    print("    Calculating resolution time in working hours...")
    # Create new columns in df_gold
    # Resolution time in working hours
    df_gold['resolution_hours'] = df_gold.apply(
        lambda row: calculate_working_hours(row['created_at'], row['resolved_at'], holidays), axis=1
    ).astype('Int64')

    print("    Calculating expected SLA (in hours)...")
    # Expected SLA (in hours)
    df_gold['sla_expected_hours'] = df_gold['priority'].apply(define_expected_sla)

    print("    Calculating SLA met vs breached indicator...")
    # SLA met or breached indicator
    df_gold['is_sla_met'] = df_gold.apply(
        lambda row: verify_sla_status(row['resolution_hours'], row['sla_expected_hours']), axis=1
    )

    # Reorder final dataframe columns
    df_gold = df_gold[['issue_id', 'issue_type', 'priority', 'analyst', 'created_at', 'resolved_at', 'resolution_hours', 'sla_expected_hours', 'is_sla_met']]

    ############################
    # expected output files
    ############################

    ############################
    # output 1: Final Table – SLA per Ticket
    print("    Generating output 1: Final Table – SLA per Ticket...")

    # Destination file path
    output_path = os.path.join("data", "gold", "gold_sla_issues.csv")

    # Saves the dataframe locally in CSV format
    df_gold.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')
    print(f"    File saved at: {output_path}")

    ############################
    # output 2: Average SLA per Analyst
    print("    Generating output 2: Average SLA per Analyst...")

    # Aggregation: Ticket count and average SLA (in hours) per analyst
    df_sla_by_analyst = df_gold.groupby('analyst').agg(
        issue_quantity=('issue_id', 'count'),
        sla_mean_hours=('resolution_hours', 'mean')
    ).reset_index().round({'sla_mean_hours': 2})

    # Destination file path
    output_path = os.path.join("data", "gold", "gold_sla_by_analyst.csv")

    # Saves the dataframe locally in CSV format
    df_sla_by_analyst.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')
    print(f"    File saved at: {output_path}")

    ############################
    # output 3: Average SLA per Issue Type
    print("    Generating output 3: Average SLA per Issue Type...")

    # Aggregation: Ticket count and average SLA (in hours) per issue type
    df_sla_by_issue_type = df_gold.groupby('issue_type').agg(
        issue_quantity=('issue_id', 'count'),
        sla_mean_hours=('resolution_hours', 'mean')
    ).reset_index().round({'sla_mean_hours': 2})

    # Destination file path
    output_path = os.path.join("data", "gold", "gold_sla_by_issue_type.csv")

    # Saves the dataframe locally in CSV format
    df_sla_by_issue_type.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')
    print(f"    File saved at: {output_path}")

    print("Gold layer successfully finished.")