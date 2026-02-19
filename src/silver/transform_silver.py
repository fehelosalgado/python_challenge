import pandas as pd
import json
import os

def process_bronze_to_silver():
    """
    Cleans, normalizes, and types data from the Bronze layer to the Silver layer.

    The function reads the raw JSON file, extracts the 'issues' list, and flattens 
    nested structures (analyst and dates). It performs type conversion to 
    datetime, removes inconsistent records, and exports the result in 
    columnar Parquet format to optimize storage and performance.

    Args:
        Does not receive direct arguments. Relies on the existence of the file:
        - 'data/bronze/bronze_issues.json'

    Returns:
        pd.DataFrame: Processed DataFrame containing the final layout with 
            selected columns (issue_id, issue_type, status, priority, analyst, 
            created_at, resolved_at).
    """

    print("Starting silver layer processing...")

    # Source file path
    path_input = os.path.join("data", "bronze", "bronze_issues.json")

    # Read the source file
    with open(path_input, 'r', encoding='utf-8') as f:
        data_source = json.load(f)

    # Generate a dataframe from the 'issues' list within the JSON
    df = pd.json_normalize(
        data=data_source, 
        record_path=['issues']
    )

    # Create new columns in the dataframe
    # Extracts the analyst name from the first element of the 'assignee' list
    df['analyst'] = df['assignee'].str[0].str.get('name')    
    # Extracts the ticket creation date from the first element of the 'timestamps' list
    df['created_at'] = df['timestamps'].str[0].str.get('created_at')    
    # Extracts the ticket resolution date from the first element of the 'timestamps' list
    df['resolved_at'] = df['timestamps'].str[0].str.get('resolved_at')

    # Conversion from str to datetime: any non-convertible value will be set as NaT (coerce)
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['resolved_at'] = pd.to_datetime(df['resolved_at'], errors='coerce')

    # Remove tickets with missing creation dates
    df = df.dropna(subset=['created_at'])

    # Selecting desired columns for the final layout
    columns_to_keep = [
        'id', 'issue_type', 'status', 'priority', 'analyst', 'created_at', 'resolved_at'
    ]

    # Rename columns mapping
    columns_rename = {
        'id': 'issue_id'
    }

    # Rename columns mapping
    df_silver = df[columns_to_keep].rename(columns=columns_rename)

    # Destination file path
    output_path = os.path.join("data", "silver", "silver_issues.parquet")

    # Save the dataframe locally in Parquet format
    df_silver.to_parquet(output_path, index=False)
    
    print(f"    File saved at: {output_path}")
    print("Silver layer successfully finished.")

    # Return the final dataframe
    return df_silver