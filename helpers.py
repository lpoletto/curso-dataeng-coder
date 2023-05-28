import pandas as pd


def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False
    
    # Primary Key check
    if not df["played_at"].is_unique:
        raise Exception("A value from played_at is not unique")
    
    # Check for nulls
    if df.isnull().values.any():
        raise Exception("A value in df is null")
        
    return True