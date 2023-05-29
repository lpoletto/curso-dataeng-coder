import pandas as pd


def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check si el dataframe está vacío
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False
    
    # Check si la Primary Key es única 
    if not df["played_at"].is_unique:
        raise Exception("A value from played_at is not unique")
    
    # Check si existen valores nulos
    if df.isnull().values.any():
        raise Exception("A value in df is null")
        
    return True
