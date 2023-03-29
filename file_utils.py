import pandas as pd

def dataframe_to_csv(df, filename):
    df.to_csv(filename)

def csv_to_dataframe(filename):
    return pd.read_csv(filename)