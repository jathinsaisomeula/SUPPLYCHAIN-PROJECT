import pandas as pd
import numpy as np

try:
    df_supply_chain = pd.read_csv('supply_chain_data.csv')
    print("Data loaded successfully for Step 2.")

    duplicate_rows = df_supply_chain.duplicated().sum()
    print(f"Number of duplicate rows: {duplicate_rows}")

    numerical_features = df_supply_chain.select_dtypes(include=np.number).columns
    categorical_features = df_supply_chain.select_dtypes(include=['object', 'category']).columns

    print(f"\nNumerical features ({len(numerical_features)}):")
    print(list(numerical_features))

    print(f"\nCategorical features ({len(categorical_features)}):")
    print(list(categorical_features))

except FileNotFoundError:
    print("Error: The file 'supply_chain_data.csv' was not found.")