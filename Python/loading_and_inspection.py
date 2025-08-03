import pandas as pd

try:
    df_supply_chain = pd.read_csv('supply_chain_data.csv')
    print("Data loaded successfully.")

    print("\nFirst 5 rows of the dataset:")
    print(df_supply_chain.head().to_markdown(index=False, numalign="left", stralign="left"))

    print("\nInformation about the dataset:")
    df_supply_chain.info()

except FileNotFoundError:
    print("Error: The file 'supply_chain_data.csv' was not found. Please ensure it is in the correct directory.")