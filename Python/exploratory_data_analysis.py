import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

try:
    df_supply_chain = pd.read_csv('supply_chain_data.csv')
    print("Data loaded successfully for Step 3.")

    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df_supply_chain, x='Number of products sold', y='Revenue generated', hue='Product type')
    plt.title('Relationship Between Revenue and Products Sold')
    plt.xlabel('Number of products sold')
    plt.ylabel('Revenue generated')
    plt.grid(True)
    plt.show()

    plt.figure(figsize=(10, 6))
    sns.barplot(data=df_supply_chain, x='Product type', y='Price')
    plt.title('Average Price by Product Type')
    plt.xlabel('Product Type')
    plt.ylabel('Average Price')
    plt.show()

except FileNotFoundError:
    print("Error: The file 'supply_chain_data.csv' was not found.")