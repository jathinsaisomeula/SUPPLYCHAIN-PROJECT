import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

try:
    df_supply_chain = pd.read_csv('supply_chain_data.csv')
    print("K-Means model trained with k=2.")

    features_sales = ['Price', 'Number of products sold', 'Revenue generated']
    X_sales = df_supply_chain[features_sales]

    scaler = StandardScaler()
    X_sales_scaled = scaler.fit_transform(X_sales)

    optimal_k = 2
    kmeans_model = KMeans(n_clusters=optimal_k, random_state=42, n_init=10)
    df_supply_chain['Sales_Cluster'] = kmeans_model.fit_predict(X_sales_scaled)

    cluster_summary = df_supply_chain.groupby('Sales_Cluster')[features_sales].mean()
    print("\nCluster Characteristics (Average values of features):\n")
    print(cluster_summary.to_markdown(numalign="left", stralign="left"))

    plt.figure(figsize=(10, 7))
    sns.scatterplot(x='Number of products sold', y='Revenue generated', hue='Sales_Cluster', data=df_supply_chain, palette='viridis', s=100)
    plt.title(f'K-Means Clusters (k={optimal_k})')
    plt.xlabel('Number of products sold')
    plt.ylabel('Revenue generated')
    plt.legend(title='Cluster')
    plt.grid(True)
    plt.show()

except FileNotFoundError:
    print("Error: The file 'supply_chain_data.csv' was not found.")