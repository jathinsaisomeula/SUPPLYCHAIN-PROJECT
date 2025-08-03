import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

try:
    df_supply_chain = pd.read_csv('supply_chain_data.csv')
    print("Data loaded successfully for K-Means.")

    features_sales = ['Price', 'Number of products sold', 'Revenue generated']
    X_sales = df_supply_chain[features_sales]

    scaler = StandardScaler()
    X_sales_scaled = scaler.fit_transform(X_sales)

    inertia = []
    range_k = range(1, 11)
    for k in range_k:
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        kmeans.fit(X_sales_scaled)
        inertia.append(kmeans.inertia_)

    plt.figure(figsize=(10, 6))
    plt.plot(range_k, inertia, marker='o')
    plt.title('Elbow Method for Sales & Revenue Clusters')
    plt.xlabel('Number of Clusters (k)')
    plt.ylabel('Inertia (Sum of squared distances)')
    plt.grid(True)
    plt.show()

except FileNotFoundError:
    print("Error: The file 'supply_chain_data.csv' was not found.")