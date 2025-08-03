import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

try:
    df_supply_chain = pd.read_csv('supply_chain_data.csv')
    print("Data loaded successfully for second clustering task.")

    features_logistics = ['Shipping costs', 'Manufacturing costs', 'Production volumes']
    X_logistics = df_supply_chain[features_logistics]

    scaler = StandardScaler()
    X_logistics_scaled = scaler.fit_transform(X_logistics)

    inertia = []
    range_k = range(1, 11)
    for k in range_k:
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        kmeans.fit(X_logistics_scaled)
        inertia.append(kmeans.inertia_)

    plt.figure(figsize=(10, 6))
    plt.plot(range_k, inertia, marker='o')
    plt.title('Elbow Method for Logistics & Costs Clusters')
    plt.xlabel('Number of Clusters (k)')
    plt.ylabel('Inertia (Sum of squared distances)')
    plt.grid(True)
    plt.show()
    
    optimal_k = 3
    kmeans_model = KMeans(n_clusters=optimal_k, random_state=42, n_init=10)
    df_supply_chain['Logistics_Cluster'] = kmeans_model.fit_predict(X_logistics_scaled)
    print(f"\nK-Means model trained with k={optimal_k}.")

    cluster_summary = df_supply_chain.groupby('Logistics_Cluster')[features_logistics].mean()
    print("\nCluster Characteristics (Average values of features):\n")
    print(cluster_summary.to_markdown(numalign="left", stralign="left"))

    plt.figure(figsize=(10, 7))
    sns.scatterplot(x='Shipping costs', y='Manufacturing costs', hue='Logistics_Cluster', data=df_supply_chain, palette='viridis', s=100)
    plt.title(f'K-Means Clusters (k={optimal_k})')
    plt.xlabel('Shipping costs')
    plt.ylabel('Manufacturing costs')
    plt.legend(title='Cluster')
    plt.grid(True)
    plt.show()

except FileNotFoundError:
    print("Error: The file 'supply_chain_data.csv' was not found.")