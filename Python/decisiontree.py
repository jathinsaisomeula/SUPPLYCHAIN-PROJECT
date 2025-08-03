import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.tree import DecisionTreeClassifier

try:
    df_supply_chain = pd.read_csv('supply_chain_data.csv')
    print("Data loaded successfully for Step 4.")

    X = df_supply_chain.drop('Inspection results', axis=1)
    y = df_supply_chain['Inspection results']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)
    print(f"\nTraining data shape: {X_train.shape}")
    print(f"Testing data shape: {X_test.shape}")

    numerical_features = X_train.select_dtypes(include=np.number).columns
    categorical_features = X_train.select_dtypes(include=['object', 'category']).columns

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), numerical_features),
            ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
        ])

    model_pipeline = Pipeline(steps=[('preprocessor', preprocessor),
                                     ('classifier', DecisionTreeClassifier(random_state=42))])

    model_pipeline.fit(X_train, y_train)

    print("\nDecision Tree Model Pipeline created and trained successfully.")

except FileNotFoundError:
    print("Error: The file 'supply_chain_data.csv' was not found.")