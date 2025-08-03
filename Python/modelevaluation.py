import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.tree import DecisionTreeClassifier, plot_tree

# Load the data again so this file can run independently
try:
    df_supply_chain = pd.read_csv('supply_chain_data.csv')

    X = df_supply_chain.drop('Inspection results', axis=1)
    y = df_supply_chain['Inspection results']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)
    
    numerical_features = X_train.select_dtypes(include=np.number).columns
    categorical_features = X_train.select_dtypes(include=['object', 'category']).columns

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), numerical_features),
            ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
        ])
    model_pipeline = Pipeline(steps=[('preprocessor', preprocessor),
                                     ('classifier', DecisionTreeClassifier(random_state=42, max_depth=5))])
    model_pipeline.fit(X_train, y_train)

    preprocessor.fit(X_train)
    feature_names_for_tree = preprocessor.get_feature_names_out()

    plt.figure(figsize=(15, 10))
    plot_tree(model_pipeline.named_steps['classifier'],
              feature_names=feature_names_for_tree,
              class_names=[str(c) for c in model_pipeline.named_steps['classifier'].classes_],
              filled=True,
              rounded=True,
              fontsize=8)
    plt.title('Decision Tree Visualization')
    plt.tight_layout()
    plt.show()
    plt.savefig('decision_tree.png')

    print("\nDecision Tree plot has been saved as 'decision_tree.png'.")

except FileNotFoundError:
    print("Error: The file 'supply_chain_data.csv' was not found.")