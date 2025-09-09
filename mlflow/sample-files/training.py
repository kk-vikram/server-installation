import mlflow
import mlflow.sklearn
import os
import pandas as pd
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
import joblib

# --- MLflow setup ---
mlflow.set_tracking_uri("http://192.168.1.200:5000")
mlflow.set_experiment("Iris_Classification")

# Required for artifact handling (MinIO)
os.environ["AWS_ACCESS_KEY_ID"] = "minio"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minio123"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://192.168.1.200:9000"

def train():
    # Load dataset
    iris = load_iris()
    X = pd.DataFrame(iris.data, columns=iris.feature_names)
    y = pd.Series(iris.target, name="target")

    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # Save test data for testing script
    os.makedirs("test_data", exist_ok=True)
    X_test.to_csv("test_data/X_test.csv", index=False)
    y_test.to_csv("test_data/y_test.csv", index=False)

    # Start MLflow run
    with mlflow.start_run() as run:
        print(f"Started MLflow Run ID: {run.info.run_id}")

        # Train model
        model = LogisticRegression(max_iter=200)
        model.fit(X_train, y_train)

        # Log parameters and metrics
        mlflow.log_param("model_type", "LogisticRegression")
        mlflow.log_param("max_iter", 200)
        accuracy = model.score(X_test, y_test)
        mlflow.log_metric("test_accuracy", accuracy)

        # Save and log model
        joblib.dump(model, "iris_model.pkl")
        mlflow.log_artifact("iris_model.pkl")

        # Log model using MLflow model registry format
        mlflow.sklearn.log_model(model, artifact_path="model")

        print(f"Model logged to MLflow with run_id={run.info.run_id}")
        print(f"Test accuracy: {accuracy:.4f}")

if __name__ == "__main__":
    train()
