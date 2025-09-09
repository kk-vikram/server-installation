import mlflow
import os
import pandas as pd
from sklearn.metrics import accuracy_score, classification_report

# --- MLflow setup ---
mlflow.set_tracking_uri("http://192.168.1.200:5000")

# Required for artifact handling (MinIO)
os.environ["AWS_ACCESS_KEY_ID"] = "minio"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minio123"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://192.168.1.200:9000"

def test_latest_model():
    experiment_name = "Iris_Classification"
    client = mlflow.tracking.MlflowClient()

    # Get experiment
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment is None:
        print(f"Experiment {experiment_name} not found!")
        return

    # Get latest run
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["attributes.start_time DESC"],
        max_results=1,
    )
    if not runs:
        print("No runs found!")
        return

    run = runs[0]
    run_id = run.info.run_id
    print(f"Using latest run_id: {run_id}")

    # Load model
    logged_model_uri = f"runs:/{run_id}/model"
    try:
        model = mlflow.sklearn.load_model(logged_model_uri)
    except Exception as e:
        print(f"Error loading model: {e}")
        return

    # Load test data
    try:
        X_test = pd.read_csv("test_data/X_test.csv")
        y_test = pd.read_csv("test_data/y_test.csv")
    except FileNotFoundError:
        print("Error: test data not found. Run training first!")
        return

    # Evaluate
    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)

    print("\n--- Test Results ---")
    print(f"Test Accuracy: {acc:.4f}")
    print("\nClassification Report:")
    print(report)
    print("---------------------")

if __name__ == "__main__":
    test_latest_model()
