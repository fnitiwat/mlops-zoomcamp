source_stream_name = "stg_ride_events"
output_stream_name = "stg_ride_predictions"
model_bucket = "stg-mlflow-models"
lambda_function_local_path = "../lambda_function.py"
docker_image_local_path = "../Dockerfile"
ecr_repo_name = "stg_stream_model_duration"
lambda_function_name = "stg_prediction_lambda"
