from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import CronSchedule
from prefect.flow_runners import SubprocessFlowRunner


DeploymentSpec(
    flow_location="score.py",
    name="ride_duration_prediction",
    parameters={
        "taxi_type": "green",
        "run_id": "62953e0cf15a48dd8434001c4b5af3b4",
    },
    flow_storage="5fb2a708-809c-4188-a3a6-2460171b0247",
    schedule=CronSchedule(cron="0 3 2 * *"),
    flow_runner=SubprocessFlowRunner(),
    tags=["ml"]
)