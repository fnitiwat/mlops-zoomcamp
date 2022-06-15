import pickle
import pandas as pd
from regex import F
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from prefect import task, flow
from prefect.task_runners import SequentialTaskRunner
from typing import Tuple, Optional
from datetime import datetime
from dateutil.relativedelta import relativedelta
from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import CronSchedule
from prefect.flow_runners import SubprocessFlowRunner


@task
def read_data(path):
    df = pd.read_parquet(path)
    return df


@task
def prepare_features(df, categorical, train=True):
    df["duration"] = df.dropOff_datetime - df.pickup_datetime
    df["duration"] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    mean_duration = df.duration.mean()
    if train:
        print(f"The mean duration of training is {mean_duration}")
    else:
        print(f"The mean duration of validation is {mean_duration}")

    df[categorical] = df[categorical].fillna(-1).astype("int").astype("str")
    return df


@task
def train_model(df, categorical):
    train_dicts = df[categorical].to_dict(orient="records")
    dv = DictVectorizer()
    X_train = dv.fit_transform(train_dicts)
    y_train = df.duration.values

    print(f"The shape of X_train is {X_train.shape}")
    print(f"The DictVectorizer has {len(dv.feature_names_)} features")

    lr = LinearRegression()
    lr.fit(X_train, y_train)
    y_pred = lr.predict(X_train)
    mse = mean_squared_error(y_train, y_pred, squared=False)
    print(f"The MSE of training is: {mse}")
    return lr, dv


@task
def run_model(df, categorical, dv, lr):
    val_dicts = df[categorical].to_dict(orient="records")
    X_val = dv.transform(val_dicts)
    y_pred = lr.predict(X_val)
    y_val = df.duration.values

    mse = mean_squared_error(y_val, y_pred, squared=False)
    print(f"The MSE of validation is: {mse}")
    return


@task
def get_data_path_from_date(date: str) -> Tuple[str, str]:

    format = "%Y-%m-%d"
    input_datetime = datetime.strptime(date, format)
    print(date)
    print(input_datetime)

    train_datetime = input_datetime - relativedelta(months=2)
    val_datetime = input_datetime - relativedelta(months=1)

    print(input_datetime)
    print(train_datetime)
    print(val_datetime)
    train_data_path = f"https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_{train_datetime.year}-{train_datetime.month:02d}.parquet"
    val_data_path = f"https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_{val_datetime.year}-{val_datetime.month:02d}.parquet"

    print("train:", train_data_path)
    print("val:", val_data_path)
    return train_data_path, val_data_path


@flow(name="homework", task_runner=SequentialTaskRunner)
def main(date: Optional[str] = "2021-08-15"):
    if date == None:
        date = datetime.today().strftime("%Y-%m-%d")

    train_path, val_path = get_data_path_from_date(date).result()

    categorical = ["PUlocationID", "DOlocationID"]

    df_train = read_data(train_path)
    df_train_processed = prepare_features(df_train, categorical)

    df_val = read_data(val_path)
    df_val_processed = prepare_features(df_val, categorical, False)

    # train the model
    lr, dv = train_model(df_train_processed, categorical).result()
    run_model(df_val_processed, categorical, dv, lr)

    with open(f"model-{date}.bin", "wb") as f_out:
        pickle.dump(dv, f_out)

    with open(f"dv-{date}.b", "wb") as f_out:
        pickle.dump(dv, f_out)


DeploymentSpec(
    flow=main,
    name="homework",
    schedule=CronSchedule(cron="0 9 15 * *", timezone="Asia/Bangkok"),
    flow_runner=SubprocessFlowRunner(),
)
