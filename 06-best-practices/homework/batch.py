#!/usr/bin/env python
# coding: utf-8

import pickle
import sys
from typing import List

import pandas as pd


def prepare_data(df: pd.DataFrame, categorical: List[str]) -> pd.DataFrame:
    df["duration"] = df.dropOff_datetime - df.pickup_datetime
    df["duration"] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()
    df[categorical] = df[categorical].fillna(-1).astype("int").astype("str")
    return df


def read_data(filename: str, categorical: List[str]):
    df = pd.read_parquet(filename)
    df = prepare_data(df, categorical=categorical)
    return df


def main(year: int, month: int) -> None:
    input_file = f"https://raw.githubusercontent.com/alexeygrigorev/datasets/master/nyc-tlc/fhv/fhv_tripdata_{year:04d}-{month:02d}.parquet"
    output_file = f"s3://nyc-duration-prediction-nitiwat/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet"

    print(input_file)

    with open("model.bin", "rb") as f_in:
        dv, lr = pickle.load(f_in)

    categorical = ["PUlocationID", "DOlocationID"]

    df = read_data(filename=input_file, categorical=categorical)
    df["ride_id"] = f"{year:04d}/{month:02d}_" + df.index.astype("str")

    dicts = df[categorical].to_dict(orient="records")
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    print("predicted mean duration:", y_pred.mean())

    df_result = pd.DataFrame()
    df_result["ride_id"] = df["ride_id"]
    df_result["predicted_duration"] = y_pred

    df_result.to_parquet(output_file, engine="pyarrow", index=False)


if __name__ == "__main__":
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    main(year=year, month=month)
