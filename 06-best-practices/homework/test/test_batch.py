"""test batch
"""
#!/usr/bin/env python
# coding: utf-8
from datetime import datetime

import pandas as pd
from batch import prepare_data


def dt(hour, minute, second=0):
    return datetime(2021, 1, 1, hour, minute, second)


def test_prepare_data():
    data = [
        (None, None, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2, 0), dt(1, 2, 50)),
        (1, 1, dt(1, 2, 0), dt(2, 2, 1)),
    ]

    columns = ["PUlocationID", "DOlocationID", "pickup_datetime", "dropOff_datetime"]
    df = pd.DataFrame(data, columns=columns)

    prepared_df = prepare_data(df, categorical=["PUlocationID", "DOlocationID"])

    expected_df = pd.DataFrame(
        {
            "PUlocationID": ["-1", "1"],
            "DOlocationID": ["-1", "1"],
            "pickup_datetime": [dt(1, 2), dt(1, 2)],
            "dropOff_datetime": [dt(1, 10), dt(1, 10)],
            "duration": [8.0, 8.0],
        }
    )

    for column_name in columns + ["duration"]:
        print("column_name:", column_name)
        print(prepared_df[column_name])
        print(expected_df[column_name])

        assert (
            prepared_df["PUlocationID"].tolist() == expected_df["PUlocationID"].tolist()
        )
        assert (
            prepared_df["DOlocationID"].tolist() == expected_df["DOlocationID"].tolist()
        )
        assert (
            prepared_df["pickup_datetime"].tolist()
            == expected_df["pickup_datetime"].tolist()
        )
        assert (
            prepared_df["dropOff_datetime"].tolist()
            == expected_df["dropOff_datetime"].tolist()
        )
        for prepared_duration, expected_duration in zip(
            prepared_df["duration"].tolist(), expected_df["duration"].tolist()
        ):
            assert prepared_duration - expected_duration < 0.0001
