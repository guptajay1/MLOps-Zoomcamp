import os
import json
from datetime import datetime
import pandas as pd

df_input = pd.read_parquet("./output/yellow_tripdata_2023-03.parquet")

def get_input_path(year, month):
    default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)


def get_output_path(year, month):
    default_output_pattern = 's3://nyc-duration-prediction-ajay/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)

input_file = get_input_path(2023, 1)
output_file = get_output_path(2023, 1)

options = {
    'client_kwargs': {
        'endpoint_url': os.getenv('S3_ENDPOINT_URL', "http://s3:4566/")
    }
}

df_input.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
)