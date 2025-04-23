import pandas as pd
import sys 
import os 

expected_data = [{'PULocationID': '-1', 'DOLocationID': '-1', 'tpep_pickup_datetime': pd.Timestamp('2022-01-01 01:02:00'), 'tpep_dropoff_datetime': pd.Timestamp('2022-01-01 01:10:00'), 'duration': 8.0},
            {'PULocationID': '1', 'DOLocationID': '-1', 'tpep_pickup_datetime': pd.Timestamp('2022-01-01 01:02:00'), 'tpep_dropoff_datetime': pd.Timestamp('2022-01-01 01:10:00'), 'duration': 8.0}, 
            {'PULocationID': '1', 'DOLocationID': '2', 'tpep_pickup_datetime': pd.Timestamp('2022-01-01 02:02:00'), 'tpep_dropoff_datetime': pd.Timestamp('2022-01-01 02:03:00'), 'duration': 1.0}]

expected_columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'duration']
expected_df = pd.DataFrame(expected_data, columns=expected_columns)


def get_input_path(year, month):
    default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)


def main(year, month):
    input_file = get_input_path(year, month)
    options = {
    'client_kwargs': {
        'endpoint_url': "http://localhost:4566"
    }
}

    expected_df.to_parquet(
        input_file,
        engine='pyarrow',
        compression=None,
        index=False,
        storage_options=options
    )


if __name__ == '__main__':
    os.environ['INPUT_FILE_PATTERN'] = "s3://nyc-duration/in/{year:04d}-{month:02d}.parquet"
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    main(year, month)