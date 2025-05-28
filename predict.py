import pickle
import influxdb_client, os, time, sys, socket, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
from sklearn.linear_model import *
from data_load import *

load_dotenv()
token = os.getenv("INFLUXDB_TOKEN")
org = "EPSEVG" #your organization
url = "http://localhost:8086"
bucket = "SMAC-EK" #your bucket


def main():
    try:
        with open('model.pkl', 'rb') as f:
            model = pickle.load(f)
    except Exception as e:
        print(f'Error loading model -> {e}')
        return -1
    latest = get_latest_influxdb(url, token, org)
    if len(latest) == 0:
        df = download_data(since='2020-01-01T00:00:00')
    else:
        df = download_data(since=latest[0])
    df.to_csv('unprocessed.csv')
    df = process_data(df)
    print(df.head())
    upload_to_influxdb(url, token, org, df)
    print(f'Successfully uploaded the data')

if __name__ == '__main__':
    sys.exit(main())