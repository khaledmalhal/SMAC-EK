import pickle
import os
import sys

from dotenv import load_dotenv
from data_load import *

from sklearn.linear_model import *
from sklearn import preprocessing
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

from time import sleep
from datetime import *

import matplotlib.pyplot as plt
import numpy as np

load_dotenv()
token = os.getenv("INFLUXDB_TOKEN")
org = "EPSEVG" #your organization
url = "http://localhost:8086"
bucket = "SMAC-EK" #your bucket

def wait_data(latest: datetime):
    if latest is None:
        return
    # added = latest + timedelta(minutes=30)
    # naive = added.replace(tzinfo=None)
    # diff = datetime.now() - naive
    print(f'Sleeping for {30} minutes')
    sleep(30*60)

def plot_test_pred(y_test, y_pred):
    # Let's take ONLY 0.2% of the predicted and test data to plot it.
    # If we try to plot everything, it's gonna take some while...
    _, y_test_plot, _, y_pred_plot = train_test_split(y_test, y_pred,
                                                      test_size=0.002,
                                                      random_state=1,
                                                      shuffle=False)
    test_index = y_test.index.to_pydatetime()
    print(test_index)

    plt.subplots(figsize=(10,5))

    plt.plot(test_index, y_test,
            label='Actual', marker='o')
    plt.plot(test_index, y_pred,
            label='Predicted', marker='x', color='red')

    plt.title("Comparison of Actual vs. Predicted Temperatures (Test Set)")
    plt.xticks(np.arange(len(test_index)), test_index, rotation='vertical')
    plt.xlabel("Date/Time (Test)")
    plt.ylabel("Temperature (Celsius)")
    plt.legend()
    plt.grid(True)
    plt.show()


def predict(model: Lasso, df: pd.DataFrame):
    # df['temp_prev'] = df['T'].shift(2*24)
    df = df.dropna()
    # print(df)
    X = df.drop(['T', 'Tn', 'Tx'], axis=1)
    y = df['T']

    scaler = preprocessing.StandardScaler().fit(X)
    X = scaler.transform(X)

    y_pred = model.predict(X)

    mae = mean_absolute_error(y, y_pred)
    r2  = r2_score(y, y_pred)

    print("Mean Absolute Error (MAE):", mae)
    print("R^2 Score:", r2)

    # plot_test_pred(y, y_pred)

    ret_y = pd.DataFrame(index=y.index, data={'T': y_pred})
    return ret_y

def main():
    try:
        with open('model.pkl', 'rb') as f:
            model = pickle.load(f)
    except Exception as e:
        print(f'Error loading model -> {e}')
        return -1

    try:
        df = pd.read_csv('data.csv')
        df = process_data(df)
    except Exception as e:
        print(f'Unable to read from file.')

    while True:
        latest = get_latest_influxdb(url, token, org)
        if latest is None:
            print('Getting latest data since 2020...')
            new_df = download_data()
        else:
            print(f'Getting latest data since {latest.isoformat()}')
            new_df = download_data(since=latest.isoformat().split('+')[0])
        new_df = process_data(new_df)

        df = pd.concat([df, new_df])
        df.sort_values('DATA', inplace=True)
        df = (df.reset_index()
                .drop_duplicates(subset='DATA', keep='last')
                .set_index('DATA').sort_index())
        df.drop("Unnamed: 0", axis=1, inplace=True, errors='ignore')

        upload_to_influxdb(url, token, org, df)
        df.to_csv('data.csv')

        new_df = prepare_for_predict(df)

        y_pred = predict(model, new_df)

        upload_to_influxdb(url, token, org, y_pred, 'predicted')

        print(f'Successfully uploaded the data')

        wait_data(latest)


if __name__ == '__main__':
    sys.exit(main())