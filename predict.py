import pickle
import os
import sys

from dotenv import load_dotenv
from data_load import *

from sklearn.linear_model import *
from sklearn import preprocessing
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

import matplotlib.pyplot as plt

load_dotenv()
token = os.getenv("INFLUXDB_TOKEN")
org = "EPSEVG" #your organization
url = "http://localhost:8086"
bucket = "SMAC-EK" #your bucket

def plot_test_pred(y_test, y_pred):
    # Let's take ONLY 0.2% of the predicted and test data to plot it.
    # If we try to plot everything, it's gonna take some while...
    _, y_test_plot, _, y_pred_plot = train_test_split(y_test, y_pred,
                                                      test_size=1,
                                                      random_state=1,
                                                      shuffle=False)
    test_index = y_test_plot.index

    plt.figure(figsize=(20,5))

    plt.plot(test_index, y_test_plot,
            label='Actual (Test)', marker='o')
    plt.plot(test_index, y_pred_plot,
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
    print(df)
    X = df.drop(['T', 'Tn', 'Tx'], axis=1)
    y = df['T']

    scaler = preprocessing.StandardScaler().fit(X)
    X = scaler.transform(X)

    # X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=.20,
    #                                                     random_state=1, shuffle=False)
    # model.fit(X_train, y_train)

    y_pred = model.predict(X)

    mae = mean_absolute_error(y, y_pred)
    r2  = r2_score(y, y_pred)

    print("Mean Absolute Error (MAE):", mae)
    print("R^2 Score:", r2)

    plot_test_pred(y, y_pred)

    return y_pred

def main():
    try:
        with open('model.pkl', 'rb') as f:
            model = pickle.load(f)
    except Exception as e:
        print(f'Error loading model -> {e}')
        return -1

    latest = get_latest_influxdb(url, token, org)
    if latest is None:
        print('Getting latest data since 2020...')
        df = download_data()
    else:
        print(f'Getting latest data since {latest.isoformat()}')
        df = download_data(since=latest.isoformat().split('+')[0])

    df.to_csv('unprocessed.csv')
    df = process_data(df)
    print(df.head())
    upload_to_influxdb(url, token, org, df)

    new_df = prepare_for_predict(df)

    y_pred = predict(model, new_df)

    upload_to_influxdb(url, token, org, y_pred, 'predicted')

    print(f'Successfully uploaded the data')

if __name__ == '__main__':
    sys.exit(main())