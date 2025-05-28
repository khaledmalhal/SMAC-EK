import pandas as pd
import numpy as np

import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

from sodapy import Socrata
import datetime

# @title Obtaining data from Vilanova i la Geltrú (last execution lasted one minute)
def download_data(since:   str  = '2022-01-01T00:00:00',
                  url:     str  = "analisi.transparenciacatalunya.cat",
                  codes:   list = ['YR'],
                  cod_var: list = ['32','40','42','3','33','44','2','34','31','51','1','30','50']):
    try:
        df = pd.read_csv('data.csv')
        return df
    except Exception as e:
        print(f'Unable to read from file.')
    try:
        df = pd.read_csv('unprocessed.csv')
        return df
    except Exception as e:
        print(f'Unable to read unprocessed file.')

    with Socrata(url, None, timeout=6000) as client:
        # Build a string of station codes wrapped in double quotes
        codes_str = ', '.join(f'"{c}"' for c in codes)
        vars_str  = ', '.join(f'"{c}"' for c in cod_var)

        # Build the $where query string
        where_query = (
            f"data_lectura >= '{since}' "
            f"AND data_lectura <= '{datetime.now().isoformat()}' "
            f"AND codi_estacio in ({codes_str})"
            f"AND codi_variable in ({vars_str})"
        )

        # Pagination parameters
        limit = 5000  # you can adjust batch size if needed
        offset = 0
        all_results = []

        while True:
            # Fetch batch of data
            results = client.get("nzvn-apee",
                                where=where_query,
                                limit=limit,
                                offset=offset)
            print("Obtained so far %d" % (offset,), end='\r')

            # If no more results, break the loop
            if not results:
                break

            all_results.extend(results)
            offset += limit  # move to next batch
    return pd.DataFrame.from_records(all_results)

def process_data(df: pd.DataFrame):
    # @title Preprocessing of the data
    try:
        df = df.drop_duplicates(subset=['id'])

        grouped = df.groupby('data_lectura')[['codi_variable', 'valor_lectura']].apply(lambda x: x)
        dates = [index[0] for index in grouped.index]
        dates = list(set(dates))
        print(dates[0])
        print(grouped.loc['2023-05-01T00:00:00.000'].to_numpy())
        format = bool(datetime.fromisoformat(dates[0]))


        new_df = pd.DataFrame(columns=['DATA', 'HR', 'HRn', 'HRx', 'P', 'Pn', 'T', 'Tn', 'Tx'])
        for date in dates:
            row = grouped.loc[date].to_numpy()
            # For reference: value[0] = CODI_VARIABLE & value[1] = VALOR_LECTURA
            obj = {
                'DATA':   [date],
                'HR':     [next((float(value[1]) for value in row if value[0] == '33'), None)],
                'HRn':    [next((float(value[1]) for value in row if value[0] == '44'), None)],
                'HRx':    [next((float(value[1]) for value in row if value[0] == '3'),  None)],
                'P':      [next((float(value[1]) for value in row if value[0] == '34'), None)],
                'Pn':     [next((float(value[1]) for value in row if value[0] == '2'),  None)],
                'T':      [next((float(value[1]) for value in row if value[0] == '32'), None)],
                'Tn':     [next((float(value[1]) for value in row if value[0] == '42'), None)],
                'Tx':     [next((float(value[1]) for value in row if value[0] == '40'), None)],
                'Px':     [next((float(value[1]) for value in row if value[0] == '1'),  None)],
                'DV10':   [next((float(value[1]) for value in row if value[0] == '31'), None)],
                'DVVx10': [next((float(value[1]) for value in row if value[0] == '51'), None)],
                'VV10':   [next((float(value[1]) for value in row if value[0] == '30'), None)],
                'VVx10':  [next((float(value[1]) for value in row if value[0] == '50'), None)]
            }
            new_row = pd.DataFrame(obj)
            new_df = pd.concat([new_df, new_row], ignore_index=True)
        print(new_df.head())

        new_df.drop("Unnamed: 0",   axis=1, inplace=True, errors='ignore')
        new_df.drop("Unnamed: 0.1", axis=1, inplace=True, errors='ignore')
        if format is False:
            new_df['DATA'] = new_df['DATA'].apply(lambda x: datetime.datetime.strptime(x, "%d/%m/%Y %H:%M").isoformat(timespec='seconds'))
        new_df.apply(pd.to_numeric, errors='ignore')
    except Exception as e:
        print(f"Unable to process data. Don't worry, it might have been processed before. Error: {e}")
        new_df = df
    new_df.dropna(inplace=True)
    new_df.set_index('DATA', inplace=True)
    new_df.sort_index(inplace=True)

    new_df.to_csv('data.csv')
    return new_df

def get_latest_influxdb(url: str, token: str, org: str):
    query = """
    from(bucket:"SMAC-EK")
        |> range(start: 2020-01-01T00:00:00Z)
        |> filter(fn:(r) => r._measurement == "Meteocat")
        |> filter(fn:(r) => r._field == "temperature")
        |> filter(fn:(r) => r._type  == "real")
        |> last()
    """
    client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
    query_api = client.query_api()
    result = query_api.query(org=org, query=query)

    results = []
    for table in result:
        for record in table.records:
            results.append(record.get_time())
    return results

def upload_to_influxdb(url: str, token: str, org: str, df: pd.DataFrame):
    client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    points = []
    for index, row in df.iterrows():
        time = datetime.datetime.fromisoformat(index)
        time = time - datetime.timedelta(hours=2)
        time = time.isoformat(timespec='seconds')
        print(f'{time}Z')
        p = (influxdb_client.Point("Meteocat")
             .tag("_type", "real")
             .field("temperature", row['T'])
             .time(f'{time}Z'))
        points.append(p)
    write_api.write(bucket="SMAC-EK", org=org, record=points)
