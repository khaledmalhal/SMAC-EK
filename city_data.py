import requests
import json
import influxdb_client, os, datetime
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv

load_dotenv()
token = os.getenv("INFLUXDB_TOKEN")
org = "EPSEVG" #your organization
url = "http://localhost:8086"
bucket = "SMAC-EK" #your bucket

class Request:
    """
    This was made as a wrapper. But it might not be used after all.
    It's kept in here, just in case.
    """
    def __init__(self, base_url=''):
        if len(base_url) == 0:
            raise Exception('The url has not been defined')
        if base_url[len(base_url)-1] != '/':
            base_url = base_url+'/'
        self.base_url = base_url
    
    def get_data(self, api):
        if len(api) == 0:
            raise Exception('API length is 0')
        if api[0] != '/':
            api = '/'+api
        try:
            r = requests.get(self.base_url+api)
            return r
        except Exception as e:
            print(f'{e}')

class Data:
    """
    The data we are obtaining is the environmental data of Santander.

    There is really not an API, but we simply obtain the data from a
    given JSON file. We will utilize this JSON to parse it and upload
    the data to our own database.

    More information: http://datos.santander.es/dataset/?id=sensores-ambientales
    """
    def __init__(self):
        try:
            self.write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
        except Exception as e:
            print('Error initializing Influx client.', e)
        pass

    def get_data(self):
        print("Obtaining data from source...\n")
        r = requests.get('http://datos.santander.es/api/rest/datasets/sensores_smart_env_monitoring.json?items=332')
        if r.status_code != 200:
            raise Exception("Not successful request")
        obj = r.json()
        # print(json.dumps(obj, indent=4))
        i = 1
        for resource in obj['resources']:
            type        = resource['ayto:type']
            noise       = resource['ayto:noise']
            light       = resource['ayto:light']
            temperature = resource['ayto:temperature']
            battery     = resource['ayto:battery']      # It's always empty.
            latitude    = resource['ayto:latitude']
            longitude   = resource['ayto:longitude']
            modified    = resource['dc:modified']
            if len(latitude)  > 0: latitude  = float(latitude)
            if len(longitude) > 0: longitude = float(longitude)
            
            if type == 'NoiseLevelObserved':
                # Upload only noise.
                if len(noise) > 0: noise = float(noise)
                point = (Point('Noise-Santander')
                         .time(modified)
                         .tag('sensor', 'noise')
                         .field('noise', noise)
                         .field('latitude', latitude)
                         .field('longitude', longitude)
                         .time(modified))
            elif type == 'WeatherObserved':
                # Upload temperature and light.
                if len(temperature) > 0: temperature = float(temperature)
                point = (Point('Weather-Santander')
                         .tag('sensor', 'weather')
                         .field('temperature', temperature)
                         .field('latitude', latitude)
                         .field('longitude', longitude)
                         .time(modified))
                if len(light) > 0: 
                    light = float(light)
                    if light < float(1000000):
                        point = (point.field('light', light))
            else:
                continue
            write_api = self.write_client.write_api(write_options=SYNCHRONOUS)
            write_api.write(bucket=bucket, org=org, record=point)
            print("Inserting point into influx (%d of %d)" % (i, len(obj['resources'])), end='\r')
            i = i+1
if __name__=='__main__':
    p = Data()
    p.get_data()