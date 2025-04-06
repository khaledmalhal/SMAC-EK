import requests
import json

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
        pass

    def get_data(self):
        r = requests.get('http://datos.santander.es/api/rest/datasets/sensores_smart_env_monitoring.json?items=332')
        if r.status_code != 200:
            raise Exception("Not successful request")
        obj = r.json()
        print(json.dumps(obj, indent=4))
        for resource in obj['resources']:
            type        = resource['ayto:type']
            noise       = resource['ayto:noise']
            light       = resource['ayto:light']
            temperature = resource['ayto:temperature']
            battery     = resource['ayto:battery']      # It's always empty.
            latitude    = resource['ayto:latitude']
            longitude   = resource['ayto:longitude']
            modified    = resource['dc:modified']
            if type == 'NoiseLevelObserved':
                # Upload only noise.
                continue
            elif type == 'WeatherObserved':
                continue
                # Upload temperature and light.
            else:
                continue

if __name__=='__main__':
    p = Data()
    p.get_data()