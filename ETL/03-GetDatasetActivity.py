import json
import pandas as pd
import requests
import sqlite3
from utils.pdToDatabase import save_to_sqlite, get_data_sqlite
from utils.datetimeTreatment import parse_date


class GetDatasetActivity:
    def __init__(self):
        self.conn = sqlite3.connect('sqlite_database.sqlite')

    def treating_and_saving_info(self, dataset_id):
        api_return = self.get_api(dataset_id)
        if api_return.status_code == 200:
            dict_response = json.loads(api_return.text)
            dict_response = dict_response['summaryStats']

            dataset_id = dict_response['datasetId']
            dataset_stats = dict_response['datasetStats']
            views = [x['value'] for x in dataset_stats if x['name'] == 'Views'][0]
            downloads = [x['value'] for x in dataset_stats if x['name'] == 'Downloads'][0]
            download_per_view = [x['value'] for x in dataset_stats if x['name'] == 'Download per view ratio'][0]
            unique_contributors = [x['value'] for x in dataset_stats if x['name'] == 'Total unique contributors'][0]
            notebooks = dict_response['kernelStats'][0]['value']

            dataset_activity = {
                "dataset_id": dataset_id,
                "views": views,
                "downloads": downloads,
                "download_per_view": download_per_view,
                "unique_contributors": unique_contributors,
                "notebooks": notebooks
            }

            df_dataset_activity = pd.DataFrame([dataset_activity])
            save_to_sqlite(df_dataset_activity, 'dataset_activity')

            # Timeseries
            ts = dict_response['timeSeries']

            # Downloads
            downloads = ts['downloads']['points']
            downloads_df = pd.DataFrame(downloads)
            downloads_df['date'] = downloads_df['date'].apply(lambda x: parse_date(x))
            downloads_df['dataset_id'] = dataset_id
            save_to_sqlite(downloads_df, 'downloads_timeseries')

            # Views
            views = ts['views']['points']
            views_df = pd.DataFrame(views)
            views_df['date'] = views_df['date'].apply(lambda x: parse_date(x))
            views_df['dataset_id'] = dataset_id
            save_to_sqlite(views_df, 'views_timeseries')

    @staticmethod
    def get_api(dataset_id):

        url = "https://www.kaggle.com/api/i/datasets.DatasetService/GetDatasetActivity"

        payload = json.dumps({
            "datasetId": dataset_id
        })
        headers = {
            'authority': 'www.kaggle.com',
            'accept': 'application/json',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'content-type': 'application/json',
            'cookie': 'ka_sessionid=5e86fac9ad7f6c5b5de802a9cf4a5c5e; ACCEPTED_COOKIES=true; CSRF-TOKEN=CfDJ8EaxSfOz2gdBqeaKHeroAhxRP9-J-dyWJPcbCiug96UIx4LrcRenCprkLxrbvGuhnDM7tpVH3TPdYXhBDkiX4Vpyp1FaL-Yww05nVmgCSA; __Host-KAGGLEID=CfDJ8EaxSfOz2gdBqeaKHeroAhzbhQ2UafTV0MonZhuvS4CNe96iMV50ssSLO7DmfWdiZETx3zAbD-1GRRQ1X43daEj7JlbIypy_Z_QKK7zH3IIWGQDfdoFsu4gc; GCLB=CNTDmcWJtOX7Bg; _gid=GA1.2.1935967325.1681862383; _ga=GA1.1.522111655.1674257473; _gat_gtag_UA_12629138_1=1; XSRF-TOKEN=CfDJ8EaxSfOz2gdBqeaKHeroAhw7USeW_vxIHkJGa3k19tF6ralGauw35bZLJlO5dQcq8sx6kQy4CLLBdjyXYgq6Vi8Z073AqG9WOsbFXzFYIHT9N4ED6aav-i67Lvt4g5QgiwJOLCyrQJA4ticUxd8MAzc; CLIENT-TOKEN=eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJpc3MiOiJrYWdnbGUiLCJhdWQiOiJjbGllbnQiLCJzdWIiOiJjbGF1Y2lvcmFuayIsIm5idCI6IjIwMjMtMDQtMTlUMDA6MDc6MTEuMTIyOTU5WiIsImlhdCI6IjIwMjMtMDQtMTlUMDA6MDc6MTEuMTIyOTU5WiIsImp0aSI6IjkxODFiNzJmLTNkYzgtNDU0Zi04MGJiLTI1YjZkMmQxYmQ1YSIsImV4cCI6IjIwMjMtMDUtMTlUMDA6MDc6MTEuMTIyOTU5WiIsInVpZCI6NjMwNTQzNywiZGlzcGxheU5hbWUiOiJDbGF1Y2lvIFJhbmsiLCJlbWFpbCI6ImNsYXVjaW9yYW5rQGdtYWlsLmNvbSIsInRpZXIiOiJOb3ZpY2UiLCJ2ZXJpZmllZCI6ZmFsc2UsInByb2ZpbGVVcmwiOiIvY2xhdWNpb3JhbmsiLCJ0aHVtYm5haWxVcmwiOiJodHRwczovL3N0b3JhZ2UuZ29vZ2xlYXBpcy5jb20va2FnZ2xlLWF2YXRhcnMvdGh1bWJuYWlscy9kZWZhdWx0LXRodW1iLnBuZyIsImZmIjpbIktlcm5lbHNEcmFmdFVwbG9hZEJsb2IiLCJLZXJuZWxzRGlzYWJsZVVudXNlZEFjY2VsZXJhdG9yV2FybmluZyIsIktlcm5lbHNGaXJlYmFzZUxvbmdQb2xsaW5nIiwiQ29tbXVuaXR5TG93ZXJIZWFkZXJTaXplcyIsIkFsbG93Rm9ydW1BdHRhY2htZW50cyIsIktlcm5lbHNTYXZlQ2VsbE91dHB1dCIsIlRwdU9uZVZtIiwiVHB1VHdvVm1EZXByZWNhdGVkIiwiRnJvbnRlbmRFcnJvclJlcG9ydGluZyIsIkRhdGFzZXRzTWFuYWdlZEZvY3VzT25PcGVuIiwiRG9pRGF0YXNldFRvbWJzdG9uZXMiLCJLZXJuZWxFZGl0b3JIYW5kbGVNb3VudE9uY2UiLCJLZXJuZWxQaW5uaW5nIiwiS01Vc2VyQWNjb3VudCIsIlBhZ2VWaXNpYmlsaXR5QW5hbHl0aWNzUmVwb3J0ZXIiLCJNYXVSZXBvcnQiLCJNb2RlbHNDYWNoZWRUYWdTZXJ2aWNlRW5hYmxlZCIsIlVzZXJSZXBvcnRzVG9aZW5kZXNrIl0sImZmZCI6eyJLZXJuZWxFZGl0b3JBdXRvc2F2ZVRocm90dGxlTXMiOiIzMDAwMCIsIkZyb250ZW5kRXJyb3JSZXBvcnRpbmdTYW1wbGVSYXRlIjoiMCIsIkVtZXJnZW5jeUFsZXJ0QmFubmVyIjoie30iLCJDbGllbnRScGNSYXRlTGltaXQiOiI0MCIsIkZlYXR1cmVkQ29tbXVuaXR5Q29tcGV0aXRpb25zIjoiMzUzMjUsMzcxNzQsMzM1NzksMzc4OTgsMzczNTQsMzc5NTksMzg4NjAiLCJBZGRGZWF0dXJlRmxhZ3NUb1BhZ2VMb2FkVGFnIjoiZGlzYWJsZWQifSwicGlkIjoia2FnZ2xlLTE2MTYwNyIsInN2YyI6IndlYi1mZSIsInNkYWsiOiJBSXphU3lBNGVOcVVkUlJza0pzQ1pXVnotcUw2NTVYYTVKRU1yZUUiLCJibGQiOiJmNjdlZTNmNGRhNTBkMDc4ZDU1NDBiN2ZmZTBmOGViMTExNGEwMDg5In0.; _ga_T7QHS60L4Q=GS1.1.1681862383.6.1.1681862831.0.0.0',
            'origin': 'https://www.kaggle.com',
            'referer': 'https://www.kaggle.com/datasets/prabhakarz/tmdb-15000-movies-dataset-with-credits',
            'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
            'x-xsrf-token': 'CfDJ8EaxSfOz2gdBqeaKHeroAhw7USeW_vxIHkJGa3k19tF6ralGauw35bZLJlO5dQcq8sx6kQy4CLLBdjyXYgq6Vi8Z073AqG9WOsbFXzFYIHT9N4ED6aav-i67Lvt4g5QgiwJOLCyrQJA4ticUxd8MAzc'
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        return response


if __name__ == '__main__':

    df_ids = get_data_sqlite('select distinct dataset_id from datasets')

    counter = 0
    errors = 0
    for i in df_ids['dataset_id']:
        counter += 1
        print(f'processing {counter} of {len(df_ids)} datasets')
        try:
            GetDatasetActivity().treating_and_saving_info(i)
        except Exception as Err:
            errors += 1
            print(f'Error processing id {i}{Err}')

    print(f'Finished, errors: {errors}')
