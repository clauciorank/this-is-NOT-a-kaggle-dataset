import json
import pandas as pd
import requests
import sqlite3
from utils.pdToDatabase import save_to_sqlite, get_data_sqlite
from utils.camelCaseToSanke import camel_case_to_snake


class GetDatasetUsabilityRating:

    def __init__(self):
        pass

    def get_info_and_save(self, dataset_id):
        api_return = self.get_api(dataset_id)
        if api_return.status_code == 200:
            dict_response = json.loads(api_return.text)
            dict_response = dict_response['rating']
            df = pd.DataFrame([dict_response])
            df['dataset_id'] = dataset_id

            df.columns = [camel_case_to_snake(x) for x in df.columns]

            save_to_sqlite(df, 'usability_rating')

    @staticmethod
    def get_api(dataset_id):
        url = "https://www.kaggle.com/api/i/datasets.DatasetDetailService/GetDatasetUsabilityRating"

        payload = json.dumps({
            "datasetId": dataset_id,
            "hashLink": ""
        })

        headers = {
            'authority': 'www.kaggle.com',
            'accept': 'application/json',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'content-type': 'application/json',
            'cookie': 'ka_sessionid=5e86fac9ad7f6c5b5de802a9cf4a5c5e; ACCEPTED_COOKIES=true; CSRF-TOKEN=CfDJ8EaxSfOz2gdBqeaKHeroAhxRP9-J-dyWJPcbCiug96UIx4LrcRenCprkLxrbvGuhnDM7tpVH3TPdYXhBDkiX4Vpyp1FaL-Yww05nVmgCSA; __Host-KAGGLEID=CfDJ8EaxSfOz2gdBqeaKHeroAhzbhQ2UafTV0MonZhuvS4CNe96iMV50ssSLO7DmfWdiZETx3zAbD-1GRRQ1X43daEj7JlbIypy_Z_QKK7zH3IIWGQDfdoFsu4gc; GCLB=CNTDmcWJtOX7Bg; _gid=GA1.2.1935967325.1681862383; XSRF-TOKEN=CfDJ8EaxSfOz2gdBqeaKHeroAhwyD_-Cb-j8al5h1ToFcTTwbAoWEN3WMI2Lb1Nm47S7aFilbeV-yEjAi-26YCeVjlNP28pcuAty5V0o8t9mb4Ecwa6x1XI-g0oFObrOUqXaUKPrU9GWGj4sleJS6gesS0w; CLIENT-TOKEN=eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJpc3MiOiJrYWdnbGUiLCJhdWQiOiJjbGllbnQiLCJzdWIiOiJjbGF1Y2lvcmFuayIsIm5idCI6IjIwMjMtMDQtMTlUMDI6MTk6MjQuMjgyNTk2OVoiLCJpYXQiOiIyMDIzLTA0LTE5VDAyOjE5OjI0LjI4MjU5NjlaIiwianRpIjoiMzVhNzczYzctZWE2Yy00OGM0LTg4NzYtM2QxNzMyZmRmMDA4IiwiZXhwIjoiMjAyMy0wNS0xOVQwMjoxOToyNC4yODI1OTY5WiIsInVpZCI6NjMwNTQzNywiZGlzcGxheU5hbWUiOiJDbGF1Y2lvIFJhbmsiLCJlbWFpbCI6ImNsYXVjaW9yYW5rQGdtYWlsLmNvbSIsInRpZXIiOiJOb3ZpY2UiLCJ2ZXJpZmllZCI6ZmFsc2UsInByb2ZpbGVVcmwiOiIvY2xhdWNpb3JhbmsiLCJ0aHVtYm5haWxVcmwiOiJodHRwczovL3N0b3JhZ2UuZ29vZ2xlYXBpcy5jb20va2FnZ2xlLWF2YXRhcnMvdGh1bWJuYWlscy9kZWZhdWx0LXRodW1iLnBuZyIsImZmIjpbIktlcm5lbHNEcmFmdFVwbG9hZEJsb2IiLCJLZXJuZWxzRGlzYWJsZVVudXNlZEFjY2VsZXJhdG9yV2FybmluZyIsIktlcm5lbHNGaXJlYmFzZUxvbmdQb2xsaW5nIiwiQ29tbXVuaXR5TG93ZXJIZWFkZXJTaXplcyIsIkFsbG93Rm9ydW1BdHRhY2htZW50cyIsIktlcm5lbHNTYXZlQ2VsbE91dHB1dCIsIlRwdU9uZVZtIiwiVHB1VHdvVm1EZXByZWNhdGVkIiwiRnJvbnRlbmRFcnJvclJlcG9ydGluZyIsIkRhdGFzZXRzTWFuYWdlZEZvY3VzT25PcGVuIiwiRG9pRGF0YXNldFRvbWJzdG9uZXMiLCJLZXJuZWxFZGl0b3JIYW5kbGVNb3VudE9uY2UiLCJLZXJuZWxQaW5uaW5nIiwiS01Vc2VyQWNjb3VudCIsIlBhZ2VWaXNpYmlsaXR5QW5hbHl0aWNzUmVwb3J0ZXIiLCJNYXVSZXBvcnQiLCJNb2RlbHNDYWNoZWRUYWdTZXJ2aWNlRW5hYmxlZCIsIlVzZXJSZXBvcnRzVG9aZW5kZXNrIl0sImZmZCI6eyJLZXJuZWxFZGl0b3JBdXRvc2F2ZVRocm90dGxlTXMiOiIzMDAwMCIsIkZyb250ZW5kRXJyb3JSZXBvcnRpbmdTYW1wbGVSYXRlIjoiMCIsIkVtZXJnZW5jeUFsZXJ0QmFubmVyIjoie30iLCJDbGllbnRScGNSYXRlTGltaXQiOiI0MCIsIkZlYXR1cmVkQ29tbXVuaXR5Q29tcGV0aXRpb25zIjoiMzUzMjUsMzcxNzQsMzM1NzksMzc4OTgsMzczNTQsMzc5NTksMzg4NjAiLCJBZGRGZWF0dXJlRmxhZ3NUb1BhZ2VMb2FkVGFnIjoiZGlzYWJsZWQifSwicGlkIjoia2FnZ2xlLTE2MTYwNyIsInN2YyI6IndlYi1mZSIsInNkYWsiOiJBSXphU3lBNGVOcVVkUlJza0pzQ1pXVnotcUw2NTVYYTVKRU1yZUUiLCJibGQiOiJmNjdlZTNmNGRhNTBkMDc4ZDU1NDBiN2ZmZTBmOGViMTExNGEwMDg5In0.; _ga=GA1.1.522111655.1674257473; _ga_T7QHS60L4Q=GS1.1.1681862383.6.1.1681870765.0.0.0',
            'origin': 'https://www.kaggle.com',
            'referer': 'https://www.kaggle.com/datasets/abubakarsiddiquemahi/house-rent-prediction',
            'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
            'x-xsrf-token': 'CfDJ8EaxSfOz2gdBqeaKHeroAhwyD_-Cb-j8al5h1ToFcTTwbAoWEN3WMI2Lb1Nm47S7aFilbeV-yEjAi-26YCeVjlNP28pcuAty5V0o8t9mb4Ecwa6x1XI-g0oFObrOUqXaUKPrU9GWGj4sleJS6gesS0w'
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
            GetDatasetUsabilityRating().get_info_and_save(i)
        except Exception as Err:
            errors += 1
            print(f'Error processing id {i}{Err}')

    print(f'Finished, errors: {errors}')