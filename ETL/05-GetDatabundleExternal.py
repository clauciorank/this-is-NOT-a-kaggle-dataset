import json
import pandas as pd
import requests
from utils.pdToDatabase import save_to_sqlite, get_data_sqlite


class GetDatabundleExternal:

    def __init__(self):
        pass

    def get_info_and_save(self, dataset_id):
        bundle_id = self.get_bundle_id(dataset_id)
        api_return = self.get_api(dataset_id, bundle_id)
        if api_return.status_code == 200:
            dict_response = json.loads(api_return.text)
            data_source = dict_response.get('dataSource')

            description = data_source.get('description')
            fileset_info = data_source.get('databundleVersion').get('filesetInfo')
            total_size = fileset_info.get('totalSize')

            # get main info
            df_main = pd.DataFrame([{
                'dataset_id': dataset_id,
                'description': description,
                'total_size': total_size
            }])

            save_to_sqlite(df_main, 'data_bundle_external')

            # get children info
            df_children = self.get_children_info(fileset_info)
            df_children['dataset_id'] = dataset_id

            save_to_sqlite(df_children, 'data_bundle_external_children')

    @staticmethod
    def get_children_info(fileset_info):
        children_list = fileset_info.get('files').get('children')

        all_children_list = []
        for element in children_list:
            size = element.get('fileInfo').get('size')
            key = element.get('key')
            extension = element.get('fileInfo').get('fileExtension')
            description = element.get('fileInfo').get('description')

            if element.get('tableInfo') is not None:
                columns = element.get('tableInfo').get('totalColumns')
                rows = element.get('tableInfo').get('totalRows')
            else:
                columns = None
                rows = None

            children_dict = {
                'file_key': key,
                'file_size': size,
                'file_extension': extension,
                'description': description,
                'n_columns': columns,
                'n_rows': rows
            }

            all_children_list.append(children_dict)

        return pd.DataFrame(all_children_list)

    @staticmethod
    def get_api(dataset_id, bundle_id):
        cookies = {
            'ACCEPTED_COOKIES': 'true',
            'ka_sessionid': '2a3bc6590bfbe15d432ab355070de063',
            'CSRF-TOKEN': 'CfDJ8EaxSfOz2gdBqeaKHeroAhzXuXU-MIVgzpvlqPQAWY93wwtdelv_htRfLEUQUCz9R1rE1Gt78wPT858bv0KqbOvd6s7Kg0uc8Vtj6vIdiw',
            '__Host-KAGGLEID': 'CfDJ8EaxSfOz2gdBqeaKHeroAhznrPqdenDET95GCicGa0vKuZBp5nXcxk6K9RSpFdht3sgOJzaLSKoxctdzsgs6rxamgL7vqJ-esxqY3omVEr9Cd5dPAGjg-eGF',
            'GCLB': 'CNfr1aHW9KOJZA',
            '_gid': 'GA1.2.1383653893.1682954251',
            '_ga': 'GA1.1.522111655.1674257473',
            '_gat_gtag_UA_12629138_1': '1',
            'XSRF-TOKEN': 'CfDJ8EaxSfOz2gdBqeaKHeroAhwFIkbql4l9Z1vSAtFTYbBdX4OlhG2BuDHMuUjTaJxuKdoYNED51gAsMw3ynXqhDBFfxf32YCfxRKQhe7ybfV2f6uFIpmEU3CJdoobOyAoIbB_rqLkNzgfR5ZVqR1Frm_E',
            'CLIENT-TOKEN': 'eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJpc3MiOiJrYWdnbGUiLCJhdWQiOiJjbGllbnQiLCJzdWIiOiJjbGF1Y2lvcmFuayIsIm5idCI6IjIwMjMtMDUtMDFUMTU6MTc6NDMuMDY1Mjc5NVoiLCJpYXQiOiIyMDIzLTA1LTAxVDE1OjE3OjQzLjA2NTI3OTVaIiwianRpIjoiNGQwYzlmMTAtNjI4Yi00NWFiLWFkNzQtOWI2MDFlYTdkNGVkIiwiZXhwIjoiMjAyMy0wNi0wMVQxNToxNzo0My4wNjUyNzk1WiIsInVpZCI6NjMwNTQzNywiZGlzcGxheU5hbWUiOiJDbGF1Y2lvIFJhbmsiLCJlbWFpbCI6ImNsYXVjaW9yYW5rQGdtYWlsLmNvbSIsInRpZXIiOiJOb3ZpY2UiLCJ2ZXJpZmllZCI6ZmFsc2UsInByb2ZpbGVVcmwiOiIvY2xhdWNpb3JhbmsiLCJ0aHVtYm5haWxVcmwiOiJodHRwczovL3N0b3JhZ2UuZ29vZ2xlYXBpcy5jb20va2FnZ2xlLWF2YXRhcnMvdGh1bWJuYWlscy9kZWZhdWx0LXRodW1iLnBuZyIsImZmIjpbIktlcm5lbHNEcmFmdFVwbG9hZEJsb2IiLCJLZXJuZWxzRGlzYWJsZVVudXNlZEFjY2VsZXJhdG9yV2FybmluZyIsIktlcm5lbHNGaXJlYmFzZUxvbmdQb2xsaW5nIiwiQ29tbXVuaXR5TG93ZXJIZWFkZXJTaXplcyIsIkFsbG93Rm9ydW1BdHRhY2htZW50cyIsIktlcm5lbHNTYXZlQ2VsbE91dHB1dCIsIlRwdU9uZVZtIiwiVHB1VHdvVm1EZXByZWNhdGVkIiwiRnJvbnRlbmRFcnJvclJlcG9ydGluZyIsIkRhdGFzZXRzTWFuYWdlZEZvY3VzT25PcGVuIiwiRG9pRGF0YXNldFRvbWJzdG9uZXMiLCJDaGFuZ2VEYXRhc2V0T3duZXJzaGlwVG9PcmciLCJLZXJuZWxFZGl0b3JIYW5kbGVNb3VudE9uY2UiLCJLZXJuZWxQaW5uaW5nIiwiS01Vc2VyQWNjb3VudCIsIlBhZ2VWaXNpYmlsaXR5QW5hbHl0aWNzUmVwb3J0ZXIiLCJNYXVSZXBvcnQiLCJNb2RlbHNDYWNoZWRUYWdTZXJ2aWNlRW5hYmxlZCIsIkNvbXBldGl0aW9uc1J1bGVzS20iLCJVc2VyUmVwb3J0c1RvWmVuZGVzayIsIk5ld1RhZ3NDb21wb25lbnQiLCJEYXRhc2V0c1NoYXJlZFdpdGhUaGVtU2VhcmNoIl0sImZmZCI6eyJLZXJuZWxFZGl0b3JBdXRvc2F2ZVRocm90dGxlTXMiOiIzMDAwMCIsIkZyb250ZW5kRXJyb3JSZXBvcnRpbmdTYW1wbGVSYXRlIjoiMCIsIkVtZXJnZW5jeUFsZXJ0QmFubmVyIjoie30iLCJDbGllbnRScGNSYXRlTGltaXQiOiI0MCIsIkZlYXR1cmVkQ29tbXVuaXR5Q29tcGV0aXRpb25zIjoiMzUzMjUsMzcxNzQsMzM1NzksMzc4OTgsMzczNTQsMzc5NTksMzg4NjAiLCJBZGRGZWF0dXJlRmxhZ3NUb1BhZ2VMb2FkVGFnIjoiZGlzYWJsZWQifSwicGlkIjoia2FnZ2xlLTE2MTYwNyIsInN2YyI6IndlYi1mZSIsInNkYWsiOiJBSXphU3lBNGVOcVVkUlJza0pzQ1pXVnotcUw2NTVYYTVKRU1yZUUiLCJibGQiOiI2OTA0N2IyMGIyMGJjMTkzMWFkZjIzZjhlMDkyNTc5ZmVlMTI0NmJhIn0.',
            '_ga_T7QHS60L4Q': 'GS1.1.1682954251.8.1.1682954263.0.0.0',
        }

        headers = {
            'authority': 'www.kaggle.com',
            'accept': 'application/json',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'content-type': 'application/json',
            # 'cookie': 'ACCEPTED_COOKIES=true; ka_sessionid=2a3bc6590bfbe15d432ab355070de063; CSRF-TOKEN=CfDJ8EaxSfOz2gdBqeaKHeroAhzXuXU-MIVgzpvlqPQAWY93wwtdelv_htRfLEUQUCz9R1rE1Gt78wPT858bv0KqbOvd6s7Kg0uc8Vtj6vIdiw; __Host-KAGGLEID=CfDJ8EaxSfOz2gdBqeaKHeroAhznrPqdenDET95GCicGa0vKuZBp5nXcxk6K9RSpFdht3sgOJzaLSKoxctdzsgs6rxamgL7vqJ-esxqY3omVEr9Cd5dPAGjg-eGF; GCLB=CNfr1aHW9KOJZA; _gid=GA1.2.1383653893.1682954251; _ga=GA1.1.522111655.1674257473; _gat_gtag_UA_12629138_1=1; XSRF-TOKEN=CfDJ8EaxSfOz2gdBqeaKHeroAhwFIkbql4l9Z1vSAtFTYbBdX4OlhG2BuDHMuUjTaJxuKdoYNED51gAsMw3ynXqhDBFfxf32YCfxRKQhe7ybfV2f6uFIpmEU3CJdoobOyAoIbB_rqLkNzgfR5ZVqR1Frm_E; CLIENT-TOKEN=eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJpc3MiOiJrYWdnbGUiLCJhdWQiOiJjbGllbnQiLCJzdWIiOiJjbGF1Y2lvcmFuayIsIm5idCI6IjIwMjMtMDUtMDFUMTU6MTc6NDMuMDY1Mjc5NVoiLCJpYXQiOiIyMDIzLTA1LTAxVDE1OjE3OjQzLjA2NTI3OTVaIiwianRpIjoiNGQwYzlmMTAtNjI4Yi00NWFiLWFkNzQtOWI2MDFlYTdkNGVkIiwiZXhwIjoiMjAyMy0wNi0wMVQxNToxNzo0My4wNjUyNzk1WiIsInVpZCI6NjMwNTQzNywiZGlzcGxheU5hbWUiOiJDbGF1Y2lvIFJhbmsiLCJlbWFpbCI6ImNsYXVjaW9yYW5rQGdtYWlsLmNvbSIsInRpZXIiOiJOb3ZpY2UiLCJ2ZXJpZmllZCI6ZmFsc2UsInByb2ZpbGVVcmwiOiIvY2xhdWNpb3JhbmsiLCJ0aHVtYm5haWxVcmwiOiJodHRwczovL3N0b3JhZ2UuZ29vZ2xlYXBpcy5jb20va2FnZ2xlLWF2YXRhcnMvdGh1bWJuYWlscy9kZWZhdWx0LXRodW1iLnBuZyIsImZmIjpbIktlcm5lbHNEcmFmdFVwbG9hZEJsb2IiLCJLZXJuZWxzRGlzYWJsZVVudXNlZEFjY2VsZXJhdG9yV2FybmluZyIsIktlcm5lbHNGaXJlYmFzZUxvbmdQb2xsaW5nIiwiQ29tbXVuaXR5TG93ZXJIZWFkZXJTaXplcyIsIkFsbG93Rm9ydW1BdHRhY2htZW50cyIsIktlcm5lbHNTYXZlQ2VsbE91dHB1dCIsIlRwdU9uZVZtIiwiVHB1VHdvVm1EZXByZWNhdGVkIiwiRnJvbnRlbmRFcnJvclJlcG9ydGluZyIsIkRhdGFzZXRzTWFuYWdlZEZvY3VzT25PcGVuIiwiRG9pRGF0YXNldFRvbWJzdG9uZXMiLCJDaGFuZ2VEYXRhc2V0T3duZXJzaGlwVG9PcmciLCJLZXJuZWxFZGl0b3JIYW5kbGVNb3VudE9uY2UiLCJLZXJuZWxQaW5uaW5nIiwiS01Vc2VyQWNjb3VudCIsIlBhZ2VWaXNpYmlsaXR5QW5hbHl0aWNzUmVwb3J0ZXIiLCJNYXVSZXBvcnQiLCJNb2RlbHNDYWNoZWRUYWdTZXJ2aWNlRW5hYmxlZCIsIkNvbXBldGl0aW9uc1J1bGVzS20iLCJVc2VyUmVwb3J0c1RvWmVuZGVzayIsIk5ld1RhZ3NDb21wb25lbnQiLCJEYXRhc2V0c1NoYXJlZFdpdGhUaGVtU2VhcmNoIl0sImZmZCI6eyJLZXJuZWxFZGl0b3JBdXRvc2F2ZVRocm90dGxlTXMiOiIzMDAwMCIsIkZyb250ZW5kRXJyb3JSZXBvcnRpbmdTYW1wbGVSYXRlIjoiMCIsIkVtZXJnZW5jeUFsZXJ0QmFubmVyIjoie30iLCJDbGllbnRScGNSYXRlTGltaXQiOiI0MCIsIkZlYXR1cmVkQ29tbXVuaXR5Q29tcGV0aXRpb25zIjoiMzUzMjUsMzcxNzQsMzM1NzksMzc4OTgsMzczNTQsMzc5NTksMzg4NjAiLCJBZGRGZWF0dXJlRmxhZ3NUb1BhZ2VMb2FkVGFnIjoiZGlzYWJsZWQifSwicGlkIjoia2FnZ2xlLTE2MTYwNyIsInN2YyI6IndlYi1mZSIsInNkYWsiOiJBSXphU3lBNGVOcVVkUlJza0pzQ1pXVnotcUw2NTVYYTVKRU1yZUUiLCJibGQiOiI2OTA0N2IyMGIyMGJjMTkzMWFkZjIzZjhlMDkyNTc5ZmVlMTI0NmJhIn0.; _ga_T7QHS60L4Q=GS1.1.1682954251.8.1.1682954263.0.0.0',
            'origin': 'https://www.kaggle.com',
            'referer': 'https://www.kaggle.com/datasets/utkarshx27/crimes-2001-to-present',
            'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
            'x-xsrf-token': 'CfDJ8EaxSfOz2gdBqeaKHeroAhwFIkbql4l9Z1vSAtFTYbBdX4OlhG2BuDHMuUjTaJxuKdoYNED51gAsMw3ynXqhDBFfxf32YCfxRKQhe7ybfV2f6uFIpmEU3CJdoobOyAoIbB_rqLkNzgfR5ZVqR1Frm_E',
        }

        json_data = {
            'verificationInfo': {
                'databundleVersionId': bundle_id,
                'datasetId': dataset_id
            },
        }

        response = requests.post(
            'https://www.kaggle.com/api/i/datasets.databundles.DatabundleService/GetDatabundleExternal',
            cookies=cookies,
            headers=headers,
            json=json_data,
        )

        return response

    @staticmethod
    def get_bundle_id(dataset_id):
        cookies = {
            'ACCEPTED_COOKIES': 'true',
            'ka_sessionid': '2a3bc6590bfbe15d432ab355070de063',
            'CSRF-TOKEN': 'CfDJ8EaxSfOz2gdBqeaKHeroAhzXuXU-MIVgzpvlqPQAWY93wwtdelv_htRfLEUQUCz9R1rE1Gt78wPT858bv0KqbOvd6s7Kg0uc8Vtj6vIdiw',
            '__Host-KAGGLEID': 'CfDJ8EaxSfOz2gdBqeaKHeroAhznrPqdenDET95GCicGa0vKuZBp5nXcxk6K9RSpFdht3sgOJzaLSKoxctdzsgs6rxamgL7vqJ-esxqY3omVEr9Cd5dPAGjg-eGF',
            'GCLB': 'CNfr1aHW9KOJZA',
            '_gid': 'GA1.2.1383653893.1682954251',
            '_ga': 'GA1.1.522111655.1674257473',
            '_gat_gtag_UA_12629138_1': '1',
            'XSRF-TOKEN': 'CfDJ8EaxSfOz2gdBqeaKHeroAhwFIkbql4l9Z1vSAtFTYbBdX4OlhG2BuDHMuUjTaJxuKdoYNED51gAsMw3ynXqhDBFfxf32YCfxRKQhe7ybfV2f6uFIpmEU3CJdoobOyAoIbB_rqLkNzgfR5ZVqR1Frm_E',
            'CLIENT-TOKEN': 'eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJpc3MiOiJrYWdnbGUiLCJhdWQiOiJjbGllbnQiLCJzdWIiOiJjbGF1Y2lvcmFuayIsIm5idCI6IjIwMjMtMDUtMDFUMTU6MTc6NDMuMDY1Mjc5NVoiLCJpYXQiOiIyMDIzLTA1LTAxVDE1OjE3OjQzLjA2NTI3OTVaIiwianRpIjoiNGQwYzlmMTAtNjI4Yi00NWFiLWFkNzQtOWI2MDFlYTdkNGVkIiwiZXhwIjoiMjAyMy0wNi0wMVQxNToxNzo0My4wNjUyNzk1WiIsInVpZCI6NjMwNTQzNywiZGlzcGxheU5hbWUiOiJDbGF1Y2lvIFJhbmsiLCJlbWFpbCI6ImNsYXVjaW9yYW5rQGdtYWlsLmNvbSIsInRpZXIiOiJOb3ZpY2UiLCJ2ZXJpZmllZCI6ZmFsc2UsInByb2ZpbGVVcmwiOiIvY2xhdWNpb3JhbmsiLCJ0aHVtYm5haWxVcmwiOiJodHRwczovL3N0b3JhZ2UuZ29vZ2xlYXBpcy5jb20va2FnZ2xlLWF2YXRhcnMvdGh1bWJuYWlscy9kZWZhdWx0LXRodW1iLnBuZyIsImZmIjpbIktlcm5lbHNEcmFmdFVwbG9hZEJsb2IiLCJLZXJuZWxzRGlzYWJsZVVudXNlZEFjY2VsZXJhdG9yV2FybmluZyIsIktlcm5lbHNGaXJlYmFzZUxvbmdQb2xsaW5nIiwiQ29tbXVuaXR5TG93ZXJIZWFkZXJTaXplcyIsIkFsbG93Rm9ydW1BdHRhY2htZW50cyIsIktlcm5lbHNTYXZlQ2VsbE91dHB1dCIsIlRwdU9uZVZtIiwiVHB1VHdvVm1EZXByZWNhdGVkIiwiRnJvbnRlbmRFcnJvclJlcG9ydGluZyIsIkRhdGFzZXRzTWFuYWdlZEZvY3VzT25PcGVuIiwiRG9pRGF0YXNldFRvbWJzdG9uZXMiLCJDaGFuZ2VEYXRhc2V0T3duZXJzaGlwVG9PcmciLCJLZXJuZWxFZGl0b3JIYW5kbGVNb3VudE9uY2UiLCJLZXJuZWxQaW5uaW5nIiwiS01Vc2VyQWNjb3VudCIsIlBhZ2VWaXNpYmlsaXR5QW5hbHl0aWNzUmVwb3J0ZXIiLCJNYXVSZXBvcnQiLCJNb2RlbHNDYWNoZWRUYWdTZXJ2aWNlRW5hYmxlZCIsIkNvbXBldGl0aW9uc1J1bGVzS20iLCJVc2VyUmVwb3J0c1RvWmVuZGVzayIsIk5ld1RhZ3NDb21wb25lbnQiLCJEYXRhc2V0c1NoYXJlZFdpdGhUaGVtU2VhcmNoIl0sImZmZCI6eyJLZXJuZWxFZGl0b3JBdXRvc2F2ZVRocm90dGxlTXMiOiIzMDAwMCIsIkZyb250ZW5kRXJyb3JSZXBvcnRpbmdTYW1wbGVSYXRlIjoiMCIsIkVtZXJnZW5jeUFsZXJ0QmFubmVyIjoie30iLCJDbGllbnRScGNSYXRlTGltaXQiOiI0MCIsIkZlYXR1cmVkQ29tbXVuaXR5Q29tcGV0aXRpb25zIjoiMzUzMjUsMzcxNzQsMzM1NzksMzc4OTgsMzczNTQsMzc5NTksMzg4NjAiLCJBZGRGZWF0dXJlRmxhZ3NUb1BhZ2VMb2FkVGFnIjoiZGlzYWJsZWQifSwicGlkIjoia2FnZ2xlLTE2MTYwNyIsInN2YyI6IndlYi1mZSIsInNkYWsiOiJBSXphU3lBNGVOcVVkUlJza0pzQ1pXVnotcUw2NTVYYTVKRU1yZUUiLCJibGQiOiI2OTA0N2IyMGIyMGJjMTkzMWFkZjIzZjhlMDkyNTc5ZmVlMTI0NmJhIn0.',
            '_ga_T7QHS60L4Q': 'GS1.1.1682954251.8.1.1682954263.0.0.0',
        }

        headers = {
            'authority': 'www.kaggle.com',
            'accept': 'application/json',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'content-type': 'application/json',
            # 'cookie': 'ACCEPTED_COOKIES=true; ka_sessionid=2a3bc6590bfbe15d432ab355070de063; CSRF-TOKEN=CfDJ8EaxSfOz2gdBqeaKHeroAhzXuXU-MIVgzpvlqPQAWY93wwtdelv_htRfLEUQUCz9R1rE1Gt78wPT858bv0KqbOvd6s7Kg0uc8Vtj6vIdiw; __Host-KAGGLEID=CfDJ8EaxSfOz2gdBqeaKHeroAhznrPqdenDET95GCicGa0vKuZBp5nXcxk6K9RSpFdht3sgOJzaLSKoxctdzsgs6rxamgL7vqJ-esxqY3omVEr9Cd5dPAGjg-eGF; GCLB=CNfr1aHW9KOJZA; _gid=GA1.2.1383653893.1682954251; _ga=GA1.1.522111655.1674257473; _gat_gtag_UA_12629138_1=1; XSRF-TOKEN=CfDJ8EaxSfOz2gdBqeaKHeroAhwFIkbql4l9Z1vSAtFTYbBdX4OlhG2BuDHMuUjTaJxuKdoYNED51gAsMw3ynXqhDBFfxf32YCfxRKQhe7ybfV2f6uFIpmEU3CJdoobOyAoIbB_rqLkNzgfR5ZVqR1Frm_E; CLIENT-TOKEN=eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJpc3MiOiJrYWdnbGUiLCJhdWQiOiJjbGllbnQiLCJzdWIiOiJjbGF1Y2lvcmFuayIsIm5idCI6IjIwMjMtMDUtMDFUMTU6MTc6NDMuMDY1Mjc5NVoiLCJpYXQiOiIyMDIzLTA1LTAxVDE1OjE3OjQzLjA2NTI3OTVaIiwianRpIjoiNGQwYzlmMTAtNjI4Yi00NWFiLWFkNzQtOWI2MDFlYTdkNGVkIiwiZXhwIjoiMjAyMy0wNi0wMVQxNToxNzo0My4wNjUyNzk1WiIsInVpZCI6NjMwNTQzNywiZGlzcGxheU5hbWUiOiJDbGF1Y2lvIFJhbmsiLCJlbWFpbCI6ImNsYXVjaW9yYW5rQGdtYWlsLmNvbSIsInRpZXIiOiJOb3ZpY2UiLCJ2ZXJpZmllZCI6ZmFsc2UsInByb2ZpbGVVcmwiOiIvY2xhdWNpb3JhbmsiLCJ0aHVtYm5haWxVcmwiOiJodHRwczovL3N0b3JhZ2UuZ29vZ2xlYXBpcy5jb20va2FnZ2xlLWF2YXRhcnMvdGh1bWJuYWlscy9kZWZhdWx0LXRodW1iLnBuZyIsImZmIjpbIktlcm5lbHNEcmFmdFVwbG9hZEJsb2IiLCJLZXJuZWxzRGlzYWJsZVVudXNlZEFjY2VsZXJhdG9yV2FybmluZyIsIktlcm5lbHNGaXJlYmFzZUxvbmdQb2xsaW5nIiwiQ29tbXVuaXR5TG93ZXJIZWFkZXJTaXplcyIsIkFsbG93Rm9ydW1BdHRhY2htZW50cyIsIktlcm5lbHNTYXZlQ2VsbE91dHB1dCIsIlRwdU9uZVZtIiwiVHB1VHdvVm1EZXByZWNhdGVkIiwiRnJvbnRlbmRFcnJvclJlcG9ydGluZyIsIkRhdGFzZXRzTWFuYWdlZEZvY3VzT25PcGVuIiwiRG9pRGF0YXNldFRvbWJzdG9uZXMiLCJDaGFuZ2VEYXRhc2V0T3duZXJzaGlwVG9PcmciLCJLZXJuZWxFZGl0b3JIYW5kbGVNb3VudE9uY2UiLCJLZXJuZWxQaW5uaW5nIiwiS01Vc2VyQWNjb3VudCIsIlBhZ2VWaXNpYmlsaXR5QW5hbHl0aWNzUmVwb3J0ZXIiLCJNYXVSZXBvcnQiLCJNb2RlbHNDYWNoZWRUYWdTZXJ2aWNlRW5hYmxlZCIsIkNvbXBldGl0aW9uc1J1bGVzS20iLCJVc2VyUmVwb3J0c1RvWmVuZGVzayIsIk5ld1RhZ3NDb21wb25lbnQiLCJEYXRhc2V0c1NoYXJlZFdpdGhUaGVtU2VhcmNoIl0sImZmZCI6eyJLZXJuZWxFZGl0b3JBdXRvc2F2ZVRocm90dGxlTXMiOiIzMDAwMCIsIkZyb250ZW5kRXJyb3JSZXBvcnRpbmdTYW1wbGVSYXRlIjoiMCIsIkVtZXJnZW5jeUFsZXJ0QmFubmVyIjoie30iLCJDbGllbnRScGNSYXRlTGltaXQiOiI0MCIsIkZlYXR1cmVkQ29tbXVuaXR5Q29tcGV0aXRpb25zIjoiMzUzMjUsMzcxNzQsMzM1NzksMzc4OTgsMzczNTQsMzc5NTksMzg4NjAiLCJBZGRGZWF0dXJlRmxhZ3NUb1BhZ2VMb2FkVGFnIjoiZGlzYWJsZWQifSwicGlkIjoia2FnZ2xlLTE2MTYwNyIsInN2YyI6IndlYi1mZSIsInNkYWsiOiJBSXphU3lBNGVOcVVkUlJza0pzQ1pXVnotcUw2NTVYYTVKRU1yZUUiLCJibGQiOiI2OTA0N2IyMGIyMGJjMTkzMWFkZjIzZjhlMDkyNTc5ZmVlMTI0NmJhIn0.; _ga_T7QHS60L4Q=GS1.1.1682954251.8.1.1682954263.0.0.0',
            'origin': 'https://www.kaggle.com',
            'referer': 'https://www.kaggle.com/datasets/utkarshx27/crimes-2001-to-present',
            'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
            'x-xsrf-token': 'CfDJ8EaxSfOz2gdBqeaKHeroAhwFIkbql4l9Z1vSAtFTYbBdX4OlhG2BuDHMuUjTaJxuKdoYNED51gAsMw3ynXqhDBFfxf32YCfxRKQhe7ybfV2f6uFIpmEU3CJdoobOyAoIbB_rqLkNzgfR5ZVqR1Frm_E',
        }

        json_data = {
            'datasetId': dataset_id,
            'count': 99999,
            'hashLink': '',
        }

        response = requests.post(
            'https://www.kaggle.com/api/i/datasets.DatasetService/GetDatasetHistory',
            cookies=cookies,
            headers=headers,
            json=json_data,
        )

        json_return = json.loads(response.text)

        items = json_return.get('items')

        last_item = [x for x in items if x.get('versionInfo') is not None]

        if len(last_item) > 0:
            bundle_id = last_item[0].get('versionInfo').get('databundleVersionId')
        else:
            raise Exception('Can´t find bundle id')

        return bundle_id


if __name__ == '__main__':
    df_ids = get_data_sqlite('select distinct dataset_id from datasets')

    counter = 0
    errors = 0
    for i in df_ids['dataset_id']:
        counter += 1
        print(f'processing {counter} of {len(df_ids)} datasets')
        try:
            GetDatabundleExternal().get_info_and_save(i)
        except Exception as Err:
            errors += 1
            print(f'Error processing id {i}{Err}')

    print(f'Finished, errors: {errors}')
