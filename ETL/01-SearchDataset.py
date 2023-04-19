import pandas as pd
import requests
import json
from utils.pdToDatabase import save_to_sqlite
from utils.datetimeTreatment import parse_date


class SearchDataset:

    def __init__(self):
        pass

    def execute_search(self):
        page = 1
        has_more = True
        while has_more:
            print(f'--Starting proccess of page {page}')
            try:
                request = self.request_api(page)
                items = self.cleaning_request(request)
                save_to_sqlite(items, 'datasets')
                has_more = self.exists_more(request)
                page += 1
            except Exception as Err:
                print(f'--Process failed at page {page}: {Err}')
                has_more = False

    @staticmethod
    def exists_more(request_response):
        text = request_response.text
        text_to_dict = json.loads(text)
        exists_more = text_to_dict.get('hasMore')
        return exists_more

    def cleaning_request(self, request_response):
        status = request_response.status_code

        if status == 200:
            text = request_response.text
            text_to_dict = json.loads(text)
            # Get request items info from request
            items = text_to_dict.get('datasetList').get('items')
            list_items = [self.get_dataset_list_items(i) for i in items]
            data_frame_items = pd.DataFrame(list_items)
            # Save categories from items
            [self.get_and_save_dataset_list_categories(i) for i in items]

            return data_frame_items

    @staticmethod
    def get_and_save_dataset_list_categories(text_to_dict):
        dataset_id = text_to_dict.get('voteButton').get('datasetId')
        categories = text_to_dict.get('categories', None)
        if categories is not None:
            categories_list = []
            for i in categories:
                category_id = i.get('id', '')
                name = i.get('name', '')
                dataset_count = i.get('datasetCount', '')
                competition_count = i.get('competitionCount', '')
                notebook_count = i.get('notebookCount', '')

                dict_append = {
                    "dataset_id": dataset_id,
                    "category_id": category_id,
                    "category_name": name,
                    "dataset_count": dataset_count,
                    "competition_count": competition_count,
                    "notebook_count": notebook_count
                }

                categories_list.append(dict_append)

            df_to_save = pd.DataFrame(categories_list)
            save_to_sqlite(df_to_save, 'categories')

    @staticmethod
    def get_dataset_list_items(text_to_dict):
        dataset_url = text_to_dict.get('datasetUrl', '')
        dataset_slug = text_to_dict.get('datasetSlug', '')
        owner_name = text_to_dict.get('ownerName', '')
        owner_user_id = str(text_to_dict.get('ownerUserId', ''))
        date_created = text_to_dict.get('dateCreated', '')
        date_created = parse_date(date_created)
        date_updated = text_to_dict.get('dateUpdated', '')
        date_updated = parse_date(date_updated)
        dataset_id = text_to_dict.get('voteButton').get('datasetId')
        dataset_title = text_to_dict.get('datasource').get('title')

        return_dict = {
            "dataset_url": dataset_url,
            "dataset_slug": dataset_slug,
            "owner_name": owner_name,
            "owner_user_id": owner_user_id,
            "date_created": date_created,
            "date_updated": date_updated,
            "dataset_id": dataset_id,
            "dataset_title": dataset_title
        }

        return return_dict

    @staticmethod
    def request_api(page):
        cookies = {
            'ka_sessionid': 'e02b9c8bbaa06824603ca542e8f306b5',
            '__Host-KAGGLEID': 'CfDJ8PVuHcwegW5Mva7E9-xEvef3KOPIBr5Xh61wINi2MrxMQnTrxF-4evzq4-Gmu3BI96r-L_NuDvnBIdPliFMcRrdDRNn1SFcPdFgsoN35gVOVcDCJX_GWJk_m',
            'ACCEPTED_COOKIES': 'true',
            'CSRF-TOKEN': 'CfDJ8PVuHcwegW5Mva7E9-xEveev2w6ZUQDnMSS-xKWAZ8W5ZzcUDSRJQiuxepo1L_VzYFqHFlme-FEL6DTZWK0dt7HKiyH3A9AEJNI2Ejpk1w',
            'GCLB': 'CJKS8rqMl8rZyQE',
            'XSRF-TOKEN': 'CfDJ8PVuHcwegW5Mva7E9-xEvefSvvh0oiNx_EBsuLIBmtum2ZaY1uKWU5UVPFwParVzUp7MEepxQIy2tfbKQLJXfJ_m3WbYad8jtl3rf_MNxifamQ5RbiyptJF_D2amhLodZDM_Yubphr_BKn4FR_5YXLc',
            'CLIENT-TOKEN': 'eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJpc3MiOiJrYWdnbGUiLCJhdWQiOiJjbGllbnQiLCJzdWIiOiJjbGF1Y2lvcmFuayIsIm5idCI6IjIwMjMtMDQtMTJUMTE6MTI6NTkuMzQ0OTU0NVoiLCJpYXQiOiIyMDIzLTA0LTEyVDExOjEyOjU5LjM0NDk1NDVaIiwianRpIjoiOTQ1ZjYzZDYtYzc5Ny00MThmLWFmMTctNWNmODNjMjJmOTZkIiwiZXhwIjoiMjAyMy0wNS0xMlQxMToxMjo1OS4zNDQ5NTQ1WiIsInVpZCI6NjMwNTQzNywiZGlzcGxheU5hbWUiOiJDbGF1Y2lvIFJhbmsiLCJlbWFpbCI6ImNsYXVjaW9yYW5rQGdtYWlsLmNvbSIsInRpZXIiOiJOb3ZpY2UiLCJ2ZXJpZmllZCI6ZmFsc2UsInByb2ZpbGVVcmwiOiIvY2xhdWNpb3JhbmsiLCJ0aHVtYm5haWxVcmwiOiJodHRwczovL3N0b3JhZ2UuZ29vZ2xlYXBpcy5jb20va2FnZ2xlLWF2YXRhcnMvdGh1bWJuYWlscy9kZWZhdWx0LXRodW1iLnBuZyIsImZmIjpbIktlcm5lbHNEcmFmdFVwbG9hZEJsb2IiLCJLZXJuZWxzRmlyZWJhc2VMb25nUG9sbGluZyIsIkNvbW11bml0eUxvd2VySGVhZGVyU2l6ZXMiLCJBbGxvd0ZvcnVtQXR0YWNobWVudHMiLCJLZXJuZWxzU2F2ZUNlbGxPdXRwdXQiLCJUcHVPbmVWbSIsIkZyb250ZW5kRXJyb3JSZXBvcnRpbmciLCJEYXRhc2V0c01hbmFnZWRGb2N1c09uT3BlbiIsIkRvaURhdGFzZXRUb21ic3RvbmVzIiwiS2VybmVsRWRpdG9ySGFuZGxlTW91bnRPbmNlIiwiS2VybmVsUGlubmluZyIsIlBhZ2VWaXNpYmlsaXR5QW5hbHl0aWNzUmVwb3J0ZXIiLCJNYXVSZXBvcnQiLCJNb2RlbHNDYWNoZWRUYWdTZXJ2aWNlRW5hYmxlZCIsIlVzZXJSZXBvcnRzVG9aZW5kZXNrIl0sImZmZCI6eyJLZXJuZWxFZGl0b3JBdXRvc2F2ZVRocm90dGxlTXMiOiIzMDAwMCIsIkZyb250ZW5kRXJyb3JSZXBvcnRpbmdTYW1wbGVSYXRlIjoiMCIsIkVtZXJnZW5jeUFsZXJ0QmFubmVyIjoie30iLCJDbGllbnRScGNSYXRlTGltaXQiOiI0MCIsIkZlYXR1cmVkQ29tbXVuaXR5Q29tcGV0aXRpb25zIjoiMzUzMjUsMzcxNzQsMzM1NzksMzc4OTgsMzczNTQsMzc5NTksMzg4NjAiLCJBZGRGZWF0dXJlRmxhZ3NUb1BhZ2VMb2FkVGFnIjoiZGlzYWJsZWQifSwicGlkIjoia2FnZ2xlLTE2MTYwNyIsInN2YyI6IndlYi1mZSIsInNkYWsiOiJBSXphU3lBNGVOcVVkUlJza0pzQ1pXVnotcUw2NTVYYTVKRU1yZUUiLCJibGQiOiJhZDA2NmI4N2RhZTkxMzk1N2JjOWE0NTY3MmFiOGMwY2UyNGY5ZTY0In0.',
        }

        headers = {
            'authority': 'www.kaggle.com',
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            # 'cookie': 'ka_sessionid=e02b9c8bbaa06824603ca542e8f306b5; __Host-KAGGLEID=CfDJ8PVuHcwegW5Mva7E9-xEvef3KOPIBr5Xh61wINi2MrxMQnTrxF-4evzq4-Gmu3BI96r-L_NuDvnBIdPliFMcRrdDRNn1SFcPdFgsoN35gVOVcDCJX_GWJk_m; ACCEPTED_COOKIES=true; CSRF-TOKEN=CfDJ8PVuHcwegW5Mva7E9-xEveev2w6ZUQDnMSS-xKWAZ8W5ZzcUDSRJQiuxepo1L_VzYFqHFlme-FEL6DTZWK0dt7HKiyH3A9AEJNI2Ejpk1w; GCLB=CJKS8rqMl8rZyQE; XSRF-TOKEN=CfDJ8PVuHcwegW5Mva7E9-xEvefSvvh0oiNx_EBsuLIBmtum2ZaY1uKWU5UVPFwParVzUp7MEepxQIy2tfbKQLJXfJ_m3WbYad8jtl3rf_MNxifamQ5RbiyptJF_D2amhLodZDM_Yubphr_BKn4FR_5YXLc; CLIENT-TOKEN=eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJpc3MiOiJrYWdnbGUiLCJhdWQiOiJjbGllbnQiLCJzdWIiOiJjbGF1Y2lvcmFuayIsIm5idCI6IjIwMjMtMDQtMTJUMTE6MTI6NTkuMzQ0OTU0NVoiLCJpYXQiOiIyMDIzLTA0LTEyVDExOjEyOjU5LjM0NDk1NDVaIiwianRpIjoiOTQ1ZjYzZDYtYzc5Ny00MThmLWFmMTctNWNmODNjMjJmOTZkIiwiZXhwIjoiMjAyMy0wNS0xMlQxMToxMjo1OS4zNDQ5NTQ1WiIsInVpZCI6NjMwNTQzNywiZGlzcGxheU5hbWUiOiJDbGF1Y2lvIFJhbmsiLCJlbWFpbCI6ImNsYXVjaW9yYW5rQGdtYWlsLmNvbSIsInRpZXIiOiJOb3ZpY2UiLCJ2ZXJpZmllZCI6ZmFsc2UsInByb2ZpbGVVcmwiOiIvY2xhdWNpb3JhbmsiLCJ0aHVtYm5haWxVcmwiOiJodHRwczovL3N0b3JhZ2UuZ29vZ2xlYXBpcy5jb20va2FnZ2xlLWF2YXRhcnMvdGh1bWJuYWlscy9kZWZhdWx0LXRodW1iLnBuZyIsImZmIjpbIktlcm5lbHNEcmFmdFVwbG9hZEJsb2IiLCJLZXJuZWxzRmlyZWJhc2VMb25nUG9sbGluZyIsIkNvbW11bml0eUxvd2VySGVhZGVyU2l6ZXMiLCJBbGxvd0ZvcnVtQXR0YWNobWVudHMiLCJLZXJuZWxzU2F2ZUNlbGxPdXRwdXQiLCJUcHVPbmVWbSIsIkZyb250ZW5kRXJyb3JSZXBvcnRpbmciLCJEYXRhc2V0c01hbmFnZWRGb2N1c09uT3BlbiIsIkRvaURhdGFzZXRUb21ic3RvbmVzIiwiS2VybmVsRWRpdG9ySGFuZGxlTW91bnRPbmNlIiwiS2VybmVsUGlubmluZyIsIlBhZ2VWaXNpYmlsaXR5QW5hbHl0aWNzUmVwb3J0ZXIiLCJNYXVSZXBvcnQiLCJNb2RlbHNDYWNoZWRUYWdTZXJ2aWNlRW5hYmxlZCIsIlVzZXJSZXBvcnRzVG9aZW5kZXNrIl0sImZmZCI6eyJLZXJuZWxFZGl0b3JBdXRvc2F2ZVRocm90dGxlTXMiOiIzMDAwMCIsIkZyb250ZW5kRXJyb3JSZXBvcnRpbmdTYW1wbGVSYXRlIjoiMCIsIkVtZXJnZW5jeUFsZXJ0QmFubmVyIjoie30iLCJDbGllbnRScGNSYXRlTGltaXQiOiI0MCIsIkZlYXR1cmVkQ29tbXVuaXR5Q29tcGV0aXRpb25zIjoiMzUzMjUsMzcxNzQsMzM1NzksMzc4OTgsMzczNTQsMzc5NTksMzg4NjAiLCJBZGRGZWF0dXJlRmxhZ3NUb1BhZ2VMb2FkVGFnIjoiZGlzYWJsZWQifSwicGlkIjoia2FnZ2xlLTE2MTYwNyIsInN2YyI6IndlYi1mZSIsInNkYWsiOiJBSXphU3lBNGVOcVVkUlJza0pzQ1pXVnotcUw2NTVYYTVKRU1yZUUiLCJibGQiOiJhZDA2NmI4N2RhZTkxMzk1N2JjOWE0NTY3MmFiOGMwY2UyNGY5ZTY0In0.',
            'origin': 'https://www.kaggle.com',
            'referer': 'https://www.kaggle.com/datasets?sort=published',
            'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
            'x-xsrf-token': 'CfDJ8PVuHcwegW5Mva7E9-xEvefSvvh0oiNx_EBsuLIBmtum2ZaY1uKWU5UVPFwParVzUp7MEepxQIy2tfbKQLJXfJ_m3WbYad8jtl3rf_MNxifamQ5RbiyptJF_D2amhLodZDM_Yubphr_BKn4FR_5YXLc',
        }

        json_data = {
            'page': page,
            'group': 'PUBLIC',
            'size': 'ALL',
            'fileType': 'ALL',
            'license': 'ALL',
            'viewed': 'ALL',
            'categoryIds': [],
            'search': '',
            'sortBy': 'VOTES',
            'hasTasks': False,
            'includeTopicalDatasets': False,
        }

        response = requests.post(
            'https://www.kaggle.com/api/i/datasets.DatasetService/SearchDatasets',
            cookies=cookies,
            headers=headers,
            json=json_data,
        )

        return response


if __name__ == '__main__':
    SearchDataset().execute_search()
