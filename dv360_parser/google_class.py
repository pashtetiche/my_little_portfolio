import contextlib
import urllib
import json
import time
from requests import session
from re import split as re_split
from io import StringIO

# Google libraries
from google.cloud import bigquery
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.credentials import Credentials

# Non-native libraries
import pandas as pd

class GoogleDV360Client():
    def __init__(self, api_cred_file, query_params_list_file=None):
        '''
        api_cred_file:
            - Should be like: 'your_file_full_name'
        '''

        self.api_name = 'doubleclickbidmanager'
        self.api_version = 'v1.1'
        self.scopes = ['https://www.googleapis.com/auth/doubleclickbidmanager',
                       ]

        #self.service_credentials = ServiceAccountCredentials.from_json_keyfile_name(api_cred_file, scopes=self.scopes)
        self.service_credentials = Credentials.from_authorized_user_file(api_cred_file, scopes=self.scopes)
        self.service = self.get_service()

        self.query_params_list = self.get_query_params_list(query_params_list_file)
        self.query_file_name = ''
        #print(self.query_params_list)
        self.session = session()
        self._query_id = ''

    def get_service(self):
        service = build(self.api_name, self.api_version, credentials=self.service_credentials)

        return service

    def get_query_params_list(self, query_params_list_file):
        content = {}
        t = ''
        if query_params_list_file:
            with open(query_params_list_file, 'r') as f:
                t = f.read()
                content = json.loads(t)

            if not content:
                if not t:
                    print(f'get_query_params_list: Reading file problem. File name: "{query_params_list_file}"')
                else:
                    print(f'get_query_params_list: Making json problem. File name: "{query_params_list_file}"')
        else:
            print(f'get_query_params_list: No file passed in. File name: "{query_params_list_file}"')

        return content

    def get_query_params(self, query_params_list, query_name):
        return query_params_list.get(query_name)

    # Block for methods for working with queries

    def _create_query_by_body(self, query_body):
        request = self.service.queries().createquery(body=query_body)
        #print(request.to_json())
        #exit()
        response = request.execute()

        return response

    def create_query(self, query_body, query_name=None):
        #print('query_name', query_name)
        if query_name:
            query_body = self.query_params_list.get(query_name)
            query = self._create_query_by_body(query_body)
        elif query_body:
            query = self._create_query_by_body(query_body)
        else:
            raise Exception('create_query: invalid query data passed')

        return query

    def _run_query(self, query_id):
        request = self.service.queries().runquery(queryId=query_id,
                                                  body={'timezoneCode': 'Europe/Kiev'}
                                                  )
        #print(request.to_json())
        response = request.execute()

        return query_id

    def run_query(self, query_id=None, query_body=None, query_name=None):
        if query_id:
            query_id = self._run_query(query_id)
        elif query_body:
            resp = self.create_query(query_name=query_name, query_body=query_body)
            query_id = resp.get('queryId')
            print(query_id)
            query_id = self._run_query(query_id)
        else:
            raise Exception('run_query: invalid query data passed')

        return query_id

    def _get_query_custom_id(self, query_id):
        request = self.service.queries().getquery(queryId=query_id)
        response = request.execute()

        return response

    def _get_query_response_id(self, query_info):
        request = self.service.queries().getquery(queryId=query_info.get('queryId'))
        response = request.execute()

        return response

    def get_query(self, query_id=None, query_info=None):
        if query_id:
            return self._get_query_custom_id(query_id)
        elif query_info:
            return self._get_query_response_id(query_info)
        else:
            raise Exception('get_query: No query_id or query_info passed.')

    def get_file(self, query_info, download_file_name=None, file_link=None):
        if not file_link:
            file_link = query_info.get('metadata').get('googleCloudStoragePathForLatestReport')
        if download_file_name:
            return self._get_file_from_link(file_link, download_file_name)
        return self._get_df(file_link)

    def _get_df(self, file_link):
        with self.session.get(file_link) as url:
            content = url.text
            if 'No data returned by the reporting service' in content:
                print('EMPHTY DATA IN', self._query_id)
                return None
            # print(content)
            # print('\n\n')
            content = re_split(r'\n,.+\n\n.+', content)[0]
            # print(content)
            df = pd.read_csv(StringIO(content))
            _headers = list(df)
            for each in _headers:
                df.loc[df[each] == '-', each] = None
            return df

    def _get_file_from_link(self, file_link, download_file_name):
        if file_link:
            with open(download_file_name, 'wb') as output:
                with contextlib.closing(urllib.request.urlopen(file_link)) as url:
                    readed = url.read()
                    #print("AAAA",readed)
                    output.write(readed)
        else:
            print(f'No file for link: "{file_link}".')

            return None
        return download_file_name

    def get_query_list(self):
        request = self.service.queries().listqueries()
        response = request.execute()
        return response



    def delete_query(self, queryId):
        request = self.service.queries().deletequery(queryId=queryId)
        response = request.execute()

        return response

    def make_csv_string_from_row_array_1(self, row_array_):
        csv_string = '\n'.join(row_array_)

        return csv_string

    # code from previous task
    def get_cleansed_csv_data_1(self, file_, columns=None):
        i = 0
        # print(file_)
        try:
            row_data = file_.decode('utf-8').split('\n')
        except:
            row_data = file_.split('\n')

        new_row_data = [] #row_data

        # print(type(new_row_data))
        # print(new_row_data)
        #exit()

        if columns:
            new_row_data.append(columns)
        else:
            new_row_data.append(row_data[0])
        # print(new_row_data)

        #print(new_row_data[-1])
        #exit()

        # print('TYPEE ',type(row_data), len(row_data))
        # print(row_data)

        counter = 0

        for j in row_data[1:]:
            if j == '':
                break
            else:
                new_row_data.append(j)
                counter+=1

        if counter > 1:
            new_row_data = new_row_data[:-1]
        else:
            pass

        csv_string = self.make_csv_string_from_row_array_1(new_row_data)

        #         print('made string')

        return csv_string

    def write_to_csv(self, file_name, csv_data, columns=None):
        enc = 'utf-8'
        #         enc = None
        with open(file_name, 'w', encoding=enc) as file_:
            print('writing into file', file_name)
            file_.write(self.get_cleansed_csv_data_1(csv_data, columns))

        return file_name

    def check_values(self, file_name, check_values=False):
        _df = pd.read_csv(file_name)
        _headers = list(_df)

        _headers = list(_df)

        if check_values:
            for each in _headers:
                _df.loc[_df[each] == '-', each] = ''

        _df.to_csv(file_name, index=False)

        return file_name

    def get_new_data(self, saved_file_name, query_id=None, query_body=None, query_name=None,
                     check_values=False, columns=None):

        self._query_id = self.run_query(query_id=query_id, query_body=query_body, query_name=query_name)
        print('QUERY ID', self._query_id)

        query_info = self.get_query(query_id=self._query_id)
        #print(query_info)
        #print(dict(query_info).keys())

        df = self.get_file(query_info=query_info)#, download_file_name=saved_file_name)
        #         print('got some_csv_data')
        #print(df)
        return df


### Class for BigQuery
class GoogleBigQueryClient():
    '''
    Class which contains methods needed to upload data.
    '''

    def __init__(self, api_cred_file):
        '''
        api_cred_file:
            - Should be like: 'your_file_full_name'
        '''
        self.client = bigquery.Client.from_service_account_json(
            api_cred_file
        )
        self.project = 'proj_1'

    def create_data_set(self, data_set_name, dataset_location):
        '''
        Method used for dataset creation.

        data_set_name:
            - May contain up to 1,024 characters.
            - Can contain letters (upper or lower case), numbers, and underscores.
            - Is case-sensitive: mydataset and MyDataset can co-exist in the same project.

        dataset_location:
            - Should be like: 'EU'
        '''
        dataset_id = f"{self.project}.temp_set"

        dataset = bigquery.Dataset(dataset_id)
        dataset.location = dataset_location

        dataset = self.client.create_dataset(dataset)

        path_dataset_name = f'{self.project}.{dataset.dataset_id}'

        print(f'Created dataset {path_dataset_name}')

    def create_data_table(self, table_name, data_set):
        '''
        Method used for tables creation.

        table_name:
            - Contain up to 1,024 characters
            - Contain letters (upper or lower case), numbers, and underscores.

        data_set_name:
            - May contain up to 1,024 characters.
            - Can contain letters (upper or lower case), numbers, and underscores.
            - Is case-sensitive: mydataset and MyDataset can co-exist in the same project.
        '''
        path_table_name = f'{self.project}.{data_set}.{table_name}'
        self.client.create_table(path_table_name)

        print(f'Created table {path_table_name}')

    def upload_df_data(self, df, table_name, data_set, dataset_location, schema,
                        truncate=False, schema_autodetect=True):

        table_ref = '.'.join([self.project, data_set, table_name])
        lambda_dtypes = lambda x: 'object' if x in ('STRING') \
            else 'datetime64' if x in ('DATE', 'DATETIME') \
            else 'float64' if x in ('FLOAT') \
            else 'int64' if x in ('INTEGER') else 'object'

        getted_table = self.client.get_table(table_ref)
        base_schema = getted_table.schema
        #print('OLD SCHEMA', base_schema)
        columns = [c.name for c in base_schema]
        types = {c.name: lambda_dtypes(c.field_type) for c in base_schema}

        df.columns = columns
        df = df.astype(types)

        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = schema_autodetect
        job_config.schema = [bigquery.schema.SchemaField(i.name, 'DATE') for i in base_schema if i.field_type == 'DATE']

        job_config.write_disposition = 'WRITE_APPEND'
        if truncate:
            job_config.write_disposition = 'WRITE_TRUNCATE'

        job = self.client.load_table_from_dataframe(
            df,
            table_ref,
            location=dataset_location,
            job_config=job_config,
        )

        job.result()
        #print('NEW SCHEMA',job.schema)
        print(f'Loaded {job.output_rows} rows in schema len {len([(i.name, i.field_type) for i in job.schema])} '
              f'into {data_set}:{table_name}, write disposition {job_config.write_disposition}')

    def upload_csv_data(self, data_file, table_name, data_set, dataset_location, schema,
                        truncate=False, schema_autodetect=True, data_from_file=True):
        '''
        Method used to uload csv-format data in some table.

        data_file:
            - Should be like: 'your_file_full_name'

        table_name:
            - Contain up to 1,024 characters
            - Contain letters (upper or lower case), numbers, and underscores.

        data_set_name:
            - May contain up to 1,024 characters.
            - Can contain letters (upper or lower case), numbers, and underscores.
            - Is case-sensitive: mydataset and MyDataset can co-exist in the same project.

        dataset_location:
            - Should be like: 'EU'
        '''
        dataset_ref = self.client.dataset(data_set)
        table_ref = dataset_ref.table(table_name)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.autodetect = schema_autodetect
        job_config.schema = schema
        if truncate:
            job_config.write_disposition = 'WRITE_TRUNCATE'

        if data_from_file:
            with open(data_file, 'rb') as source_file:
                job = self.client.load_table_from_file(
                    source_file,
                    table_ref,
                    location=dataset_location,
                    job_config=job_config,
                )
        else:
            job = self.client.load_table_from_file(
                data_file,
                table_ref,
                location=dataset_location,
                job_config=job_config,
            )

        job.result()

        print(f'Loaded {job.output_rows} rows into {data_set}:{table_name}.')


class GoogleDCMClient():
    def __init__(self, api_cred_file):
        '''
        api_cred_file:
            - Should be like: 'your_file_full_name'
        '''

        self.reports_pages_limiter = 3

        self.profileId = None
        self.reportId = None
        self.api_name = 'dfareporting'
        self.api_version = 'v4'#'v3.5'
        self.scopes = ['https://www.googleapis.com/auth/dfareporting',
                       'https://www.googleapis.com/auth/dfatrafficking',
                       'https://www.googleapis.com/auth/ddmconversions',
                       ]

        self.service_credentials = ServiceAccountCredentials.from_json_keyfile_name(api_cred_file, scopes=self.scopes)
        self.service = self.get_service()

    #         self.reports_columns = self.get_reports_columns(reports_columns_file)

    def get_service(self):
        service = build(self.api_name, self.api_version, credentials=self.service_credentials)

        return service

    def get_reports_columns(self, reports_columns_file):
        with open(reports_columns_file, 'r') as file:
            _temp_file = file.read()
            _content = json.loads(_temp_file)

        return _content

    def get_profiles_info(self):
        request = self.service.userProfiles().list()
        response = request.execute()

        return response

    def get_profile_id(self, profiles_info, profile_name):
        profile_id = None
        for profiles in profiles_info.get('items'):
            if profiles.get('userName') == profile_name:
                profile_id = profiles.get('profileId')
                break

        return profile_id

    def _get_reports_info(self, profile_id, pageToken=None):
        #         print(f'{__name__}')
        request = self.service.reports().list(profileId=profile_id, pageToken=pageToken)
        response = request.execute()

        return response

    def get_reports_info(self, profile_id):
        #         print(f'{__name__}')
        counter = 1
        resp = {'items': []}

        response = self._get_reports_info(profile_id)

        resp['items'].extend(response['items'])

        while response['nextPageToken'] and (counter <= self.reports_pages_limiter):
            counter += 1

            response = self._get_reports_info(profile_id=profile_id, pageToken=response['nextPageToken'])

            resp['items'].extend(response['items'])

        #         print(resp)
        return resp

    def get_report_id(self, reports_info, report_name):
        report_id = None
        for reports in reports_info.get('items'):
            if reports.get('name') == report_name:
                report_id = reports.get('id')
                break
        print(report_id)
        return report_id

    def run_report(self, profile_id, report_id, max_time=None):
        sleep = 2
        incr_ = 1
        report_processing_time = 0
        max_run_time = max_time or 3600
        request = (self.service.reports().run(profileId=profile_id,
                                              reportId=report_id))

        report_file = request.execute()

        start_time = time.time()
        while True:
            report_file = self.service.files().get(reportId=report_id,
                                                   fileId=report_file['id']).execute()

            status = report_file['status']
            if status == 'REPORT_AVAILABLE':
                print(f'Report was processed for {report_processing_time}seconds.')
                print(f'File status is {status}, ready to download.')
                return int(report_file['id'])
            elif status != 'PROCESSING':
                print(f'Report was processed for {report_processing_time}seconds.')
                print(f'File status is {status}, processing failed.')
                return int(report_file['id'])
            elif time.time() - start_time > max_run_time:
                print(f'Report was processed for {report_processing_time}seconds.')
                print(f'Max_run_time={max_run_time} exceeded.')
                raise Exception(f'Report run exceeded mar_run_time={max_run_time}')

            #return int(report_file['id'])

            print(f'File status is {status}, sleeping for {sleep} seconds.')
            time.sleep(sleep)
            report_processing_time += sleep
            sleep += incr_

    def get_file_id(self, profile_id, report_id):
        file_id = None

        request = self.service.files().list(profileId=profile_id)

        response = request.execute()

        for files in response.get('items'):
            if files.get('reportId') == report_id:
                file_id = files.get('id')
                break

        return file_id

    def get_file(self, report_id, file_id):
        #         print('get_file')
        request = self.service.files().get_media(reportId=report_id,
                                                 fileId=file_id)

        #         print('executing_get_file')
        response = request.execute()
        #         print('executed_get_file')

        return response

    def make_csv_string_from_row_array(self, row_array_):
        csv_string = ''
        for i in range(len(row_array_)):
            csv_string += row_array_[i]

            if i == (len(row_array_) - 1):
                break

            csv_string += '\n'

        return csv_string

    def make_csv_string_from_row_array_1(self, row_array_):
        csv_string = '\n'.join(row_array_)

        return csv_string

    def get_cleansed_csv_data_1(self, file_, columns=None):
        i = 0

        try:
            row_data = file_.decode('utf-8').split('\n')
        except:
            row_data = file_.split('\n')

        new_row_data = row_data

        for rows in row_data:
            if 'Report Fields' in rows:
                i += 1
                break

            i += 1

        #         print(i)

        new_row_data = new_row_data[i:]

        if columns:
            new_row_data[0] = columns

        for j in range(500):
            #             print(len(new_row_data))
            if 'Grand Total:' in new_row_data[-j]:
                break
        new_row_data = new_row_data[:-(j)]

        #         print('deleted grand total')

        csv_string = self.make_csv_string_from_row_array_1(new_row_data)

        #         print('made string')

        return csv_string

    def write_to_csv(self, file_name, csv_data, columns=None):
        with open(file_name, 'w', encoding='utf-8') as file_:
            print('writing into file', file_name)
            file_.write(self.get_cleansed_csv_data_1(csv_data, columns))

    def check_values(self, file_name, check_values=False):
        _df = pd.read_csv(file_name)
        _headers = list(_df)

        if check_values:
            for each in _headers:
                _df.loc[_df[each] == '-', each] = ''

        _df.to_csv(file_name, index=False)

        return file_name


def function_read_and_write_data(credential_file,
                                 saved_file_name, query_body,
                                 bq_table_name, bq_dataset_name, bq_dataset_location, schema,
                                 query_id=None, query_name=None,
                                 truncate=False, schema_autodetect=False, check_values=False, dv_cred_file=''):

    #query_body = {"params": {"filters": []}}

    GoogleDV360Client_obj = GoogleDV360Client(dv_cred_file)
    GoogleBigQueryClient_obj = GoogleBigQueryClient(credential_file)

    df_ = GoogleDV360Client_obj.get_new_data(saved_file_name=saved_file_name, query_id=query_id,
                                               query_body=query_body, query_name=query_name,
                                               check_values=check_values, columns=None)

    if df_ is None:
        return None

    # GoogleBigQueryClient_obj.upload_csv_data(df_, bq_table_name, bq_dataset_name, bq_dataset_location, schema=schema,
    #                                          truncate=truncate, schema_autodetect=schema_autodetect)
    GoogleBigQueryClient_obj.upload_df_data(df_, bq_table_name, bq_dataset_name, bq_dataset_location, schema=schema,
                                             truncate=truncate, schema_autodetect=schema_autodetect)
#    os.remove(file_)
