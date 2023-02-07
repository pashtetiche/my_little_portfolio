from google_class import GoogleDV360Client
from google_class import function_read_and_write_data
import traceback
# from google.oauth2.credentials import Credentials
# from googleapiclient import discovery
import concurrent.futures
# from memory_profiler import profile
import dask
from google.oauth2 import service_account
from google.cloud import bigquery

def format_query_body(body):
    type_report = body['params']['type']
    groupbys = body['params']['groupBys']
    metrics = body['params']['metrics']
    datarange = body['metadata']['dataRange']
    title = body['metadata']['title']
    return {
        "kind": "doubleclickbidmanager#query",

        "params": {
            "type": type_report,
            "groupBys": groupbys,
            "metrics": metrics,
            'filters': [
                {'type': 'FILTER_PARTNER', 'value': 'xxxxxx'},
            ],
        },
        "metadata": {
            "dataRange": datarange,
            "format": "CSV",
            "title": title,
        },
        #"timezoneCode": "Europe/Kiev",
        "schedule": {
            "frequency": "ONE_TIME"
        }
    }

new_analytics_querys_dict = {
    # id rep: name table
}

new_analytics_querys_dict_top = {
    # id rep: name table
}

credential_file = 'db_reporting.json'
project_id = 'proj_1'

source = 'temporary.csv'
data_set = 'DV360'
data_set_location = 'US'

dv_cred_file = 'dvcred1.json'

client = GoogleDV360Client(dv_cred_file)

dv_cred_file_top = 'dvcred2.json'

client_top = GoogleDV360Client(dv_cred_file_top)

bq_service_cred = service_account.Credentials.from_service_account_file(credential_file)
client_bq = bigquery.Client(project_id, credentials=bq_service_cred)

els_in_chunk = 10
dask.config.set(pool=concurrent.futures.ThreadPoolExecutor(els_in_chunk))

def corutine(n, **kwargs):
    # try:
    print(f"BEGIN QUERY {n['metadata']['title']}")
    function_read_and_write_data(credential_file=credential_file,
                                 saved_file_name=source, #НЕ ИСПОЛЬЗУЕТСЯ
                                 query_body=None,
                                 bq_table_name=n['metadata']['title'],
                                 bq_dataset_name=data_set,
                                 bq_dataset_location=data_set_location,
                                 schema=None,
                                 query_id=n['queryId'],
                                 #truncate=True,
                                 truncate=kwargs['is_trunc'],
                                 schema_autodetect=True,
                                 check_values=False,
                                 dv_cred_file=kwargs['dv_cred_file'])
    print(f"END QUERY {n['metadata']['title']}")
    # except IndexError as e:
    #     print(traceback.format_exc())
    #     print('FUCK DV', e, n)
    #     #continue

def concurrent_pipeline(data, corutine, **kwargs):
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = []
        data_new = []

        for n in data:
            futures.append(executor.submit(corutine, n, **kwargs))
        for future in concurrent.futures.as_completed(futures):
            try:
                data_new.append(future.result())
            except IndexError as e:
                print(traceback.format_exc())
                print('FUCK DV', e, n)
                # continue

        return data_new

@dask.delayed
def trunc_bq(t, client_bq, project_id, dataset):
    client_bq.query(f"TRUNCATE TABLE `{project_id}.{dataset}.{t}`").result()
    print(f"TRUNCATE TABLE {project_id}.{dataset}.{t}")

dask_exec = [trunc_bq(t, client_bq, project_id, data_set) for t in list(new_analytics_querys_dict.values())]
# print(dask_exec)
dask.delayed(dask_exec).compute()

new_querys = [client.service.queries().getquery(queryId=int(k)).execute() for k, _ in
              new_analytics_querys_dict.items()]

## creating reports
# new_query_bodys = [format_query_body(query) for query in new_querys]
# resps = [client_top.service.queries().createquery(body=b).execute() for b in new_query_bodys]
# print(resps)

new_querys_top = [client_top.service.queries().getquery(queryId=int(k)).execute() for k, _ in
                  new_analytics_querys_dict_top.items()]

#print(new_querys)
#print(new_querys_top)

def main(resp, reqv):
    concurrent_pipeline(new_querys, corutine, **{'is_trunc': True, 'dv_cred_file': dv_cred_file})
    concurrent_pipeline(new_querys_top, corutine, **{'is_trunc': False, 'dv_cred_file': dv_cred_file_top})
