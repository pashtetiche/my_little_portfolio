from google.cloud import bigquery
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.api_core.exceptions import DeadlineExceeded
from pandas import json_normalize, DataFrame, concat
from numpy import nan
from google.protobuf import json_format
#import json
from google.oauth2 import service_account
import re
import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import dask
#import os
import traceback
#import more_itertools
from google.api_core.retry import Retry

#start = time.time()

class TimeoutException500s(Exception):
    def __init__(self, message="500 SECONDS PASS"):
        self.message = message
        super().__init__(self.message)

class AdsErrorException(Exception):
    def __init__(self, message="ERROR GoogleAdsException WRAPPER"):
        self.message = message
        super().__init__(self.message)

class TimeoutRequestException(Exception):
    def __init__(self, message="ERROR DeadlineExceeded WRAPPER"):
        self.message = message
        super().__init__(self.message)

class Ads_to_BQ:

    project_id = 'proj_1'
    dataset = 'Google_Ads' #'checklist_data_async'
    bq_cred = 'db_reporting.json'
    bq_service_cred = None
    client_bq = None
    ads_client = None
    ads_service = None
    tokens_table = 'proj_1.Active_dataset.tokens'
    manager_id = '*******'

    ads_cred = 'google-ads.yaml'
    script_name = 'google_ads_main_parser_production'
    #str(os.path.basename(__file__))
    global_batch_shape = 15
    _CLIENT_TIMEOUT_SECONDS = 30

    def __init__(self, client_obj, default_ads_query=None):

        self.currency = 'UAH'

        self.client_name = client_obj['name']
        self.client_id = client_obj['id']
        self.default_ads_query = default_ads_query
        #self.counter = 0
        self.table = ''
        self.query = ''
        self.col = []
        self.where_stetement = ''
        self.first_bq_operation = 'WRITE_APPEND'
        self.team = 'SIM-SIM'
        self.stream = None
        self.batch_n = 0
        self.done = True
        self.page_token = 'CODE'
        self.rows_count = 0
        self.rows_columns = 0
        self.df_object = DataFrame()
        #self.schema = {}

    def __repr__(self):
        return f"Parser obj: (client {self.client_name}, table {self.table})"

    def __del__(self):
        pass
        #print(f'DELET SELF {self}, DONE {self.done}, SHAPE {self.df_object.shape}, TOKEN {self.page_token}')

    # @classmethod
    # def get_bq_service(self):
    #     return self.client_bq

    def df_unnest(self, df):
        df2 = json_normalize(df.to_dict('records'))
        df2.columns = [f"{c.replace('.', '__')}" for c in df2.columns]
        #df_zagl = DataFrame([{k:'' for k in self.col}])
        #df2 = concat([df_zagl, df2]).....
        df2 = df2.fillna('').replace({nan: ''}).astype(str)
        return df2.drop([i for i in df2.columns if '__' not in i], axis=1)

    def ads_chunk_to_df(self, chunk):
        df = self.df_unnest(
            DataFrame(chunk)
        )
        if df.shape[0] == 0:
            df = DataFrame()

        self.df_object = df
        self.rows_count = df.shape[0]
        # print(f"CREATE DF FOR TABLE {self.table} AND CLIENT "
        #       f"{self.client_name} WITH SHAPE {df.shape} from BATCH {self.batch_n}, PAGE TOKEN IS "
        #       f"{self.page_token} AND STATUS IS {self.done}")

    @classmethod
    def create_bq_job_resources(cls, df, client_bq, table, dataset, project, schema):
        bq_data_operation = 'WRITE_APPEND'
        table_id = '.'.join([project, dataset, table])
        job_config = bigquery.LoadJobConfig(write_disposition=bq_data_operation)
        job_config.autodetect = True
        #print('F', df.columns.values.tolist())

        schema = [bigquery.schema.SchemaField(column_bq, type_bq) for type_bq, column_bq in schema] ### AAA
        job_config.schema = schema ### AAA
        return {
            #'job': client_bq.load_table_from_dataframe(df, table_id, job_config=job_config),
            'client_bq': client_bq,
            'df': df,
            'table_id': table_id,
            'job_config': job_config,
            'message': f"Loaded {df.shape[0]} rows and {df.shape[1]} columns to {table_id} with operation {bq_data_operation}"
        }

    @classmethod
    def total_df_to_bq(cls, df, client_bq, table, dataset, project, schema):
        bq_data_operation = 'WRITE_APPEND'
        table_id = '.'.join([project, dataset, table])
        job_config = bigquery.LoadJobConfig(write_disposition=bq_data_operation)
        job_config.autodetect = True
        #print('F', df.columns.values.tolist())

        schema = [bigquery.schema.SchemaField(column_bq, type_bq) for type_bq, column_bq in schema] ### AAA
        job_config.schema = schema ### AAA
        job = client_bq.load_table_from_dataframe(df, table_id, job_config=job_config)
        #try:
        job.result()
        print(
            f"Loaded {df.shape[0]} rows and {df.shape[1]} columns to {table_id} with operation {bq_data_operation}")

    def ads_chunk_to_bq(self, chunk):
        df = self.df_unnest(
            DataFrame(chunk)
        )
        #print(df.shape)
        self.rows_count = df.shape[0]
        self.rows_columns = df.shape[1]
        if df.shape == (0, 0):
            #print('SHAPE ZERO')
            raise StopIteration
        bq_data_operation = 'WRITE_APPEND'

        table_id = '.'.join([self.project_id, self.dataset, self.table])
        job_config = bigquery.LoadJobConfig(write_disposition=bq_data_operation)
        job_config.autodetect = True
        #print('F', df.columns.values.tolist())
        job = self.client_bq.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"Loaded {self.rows_count} rows and {self.rows_columns} columns to {table_id} for client {self.client_name} and operation {bq_data_operation} from BATCH {self.batch_n}")

    def get_ads_batch(self, message):
        #print(f'{message}: REQUEST TO ADS STARTED for table {self.table}, client {self.client_name}')
        search_request = self.ads_client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = self.client_id
        search_request.query = self.query
        search_request.page_size = 10000
        # когда мы инициируем парсер, мы можем протащить токен из другого источника
        if self.page_token and self.page_token != 'CODE':
            search_request.page_token = self.page_token
        try:
            response = self.ads_service.search(
                request=search_request,
                retry=Retry(
                    deadline=self._CLIENT_TIMEOUT_SECONDS,
                    initial=self._CLIENT_TIMEOUT_SECONDS,
                    maximum=self._CLIENT_TIMEOUT_SECONDS,
                )
            )
            return response
        except DeadlineExceeded as ex:
            print("The server streaming call did not complete before the timeout.")
            raise TimeoutRequestException(f'ERROR DeadlineExceeded in CLIENT {self.client_name} and TABLE {self.table}')
        except GoogleAdsException as ex:
            print(
                f"Request with ID '{ex.request_id}' failed with status "
                f"'{ex.error.code().name}' and includes the following errors:"
            )
            for error in ex.failure.errors:
                print(f"\tError with message '{error.message}'.")
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        print(f"\t\tOn field: {field_path_element.field_name}")
            raise AdsErrorException(f'ERROR GoogleAdsException in CLIENT {self.client_name} and TABLE {self.table}')

    def stream_init(self, message):
        stream = self.get_ads_batch(message)
        self.stream = stream
        return True

    def stream_click(self):
        if self.stream is None:
            #print('STREAM IS NONE')
            self.page_token = None
            return []
        iterat = next(self.stream.pages)
        #print('ITERAT ',iterat)
        self.page_token = None if iterat.next_page_token == '' else iterat.next_page_token
        data = [json_format.MessageToDict(i) for i in iterat.results]
        del self.stream
        return data

    def stream_processing_click(self):

        try:

            if self.page_token is None:
                #print('PAGE TOKEN IS NONE', self.client_name)
                raise StopIteration

            click = self.stream_click()

            #self.ads_chunk_to_bq(click)
            self.ads_chunk_to_df(click)
            del click

            if self.df_object.empty:
                #print('SHAPE ZERO', self.client_name)
                raise StopIteration

            self.batch_n += 1
            self.done = False

            #self.stream_init(f'CLICK BATCH {self.batch_n}')
            return self
        except StopIteration:
            #print(f"{self} END in time {datetime.datetime.now().strftime('%H:%M:%S')}")
            self.done = True
            return self

def convert_to_upper(match_obj):
    if match_obj.group() is not None:
        return match_obj.group().upper().replace('_', '')

to_tokens_table = []

querys = [
    ( #0
    'main_metric_daily',
    """

SELECT campaign.name, campaign.id, ad_group.name, ad_group.id, segments.date, 
            customer.currency_code, customer.id, customer.descriptive_name,
            metrics.impressions, metrics.clicks, metrics.average_cpm, metrics.active_view_impressions, 
            metrics.video_views, metrics.video_quartile_p25_rate, metrics.video_quartile_p50_rate, 
            metrics.video_quartile_p75_rate, metrics.video_quartile_p100_rate, metrics.conversions, 
            metrics.conversions_value,metrics.active_view_viewability, 
            metrics.content_rank_lost_impression_share,metrics.search_rank_lost_impression_share,
            ad_group.status
            FROM ad_group 
""",
    {
        'customer__descriptiveName': ('STRING', 'client_name'), 'customer__id': ('STRING', 'client_id'),
        'campaign__name': ('STRING', 'campaign_name'), 'campaign__id': ('STRING', 'campaign_id'),
        'customer__currencyCode': ('STRING', 'campaign_currency'), 'adGroup__name': ('STRING', 'ad_group_name'),
        'adGroup__id': ('STRING', 'ad_group_id'), 'segments__date': ('DATE', 'date'),
        'metrics__impressions': ('INTEGER', 'impressions'), 'metrics__clicks': ('INTEGER', 'clicks'),
        'metrics__averageCpm': ('FLOAT', 'average_cpm'),
        'metrics__activeViewImpressions': ('INTEGER', 'active_view_impressions'),
        'metrics__videoViews': ('INTEGER', 'video_views'),
        'metrics__videoQuartileP25Rate': ('FLOAT', 'video_quartile_25_rate'),
        'metrics__videoQuartileP50Rate': ('FLOAT', 'video_quartile_50_rate'),
        'metrics__videoQuartileP75Rate': ('FLOAT', 'video_quartile_75_rate'),
        'metrics__videoQuartileP100Rate': ('FLOAT', 'video_quartile_100_rate'),
        'metrics__conversions': ('FLOAT', 'conversions'), 'metrics__conversionsValue': ('FLOAT', 'conversions_value'),
        'metrics__activeViewViewability': ('FLOAT', 'active_view_viewability'),
        'metrics__contentRankLostImpressionShare': ('FLOAT', 'content_rank_lost_impression_share'),
        'metrics__searchRankLostImpressionShare': ('FLOAT', 'search_rank_lost_impression_share'),
        'adGroup__resourceName': ('STRING', 'adGroup__resourceName'),
        'campaign__resourceName': ('STRING', 'campaign__resourceName'),
        'customer__resourceName': ('STRING', 'customer__resourceName'),
        'adGroup__status': ('STRING', 'ad_group_status')
    },
),
    (
        'demographic_gender',
        """
SELECT customer.id, customer.descriptive_name, campaign.name, campaign.id, 
             ad_group.name, ad_group.id, segments.date, 
             gender_view.resource_name, metrics.impressions 
             FROM gender_view 
        """,
        {
            'customer__descriptiveName': ('STRING', 'client_name'), 'customer__id': ('STRING', 'client_id'),
            'campaign__name': ('STRING', 'campaign_name'), 'campaign__id': ('STRING', 'campaign_id'),
            'adGroup__name': ('STRING', 'ad_group_name'), 'adGroup__id': ('STRING', 'ad_group_id'),
            'segments__date': ('DATE', 'date'), 'genderView__resourceName': ('STRING', 'genderView__resourceName'),
            'metrics__impressions': ('INTEGER', 'impressions'), 'adGroup__resourceName': ('STRING', 'adGroup__resourceName'),
            'campaign__resourceName': ('STRING', 'campaign__resourceName'),
            'customer__resourceName': ('STRING', 'customer__resourceName'),
            'gender': ('STRING', 'gender')
        }
    ),
    (
        'demographic_age',
        """
SELECT customer.id, customer.descriptive_name, campaign.name, campaign.id, 
             ad_group.name, ad_group.id, segments.date, 
             age_range_view.resource_name, metrics.impressions 
             FROM age_range_view 
        """,
        {
            'customer__descriptiveName': ('STRING', 'client_name'), 'customer__id': ('STRING', 'client_id'),
            'campaign__name': ('STRING', 'campaign_name'), 'campaign__id': ('STRING', 'campaign_id'),
            'adGroup__name': ('STRING', 'ad_group_name'), 'adGroup__id': ('STRING', 'ad_group_id'),
            'segments__date': ('DATE', 'date'), 'ageRangeView__resourceName': ('STRING', 'ageRangeView__resourceName'),
            'metrics__impressions': ('INTEGER', 'impressions'), 'adGroup__resourceName': ('STRING', 'adGroup__resourceName'),
            'campaign__resourceName': ('STRING', 'campaign__resourceName'),
            'customer__resourceName': ('STRING', 'customer__resourceName'),
            'age_range': ('STRING', 'age_range')
        }
    ),
    # (
    #     'keywords_metric',
    #     """
    #     SELECT keyword_view.resource_name, ad_group_criterion.display_name, ad_group_criterion.negative,
    #     customer.id, customer.manager, customer.resource_name, customer.status, customer.descriptive_name,
    #     customer.currency_code, metrics.cost_micros, metrics.impressions, metrics.clicks, metrics.video_views,
    #     metrics.interactions, metrics.engagements, metrics.conversions, metrics.all_conversions,
    #     metrics.view_through_conversions, metrics.cross_device_conversions
    #     FROM keyword_view
    #     """
    # )
]

#querys = [querys[1], querys[2]]

def create_parsers(querys, bq_service_cred, client_bq, ads_client, ads_service, accounts):
    date_end = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    date_start = (datetime.date.today() - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
    date_start_gender_age = (datetime.date.today() - datetime.timedelta(days=60)).strftime('%Y-%m-%d')
    date_start_keywords = (datetime.date.today() - datetime.timedelta(days=20)).strftime('%Y-%m-%d')

    #all_geo = []
    parser_list = []

    #counter = 0
    for customer_name, customer_id, currency, status_customer in accounts:
        bq_operation = 'WRITE_APPEND'
        for ind, template in enumerate(querys):
            client = {'id': str(customer_id), 'name': customer_name}
            parser = Ads_to_BQ(client, template)
            parser.bq_service_cred = bq_service_cred
            parser.client_bq = client_bq
            parser.ads_client = ads_client
            parser.ads_service = ads_service
            parser.manager_id = Ads_to_BQ.manager_id
            parser.first_bq_operation = bq_operation
            parser.currency = currency
            #parser.team = team
            parser.col = [re.sub(r"_[a-z]", convert_to_upper, i).replace('.', '__')
                          for i in re.findall(r"[\w\.]+", template[1])[1:-2]]

            if template[0] == 'main_metric_daily':
                pass
                parser.where_stetement = f"""
                WHERE segments.date
                BETWEEN '{date_start}' AND '{date_end}'
                """
            elif template[0] == 'demographic_gender' or template[0] == 'demographic_age':
                pass
                parser.where_stetement = f"""
                WHERE segments.date
                BETWEEN '{date_start_gender_age}' AND '{date_end}'
                """
            elif template[0] == 'keywords_metric':
                pass
                parser.where_stetement = f"""
                WHERE segments.date
                BETWEEN '{date_start_keywords}' AND '{date_end}'
                AND metrics.cost_micros > 0
                """
            else:
                parser.where_stetement = """
                """

            worked_query = ('foo', 'bar')
            if parser.default_ads_query:
                worked_query = parser.default_ads_query

            table, query, schema = worked_query
            parser.table = table
            parser.query = query
            #parser.schema = schema

            if parser.where_stetement != '':
                parser.query = parser.query + " " + parser.where_stetement
            #print(parser.query)
            parser_list.append(parser)
    return parser_list

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def parser_initiator(parser):
    #parser.query_wrapper()
    return parser.stream_init('INIT')

def parser_processor(parser):
    if parser:
        return parser.stream_processing_click()
    else:
        return parser

def threads_pool_manager(corutine, parsers, threads):
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = []
        data_new = []

        for n in parsers:
            futures.append(executor.submit(corutine, n))
        for future in as_completed(futures):
            data_new.append(future.result())

        return data_new

@dask.delayed
def trunc_bq(t, client_bq, project_id, dataset):
    r = client_bq.query(f"TRUNCATE TABLE `{project_id}.{dataset}.{t}`")
    #print(dir(r))
    r.result()
    #print(f"TRUNCATE TABLE {project_id}.{dataset}.{t}")

def main_parser_initiator(parser_list, els_in_chunk):
    threads_pool_manager(parser_initiator, parser_list, els_in_chunk)

def deleter(a):
    del a

def main_pipe_clicker(parser_list, els_in_chunk):
    parser_list = threads_pool_manager(parser_processor, parser_list, els_in_chunk)
    return list(filter(lambda x: x is not None, map(lambda a: deleter(a) if a.done is True else a, parser_list)))

def determine_age(criteria_id):
    if criteria_id == '503001':
        return '18-24' #18to24
    if criteria_id == '503002':
        return '25-34'
    if criteria_id == '503003':
        return '35-44'
    if criteria_id == '503004':
        return '45-54'
    if criteria_id == '503005':
        return '55-64'
    if criteria_id == '503006':
        return '65 or more'
    if criteria_id == '503999':
        return 'Undetermined'
    else:
        return criteria_id

def determine_gender(criteria_id):
    if criteria_id == '10':
        return 'Male'
    if criteria_id == '11':
        return 'Female'
    if criteria_id == '20':
        return 'Undetermined'
    else:
        return criteria_id

@dask.delayed
def write_bq_job(**kwargs):
    job = kwargs['client_bq'].load_table_from_dataframe(kwargs['df'], kwargs['table_id'], job_config=kwargs['job_config'])
    job.result()
    del kwargs['df']
    print(kwargs['message'])

def create_bq_job(parser_list, bq_client, t, schema):
    df = DataFrame(columns=list(schema.keys()))
    rows = 0
    for p in parser_list:
        # print(p.table, t, True if p.df_object.empty is False else False)
        if p.table == t:
            rows += p.rows_count
            ###print(f"DF CONCATED, CLIENT {p.client_name}, ROWS_COUNT {p.rows_count}, DF SHAPE {p.df_object.shape}")
            df = concat([df, p.df_object], ignore_index=True)
            p.df_object = p.df_object.truncate(after=0)
    if df.shape[0] != 0:
        if t in ('demographic_gender'):
            df['gender'] = df['genderView__resourceName'].map(determine_gender)
        elif t in ('demographic_age'):
            df['age_range'] = df['ageRangeView__resourceName'].map(determine_age)
        lambda_dtypes = lambda x: 'object' if x in ('STRING') \
            else 'datetime64' if x in ('DATE', 'DATETIME') \
            else 'float64' if x in ('FLOAT') \
            else 'int64' if x in ('INTEGER') else 'object'
        types = {old_name: lambda_dtypes(field_type) for old_name, field_type in
                 zip(list(schema.keys()), list(map(lambda a: a[0], schema.values())))
                 }
        df = df.replace({'': nan})
        df = df.astype(types)  ### AAAAA
        new_columns = [i[1][1] for i in schema.items()]  ### AAAAA
        df = df[[k for k in schema.keys()]]  ### AAAAA
        df.columns = new_columns  ### AAAAA
        job = Ads_to_BQ.create_bq_job_resources(df, bq_client, t, Ads_to_BQ.dataset, Ads_to_BQ.project_id, list(schema.values()))
        #print('JOB', job)
        return job
    else:
        pass
        #print('shape in create_bq_job is ', df.shape)

def main_pipe_bq_write_async(parser_list, bq_client):
    jobs = []
    for t, _, schema in querys:
        job_conf = create_bq_job(parser_list, bq_client, t, schema)
        if job_conf is None:
            #print('job_conf is NONE, parser list', parser_list, 'TABLE', t)
            continue
        jobs.append(write_bq_job(**job_conf))
    dask.delayed(jobs).compute()

def main_pipe_bq_write(parser_list, bq_client):
    for t, _, schema in querys:
        df = DataFrame(columns=list(schema.keys()))
        rows = 0
        for p in parser_list:
            # print(p.table, t, True if p.df_object.empty is False else False)
            if p.table == t:
                rows += p.rows_count
                ###print(f"DF CONCATED, CLIENT {p.client_name}, ROWS_COUNT {p.rows_count}, DF SHAPE {p.df_object.shape}")
                df = concat([df, p.df_object], ignore_index=True)
                p.df_object = p.df_object.truncate(after=0)
        # print('FACTICAL AND DFed NUMBER OF ROWS IN SUMMARY', rows, df.shape[0])

        if df.shape[0] != 0:
            if t in ('demographic_gender'):
                df['gender'] = df['genderView__resourceName'].map(determine_gender)
            elif t in ('demographic_age'):
                df['age_range'] = df['ageRangeView__resourceName'].map(determine_age)
            lambda_dtypes = lambda x: 'object' if x in ('STRING') \
                else 'datetime64' if x in ('DATE', 'DATETIME') \
                else 'float64' if x in ('FLOAT') \
                else 'int64' if x in ('INTEGER') else 'object'
            types = {old_name: lambda_dtypes(field_type) for old_name, field_type in
                     zip(list(schema.keys()), list(map(lambda a: a[0], schema.values())))
                     }
            df = df.replace({'': nan})
            df = df.astype(types) ### AAAAA
            new_columns = [i[1][1] for i in schema.items()] ### AAAAA
            df = df[[k for k in schema.keys()]] ### AAAAA
            df.columns = new_columns ### AAAAA
            #schema = list(schema.values())
            Ads_to_BQ.total_df_to_bq(df, bq_client, t, Ads_to_BQ.dataset, Ads_to_BQ.project_id, list(schema.values()))
            del df

def checker_time(etap, parser_list, start):
    print('checker time', etap, (time.time() - start))
    if etap != 'prebq':
        stoper = 470.0
        if (time.time() - start) > stoper:  ### AAAA 470.0
            print(f'main_pipe: {stoper} SECONDS PASS')
            for parser in parser_list:
                to_tokens_table.append({
                    'script': parser.script_name,
                    'dataset': parser.dataset,
                    'table': parser.table,
                    'token': parser.page_token,
                    'client_name': parser.client_name,
                    'client_id': str(parser.client_id)
                })
            raise TimeoutException500s

def _main_pipe_manager(parser_list, bq_client, els_in_chunk):
    print('time finalize', (time.time() - start))
    checker_time('preinit', parser_list, start)
    main_parser_initiator(parser_list, els_in_chunk)
    checker_time('preclick', parser_list, start)
    parser_list = main_pipe_clicker(parser_list, els_in_chunk)
    checker_time('prebq', parser_list, start)
    main_pipe_bq_write_async(parser_list, bq_client)

    #time.sleep(1)
    print('ANOTHER ITERATION OF PARSERS')
    return {'parser_list': parser_list, 'message': 'OK'}

def main_pipe_manager(parser_list, bq_client, queue_limit=20, els_in_chunk=10):

    #queue_limit = 20
    last_index = len(parser_list)-1
    queue = []
    object = {'parser_list': [], 'message': 'OK'}
    global start
    start = time.time()
    while len(parser_list)+len(queue) > 0:
        print('LEN PARSER', len(parser_list))
        while len(queue) < queue_limit:
            try:
                queue.append(parser_list.pop(last_index))
                #print('TO QUEUE APPEND', last_index, len(queue), queue_limit, len(parser_list))
                last_index -= 1
            except IndexError:
                #print('BREAKER', last_index, len(queue), queue_limit, len(parser_list))
                break
        print('LEN QUEUE', len(queue), 'LEN PARSER', len(parser_list))
        object = _main_pipe_manager(queue, bq_client, els_in_chunk)
        if object['message'] not in ('OK'):
            return {'parser_list': object['parser_list'] + parser_list, 'message': object['message']}
        queue = object['parser_list']
        if len(queue) == 0:
            print('ALARM BREAKER FUCK WITH len(queue) == 0, len of parsers is', len(parser_list))
            continue
        last_index = len(parser_list)-1
    return object

def delete_tokens_from_bq(client_bq):
    print(f'DELETE {Ads_to_BQ.script_name} from {Ads_to_BQ.tokens_table}')
    query = f"""
    DELETE FROM `{Ads_to_BQ.tokens_table}` WHERE script = '{Ads_to_BQ.script_name}'
    """
    client_bq.query(query).result()

def append_tokens_to_bq(df, client_bq):
    if df.shape[0] == 0:
        return False
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = 'WRITE_APPEND'
    job_config.autodetect = True
    client_bq.load_table_from_dataframe(df, Ads_to_BQ.tokens_table, job_config=job_config).result()
    return True

def check_tokens_from_bq(client_bq):
    query = f"""
    SELECT * FROM {Ads_to_BQ.tokens_table} WHERE script = '{Ads_to_BQ.script_name}'
    """
    return True if client_bq.query(query).result().total_rows > 0 else False

def get_tokens_from_bq(client_bq):
    query = f"""
    SELECT * FROM {Ads_to_BQ.tokens_table} WHERE script = '{Ads_to_BQ.script_name}'
    """
    return client_bq.query(query).result().to_dataframe()

def inject_old_tokens(parser_list, df_tokens):
    new_parsers = []
    for p in parser_list:
        df_el = df_tokens[
            (df_tokens['client_id'] == p.client_id) &
            (df_tokens['table'] == p.table) &
            (df_tokens['dataset'] == p.dataset)
        ]
        if df_el.shape[0] != 0:
            p.page_token = df_el['token'].values[0]
            #print('OLD TOKENS',p.page_token, p.table, p.client_name)
            new_parsers.append(p)

    return new_parsers

def get_account_ads(ads_client, ads_service, customer_id):

    query = """
    SELECT  customer_client.client_customer, customer_client.descriptive_name, customer_client.currency_code, 
    customer_client.status
    FROM customer_client
    WHERE customer_client.manager = FALSE AND customer_client.status NOT IN ('CANCELED', 'CLOSED', 'SUSPENDED')
    """

    search_request = ads_client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = customer_id
    search_request.query = query
    response = ads_service.search_stream(search_request)

    clients = [[json_format.MessageToDict(i) for i in g.results] for g in response]
    clients = [item for sublist in clients for item in sublist]
    return [(i['customerClient'].get('descriptiveName'),
             i['customerClient'].get('clientCustomer', 'customers/')[10:],
             i['customerClient'].get('currencyCode'),
             i['customerClient'].get('status'))
            for i in clients]

def safe_tokens(client_bq, safed_data):
    for_df = [parser
        for parser in safed_data
        if parser.get('token') != '' and parser.get('token') is not None
    ]
    df = DataFrame(for_df)
    append_tokens_to_bq(df, client_bq)

bq_service_cred = service_account.Credentials.from_service_account_file(Ads_to_BQ.bq_cred)
client_bq = bigquery.Client(Ads_to_BQ.project_id, credentials=bq_service_cred)

def main(reqv, resp):
    try:
        ads_client = GoogleAdsClient.load_from_storage(Ads_to_BQ.ads_cred)
        ads_service = ads_client.get_service('GoogleAdsService', version='v12')

        queue_limit = 30
        els_in_chunk = 10
        dask.config.set(pool=ThreadPoolExecutor(els_in_chunk))

        accounts = get_account_ads(ads_client, ads_service, Ads_to_BQ.manager_id)
        print(accounts)
        print(len(accounts))

        parser_list = create_parsers(querys, bq_service_cred, client_bq, ads_client, ads_service, accounts)

        #exit()

        df_tokens_bq = get_tokens_from_bq(client_bq)
        if df_tokens_bq.shape[0] > 0:
            print('WITH TOKENS')
            delete_tokens_from_bq(client_bq)
            parser_list = inject_old_tokens(parser_list, df_tokens_bq)
        else:
            print('WITHOUT TOKENS')
            tables = [t[0] for t in querys]
            print(tables)

            dask_exec = [trunc_bq(t, client_bq, Ads_to_BQ.project_id, Ads_to_BQ.dataset) for t in tables]
            # print(dask_exec)
            dask.delayed(dask_exec).compute()
        print('ORIGINAL PARSERS LEN', len(parser_list))
        main_pipe_manager(parser_list, client_bq, queue_limit, els_in_chunk)

    except TimeoutException500s as es:
        print("TIME HAS PASS, current time passed", time.time() - start, 'EXCEPTION', es)
        safe_tokens(client_bq, to_tokens_table)
        #start = 0
        raise TimeoutException500s
    # except AdsErrorException as es: #GoogleAdsException as es:
    #     print("UNKNOWN ADS EXCEPTION, current time passed", time.time() - start, 'EXCEPTION', es)
    #     safe_tokens(client_bq, to_tokens_table)
    #     raise AdsErrorException(es.message)
    except TimeoutRequestException as es: #DeadlineExceeded as es:
        print("REQUEST ADS TIMEOUT, current time passed", time.time() - start, 'EXCEPTION', es)
        safe_tokens(client_bq, to_tokens_table)
        raise TimeoutRequestException(es.message)
    except Exception as e:
        print('EXCEPTION: UNKNOWN EXCEPTION', e, traceback.print_exc())
    finally:
        end = time.time()
        print(end - start)

#main('a', 'b')