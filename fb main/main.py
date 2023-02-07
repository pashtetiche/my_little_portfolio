from time import sleep
from facebook_business import FacebookSession
from facebook_business import FacebookAdsApi
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adaccountuser import AdAccountUser as AdUser
from facebook_business.adobjects.adreportrun import AdReportRun
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, date
import pandas as pd
from multiprocessing.pool import ThreadPool
from numpy import NaN, nan, datetime64
from facebook_business.exceptions import FacebookRequestError
import json
import os

class FB_Async_Parser:

    project = 'proj_1'
    dataset_id = 'FB_Data'
    table_id = 'FB_DB_Stats_'
    #table_id_total = 'FB_AdGroups_Total'
    #table_id_campaigns = 'FB_Campaigns_Daily'
    #table_id_campaigns_total = 'FB_Total_Reach_PK'
    #ndjson_file = table_id + '.ndjson'
    #ndjson_file_camp = table_id_campaigns + '.ndjson'
    credentials = service_account.Credentials.from_service_account_file('db_reporting.json')

    optimization_indicator = 0
    update_df_indicator = False

    client_bq = bigquery.Client(project, credentials=credentials)

    #pp = pprint.PrettyPrinter(indent=4)
    #this_dir = os.path.dirname(__file__)
    config_filename = 'config.json'

    config_file = open(config_filename)
    config = json.load(config_file)
    config_file.close()

    ### Setup session and api objects
    session = FacebookSession(
        config['app_id'],
        config['app_secret'],
        config['access_token'],
    )
    api = FacebookAdsApi(session)
    #print(api.API_VERSION)
    #exit()

    FacebookAdsApi.set_default_api(api)

    #print('\n\n\n********** Reading objects example. **********\n')

    ### Setup user and read the object from the server
    me = AdUser(fbid='me')

    my_account = me.get_ad_account()
    my_accounts_iterator = me.get_ad_accounts(fields=['name'], params={'limit': 1000})
    my_accounts_iterator = [i for i in my_accounts_iterator]
    print([i['name'] for i in list(my_accounts_iterator)])

    async_jobs = []
    #print()

    level = 'level'
    breakdowns = ['device_platform']
    fields = [

        'account_name',
        'account_id',
        'campaign_name',
        'campaign_id',
        'adset_name',
        'adset_id',
        'account_currency',
        'impressions',
        'reach',
        'frequency',
        'inline_link_clicks',
        'clicks',
        'spend',
        'unique_clicks',
        'actions',
        'date_start',
        'date_stop',
        'video_play_actions',
        'video_p25_watched_actions',
        'video_p50_watched_actions',
        'video_p75_watched_actions',
        'video_p100_watched_actions'
    ]
    del_time_increment = False
    del_breakdowns = False

    streams = 7
    iterations = 5

    level_df = 'base'
    date_preset = 'last_14d'


    def thread_manager(self, threads, function, data):
        pool = ThreadPool(threads)
        #print('LEN DATA ', len(data), type(data))
        res = pool.map(function, data)
        pool.close()
        pool.join()
        del pool
        return res

    def check_status(self, up_job):
        sleep(10)
        job = up_job.api_get()
        #print('JOBS) ', type(job), dir(job), )
        check = job[AdReportRun.Field.async_percent_completion]
        del job
        if check == 100:
            return True
        else:
            return False
        #return job[AdReportRun.Field.async_percent_completion]

    def checker_iterator(self, iterations, threads, data):
        for i in range(iterations):
            checker = self.thread_manager(threads, self.check_status, data)  # list(map(check_status, first_dat))
            #print('CHECKER', checker)
            if all(checker):
                print('done')
                return True
            print('continue')
            if i+1 == iterations:
                print('Iterations too long')
                #raise Exception('Iterations too long')
                return False
        return True

    def send_insight(self, account):
        #sleep(1)
        #print('IMPOSTOR CHECK',account)
        params = {
            'level': self.level,
            'breakdowns':self.breakdowns,
            'date_preset': self.date_preset,
            'time_increment': 1,
            'action_attribution_windows': ["1d_click", "1d_view"],
            'limit': 1000
        }

        if self.del_time_increment:
            del params['time_increment']
        if self.del_breakdowns:
            del params['breakdowns']
        async_job = account.get_insights(fields=self.fields,
                                                params=params,
                                         is_async=True)
        #print(async_job)
        return async_job

    def send_ad(self, account):
        #sleep(1)
        return account.get_ads(fields=[Ad.Field.name], is_async=True, params={'limit': 1000})

    def get_async(self, async_job):
        try:
            res = async_job.get_result(params={'limit': 1000})
        except FacebookRequestError:
            print(async_job, dir(async_job))
            raise Exception('ERROR IN GET ASYNC RESULT')
        return [i for i in res]

    def pypeline_elem(self):
        acc_dat = []
        for attempt in range(3):
            acc_dat = self.thread_manager(self.streams, self.send_insight, self.my_accounts_iterator)
            print(f'FIRST_DAT attempt {attempt} with acc {len(acc_dat)}')
            status = self.checker_iterator(self.iterations, self.streams, acc_dat)
            if status:
                break
        sleep(3)
        acc_dat = self.thread_manager(self.streams, self.get_async, acc_dat)
        print('PRE_DF data', len(acc_dat))
        #print(acc_dat)
        flat_list = [dict(x) for xs in acc_dat for x in xs]
        print(len(flat_list))
        # print(flat_list)
        df = pd.DataFrame(flat_list)
        df.columns = [str(i) for i in df.columns]
        if 'video_play_actions' in df.columns:
            df['video_play_actions'] = df['video_play_actions'].apply(lambda di: di[0]['value'] if di is not NaN else None)

        if 'video_p25_watched_actions' in df.columns:
            df['video_p25_watched_actions'] = df['video_p25_watched_actions'].apply(lambda di: di[0]['value'] if di is not NaN else None)

        if 'video_p50_watched_actions' in df.columns:
            df['video_p50_watched_actions'] = df['video_p50_watched_actions'].apply( lambda di: di[0]['value'] if di is not NaN else None)

        if 'video_p75_watched_actions' in df.columns:
            df['video_p75_watched_actions'] = df['video_p75_watched_actions'].apply(lambda di: di[0]['value'] if di is not NaN else None)

        if 'video_p100_watched_actions' in df.columns:
            df['video_p100_watched_actions'] = df['video_p100_watched_actions'].apply(lambda di: di[0]['value'] if di is not NaN else None)

        #print(df.to_string())

        def apply_key_handle(di):
            try:
                return di['text']
            except KeyError:
                return 'Text missing'

        if 'body_asset' in df.columns:
            df['body_asset'] = df['body_asset'].apply(
                lambda di: apply_key_handle(di) if di is not NaN else None)

        if 'title_asset' in df.columns:
            df['title_asset'] = df['title_asset'].apply(
                lambda di: apply_key_handle(di) if di is not NaN else None)

        if 'website_ctr' in df.columns:
            df['website_ctr'] = df['website_ctr'].apply(lambda di: di[0]['value'] if di is not NaN else None)

        if 'link_url_asset' in df.columns:
            df['link_url_asset'] = df['link_url_asset'].apply(
                lambda di: di['website_url'] if di is not NaN else None)

        if 'image_asset' in df.columns:
            df['img_url'] = df['image_asset'].apply(
                lambda di: di['url'] if di is not NaN else None)
            df['img_name'] = df['image_asset'].apply(
                lambda di: di['name'] if di is not NaN else None)
            df = df.drop(['image_asset'], axis=1)

        df['actions_data'] = df['actions'].apply(lambda di: list(map(lambda di_s: {'action_type': str(di_s.get('action_type', 'link_click_null_newage')),
                                                                                   'value': int(di_s.get('value', 0)), #int
                                                                                   'PC': float(di_s.get('1d_click', 0)), #float
                                                                                   'PV': float(di_s.get('1d_view', 0)) #float
        }, di)) if di is not NaN else None)

        df = df.drop(['actions'], axis=1)
        df.rename(columns={'actions_data': 'actions'}, inplace=True)
        df = df.apply(pd.to_numeric, errors='ignore')
        df = df.replace({nan: None})
        return df

    def update_df(self, df):
        base_obj_li_df = df
        base_obj_li_df.set_index('index', inplace=True)
        query_old_data = f"""
        select * except(index),
        CONCAT(account_id,cast(campaign_id as STRING),cast(adset_id as STRING),cast(ad_id as STRING), cast(date_start as STRING)) as index
        from FB_Data.{self.table_id}
        """
        # table_id_total
        # table_id
        getted_data = self.client_bq.query(query_old_data).result().to_dataframe().to_dict()
        getted_data = pd.DataFrame(getted_data)  # .astype({'actions':list})
        getted_data.set_index('index', inplace=True)
        # df 1 new
        # df 2 old
        getted_data.update(base_obj_li_df)
        drop_index = list(set(getted_data.index) & set(base_obj_li_df.index))

        df2 = pd.concat([getted_data, base_obj_li_df.drop(index=drop_index)])
        #getted_data.append(base_obj_li_df.drop(index=drop_index))
        print('DF updatet')
        return df2 #.to_dict('records')

def get_fb_campaign_bq(client):
    df = client.query(
        """
SELECT * FROM(SELECT DISTINCT a.campaign_name,b.account_id, a.campaign_cm, STRING_AGG(a.campaign_id) AS campaignID
        FROM `Dashboard_v4.Plan` a
        LEFT JOIN 
        (SELECT DISTINCT campaign_id, account_id FROM `proj_1.FB_Data.FB_Campaigns_Daily`)  b
        ON CAST(a.campaign_id AS STRING) = CAST(b.campaign_id AS STRING) 
        AND a.end_date > DATE_ADD(CURRENT_DATE(), INTERVAL -90 DAY)
        WHERE a.vendor = "Facebook" or a.vendor = "*****" 
        GROUP BY campaign_name,b.account_id,a.campaign_cm)
        where campaignID is not null AND account_id IS NOT NULL
        """
    ).result().to_dataframe()

    return df

def load_to_bq(client, dataset_id, table_id, source, write_disposition):

    start_date_time = datetime.now()
    start_date_time = start_date_time.strftime('%d.%m.%y %H:%M:%S')
    print('Start loading to Bigquery at {}'.format(start_date_time))
    #dataset_ref = client.dataset(dataset_id)
    table_ref = f"{dataset_id}.{table_id}" #dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    # job_config.schema = schema
    # job_config.skip_leading_rows = 1
    job_config.write_disposition = write_disposition
    job_config.autodetect = True

    with open(source, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Waits for table load to complete.
    end_date_time = datetime.now()
    end_date_time = end_date_time.strftime('%d.%m.%y %H:%M:%S')
    print("Load {}, {} rows. End at {}".format(job.output_rows, table_id, end_date_time))
    os.remove(source)

def upload_df_data(df, table_name, data_set, dataset_location, project, client,
                    truncate=False, schema_autodetect=True):

    table_ref = '.'.join([project, data_set, table_name])
    # lambda_dtypes = lambda x: 'object' if x in ('STRING') \
    #     else 'datetime64' if x in ('DATE', 'DATETIME') \
    #     else 'float64' if x in ('FLOAT') \
    #     else 'int64' if x in ('INTEGER') else 'object'
    #
    # getted_table = client.get_table(table_ref)
    # base_schema = getted_table.schema
    # print('OLD SCHEMA', base_schema)
    # columns = [c.name for c in base_schema]
    # types = {c.name: lambda_dtypes(c.field_type) for c in base_schema}
    #
    # df.columns = columns
    # df = df.astype(types)
    if 'date_start' in df.columns.tolist():
        df['date_start'] = df['date_start'].apply(lambda x: datetime64(x)) #pd.to_datetime(df['date_start'])
    if 'date_stop' in df.columns.tolist():
        df['date_stop'] = df['date_stop'].apply(lambda x: datetime64(x)) #pd.to_datetime(df['date_stop'])
    #df.astype({'date_start': 'datetime64', 'date_stop': 'datetime64'})

    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = schema_autodetect
    job_config.schema = [
        bigquery.schema.SchemaField('date_start', 'DATE'),
        bigquery.schema.SchemaField('date_stop', 'DATE'),
    ]

    #print('NEW SCHEMA', job_config.schema)

    job_config.write_disposition = 'WRITE_APPEND'
    if truncate:
        job_config.write_disposition = 'WRITE_TRUNCATE'

    job = client.load_table_from_dataframe(
        df,
        table_ref,
        location=dataset_location,
        job_config=job_config,
    )

    job.result()
    print(f'Loaded {df.shape[0]} and {df.shape[1]} columns in {str(table_ref)}')
    #print('NEW SCHEMA',job.schema)
    # print(f'Loaded {job.output_rows} rows in schema len {len([(i.name, i.field_type) for i in job.schema])} '
    #       f'into {data_set}:{table_name}, write disposition {job_config.write_disposition}')

parser = FB_Async_Parser()

breakdowns_list = [
    ([], 'FB_AdGroups_Daily', 'adset', False, False),
    ([], 'FB_AdGroups_Total', 'adset', True, False),
    ([], 'FB_Campaigns_Daily', 'campaign', False, False),
    ([], 'FB_Campaigns_Total', 'campaign', True, False),

    #([], 'FB_RK_Daily', 'campaign', False, False),
    #([], 'FB_RK_Total', 'campaign', True, False),

                   # (['gender', 'age'], 'Demography'),
                   # (['device_platform'], 'Device'),
                   # (['country', 'region'], 'Geo'),
                   # (['hourly_stats_aggregated_by_advertiser_time_zone'], 'Time'),
                   # (['link_url_asset'], 'Targetlink'),
                   # (['title_asset'], 'Adtitle'),
                   # (['body_asset'], 'Adbody'),
                   # (['image_asset'], 'Adimage')
                   ]

# FB_AdGroups_Daily
# FB_AdGroups_Total
# FB_Campaigns_Daily
# FB_Campaigns_Total

def main(reqv, resp):
  for group, table, level, del_time_increment, update_df_indicator in breakdowns_list:
      #print('BEGIN BREAK', group)

      parser.date_preset = 'last_90d' #'last_14d' #
      #default_table = 'FB_Maindata_daily_'
      parser.table_id = table
      parser.level = level #'campaign' #'adset'
      parser.breakdowns = group
      parser.update_df_indicator = update_df_indicator
      parser.fields = parser.fields
      parser.del_time_increment = del_time_increment
      if len(group) == 0:
          parser.del_breakdowns = True
      else:
          parser.del_breakdowns = False

      df = parser.pypeline_elem()
      if group == [] and parser.optimization_indicator == 0:
          active_acc = ['act_'+str(i) for i in list(set(df['account_id'].tolist()))]

          parser.my_accounts_iterator = [i for i in parser.my_accounts_iterator if i['id'] in active_acc]
          parser.optimization_indicator += 1

      #print()
      print('DONE BREAK', group)
      #exit()

      #df = parser.update_df(df)

      ### !!!! ИНДЕКС

      data_new = df
      if 'adset_id' in data_new.columns:
          data_new['iindex'] = data_new['campaign_id'].apply(lambda x: str(x)) + '_' + \
                              data_new['adset_id'].apply(lambda x: str(x)) + '_' + \
                              data_new['date_start'].apply(lambda x: str(x).replace('-', '_'))
      else:
          data_new['iindex'] = data_new['campaign_id'].apply(lambda x: str(x)) + '_' + \
                              data_new['date_start'].apply(lambda x: str(x).replace('-', '_'))

      data_new.set_index('iindex', inplace=True)

      if parser.update_df_indicator is False:
          zagluska = [{'reach': 0, 'account_name': 'ZAGLUSHKA_ACC', 'account_id': 13, 'date_start': date(2014, 1, 1),
                      'video_play_actions': 0.0, 'impressions': 0, 'unique_clicks': 0, 'clicks': 0,
                      'campaign_id': 13, 'account_currency': 'UAH', 'spend': 0,
                      'date_stop': date(2014, 1, 1), 'frequency': 0, 'campaign_name': 'ZAGLUSHKA_CAMP',
                      'video_p50_watched_actions': 0.0, 'inline_link_clicks': 0,
                      'video_p25_watched_actions': 0.0, 'video_p75_watched_actions': 0.0,
                      'actions': [{'action_type': 'link_click', 'value': 0, 'PC': 0.0, 'PV': 0.0},
                                  {'action_type': 'page_engagement', 'value': 0, 'PC': 0.0, 'PV': 0.0},
                                  {'action_type': 'post_engagement', 'value': 0, 'PC': 0.0, 'PV': 0.0}],
                      'video_p100_watched_actions': 0.0}]

          if parser.level == 'adset':
              zagluska[0]['adset_id'] = 13
              zagluska[0]['adset_name'] = 'ZAGLUSHKA_ADSET'

          data_old = pd.DataFrame(zagluska)
      else:
          data_old = parser.client_bq.query(f"""select * from FB_Data.{parser.table_id}""").result()  ### !!!! ЗАПРОС
          data_old = pd.DataFrame(data_old.to_arrow().to_pydict())
      # print(data_old.head(1).to_dict('records'))
      # exit()

      ### !!!! ИНДЕКС

      #data_old['actions'] = data_old['actions'].apply(lambda x: x)
      if 'adset_id' in data_old.columns:
          data_old['iindex'] = data_old['campaign_id'].apply(lambda x: str(x)) + '_' + \
                              data_old['adset_id'].apply(lambda x: str(x)) + '_' + \
                              data_old['date_start'].apply(lambda x: str(x).replace('-', '_'))
      else:
          data_old['iindex'] = data_old['campaign_id'].apply(lambda x: str(x)) + '_' + \
                              data_old['date_start'].apply(lambda x: str(x).replace('-', '_'))

      data_old['date_start'] = data_old['date_start'].apply(lambda x: datetime.strftime(x, '%Y-%m-%d'))
      data_old['date_stop'] = data_old['date_stop'].apply(lambda x: datetime.strftime(x, '%Y-%m-%d'))
      #data_old['actions'] = df['actions'].apply(lambda x: x.tolist())
      data_old.set_index('iindex', inplace=True)

      # print(data_new.head(10).to_string())
      # print(data_old.tail(10).to_string())
      # exit()

      data_old.update(data_new)
      drop_index = list(set(data_old.index) & set(data_new.index))
      #print(len(drop_index), len(set(data_old.index)), len(set(data_new.index)))
      df = pd.concat([data_old, data_new.drop(index=drop_index)])
      #data_old.append(data_new.drop(index=drop_index))

      if 'account_id' in df.columns:
          #df.drop(columns=['account_id'], inplace=True)
          #df['account_id'] = df['account_id'].fillna(0)
          df['account_id'] = df['account_id'].apply(lambda di: int(di))

      if 'adset_id' in df.columns:
          df['adset_id'] = df['adset_id'].apply(lambda di: int(di))

      if 'campaign_id' in df.columns:
          df['campaign_id'] = df['campaign_id'].apply(lambda di: int(di))

      if 'clicks' in df.columns:
          df['clicks'] = df['clicks'].apply(lambda di: int(di))

      if 'impressions' in df.columns:
          df['impressions'] = df['impressions'].apply(lambda di: int(di))

      if 'reach' in df.columns:
          df['reach'] = df['reach'].apply(lambda di: int(di))

      if 'inline_link_clicks' in df.columns:
          df['inline_link_clicks'] = df['inline_link_clicks'].apply(lambda di: int(di) if di is not None else int(0))

      if 'unique_clicks' in df.columns:
          df['unique_clicks'] = df['unique_clicks'].apply(lambda di: int(di))

      # df['actions'] = df['actions'].apply(lambda x: x)

      df['video_play_actions'] = df['video_play_actions'].fillna(0)
      df['video_p25_watched_actions'] = df['video_p25_watched_actions'].fillna(0)
      df['video_p50_watched_actions'] = df['video_p50_watched_actions'].fillna(0)
      df['video_p75_watched_actions'] = df['video_p75_watched_actions'].fillna(0)
      df['video_p100_watched_actions'] = df['video_p100_watched_actions'].fillna(0)

      df.reset_index(drop=True, inplace=True) #drop(columns=['index'], inplace=True)

      # ndjson_file = 'FB_TMP_JSON.ndjson'

      # df = df.to_dict('records')
      # with open(ndjson_file, 'w') as f:
      #     ndjson.dump(df, f)
      #exit()
      #load_to_bq(parser.client_bq, parser.dataset_id, parser.table_id, ndjson_file, write_disposition='WRITE_TRUNCATE')
      data_set_location = 'US'
      upload_df_data(df, parser.table_id, parser.dataset_id, data_set_location, parser.project, parser.client_bq,
                    truncate=True, schema_autodetect=True)

      # df, table_name, data_set, dataset_location, project, client,
      # truncate = False, schema_autodetect = True