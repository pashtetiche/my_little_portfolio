import pandas as pd
from googleapiclient.discovery import build
import traceback
from google.oauth2.credentials import Credentials
from dcm_google import GoogleBigQueryClient
# from classes.currency import function_get_currency_and_write_data
from pandas import read_csv, notnull
from telegram import telegram_bot_sendtext
import more_itertools
from zipfile import ZipFile
import re
from datetime import date
from base64 import urlsafe_b64decode
from io import StringIO, BytesIO
from time import ctime, time
import json
import dask
from concurrent.futures import ThreadPoolExecutor
import google_auth_httplib2
import httplib2
from google.cloud import bigquery
from google.oauth2 import service_account

class Morning_Unloading:
    project_id = 'proj_1'
    data_set = ''
    dataset_location = 'US'
    truncate = True
    gmail_api_cred_file = 'gmail_key_api_new.json'
    bq_api_cred_file = 'db_reporting.json'
    SCOPES = ['https://mail.google.com/']
    rep_regular = r'Report ID: (\w+)'
    acc_regular = r'Account: \D*(\d+)\)'
    rep_name_reg = r'Report Name: [REPORT]*!_([a-zA-Z0-9_]+)'
    file_name_body_reg = r'File Name:\s+(.+)\.(csv|zip)'

    rep_regular_ru = r'Идентификатор отчета: (\w+)'
    acc_regular_ru = r'Аккаунт: \D*(\d+)\)'
    rep_name_reg_ru = r'Название отчета: [REPORT]*!_([a-zA-Z0-9_]+)'
    file_name_body_reg_ru = r'Имя файла:\s+(.+)\.(csv|zip)'

    list_data_newage = list()
    #list_data_citrus = list()
    list_data_dv_newage = list()
    list_data_dv_newage_append = list()
    list_data_top = list()
    list_data_floodlight = list()

    list_data_missing_report = list()

    resources_file = 'reports_file_morning.json'
    with open(resources_file,'r') as res:
        resources = json.loads(res.read())
    #parsed = 'newage'

    table_name_res = {}
    step = 0

    dv_table_name_res = {
        # id rep: name table
    }

    dv_table_append_name_res = {
        # id rep: name table
    }

    cm_floodlights = {
        # id rep: name table
    }

    cm_table_name_res = {
        # id rep: name table
    }

    cm_top_booking_name_res = {
        # id rep: name table
    }

    ### СУММАРНО, по состоянию на 28 ноября 22 года, 61 репорт в bq должен по итогу выгрузится

    counter = list()

    def __init__(self,labelIds):
        self.els_in_chunk = 10
        self.labelIds = labelIds
        self.service_credentials = Credentials.from_authorized_user_file(self.gmail_api_cred_file, self.SCOPES)
        self.service = build('gmail', 'v1', credentials=self.service_credentials)

    def has_cyrillic(self, text):
        return bool(re.search('[\u0400-\u04FF]', text))

    def get_mails_all_ids(self):
        all_messages_id = list()
        page_token = ''
        #print(self.labelIds)
        for l in self.labelIds:
            #print('LABEL', l)
            while page_token == '' or (page_token != None and len(page_token) > 0):
                results = self.service.users().messages().list(
                    userId='********',
                    labelIds=l,
                    maxResults=500,
                    pageToken=page_token
                ).execute()
                page_token = results.get('nextPageToken')
                if results == {'resultSizeEstimate': 0}:
                    #print('resultSizeEstimate IS 0')
                    break
                #print(results)
                for m in results['messages']:
                    all_messages_id.append(m['id'])
                #print(l, len(all_messages_id))
            page_token = ''
        #exit()
        return all_messages_id

    @dask.delayed
    def get_message_corutine(self, messageid):
        http = google_auth_httplib2.AuthorizedHttp(self.service_credentials, http=httplib2.Http())
        return self.service.users().messages().get(userId='******', id=messageid).execute(http=http)

    def sort_messages(self):
        all_mess_ids = self.get_mails_all_ids()
        #print(all_mess_ids)
        corutines = [self.get_message_corutine(m) for m in all_mess_ids]
        messages = dask.delayed(corutines).compute()
        for get_obj in messages:
            #print(get_obj['snippet'])
            #get_obj = self.service.users().messages().get(userId='******', id=messageid).execute()

            rep_regular = self.rep_regular
            acc_regular = self.acc_regular
            rep_name_reg = self.rep_name_reg
            file_name_body_reg = self.file_name_body_reg

            if 'Идентификатор отчета' in get_obj['snippet']:
                print('NO RUSSIAN', get_obj)
                rep_regular = self.rep_regular_ru
                acc_regular = self.acc_regular_ru
                rep_name_reg = self.rep_name_reg_ru
                file_name_body = self.file_name_body_reg_ru

            filename = re.findall(r' filename="(.+)\.(csv|zip)', str(get_obj['payload']['parts']))
            filename_body = re.findall(file_name_body_reg, str(get_obj['snippet']))
            if len(filename) == 0:
                print(f'MESSAGE HAVE ZERO FILENAME and have snippet {get_obj["snippet"]}')
                continue
            date_oclock = re.findall(r'(20\d{2}[01]\d[0123]\d)_([012]\d[012345]\d[012345]\d)', filename[0][0])[0]
            _date_obj = date_oclock[0].strip()
            oclock = date_oclock[1].strip()[0:2]

            try:
                re.findall(rep_name_reg, get_obj['snippet'])[0]
            except IndexError:
                re_message = f"INCORRECT REPORT NAME: INDEX ERROR IN REPORT WITH SNIPPET '{get_obj['snippet']}'"
                print(re_message)
                telegram_bot_sendtext(re_message)
                continue

            obj = {
                'thread_id': get_obj['threadId'],
                'message_id': get_obj['id'],
                'att_id': get_obj['payload']['parts'][1]['body']['attachmentId'],
                'report_id': re.findall(rep_regular, get_obj['snippet'])[0],
                #'cm_acc_id': int(re.findall(acc_regular, get_obj['snippet'])[0]),
                'cm_report_name': '!_' + re.findall(rep_name_reg, get_obj['snippet'])[0],
                'date': (int(_date_obj[0:4]), int(_date_obj[4:6]), int(_date_obj[6:8])),
                'oclock': oclock,
                'file_type': get_obj['payload']['parts'][1]['mimeType'],
                'from_whoom': [h['value'] for h in get_obj['payload']['headers'] if h['name'] == 'Return-Path'][0],
                'ident': get_obj['snippet'],
                'filename': filename,
                'filename_body': filename_body
            }
            dateobj = date(int(obj['date'][0]), int(obj['date'][1]), int(obj['date'][2]))
            # print('print checker ',dateobj, date.today(), int(obj['oclock']),
            #       (int(obj['oclock']) <= 17 and dateobj == date.today()), obj['report_id'])

            try:
                if len(filename_body)>0 and filename[0][0] != filename_body[0][0]:
                    mess = f"FILE-SNIPPET COMPLIMENTANCY ERROR: file {filename[0][0]}, body {filename_body[0][0]}, report {re.findall(rep_regular, get_obj['snippet'])[0]}"
                    telegram_bot_sendtext(mess)
                    continue
            except IndexError:
                print('ERROR INDEX', get_obj, filename, filename_body, obj)
                raise IndexError

            if obj['report_id'] in list(self.cm_table_name_res.keys()):
                self.list_data_newage.append(obj)
            elif obj['report_id'] in list(self.cm_top_booking_name_res.keys()):
                self.list_data_top.append(obj)
            elif obj['report_id'] in list(self.dv_table_append_name_res.keys()):
                self.list_data_dv_newage_append.append(obj)
            elif obj['report_id'] in list(self.dv_table_name_res.keys()):  # obj['cm_acc_id'] == 138402:
                self.list_data_dv_newage.append(obj)
            elif obj['report_id'] in list(self.cm_floodlights.keys()):
                self.list_data_floodlight.append(obj)
            else:
                self.list_data_missing_report.append(obj)
                message = f'MORNING UNLOADING: UNKNOWN PROFILE ID: {obj["report_id"]}'
                print(message)

    #@dask.delayed
    def get_attachment(self, obj):
        if obj is None:
            return None
        http = google_auth_httplib2.AuthorizedHttp(self.service_credentials, http=httplib2.Http())
        obj['att'] = self.service.users().messages().attachments().get(
            userId='******',
            messageId=obj['message_id'],
            id=obj['att_id']).execute(http=http)
        return obj

    def try_missed(self, obj):
        try:
            obj['report_name_by_id'] = self.table_name_res[obj['report_id']]
            return obj
        except KeyError:
            message = 'MISSING IN RESOURCES:' + str(obj['report_id'])
            telegram_bot_sendtext(message)

    def time_checker(self, obj):
        if (
                # True
                int(obj['oclock']) <= 17
                and date(int(obj['date'][0]), int(obj['date'][1]), int(obj['date'][2])) == date.today()
                #(2023,1,25)
                # dateobj == date(2022, 12, 4)
        ):
            return obj

    #@dask.delayed
    def bq_corutine(self, obj):
        if obj is None:
            return None
        try:
            if (self.table_name_res[obj['report_id']] == '!_dcm_geo_traffic' and self.data_set != 'DV360_Morning') or \
                    (self.table_name_res[
                         obj['report_id']] == '!_dcm_geo_traffic_dv' and self.data_set == 'DV360_Morning'):
                GoogleBigQueryClient(self.bq_api_cred_file).upload_df_data(obj['df'],
                                                                           obj['report_name_by_id'].replace('!_', ''),
                                                                           self.data_set, self.dataset_location, False)
                self.counter.append(obj['report_name_by_id'])
            else:
                GoogleBigQueryClient(self.bq_api_cred_file).upload_df_data(obj['df'],
                                                                           obj['report_name_by_id'].replace('!_', ''),
                                                                           self.data_set, self.dataset_location,
                                                                           self.truncate)
                self.counter.append(obj['report_name_by_id'])
            return True
        except Exception as e:
            message = f'MORNING UNLOADING: BQ ERROR: {e},{obj["report_id"]}'
            print(message)
            print(traceback.format_exc())
            telegram_bot_sendtext(message)
            raise Exception(message)

    def parse_attach(self, obj):
        if obj is None:
            return None
        if obj['file_type'] == 'text/csv':
            obj['att'] = urlsafe_b64decode(obj['att']['data'].encode('UTF-8')).decode('utf-8').strip()
            return obj
        elif obj['file_type'] == 'application/zip':
            file_data = urlsafe_b64decode(obj['att']['data'].encode('UTF-8'))
            file_like_object = BytesIO(file_data)
            zf = ZipFile(file_like_object, "r")
            obj['att'] = zf.read(zf.infolist()[0]).decode('utf8').strip()
            return obj
        else:
            message = f'MORNING UNLOADING: UNKNOW FILE FROMAT: {obj["file_type"]},{obj["report_id"]}'
            print(message)
            print(traceback.format_exc())
            telegram_bot_sendtext(message)
            raise Exception(message)

    def is_rep_emphty(self, obj):
        if obj is None:
            return None
        if 'No data returned by the reporting service' not in obj['att']:
            #self.list_data_correct.append(obj)
            return obj
        else:
            messag = f"{obj['report_name_by_id']} is emphty, {obj['report_id']}"
            print(messag)
            telegram_bot_sendtext(messag)

    def parse_in_df(self, obj, whatpars):
        if obj is None:
            return None
        if whatpars == 'CM':
            cm_end_sub = re.search(r'Grand Total:[\s\S]+', obj['att']).group(0)
            cm_begin_sub = re.search(r'[\s\S]+Report Fields', obj['att']).group(0)
            mod_string = StringIO(obj['att'].replace(cm_end_sub, '').replace(cm_begin_sub, '').strip())
        elif whatpars == 'DV':
            if ',,,' in obj['att']:
                dv_sub = re.search(r",,,[\s\S]+", obj['att']).group(0)
            else:
                dv_sub = re.search(r"\n\n[\s\S]+", obj['att']).group(0)
            mod_string = StringIO(obj['att'].replace(dv_sub, '').strip())
        else:
            mod_string = 'None'
            message = f'WHATPARS: UNEXPECTED SOURCE {obj["report_id"]}'
            print(message)
            print(traceback.format_exc())
            telegram_bot_sendtext(message)
            raise Exception(message)
        df = read_csv(mod_string, delimiter=',', na_values='-')
        del mod_string
        del obj['att']
        #print('SHAPE OF TABLE', df.shape, obj['report_id'], self.table_name_res[obj['report_id']], self.data_set)
        if df.shape[0] > 0:
            obj['df'] = df.where(notnull(df), None)
        else:
            messag = f"CHECKED IN DF: {obj['report_name_by_id']} is emphty, {obj['report_id']}"
            print(messag)
            telegram_bot_sendtext(messag)
            obj['df'] = pd.DataFrame()
        return obj

    def set_columns(self, obj):
        if obj is None or obj['df'].empty:
            return None
        try:
            #print('AAA', obj['report_name_by_id'], obj['report_id'], obj['cm_report_name'], obj['filename'])
            last_columns = list(obj['df'].columns)
            new_columns = list(self.resources[obj['report_name_by_id']].replace(' ', '').lower().split(','))

            if len(last_columns) != len(new_columns):
                message = f"COLUMNS ERROR: NO COMPLIMENTAR COLUMNS: {obj['report_name_by_id']}, {obj['report_id']}"
                print(last_columns)
                print(new_columns)
                telegram_bot_sendtext(message)
                raise Exception(message)

            obj['df'].columns = new_columns
            return obj
        except KeyError as ke:
            print(ke)
            print(traceback.format_exc())
            print('KEY ERROR: ', obj['cm_acc_id'], obj['report_id'], obj['cm_report_name'])

    @dask.delayed
    def atomaric_get_write(self, obj, whatpars):
        # ЖЕЛАТЕЛЬНО ПРОВЕРКИ АРГУМЕНТОВ ЗАПХАТЬ В ДЕКОРАТОР
        obj = self.get_attachment(obj) #for obj in chunk]
        obj = self.parse_attach(obj) #list(filter(self.check_none, map(self.parse_attach, chunk)))
        obj = self.is_rep_emphty(obj) #list(filter(self.check_none, map(self.is_rep_emphty, chunk)))
        obj = self.parse_in_df(obj, whatpars) #list(filter(self.check_none, map(lambda x: self.parse_in_df(x, whatpars), chunk)))
        obj = self.set_columns(obj) #list(filter(self.check_none, map(self.set_columns, chunk)))
        return self.bq_corutine(obj)

    def main_pipeline(self, mail_objs, whatpars):
        check_none = lambda x: x is not None
        for ind, chunk in enumerate(list(more_itertools.sliced(mail_objs, int(self.els_in_chunk-5)))):
            print(f'BEGIN CHUNK {ind} WITH LEN {len(chunk)}')
            chunk = list(filter(check_none, map(self.try_missed, chunk)))
            self.step += 1
            print(f'{self.step} step', len(chunk))
            chunk = list(filter(check_none, map(self.time_checker, chunk)))
            self.step += 1
            print(f'{self.step} step', len(chunk))
            chunk = [self.atomaric_get_write(obj, whatpars) for obj in chunk]
            chunk = list(filter(check_none, dask.delayed(chunk).compute()))
            self.step += 1
            print(f'{self.step} step', len(chunk))
            print(f'END CHUNK {ind} WITH LEN {len(chunk)}')
            self.step = 0

            ### ДОПИСАТЬ УДАЛЕНИЕ БАТЧЕВОЕ ДЛЯ ПИСЕМ

    @dask.delayed
    def trunc_bq(self, t, client_bq):
        r = client_bq.query(f"TRUNCATE TABLE `{self.project_id}.{self.data_set}.{t}`")
        r.result()
        print(f'TABLE TRUNCATED {self.project_id}.{self.data_set}.{t}')

    def get_labels_id(self):
        return self.service.users().labels().list(userId='******').execute()

    @dask.delayed
    def trasher(self, thread_id):
        http = google_auth_httplib2.AuthorizedHttp(self.service_credentials, http=httplib2.Http())
        self.service.users().threads().trash(userId='******', id=thread_id).execute(http=http)
        #print(f'DELETED THREAD {thread_id}')
        #m['thread_id']


def main(reqv, resp):
    start_t = time()
    labels = ['******', '******']
    ex = Morning_Unloading(labels)
    dask.config.set(pool=ThreadPoolExecutor(ex.els_in_chunk))
    ex.sort_messages()
    print('SORT', time() - start_t)

    bq_service_cred = service_account.Credentials.from_service_account_file(ex.bq_api_cred_file)
    client_bq = bigquery.Client(ex.project_id, credentials=bq_service_cred)

    ex.table_name_res = ex.cm_table_name_res
    ex.data_set = 'CM_Morning'

    union_resource = dict(ex.cm_table_name_res, **ex.cm_top_booking_name_res)
    active_tables_cm = ex.list_data_newage + ex.list_data_top
    tables = list(filter(lambda x: x not in ('dcm_geo_traffic'),
                        list(set([union_resource[i['report_id']].replace('!_', '') for i in active_tables_cm]))))

    # tables = list(set([t.replace('!_', '') for t in
    #             list(ex.cm_table_name_res.values()) +
    #             list(ex.cm_top_booking_name_res.values())
    #             if t not in ('!_dcm_geo_traffic')
    #           ]))
    dask.delayed([ex.trunc_bq(t, client_bq) for t in tables]).compute()

    # # Парсим с трункейтом
    ex.truncate = False
    print('BEFORE MAIN PIPELINE NEWAGE',ctime())
    ex.main_pipeline(ex.list_data_newage,'CM')
    message = f'NEWAGER TRUNCATED {len(ex.counter)} tables: ' + str(list(enumerate(ex.counter)))
    telegram_bot_sendtext(message)
    ex.counter = list()

    ex.table_name_res = ex.cm_top_booking_name_res

    ex.truncate = False
    print('BEFORE MAIN PIPELINE TOP', ctime())
    ex.main_pipeline(ex.list_data_top, 'CM')
    message = f'TOP APPENDED {len(ex.counter)} tables: ' + str(list(enumerate(ex.counter)))
    telegram_bot_sendtext(message)
    ex.counter = list()
    print('CM', time() - start_t)

    # # Парсим dv
    ex.table_name_res = ex.dv_table_name_res
    ex.data_set = 'DV360_Morning'

    union_resource = dict(ex.dv_table_name_res, **ex.dv_table_append_name_res)
    active_tables_dv = ex.list_data_dv_newage + ex.list_data_dv_newage_append
    tables = list(filter(lambda x: x not in ('dcm_geo_traffic_dv'),
                        list(set([union_resource[i['report_id']].replace('!_', '') for i in active_tables_dv]))))

    dask.delayed([ex.trunc_bq(t, client_bq) for t in tables]).compute()

    ex.truncate = False
    print('BEFORE MAIN PIPELINE DV TRUNCATE',ctime())
    ex.main_pipeline(ex.list_data_dv_newage,'DV')
    message = f'DV TRUNCATED {len(ex.counter)} tables: ' + str(list(enumerate(ex.counter)))
    telegram_bot_sendtext(message)
    ex.counter = list()

    ex.truncate = False
    ex.table_name_res = ex.dv_table_append_name_res
    print('BEFORE MAIN PIPELINE DV APPEND',ctime())
    ex.main_pipeline(ex.list_data_dv_newage_append,'DV')
    message = f'DV APPENDED {len(ex.counter)} tables: ' + str(list(enumerate(ex.counter)))
    telegram_bot_sendtext(message)
    ex.counter = list()
    print('DV', time() - start_t)

    dask.config.set(pool=ThreadPoolExecutor(1))
    ex.table_name_res = ex.cm_floodlights
    ex.data_set = 'Frequency_CM'

    ex.truncate = True
    #ex.table_name_res = ex.cm_floodlights
    print('BEFORE MAIN PIPELINE FLOODLIGHT', ctime())
    ex.main_pipeline(ex.list_data_floodlight, 'CM')
    message = f'FLOODLIGHT {len(ex.counter)} tables: ' + str(list(enumerate(ex.counter)))
    telegram_bot_sendtext(message)
    ex.counter = list()
    print('FLOODLIGHT', time() - start_t)

    agreg_all = active_tables_cm +\
                active_tables_dv +\
                ex.list_data_missing_report + ex.list_data_floodlight

    dask.delayed([ex.trasher(m) for m in list(set([f['thread_id'] for f in agreg_all]))]).compute()
    print('END', time() - start_t)
