
from google.cloud import bigquery

from telegram import telegram_bot_sendtext


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

    #         print(f'Created dataset {path_dataset_name}')

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

    #         print(f'Created table {path_table_name}')

    def upload_csv_data(self, data_file, table_name, data_set, dataset_location,
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

        message = f'Loaded {job.output_rows} rows into {data_set}:{table_name}.'
        print(message)
        telegram_bot_sendtext(message)


    def upload_df_data(self, df, table_name, data_set, dataset_location,
                        truncate=False, schema_autodetect=True):

        dataset_ref = self.client.dataset(data_set)
        table_ref = dataset_ref.table(table_name)
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = schema_autodetect
        if truncate:
            job_config.write_disposition = 'WRITE_TRUNCATE'

        job = self.client.load_table_from_dataframe(
            df,
            table_ref,
            location=dataset_location,
            job_config=job_config,
        )

        job.result()

        message = f'Morning Datasets: Loaded {job.output_rows} rows into {data_set}:{table_name}, trunc {truncate}.'
        print(message)
        telegram_bot_sendtext(message)
