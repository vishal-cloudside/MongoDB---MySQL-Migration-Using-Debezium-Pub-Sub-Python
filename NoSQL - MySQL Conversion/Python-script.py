from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1, storage
import json
import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import time
import numpy as np

def fetch_config_from_gcs(bucket_name, file_path):
    """Fetches configuration JSON file from GCS."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    content = blob.download_as_text()
    return json.loads(content)

project_id = <project_id>

# Function to flatten a nested JSON
def flatten_json(key_value):
    result = {}

    def flatten(value, key=''):
        if isinstance(value, dict):
            for unpack_dict in value:
                flatten(value[unpack_dict], key + unpack_dict + '.')
        elif isinstance(value, list):
            for unpack_list, unpack_dict in enumerate(value):
                flatten(unpack_dict, key + str(unpack_list) + '.')
        else:
            result[key[:-1]] = value

    flatten(key_value)
    return result

# Function to handle o_ids
def convert_objectid(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():    
            if k.startswith('$'):
                return v
        return {k: convert_objectid(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_objectid(i) for i in obj]
    return obj

#Handle Update & Insert Records

def handle_replace(subscription_id, flattened_data, config, after_dict):
    collection_config = next((dm for dm in config['data_mapping'] if dm['subscription_id'] == subscription_id), None)
    if not collection_config:
        print(f"No configuration found for subscription_id: {subscription_id}")
        return

    mysql_conn_str = 'mysql+pymysql://user_name:password@host/db_name'
    engine = create_engine(mysql_conn_str)

    for idx, sub_table in enumerate(collection_config['sub_tables']):
        sub_table_name = sub_table.get('sub_table_name')
        table_schema = sub_table.get('table_schema', '')

        fields = sub_table['fields']
        mapping = sub_table['mapping']

        # Initialize column_names here
        column_names = []

        if idx == 0:
            df = pd.DataFrame(flattened_data)

            try:
                df = df[fields]
            except KeyError as e:
                missing_cols = list(set(fields) - set(df.columns))
                print(f"Missing columns: {missing_cols}")
                df = df.reindex(columns=fields, fill_value=None)

            df.rename(columns=mapping, inplace=True)

            for date_column in sub_table.get("Date_column", []):
                if date_column in df.columns:
                    df[date_column] = df[date_column].astype(str)
                    if df[date_column].str.match(r'\w{3} \w{3} \d{2} \d{4} \d{2}:\d{2}:\d{2}').all():
                        df[date_column] = df[date_column].str.extract(r'(\w{3} \w{3} \d{2} \d{4} \d{2}:\d{2}:\d{2})')
                        df[date_column] = pd.to_datetime(df[date_column], format='%a %b %d %Y %H:%M:%S', errors='coerce')
                    else:
                        df[date_column] = pd.to_datetime(df[date_column], format='%Y-%m-%dT%H:%M:%S.%fZ', errors='coerce')

            with engine.connect() as conn:
                if table_schema:
                    conn.execute(text(table_schema))

                for _, row in df.iterrows():
                    row_dict = row.replace({np.nan: None}).to_dict()
                    row_dict['_id'] = after_dict['_id']

                    select_stmt = text(f"SELECT COUNT(*) FROM {sub_table_name} WHERE _id = :_id")
                    result = conn.execute(select_stmt, {'_id': row_dict['_id']}).scalar()

                    if result > 0:
                        update_stmt = text(f"UPDATE {sub_table_name} SET " +
                                           ", ".join([f"{key} = :{key}" for key in row_dict.keys() if key != '_id']) +
                                           " WHERE _id = :_id")
                        try:
                            conn.execute(update_stmt, row_dict)
                            conn.commit()
                            print(f"UPDATE executed successfully on table {sub_table_name}")
                        except Exception as e:
                            print(f"An error occurred while executing UPDATE on table {sub_table_name}: {e}")
                    else:
                        try:
                            df.to_sql(sub_table_name, con=engine, if_exists='append', index=False)
                            conn.commit()
                            num_rows_inserted = len(df)
                            print(f"Number of rows inserted into {sub_table_name}: {num_rows_inserted}")
                            break
                        except Exception as e:
                            print(f"An error occurred while inserting into {sub_table_name}: {e}")

        elif idx > 0:
            cols = sub_table.get('col')
            select_col = {col: after_dict.get(col) for col in cols}

            items_key = next((key for key, value in select_col.items() if isinstance(value, list)), None)
            filtered_keys = {key: value for key, value in select_col.items() if not isinstance(value, list)}

            items = select_col[items_key]
            items = [flatten_json(item) for item in items]

            k = []
            for idx, item in enumerate(items):
                s = list(item.keys())
                if '_id' in s:
                    for j, key in enumerate(s):
                        if key == '_id':
                            s[j] = f"{items_key}_id"
                items[idx] = dict(zip(s, item.values()))
                k.extend(s)

            column_names = list(filtered_keys.keys()) + k

            data = []
            for item in items:
                row = {**filtered_keys, **item}
                data.append(row)

            all_columns = set(column_names)

            normalized_data = [{col: row.get(col, None) for col in all_columns} for row in data]

            df1 = pd.DataFrame(normalized_data)
            df1 = df1.reindex(columns=fields, fill_value=None)
            df1.rename(columns=mapping, inplace=True)
            print(f"Processing sub-table: {sub_table_name}")

            for date_column in sub_table.get("Date_column", []):
                if date_column in df1.columns:
                    df1[date_column] = df1[date_column].astype(str)

                    if df1[date_column].str.match(r'\w{3} \w{3} \d{2} \d{4} \d{2}:\d{2}:\d{2}').all():
                        df1[date_column] = df1[date_column].str.extract(r'(\w{3} \w{3} \d{2} \d{4} \d{2}:\d{2}:\d{2})')
                        df1[date_column] = pd.to_datetime(df1[date_column], format='%a %b %d %Y %H:%M:%S', errors='coerce')

                    elif df1[date_column].str.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}').all():
                        df1[date_column] = df1[date_column].str.extract(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6})')
                        df1[date_column] = pd.to_datetime(df1[date_column], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')

                    else:
                        df1[date_column] = pd.to_datetime(df1[date_column], format='%Y-%m-%dT%H:%M:%S.%fZ', errors='coerce')

            with engine.connect() as conn:
                if table_schema:
                    conn.execute(text(table_schema))

                for _, row in df1.iterrows():
                    row_dict = row.replace({np.nan: None}).to_dict()
                    row_dict['_id'] = after_dict['_id']
                    row_dict['o_id'] = row.get('o_id')

                    select_stmt = text(f"SELECT COUNT(*) FROM {sub_table_name} WHERE _id = :_id")
                    result = conn.execute(select_stmt, {'_id': row_dict['_id']}).scalar()

                    if result > 0:
                        update_stmt = text(f"UPDATE {sub_table_name} SET " +
                                           ", ".join([f"{key} = :{key}" for key in row_dict.keys() if key != 'o_id']) +
                                           " WHERE o_id = :o_id")
                        try:
                            conn.execute(update_stmt, row_dict)
                            conn.commit()
                            print(f"UPDATE executed successfully on table {sub_table_name}")
                        except Exception as e:
                            print(f"An error occurred while executing UPDATE on table {sub_table_name}: {e}")
                    else:
                        try:
                            df1.to_sql(sub_table_name, con=engine, if_exists='append', index=False)
                            conn.commit()
                            num_rows_inserted = len(df1)
                            print(f"Number of rows inserted into {sub_table_name}: {num_rows_inserted}")
                            break
                        except Exception as e:
                            print(f"An error occurred while inserting into {sub_table_name}: {e}")
                          
#Handle delete Records

def handle_delete(subscription_id, json_2dict, config, before_dict):
    source = json_2dict.get('source')
    collection_name = source['collection']

    collection_config = next((dm for dm in config['data_mapping'] if dm['subscription_id'] == subscription_id), None)
    if not collection_config:
        print(f"No configuration found for subscription_id: {subscription_id}")
        return

    sub_tbl_lst = []
    for config in config['data_mapping']:
        collection = config['source_collection_name']
        if collection_name == collection:
            sub_tables = config['sub_tables']
            for sub_table in sub_tables:
                sub_tbl_lst.append(sub_table['sub_table_name'])

    mysql_conn_str = 'mysql+pymysql://user_name:password@host/db_name'
    engine = create_engine(mysql_conn_str)

    try:
        deleted_id = before_dict.get('_id')
        print('deleted_id', deleted_id)

        for sub_tbl in sub_tbl_lst:
            delete_stmt = f"DELETE FROM {sub_tbl} WHERE _id = :_id"
            with engine.connect() as conn:
                try:
                    conn.execute(text(delete_stmt), {'_id': deleted_id})
                    conn.commit()
                except Exception as e:
                    print(f"An error occurred while deleting from table {sub_tbl}: {e}")

    except (json.JSONDecodeError, ValueError) as e:
        print(f"Failed to parse ordering key: {deleted_id} with error: {e}")

# Get the Messages in Pub/Sub

def process_message(subscription_id, config, timeout=15.0):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        nonlocal config
        message_data = message.data.decode('utf-8')
        json_2dict = json.loads(message_data)

        stream_type = json_2dict.get('op')

        if stream_type in ['c', 'u' ,'r']:
            after_data = json_2dict.get('after')
            after_dict = json.loads(after_data)
            after_dict = convert_objectid(after_dict)
            flattened_data = [flatten_json(after_dict)]

            handle_replace(subscription_id, flattened_data, config, after_dict)

        elif stream_type == 'd':
            before_data = json_2dict.get('before')
            before_dict = json.loads(before_data)
            before_dict = convert_objectid(before_dict)
            handle_delete(subscription_id, json_2dict, config, before_dict)

        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

# Listening Multiple Subscription Parallelly

def main():
    bucket_name = 'utils'
    file_path = 'Config_files/stream.json'

    while True:
        try:
            config = fetch_config_from_gcs(bucket_name, file_path)
            for data_mapping in config['data_mapping']:
                subscription_id = data_mapping['subscription_id']
                process_message(subscription_id, config)
            time.sleep(3)
        except Exception as e:
            print(f"Error fetching or processing messages: {e}")

if __name__ == "__main__":
    main()



