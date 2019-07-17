from twython import Twython
from pprint import pprint
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import json
import os

def twitter_auth():

    # All twitter keys are stored in a separate JSON file, and are not to be stored in the git repo
    with open('credentials.json') as data:
        api_keys = json.load(data)
    APP_KEY = api_keys['CONSUMER_KEY']
    APP_SECRET = api_keys['CONSUMER_SECRET']

    # Instantiate the Twython lin
    twitter = Twython(APP_KEY, APP_SECRET, oauth_version=2)
    ACCESS_TOKEN = twitter.obtain_access_token()

    twitter = Twython(APP_KEY, access_token=ACCESS_TOKEN)

    return twitter

def fetch_data(twitter):

    try:

        incident_data = {'date': [], 'tweet': [], 'location': [], 'keyword': []}

        result = twitter.search(q='flooding', geocode='53.8108176,-1.76261,20km')

        for i in result['statuses']:
            incident_data['date'].append(['created_at'])
            print(i['created_at'])
            incident_data['tweet'].append(i['text'])
            print(i['text'])
            incident_data['location'].append(i['place'])
            print(i['place'])
            incident_data['keyword'].append('flooding')

        dataframe = pd.DataFrame(incident_data)
        incident_data = {'date': '', 'tweet': '', 'location': '', 'keyword': ''}

        print(dataframe)
        
    except Exception as e:
        print(e)

    return dataframe

def push_pandas_to_bigquery(df):

    # Service account variables for authentication
    current_directory = os.getcwd()
    service_account_file = current_directory + '/service-account.json'
    bigquery_credentials = service_account.Credentials.from_service_account_file(service_account_file)

    client = bigquery.Client(credentials=bigquery_credentials)
    bigquery_config = bigquery.job.LoadJobConfig(ignore_unknown_values=True)
    dataset_ref = client.dataset('engagility')
    table_ref = dataset_ref.table('incidents')

    client.load_table_from_dataframe(df, table_ref, job_config=bigquery_config).result()
    print("%s loaded to BQ" % (df))



if __name__ == "__main__":
    twitter = twitter_auth()
    df = fetch_data(twitter)
    push_pandas_to_bigquery(df)