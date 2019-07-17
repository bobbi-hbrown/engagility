from twython import Twython
from pprint import pprint
from google.cloud import bigquery
from google.oauth2 import service_account
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
        data = []
        incident_data = {'date': '', 'tweet': '', 'location': '', 'keyword': ''}

        result = twitter.search(q='flooding', geocode='53.8108176,-1.76261,20km')

        for i in result['statuses']:
            incident_data['date'] = i['created_at']
            print(i['created_at'])
            incident_data['tweet'] = i['text']
            print(i['text'])
            incident_data['location'] = i['place']
            print(i['place'])
            incident_data['keyword'] = 'flooding'
            data.append(incident_data)
            incident_data = {'date': '', 'tweet': '', 'location': '', 'keyword': ''}

        print(data)
    except Exception as e:
        print(e)

def push_to_bq(data):

    current_directory = os.getcwd()
    service_account_file = current_directory + 'service-account.json'
    bq_credentials = service_account.Credentials.from_service_account_file(service_account_file)

    # Instantiate BQ client and pass in credentials
    client = bigquery.Client(credentials=bq_credentials)

if __name__ == "__main__":
    twitter = twitter_auth()
    fetch_data(twitter)