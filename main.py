from twython import Twython
from pprint import pprint
import json

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
        result = twitter.search(q='flooding&fire', geocode='53.8108176,-1.76261,20km')
        #print(result)
        for i in result['statuses']:
            #pprint(i)
            print(i['created_at'])
            print('text: ',i['text'])
            print(i['user']['location'])
            print(i['place']['full_name'])
            

    except Exception as e:
        print(e)
