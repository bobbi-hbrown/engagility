import pickle, os, json, time
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow

class AuthenticateGCP:
    'Authentication Library for GCP projects'

    def __init__(self):
        self.current_directory = os.path.dirname(os.path.realpath(__file__))
        

    def oauth_flow(self, suffix, redirect_uri, oauth_scope):
        '''
        This function requires you to have a client secret stored as client_secret.json.
        1) Ensure you have correct OAuth client ID set up to access your required API: https://console.cloud.google.com/apis/credentials
        2) Download your project's JSON file by clicking the Download icon
        3) Rename the file client_secret.json and place it in the /config directory of your project's repo
        '''
        # oauth scope - Find out if you need readonly access, or write/execute permissions, this will depend on the intent of your project
        redirect_uri = redirect_uri
        oauth_scope = oauth_scope
        try:
            credentials = pickle.load(open(self.current_directory + '/config-'+ suffix + '/credentials.pickle', 'rb'))
        except (OSError, IOError):
            try:
                flow = InstalledAppFlow.from_client_secrets_file(self.current_directory + '/config-' + suffix + '/client_secret.json', scopes=oauth_scope)
                credentials = flow.run_console()
                pickle.dump(credentials, open(self.current_directory + '/config-' + suffix + '/credentials.pickle', 'wb'))
            except (OSError, IOError) as e:
                print(e)
                print('Client secret file not found. Make sure you have OAuth client ID set up to access your required API: https://console.cloud.google.com/apis/credentials /n Also make sure to download your porject\'s JSON file and place in the /config directory of this repo.')

        return credentials

    def export_google_credentials(self):
        # Use OS library to export service account credentials to script's environment 
        #service_account_file = self.current_directory + '/service-account.json'
        service_account_file = 'service-account.json'
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_file

        return os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

    def service_account(self):
        '''
        Make sure service account file in saved in project's config folder
        '''
        
        service_account_file = self.current_directory + '/service-account.json'
        credentials = service_account.Credentials.from_service_account_file(service_account_file)

        return credentials

class ErrorHandling:

    def __init__(self, HttpError, max_retries, retry_errors, wait_interval):

        self.max_retries = max_retries
        self.retry_errors = retry_errors
        self.wait_interval = wait_interval
        self.HttpError = HttpError


    def http_retry(self):

        # This function ensures code is retried max_retries number of times if failed due to Http error

        retries = 0
        while retries <= self.max_retries:
            decoded_error_body = self.HttpError.content.decode('utf-8')
            json_error = json.loads(decoded_error_body)
            if json_error['error']['code'] in self.retry_errors:
                time.sleep(self.wait_interval)
                retries += 1
                continue

        return