from decouple import config


# Now you can access the environment variables using config
api_key_youtube = config('API_KEY_YOUTUBE')
channel_id = config('CHANNEL_ID')
account_sid = config('ACCOUNT_SID')
auth_token = config('AUTH_TOKEN')

# You can now use these variables in your code
print(f"API Key (YouTube): {api_key_youtube}")
print(f"Channel ID: {channel_id}")
print(f"Account SID: {account_sid}")
print(f"Auth Token: {auth_token}")
