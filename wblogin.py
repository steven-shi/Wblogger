# coding: utf-8
import requests
from weibo import APIClient
from weibo import JsonDict
import datetime
import os


save_access_token_file  = 'access_token.txt'
file_path = "." + os.path.sep
access_token_file_path = file_path + save_access_token_file

def login_wb(client, username, passwd):
	login_url = 'https://api.weibo.com/oauth2/authorize'
	data={'action':'submit','withOfficalFlag':'0','ticket':'',\
			'isLoginSina':'', 'response_type':'code', 'regCallback':'', \
			'redirect_uri':CALLBACK_URL, 'client_id':APP_KEY, 'state':'',\
			 'from':'', 'userId':username, 'passwd':passwd}
	auth_url=client.get_authorize_url()
	print auth_url
	r=requests.post(login_url,data=data, headers={'User-agent':'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)','Referer' : auth_url}, verify=False)
	print r.url
	return r.url.split('=')[1]

def unixToDt(unixtime):
	return datetime.datetime.fromtimestamp(int(unixtime))

def save_access_token(token):
    f = open(access_token_file_path, 'w')
    f.write(token['access_token']+' ' + str(token['expires_in']))
    f.close()

def get_access_token():
	try:
		print access_token_file_path
		token = open(access_token_file_path, 'r').read().split()
		if len(token) != 2:
			print len(token)
			return None
		access_token, expires_in = token
		print "Valid until" + unixToDt(expires_in).strftime('%Y-%m-%d %H:%M:%S')
		return JsonDict(access_token=access_token, expires_in=expires_in)
	except:
		return None

def get_client(config):
	client = APIClient(app_key=config.APP_KEY, app_secret=config.APP_SECRET, redirect_uri=config.CALLBACK_URL)
	
	token = get_access_token()
	if token is None:
		code = login_wb(client, config.WEIBO_ACCOUNT, config.WEIBO_PASSWORD)
		token = client.request_access_token(code)
		save_access_token(token)
		print token.access_token + ' ' + str(token.expires_in) + ' ' + unixToDt(token.expires_in).strftime('%Y-%m-%d %H:%M:%S')

	client.set_access_token(token.access_token, token.expires_in)
	return client
