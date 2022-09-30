#=============================================================================
#   This source code is provided under the Apache 2.0 license
#   and is provided AS IS with no warranty or guarantee of fit for purpose.
#   Copyright (C) 2022 Refinitiv. All rights reserved.
#=============================================================================
#
#       newsMessagesFilteredSifDev.py
#
#		Subscribes to Significant Developments in News  
#
#		is built upon Quickstart Python example 
# 		https://developers.refinitiv.com/en/api-catalog/refinitiv-data-platform/refinitiv-data-platform-apis/download 
#		
#		uses rdpToken.py to authenticate with RDP
#		uses sqsQueueRetrieveAll.py to work with AWS queue
#		RDP credentials are expected by default in credentials.ini file	in the same folder
#
#		uses config file lastSubscriptionFile to store and retrieve the last subscription made
#
#		command line arguments:
# 	 	-l		<List active subscriptions>
#		-d		<Delete last or all subscriptions>
#		-s		<create subscription to news stories>
#		-r 		<retrieve, print and remove all queued message per specified subscription, default is the last created
# 				and stored subscription>
#
# #=============================================================================

import requests
import json
import rdpToken
import sqsQueueRetrieveAll
import atexit
import sys
import boto3
from botocore.exceptions import ClientError
import time
from os.path import exists

# Application Constants
base_URL = "https://api.refinitiv.com"
RDP_version = "/v1"
category_URL = "/message-services"
endpoint_URL_headlines = "/news-headlines/subscriptions"
endpoint_URL_stories = "/news-stories/subscriptions"

currentSubscriptionID = None
endpoint_with_subscriptionId = None
crypto_key = None
gHeadlines = False

lastSubscriptionFile = "./lastSubscribed.cfg"

#==============================================
def subscribeToNews():
#==============================================
	if gHeadlines:
		RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL_headlines
	else:
		RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL_stories

# Optional filters can be applied to subscription. Filter by attribution to NS:SIGDEV and multiple PermIds
	requestData = {
		"transport": {
			"transportType": "AWS-SQS"
		},
		"filter": {
			"type": "operator",
    		"operator": "and",
    		"operands": [
		{
			"type": "attribution",
    		"value": "NS:SIGDEV"
		},
			{
				"operator": "or",
				"operands": [
				{
					"value": "R:AMZN.O",
					"type": "newscode"
				},
				 {
        			"value": "P:4295905494",
        			"type": "permid"
      			},
				{
					"value": "R:GOOG.O",
					"type": "newscode"
				},
      			{
        			"value": "P:5030853586",
        			"type": "permid"
      			},
				{
        			"value": "R:META.O",
        			"type": "newscode"
      			},
      			{
        			"value": "P:4297297477",
        			"type": "permid"
      			},
				{
        			"value": "R:TSLA.O",
        			"type": "newscode"
      			},
      			{
        		"value": "P:4297089638",
        		"type": "permid"
      			}],
				"type": "operator"
			}
]},
	"filterOverrides": {},
  "maxcount": 10,
  "dateFrom": "null",
  "dateTo": "null",
  "relevanceGroup": "all",
		"payloadVersion": "2.0"
	}

	# get the latest access token
	accessToken = rdpToken.getToken()
	hdrs = {
		"Authorization": "Bearer " + accessToken,
		"Content-Type": "application/json"
	}

	dResp = requests.post(RESOURCE_ENDPOINT, headers = hdrs, data = json.dumps(requestData))
	if dResp.status_code != 200:
		raise ValueError("Unable to subscribe. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		jResp = json.loads(dResp.text)
		preserveLastSubscription(jResp["transportInfo"]["endpoint"], 
			jResp["transportInfo"]["cryptographyKey"], jResp["subscriptionID"])
		return jResp["transportInfo"]["endpoint"], jResp["transportInfo"]["cryptographyKey"], jResp["subscriptionID"]

#==============================================
def preserveLastSubscription(line1, line2, line3):
#==============================================	
	f = open(lastSubscriptionFile, "w")
	f.write(line1)
	f.write("\n")
	f.write(line2)
	f.write("\n")
	f.write(line3)
	f.close()

#==============================================
def getCloudCredentials(endpoint):
#==============================================
	CC_category_URL = "/auth/cloud-credentials"
	CC_endpoint_URL = "/"
	RESOURCE_ENDPOINT = base_URL + CC_category_URL + RDP_version + CC_endpoint_URL
	requestData = {
		"endpoint": endpoint
	}

	# get the latest access token
	accessToken = rdpToken.getToken()
	dResp = requests.get(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken}, params = requestData)
	if dResp.status_code != 200:
		raise ValueError("Unable to get credentials? Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		jResp = json.loads(dResp.text)
		return jResp["credentials"]["accessKeyId"], jResp["credentials"]["secretKey"], jResp["credentials"]["sessionToken"]


#==============================================
def startNewsSubscription(headlines = True):
#==============================================
	global currentSubscriptionID
	global gHeadlines
	try:
		gHeadlines = headlines
		if gHeadlines:
			print("Subscribing to news headline messages...")
		else:
			print("Subscribing to news stories messages...")

		endpoint, cryptographyKey, currentSubscriptionID = subscribeToNews()
		print("  Queue endpoint: %s" % (endpoint) )
		print("  cryptographyKey: %s" % (cryptographyKey) )
		print("  Subscription ID: %s" % (currentSubscriptionID) )

		# unsubscribe before shutting down
	#ZF	atexit.register(removeSubscription)
	except KeyboardInterrupt:
		print("User requested break, cleaning up...")
		sys.exit(0)

#==============================================
def retrieveAndRemoveMessages(endpoint,cryptographyKey):
#==============================================
	try:
		print("Getting credentials to connect to AWS Queue...")
		accessID, secretKey, sessionToken = getCloudCredentials(endpoint)
		print("Queue access ID: %s" % (accessID) )
		print("Getting news, press BREAK to exit and delete subscription...")
			
		sqsQueueRetrieveAll.retrieveAndRemove(accessID, secretKey, sessionToken, endpoint, cryptographyKey)

	except ClientError as e:
				print("Cloud credentials likely exprired, ClientError message: ", e)
	


#==============================================
def removeSubscription():
#==============================================
	if gHeadlines:
		RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL_headlines
	else:
		RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL_stories

	# get the latest access token
	accessToken = rdpToken.getToken()

	if currentSubscriptionID:
		print("Deleting the subscription ",currentSubscriptionID)
		dResp = requests.delete(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken}, params = {"subscriptionID": currentSubscriptionID})
	else:
		print("Deleting ALL open headline and stories subscription")
		dResp = requests.delete(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken})

	if dResp.status_code > 299:
		print(dResp)
		print("Warning: unable to remove subscription. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		print("News messages unsubscribed!")


#==============================================
def showActiveSubscriptions():
#==============================================
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version

	# get the latest access token
	accessToken = rdpToken.getToken()

	print("Getting all open headlines subscriptions")
	dResp = requests.get(RESOURCE_ENDPOINT + endpoint_URL_headlines, headers = {"Authorization": "Bearer " + accessToken})

	if dResp.status_code != 200:
		raise ValueError("Unable to get subscriptions. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		jResp = json.loads(dResp.text)
		print(json.dumps(jResp, indent=2))

	print("Getting all open stories subscriptions")
	dResp = requests.get(RESOURCE_ENDPOINT + endpoint_URL_stories, headers = {"Authorization": "Bearer " + accessToken})

	if dResp.status_code != 200:
		raise ValueError("Unable to get subscriptions. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		jResp = json.loads(dResp.text)
		print(json.dumps(jResp, indent=2))

def retrieveLastSubscription():

	global endpoint_with_subscriptionId
	global crypto_key
	global currentSubscriptionID

	if exists(lastSubscriptionFile):
		f = open(lastSubscriptionFile, "r")
		endpoint_with_subscriptionId = f.readline().rstrip('\n')
		crypto_key = f.readline().rstrip('\n')
		currentSubscriptionID = f.readline().rstrip('\n')
		return True
	else:
		return False

#==============================================
if __name__ == "__main__":
#==============================================
	if len(sys.argv) > 1:
		if sys.argv[1] == '-l':
			showActiveSubscriptions()
		elif sys.argv[1] == '-d':
			retrieveLastSubscription()
			removeSubscription()
		elif sys.argv[1] == '-h':
			startNewsSubscription()
		elif sys.argv[1] == '-s':
			startNewsSubscription(headlines = False)
		elif sys.argv[1] == '-r':
			if retrieveLastSubscription():
				retrieveAndRemoveMessages(endpoint_with_subscriptionId,crypto_key)
			else:	
				retrieveAndRemoveMessages(sys.argv[2],sys.argv[3])
	else:
		print("Arguments:")
		print("  -l		<List active subscriptions>")
		print("  -d		<Delete all subscriptions>")
		print("  -h		<create subscription to news headlines>")
		print("  -s		<create subscription to news stories>")
		print("  -r endpointPerSubscription cryptographyKey     <retrieve&remove queued messages per subscriptionId>")
