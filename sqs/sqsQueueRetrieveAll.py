#=============================================================================
# Module for polling and extracting news or research alerts from an AWS SQS Queue
# This module uses boto3 library from Anazon for fetching messages
# It uses pycryptodome - for AES GCM decryption
#-----------------------------------------------------------------------------
#   This source code is provided under the Apache 2.0 license
#   and is provided AS IS with no warranty or guarantee of fit for purpose.
#   Copyright (C) 2021 Refinitiv. All rights reserved.
#=============================================================================

import boto3
import json
import base64
from Crypto.Cipher import AES
import html

REGION = 'us-east-1'

#==============================================
def decrypt(key, source):
#==============================================
	GCM_AAD_LENGTH = 16
	GCM_TAG_LENGTH = 16
	GCM_NONCE_LENGTH = 12
	key = base64.b64decode(key)
	cipherText = base64.b64decode(source)
	
	aad = cipherText[:GCM_AAD_LENGTH]
	nonce = aad[-GCM_NONCE_LENGTH:] 
	tag = cipherText[-GCM_TAG_LENGTH:]
	encMessage = cipherText[GCM_AAD_LENGTH:-GCM_TAG_LENGTH]
	
	cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
	cipher.update(aad)
	decMessage = cipher.decrypt_and_verify(encMessage, tag)
	return decMessage


#==============================================
def processPayload(payloadText, callback):
#==============================================
	pl = json.loads(payloadText)
	# handover the decoded message to calling module
	if callback is not None:
		callback(pl)
	else:
		print(json.dumps(pl, indent=2))


#==============================================
def retrieveAndRemove(accessID, secretKey, sessionToken, endpoint, cryptographyKey, callback=None):
#==============================================
	print("  accessID: %s" % (accessID) )
	print("  secretKey: %s" % (secretKey) )
	print("  cryptographyKey: %s" % (cryptographyKey) )
	print("  sessionToken: %s" % (sessionToken) )

	# create a SQS session
	session = boto3.Session(
		aws_access_key_id = accessID,
		aws_secret_access_key = secretKey,  
		aws_session_token = sessionToken,
		region_name = REGION
	)

	sqs = session.client('sqs')
#	sqs = session.client('sqs', use_ssl=True, verify=False)

	response = sqs.get_queue_attributes(
    QueueUrl=endpoint,
    AttributeNames=[
        'All',
    ]
	)

	print('Queue Attributes are:\n')
	print(json.dumps(response, indent=2))


	print('*** Retrieving all messages from queue... ***')
	i = 0
	while 1: 
		resp = sqs.receive_message(QueueUrl = endpoint, MaxNumberOfMessages=10, WaitTimeSeconds = 0)
			
		if 'Messages' in resp:
			messages = resp['Messages']

			print(f"&&&Number of messages received: {len(messages)}, iteration {i} &&&")
			# print and remove all the nested messages
			for message in messages:
				mBody = message['Body']
				# decrypt this message
				m = decrypt(cryptographyKey, mBody)
				processPayload(m, callback)
				# *** accumulate and remove all the nested message
				sqs.delete_message(QueueUrl = endpoint, ReceiptHandle = message['ReceiptHandle'])
			i += 1
		else:
			print('No more messages on queue... after ',i, ' iterations')
			break



#==============================================
if __name__ == "__main__":
#==============================================
	print("SQS module cannot run standalone. Please use newsAlerts or researchAlerts")
