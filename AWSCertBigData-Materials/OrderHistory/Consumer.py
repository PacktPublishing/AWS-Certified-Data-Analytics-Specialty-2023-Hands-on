#!/usr/bin/python

import boto3
import time
import json
import decimal

# Kinesis setup
kinesis = boto3.client("kinesis")
shard_id = "shardId-000000000000" 
pre_shard_it = kinesis.get_shard_iterator(StreamName="CadabraOrders", ShardId=shard_id, ShardIteratorType="LATEST")
shard_it = pre_shard_it["ShardIterator"]

# DynamoDB setup
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('CadabraOrders')

while 1==1:
	out = kinesis.get_records(ShardIterator=shard_it, Limit=100)
	for record in out['Records']:
		print record
		data = json.loads(record['Data'])
		invoice = data['InvoiceNo']
		customer = int(data['Customer'])
		orderDate = data['InvoiceDate']
		quantity = data['Quantity']
		description = data['Description']
		unitPrice = data['UnitPrice']
		country = data['Country'].rstrip()
		stockCode = data['StockCode']

		# Construct a unique sort key for this line item
		orderID = invoice + "-" + stockCode

		response = table.put_item(
			Item = {
				'CustomerID': decimal.Decimal(customer),
				'OrderID': orderID,
				'OrderDate': orderDate,
				'Quantity': decimal.Decimal(quantity),
				'UnitPrice': decimal.Decimal(unitPrice),
				'Description': description,
				'Country': country
			}
		)

	shard_it = out["NextShardIterator"]
	time.sleep(1.0)

