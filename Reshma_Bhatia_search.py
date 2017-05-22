import sys
import boto3
from boto3.dynamodb.conditions import Key, Attr
dynamodb = boto3.resource('dynamodb')
table=dynamodb.Table('LAX')
da=sys.argv[1]
year=sys.argv[2]
#print (da)
#print (year)
response = table.scan(FilterExpression=Attr('DA').eq(da) & Attr('Year').eq(year), ProjectionExpression='NOP')
items = response['Items']
sum=0
for item in items:
	sum+=int(item['NOP'])
print (sum)