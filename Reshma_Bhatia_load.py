import boto3
import json
import sys
input = json.loads(open(sys.argv[1]).read())
data=input['data']
dynamodb = boto3.resource('dynamodb')
table = dynamodb.create_table( 
TableName='LAX',
KeySchema=[
{
'AttributeName': 'ID',
'KeyType': 'HASH'
}
],
AttributeDefinitions=[
{
'AttributeName': 'ID',
'AttributeType': 'N'
}
],
ProvisionedThroughput={
'ReadCapacityUnits': 5,
'WriteCapacityUnits': 5
}
)

table.meta.client.get_waiter('table_exists').wait(TableName='LAX')
i=1
with table.batch_writer() as batch:
	for row in data:
		if i<=1000:
			batch.put_item(
			Item={
			'ID':row[0],
			'Year':row[9][0:4],
			'DA':row[11],
			'NOP':row[13],
			}
			)
		else:
			break
		i+=1

	
	