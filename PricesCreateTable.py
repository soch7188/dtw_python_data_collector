import boto3

dynamodb = boto3.resource('dynamodb')

table = dynamodb.create_table(
    TableName='market-prices',
    KeySchema=[
        {
            'AttributeName': 'date',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'time',
            'KeyType': 'RANGE'
        }
    ], 
    AttributeDefinitions=[
        {
            'AttributeName': 'date',
            'AttributeType': 'N'
        }, 
        {
            'AttributeName': 'time',
            'AttributeType': 'N'
        }
    ], 
    ProvisionedThroughput={
        'ReadCapacityUnits': 2,
        'WriteCapacityUnits': 1
    }
)

table.meta.client.get_waiter('table_exists').wait(TableName='market-prices')
print(table.item_count)