import boto3

dynamodb = boto3.resource('dynamodb')
TABLE_NAME = 'Holdings'

table = dynamodb.create_table(
    TableName=TABLE_NAME,
    KeySchema=[
        {
            'AttributeName': 'ticker',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'date',
            'KeyType': 'RANGE'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'ticker',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'date',
            'AttributeType': 'S'
        },
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 1,
        'WriteCapacityUnits': 1
    }
)

table.meta.client.get_waiter('table_exists').wait(TableName=TABLE_NAME)
print(f"{TABLE_NAME} successfully provisioned! Items: {table.item_count}")