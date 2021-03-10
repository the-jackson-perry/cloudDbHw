import boto3
import csv

user_creds = {}
with open('new_user_credentials.csv') as csv_file:
	csv_reader = csv.reader(csv_file, delimiter=',')
	line = 0
	for row in csv_reader:
		if line == 1:
			user_creds['User name'] = row[0]
			user_creds['Password'] = row[1]
			user_creds['Access key ID'] = row[2]
			user_creds['Secret access key'] = row[3]
			user_creds['Console login link'] = row[4]
		line += 1


# Use account credentials to access s3
s3 = boto3.resource('s3',
	aws_access_key_id= user_creds['Access key ID'],
	aws_secret_access_key= user_creds['Secret access key'])


# Create a new bucket unless it already exists
try:
	s3.create_bucket(Bucket='db-hw-bucket-18756895', CreateBucketConfiguration={
		'LocationConstraint': 'us-west-2'})
	print('Bucket successfully created')
except:
	print ("Bucket already exists")


# Retreive the bucket
bucket = s3.Bucket("db-hw-bucket-18756895")

# Set bucket to be publicly readable
response = bucket.Acl().put(ACL='public-read')
print()
print(response)
print()
response = None

# Open file to upload in read-binary mode
body = open('test_upload.txt', 'rb')

# Upload the file to the bucket
o = s3.Object('db-hw-bucket-18756895', 'test_upload.txt').put(Body=body)

# Mark the binary object as publically readable
response = s3.Object('db-hw-bucket-18756895', 'test_upload.txt').Acl().put(ACL='public-read')
print()
print(response)
print()
response = None

# Use account credentials to access dynamoDB
dyndb = boto3.resource('dynamodb',
	region_name='us-west-2',
	aws_access_key_id= user_creds['Access key ID'],
	aws_secret_access_key= user_creds['Secret access key']
)


try:
	table = dyndb.create_table(
		TableName='DataTable-18756895',
		KeySchema=[
			{
			'AttributeName': 'PartitionKey',
			'KeyType': 'HASH'
			},
			{
			'AttributeName': 'RowKey',
			'KeyType': 'RANGE'
			}
			],
			AttributeDefinitions=[
			{
			'AttributeName': 'PartitionKey',
			'AttributeType': 'S'
			},
			{
			'AttributeName': 'RowKey',
			'AttributeType': 'S'
			},
		],
		ProvisionedThroughput={
			'ReadCapacityUnits': 5,
			'WriteCapacityUnits': 5
		}
	)
except:
	# Table might exist already
	print('Table might already exist')
	table = dyndb.Table("DataTable-18756895")

# Pause until we know the table exists
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable-18756895')

# Print number of items in table
print('Items in table: ' + str(table.item_count))

# Upload files to the bucket
with open('experiments.csv', 'r') as csvfile:
	csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
	next(csvf)
	for item in csvf:
		# Print the contents of the row
		print(item)

		# Open the datafile that corresponds to the url in the csv
		body = open(item[4], 'rb')

		# Upload the binary file to the bucket
		s3.Object('db-hw-bucket-18756895', item[4]).put(Body=body)

		# Mark it as publicly readable
		md = s3.Object('db-hw-bucket-18756895', item[4]).Acl().put(ACL='public-read')

		# Generate the url of the file in the bucket to store in the table
		url = "https://s3-us-west-2.amazonaws.com/db-hw-bucket-18756895/"+item[4]

		# Put together the item that will be inserted into the table
		metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
		'date' : item[2], 'comment' : item[3],  'url':url}

		# Try to add the item to the table
		try:
			table.put_item(Item=metadata_item)
		except:
			print("put_item() failed. The item may already exist in the table")

# Retreive an item from the table
response = table.get_item(
	Key={
	'PartitionKey': 'experiment1',
	'RowKey': 'data1'
	}
)


# Print the response
print()
print(response)
print()

# Extract the 'Item' from the reponse and print it.
item = response['Item']
print(item)
