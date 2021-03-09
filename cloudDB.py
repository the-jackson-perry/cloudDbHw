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


s3 = boto3.resource('s3',
	aws_access_key_id=user_creds['Access key ID'],
	aws_secret_access_key=user_creds['Secret access key'])

try:
	s3.create_bucket(Bucket='db-hw-bucket-18756895', CreateBucketConfiguration={
		'LocationConstraint': 'us-west-2'})
except:
	print ("Bucket already exist")


bucket = s3.Bucket("db-hw-bucket-18756895")

bucket.Acl().put(ACL='public-read')

