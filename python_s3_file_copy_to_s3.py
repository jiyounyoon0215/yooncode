from datetime import timedelta, datetime
import boto3
import tempfile


a_key = 'ACCESSKEy'
s_key = 'SECRETKEY'
region_name = "us-west-2"

def s3_file_copy(s3_bucket_source, s3_key_source, s3_bucket_target, s3_key_target, s3_key_filter = None):
    print('source: s3://{0}/{1}'.format(s3_bucket_source, s3_key_source))
    print('target: s3://{0}/{1}'.format(s3_bucket_target, s3_key_target))
    client = boto3.resource('s3', aws_access_key_id=a_key , aws_secret_access_key=s_key)

    bucket=client.Bucket(s3_bucket_source)
    for key in bucket.objects.filter(Prefix = s3_key_source):
        print(key)
        try:
            if(s3_key_filter in key.key):
                copy_source=s3_bucket_source+'/'+key.key
                copy_dest_key = s3_key_target + key.key.replace(s3_key_source, '')

                # copy key
                client.Object(s3_bucket_target, copy_dest_key).copy_from(CopySource = copy_source)
                print('Key {0} copied to {1}'.format(copy_source,copy_dest_key ))
        except Exception as e:
            print(e)

    # finish
    print('keys copied')

print('START')
s3_bucket_source='SOURCEBUCKET'
s3_bucket_target='TARGETBUCKET'
s3_key_target='TARGETKEY'
start_date=datetime.strptime('2018-11-22 00:00:00', '%Y-%m-%d %H:%M:%S')
end_date=datetime.today()

while start_date < end_date:
    print('start_date: {0}'.format(start_date))
    key_date=start_date.strftime('%Y/%m/%d')
    s3_key_source='SOURCEKEY/archive/{0}/RESTSOURCEKEY'.format(key_date)
    print('s3_key_source: {0}'.format(s3_key_source))
    s3_key_filter='FILTER_'

    s3_file_copy(s3_bucket_source, s3_key_source, s3_bucket_target, s3_key_target, s3_key_filter)

    # increment date
    start_date = start_date + timedelta(days = 1)


    
