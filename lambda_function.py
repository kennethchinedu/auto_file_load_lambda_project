import os, json
from download import download_file
from upload2 import upload_s3
from util2 import get_prev_file_name, get_next_file_name, upload_bookmark

#lambder handler function
def lambda_handler(event, context):
    global upload_res
    environ = os.environ.get("ENVIRON")
    if environ == "DEV":
        print(f'Running in {environ} environment')
        os.environ.setdefault('AWS_DEFAULT_PROFILE', 'ken_new')
    bucket_name = os.environ.get('BUCKET_NAME')
    bookmark_file = os.environ.get('BOOKMARK_FILE')
    baseline_file = os.environ.get('BASELINE_FILE')
    file_prefix = os.environ.get('FILE_PREFIX')
      
    #Writing the get object funtion
    while True:
        prev_file_name = get_prev_file_name(bucket_name, file_prefix, bookmark_file, baseline_file)
        file_name = get_next_file_name(prev_file_name)
        download_res = download_file(file_name)
        if download_res.status_code == 404:
            print(f'invalid file name or douwnload for till {prev_file_name}')
            break
        upload_res = upload_s3(
            download_res.content,
            bucket_name,
            f'{file_prefix}/{file_name}'
        )
        print(f'File {file_name}  successfully processed')
        upload_bookmark(bucket_name, file_prefix, bookmark_file, file_name)
    return upload_res
        
        
        
        
        # try:
        #     bookmark_file = s3_client.get_object(
        #         Bucket = 'anamsbucket',
        #         Key='sandbox/bookmark'
        #     )
        #     prev_file = bookmark_file['Body'].read().decode('utf-8')
            
        # except ClientError as e:
        #     if e.response['Error']['Code'] == 'NoSuchKey':
        #         prev_file = baseline_file
        #     else:
        #         raise
        
        # #Getting file from url using request       
        # dt_part = prev_file.split('.')[0]
        # next_file = f"{dt.strftime(dt.strptime(dt_part, '%Y-%M-%d-%H') + td(hours=1), '%Y-%M-%d-%H')}.json.gz"
        # res = requests.get(f'https://data.gharchive.org/{next_file}')
        
        # if res.status_code != 200:
        #     break
        # #Processing the next filr and uploading to s3
        # print(f' Status code of {next_file} is {res.status_code}')
        # bookmark_contents = next_file
        # s3_client.put_object(
        #     Bucket='anamsbucket',
        #     Key='sandbox/bookmark',
        #     Body=bookmark_contents.encode('utf-8')
        # )
        
   
