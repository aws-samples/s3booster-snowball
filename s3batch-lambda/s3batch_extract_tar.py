import logging
from urllib import parse
import boto3
from botocore.exceptions import ClientError
from io import BytesIO
import tarfile

logger = logging.getLogger(__name__)
logger.setLevel('INFO')

s3 = boto3.client('s3')


def lambda_handler(event, context):
    """
    extract tarfile on S3.

    :param event: The Amazon S3 batch event that contains object name and bucket_name
    :param context: Context about the event.
    :return: A result structure that Amazon S3 uses to interpret the result of the
             operation.
    """
    # Parse job parameters from Amazon S3 batch operations
    invocation_id = event['invocationId']
    invocation_schema_version = event['invocationSchemaVersion']

    results = []
    result_code = None
    result_string = None

    task = event['tasks'][0]
    task_id = task['taskId']
    # obj_key: tarfile on S3
    obj_key = parse.unquote(task['s3Key'], encoding='utf-8')
    bucket_name = task['s3BucketArn'].split(':')[-1]

    logger.info("Got task: extract tarfile - %s", obj_key)
    try:
        tar_file_obj = s3.get_object(Bucket=bucket_name,Key=obj_key)
        tar_content = tar_file_obj['Body'].read()

        with tarfile.open(fileobj = BytesIO(tar_content)) as tar:
            for tar_resource in tar:
                if (tar_resource.isfile()):
                    inner_file_bytes = tar.extractfile(tar_resource).read()
                    s3.upload_fileobj(BytesIO(inner_file_bytes), Bucket = bucket_name, Key = tar_resource.name)
        result_string = "%s is extracted" % obj_key
        logger.info(result_string)
        result_code = 'Succeeded'
    except ClientError as error:
        if error.response['Error']['Code'] == 'NoSuchKey':
            result_code = 'Succeeded'
            result_string = "%s not found" % obj_key
            logger.info(result_string)
        else:
            result_code = 'PermanentFailure'
            result_string = f"Got exception when extracting  " \
                            f"{obj_key}: {error}."
            logger.exception(result_string)
    finally:
        results.append({
            'taskId': task_id,
            'resultCode': result_code,
            'resultString': result_string
        })
    return {
        'invocationSchemaVersion': invocation_schema_version,
        'treatMissingKeysAs': 'PermanentFailure',
        'invocationId': invocation_id,
        'results': results
    }
