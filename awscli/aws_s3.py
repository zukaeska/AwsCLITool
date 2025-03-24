import boto3
from os import getenv
from dotenv import load_dotenv
import logging
from botocore.exceptions import ClientError
import json


def init_client(env_path: str = ".env"):
    try:
        load_dotenv(dotenv_path=env_path)

        client = boto3.client(
            "s3",
            aws_access_key_id=getenv("aws_access_key_id"),
            aws_secret_access_key=getenv("aws_secret_access_key"),
            aws_session_token=getenv("aws_session_token"),
            region_name=getenv("region"),
        )
        client.list_buckets()
        return client
    except ClientError as e:
        logging.error(f"ClientError occurred: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")


def list_buckets(client) -> list:
    try:
        response = client.list_buckets()
        buckets = [bucket["Name"] for bucket in response["Buckets"]]
        return buckets
    except ClientError as e:
        logging.error(f"ClientError occurred: {e}")
        return []
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return []


def create_bucket(aws_s3_client, bucket_name, region='us-west-2'):
    """Create an Amazon S3 bucket in a specified region"""
    try:
        location = {'LocationConstraint': region}
        aws_s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def delete_bucket(aws_s3_client, bucket_name):
    """Delete an Amazon S3 bucket"""
    try:
        aws_s3_client.delete_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def bucket_exists(aws_s3_client, bucket_name):
    try:
        response = aws_s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False


def create_bucket_policy(aws_s3_client, bucket_name):
    aws_s3_client.delete_public_access_block(Bucket=bucket_name)
    aws_s3_client.put_bucket_policy(
        Bucket=bucket_name, Policy=generate_public_read_policy(bucket_name))


def read_bucket_policy(aws_s3_client, bucket_name):
    try:
        policy = aws_s3_client.get_bucket_policy(Bucket=bucket_name)
        policy_str = policy["Policy"]
        print(policy_str)
        return policy_str
    except ClientError as e:
        print(e)
        return False


def generate_public_read_policy(bucket_name):
    policy = {
        "Version":
        "2012-10-17",
        "Statement": [{
            "Sid": "PublicReadGetObject",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:::{bucket_name}/*",
        }],
    }

    return json.dumps(policy)


def set_object_access_policy(aws_s3_client, bucket_name, file_name):
    try:
        response = aws_s3_client.put_object_acl(ACL="public-read",
                                                Bucket=bucket_name,
                                                Key=file_name)
    except ClientError as e:
        print(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False


def download_file_and_upload_to_s3(aws_s3_client, bucket_name, url, file_name, keep_local=False):
    from urllib.request import urlopen
    import io
    import magic

    # 1. Download the file from the URL
    try:
        with urlopen(url) as response:
            content = response.read()
    except Exception as e:
        logging.error(f"Error downloading file from URL: {e}")
        return False

    # 2. Detect the file's MIME type
    mime_type = magic.from_buffer(content, mime=True)
    allowed_mime_types = {
        "image/bmp": [".bmp"],
        "image/jpeg": [".jpg", ".jpeg"],  # JPEG covers .jpg and .jpeg
        "image/png": [".png"],
        "image/webp": [".webp"],
        "video/mp4": [".mp4"]
    }

    if mime_type not in allowed_mime_types:
        logging.error(f"Unsupported file type: {mime_type}")
        return False

    # 3. Upload to S3 (if the file type is valid)
    try:
        aws_s3_client.upload_fileobj(
            Fileobj=io.BytesIO(content),
            Bucket=bucket_name,
            Key=file_name,
            ExtraArgs={
                "ContentType": mime_type,
                "ContentDisposition": "inline",
            }
        )
    except ClientError as e:
        logging.error(f"ClientError uploading file to S3: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error uploading file to S3: {e}")
        return False

    # 4. Optionally save a local copy
    if keep_local:
        try:
            with open(file_name, "wb") as local_file:
                local_file.write(content)
        except Exception as e:
            logging.error(f"Error saving file locally: {e}")

    # 5. Return a (potential) public URL
    #    You could also set the object's ACL to public-read or configure your bucket policy for public access.
    region = "us-west-2"  # Or fetch from env/config
    return f"https://s3-{region}.amazonaws.com/{bucket_name}/{file_name}"
