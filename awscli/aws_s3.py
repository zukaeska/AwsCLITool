import boto3
import os
from os import getenv
from dotenv import load_dotenv
import logging
from botocore.exceptions import ClientError
import json
import magic
from datetime import datetime, timedelta, timezone


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


def upload_file(aws_s3_client, filename, bucket_name, object_name):
    """Upload a file to S3 by filename"""
    try:
        aws_s3_client.upload_file(filename, bucket_name, object_name)
        return True
    except ClientError as e:
        logging.error(f"Error uploading file: {e}")
        return False


def upload_file_obj(aws_s3_client, filename, bucket_name, object_name):
    """Upload a file to S3 using a file object"""
    try:
        with open(filename, "rb") as file:
            aws_s3_client.upload_fileobj(file, bucket_name, object_name)
        return True
    except ClientError as e:
        logging.error(f"Error uploading file object: {e}")
        return False


def upload_file_put(aws_s3_client, filename, bucket_name, object_name):
    """Upload a file to S3 using put_object (manually reading file content)"""
    try:
        with open(filename, "rb") as file:
            aws_s3_client.put_object(
                Bucket=bucket_name,
                Key=object_name,
                Body=file.read()
            )
        return True
    except ClientError as e:
        logging.error(f"Error uploading with put_object: {e}")
        return False


def multipart_upload(aws_s3_client, bucket_name, filename, object_name, part_size=1024 * 1024 * 5):
    """
    Perform multipart upload to S3.

    :param aws_s3_client: Initialized boto3 S3 client
    :param bucket_name: Target bucket name
    :param filename: Local file to upload
    :param object_name: Desired S3 object key
    :param part_size: Size of each part in bytes (default 5MB)
    """
    try:
        mpu = aws_s3_client.create_multipart_upload(Bucket=bucket_name, Key=object_name)
        mpu_id = mpu["UploadId"]

        parts = []
        uploaded_bytes = 0
        total_bytes = os.stat(filename).st_size

        with open(filename, "rb") as f:
            part_number = 1
            while True:
                data = f.read(part_size)
                if not data:
                    break
                part = aws_s3_client.upload_part(
                    Body=data,
                    Bucket=bucket_name,
                    Key=object_name,
                    UploadId=mpu_id,
                    PartNumber=part_number
                )
                parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                uploaded_bytes += len(data)
                print(f"{uploaded_bytes} of {total_bytes} bytes uploaded.")
                part_number += 1

        result = aws_s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=object_name,
            UploadId=mpu_id,
            MultipartUpload={"Parts": parts}
        )
        return result
    except ClientError as e:
        logging.error(f"Multipart upload failed: {e}")
        return False


def put_lifecycle_policy(aws_s3_client, bucket_name, prefix="", expiration_days=120):
    """
    Create or update a lifecycle policy to expire objects after expiration_days.

    :param aws_s3_client: boto3 S3 client
    :param bucket_name: Name of the bucket
    :param prefix: Optional object key prefix to filter objects
    :param expiration_days: Number of days before objects expire
    """

    lifecycle_configuration = {
        "Rules": [
            {
                "ID": "devobjects",
                "Filter": {"Prefix": prefix} if prefix else {},
                "Status": "Enabled",
                "Expiration": {"Days": expiration_days}
            }
        ]
    }

    try:
        aws_s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_configuration
        )
        print("Lifecycle policy 'devobjects' applied successfully.")
        return True
    except ClientError as e:
        logging.error(f"Failed to apply lifecycle policy: {e}")
        return False


def delete_object(aws_s3_client, bucket_name, object_key):
    """
    Delete an object from an S3 bucket.

    :param aws_s3_client: boto3 S3 client
    :param bucket_name: The bucket name
    :param object_key: The object (file) key to delete
    """
    try:
        aws_s3_client.delete_object(Bucket=bucket_name, Key=object_key)
        return True
    except ClientError as e:
        logging.error(f"Failed to delete object: {e}")
        return False


def enable_versioning(aws_s3_client, bucket_name):
    """Enable versioning on a bucket."""
    try:
        aws_s3_client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={"Status": "Enabled"}
        )
        print(f"Versioning enabled for bucket '{bucket_name}'.")
        return True
    except ClientError as e:
        logging.error(f"Error enabling versioning: {e}")
        return False


def check_bucket_versioning(aws_s3_client, bucket_name):
    """Check if versioning is enabled on a bucket."""
    try:
        response = aws_s3_client.get_bucket_versioning(Bucket=bucket_name)
        status = response.get("Status", "Disabled")
        print(f"Versioning status for bucket '{bucket_name}': {status}")
        return status
    except ClientError as e:
        logging.error(f"Error checking versioning status: {e}")
        return None


def list_object_versions(aws_s3_client, bucket_name, object_key):
    """List all versions of an object."""
    try:
        response = aws_s3_client.list_object_versions(Bucket=bucket_name, Prefix=object_key)
        versions = response.get("Versions", [])
        if versions:
            print(f"Found {len(versions)} versions for '{object_key}':")
            for v in versions:
                print(f" - VersionId: {v['VersionId']}, IsLatest: {v['IsLatest']}, LastModified: {v['LastModified']}")
            return versions
        else:
            print("No versions found.")
            return []
    except ClientError as e:
        logging.error(f"Error listing object versions: {e}")
        return []


def restore_previous_version(aws_s3_client, bucket_name, object_key):
    """Restore the previous version by re-uploading it as the latest version."""
    try:
        response = aws_s3_client.list_object_versions(Bucket=bucket_name, Prefix=object_key)
        versions = response.get("Versions", [])
        if len(versions) < 2:
            print("No previous version to restore.")
            return False

        previous_version = versions[1]  # second latest
        version_id = previous_version["VersionId"]

        obj = aws_s3_client.get_object(Bucket=bucket_name, Key=object_key, VersionId=version_id)
        content = obj["Body"].read()

        aws_s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=content)

        print(f"Restored previous version of '{object_key}' as the latest version.")
        return True

    except ClientError as e:
        logging.error(f"Error during restore: {e}")
        return False


def organize_by_extension(aws_s3_client, bucket_name, prefix=""):
    """
    Organize objects into folders based on file extensions.
    Automatically creates folders like /csv/, /jpg/, etc.
    Prints extension - number of files.
    """
    try:
        response = aws_s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' not in response:
            print("No objects found.")
            return

        counts = {}

        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith("/"):
                continue  # skip folders

            # Extract extension
            extension = key.split(".")[-1].lower() if "." in key else "no_extension"

            # Count
            counts[extension] = counts.get(extension, 0) + 1

            # Move to folder
            filename = key.split("/")[-1]
            new_key = f"{extension}/{filename}"
            aws_s3_client.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': key}, Key=new_key)
            aws_s3_client.delete_object(Bucket=bucket_name, Key=key)
            print(f"Moved {key} -> {new_key}")

        print("\nSummary:")
        for ext, count in counts.items():
            print(f"{ext} - {count}")

    except ClientError as e:
        logging.error(f"Error during organization: {e}")


def smart_upload_file_with_mimetype(s3_client, filename, bucket_name):
    """
    Detect MIME type and upload file to S3 under a folder matching the top-level MIME type.
    Example: image/photo.jpg → image/photo.jpg
    """
    if not os.path.exists(filename):
        logging.error(f"File not found: {filename}")
        return False, "File not found"

    try:
        mime_type = magic.from_file(filename, mime=True)
        folder = mime_type.split("/")[0]
        base_name = os.path.basename(filename)
        s3_key = f"{folder}/{base_name}"

        logging.info(f"Uploading {filename} as {s3_key} with MIME type {mime_type}")

        success = upload_file_put(s3_client, filename, bucket_name, s3_key)
        if success:
            return True, s3_key
        else:
            return False, "Upload failed"

    except Exception as e:
        logging.error(f"smart_upload_file failed: {e}")
        return False, str(e)


def delete_old_versions(s3_client, bucket_name, object_key, months=6):
    """
    Deletes all versions of a specific object that are older than the given number of months.
    """
    try:
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=months * 30)

        versions = s3_client.list_object_versions(Bucket=bucket_name, Prefix=object_key)

        deleted = []
        if "Versions" in versions:
            for version in versions["Versions"]:
                last_modified = version["LastModified"]
                if last_modified < cutoff_date:
                    s3_client.delete_object(
                        Bucket=bucket_name,
                        Key=object_key,
                        VersionId=version["VersionId"]
                    )
                    deleted.append((version["VersionId"], last_modified))

        return deleted

    except Exception as e:
        logging.error(f"Failed to delete old versions for {object_key}: {e}")
        return []


def host_static_html(s3_client, filename, bucket_name):
    """
    Upload index.html, enable public access, and configure static website hosting.
    """
    if not os.path.exists(filename):
        logging.error(f"File not found: {filename}")
        return False, "File not found"

    try:
        # Upload index.html with correct content type
        with open(filename, "rb") as f:
            s3_client.put_object(
                Bucket=bucket_name,
                Key="index.html",
                Body=f,
                ContentType="text/html"
            )
        logging.info("index.html uploaded successfully.")

        # Enable public access using existing method
        create_bucket_policy(s3_client, bucket_name)

        # Enable static website hosting
        s3_client.put_bucket_website(
            Bucket=bucket_name,
            WebsiteConfiguration={
                "IndexDocument": {"Suffix": "index.html"},
                "ErrorDocument": {"Key": "error.html"}
            }
        )
        logging.info("Static website hosting enabled.")

        return True

    except ClientError as e:
        logging.error(f"AWS error: {e}")
        return False, str(e)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return False, str(e)


def host_static_site_from_folder(s3_client, source_folder: str, bucket_name: str):
    import mimetypes
    from pathlib import Path
    """
    Upload all files from a local folder to an S3 bucket, configure it for static hosting.
    """
    folder_path = Path(source_folder)
    if not folder_path.exists() or not folder_path.is_dir():
        logging.error(f"Folder '{source_folder}' does not exist or is not a directory.")
        return False, "Source folder not found"

    # Upload files with proper MIME types and ACL
    for file in folder_path.rglob("*"):
        if file.is_file():
            key = str(file.relative_to(folder_path)).replace("\\", "/")
            content_type, _ = mimetypes.guess_type(str(file))
            content_type = content_type or "binary/octet-stream"

            try:
                with open(file, "rb") as f:
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=key,
                        Body=f,
                        ContentType=content_type
                    )
                logging.info(f"Uploaded: {key}")
            except Exception as e:
                logging.error(f"Failed to upload '{key}': {e}")
                return False, str(e)

    # Set bucket policy and website config
    try:
        create_bucket_policy(s3_client, bucket_name)

        s3_client.put_bucket_website(
            Bucket=bucket_name,
            WebsiteConfiguration={
                "IndexDocument": {"Suffix": "index.html"},
                "ErrorDocument": {"Key": "error.html"}
            }
        )

        return True

    except ClientError as e:
        logging.error(f"Website config failed: {e}")
        return False, str(e)


def get_quote(author: str = None):
    """
    Fetch a random quote or one by a specific author from quotable API.
    """
    from urllib.request import Request, urlopen
    from urllib.parse import quote_plus

    base_url = "https://api.quotable.kurokeita.dev/api/quotes/random"
    if author:
        author_encoded = quote_plus(author)
        url = f"{base_url}?author={author_encoded}"
    else:
        url = base_url

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
    }

    try:
        req = Request(url, headers=headers)
        with urlopen(req) as response:
            data = json.loads(response.read().decode())
            return True, data
    except Exception as e:
        return False, str(e)


def save_quote_to_s3(aws_s3_client, bucket_name: str, quote_data: dict):
    """
    Save quote data as a .json file to S3.
    """
    author = quote_data.get("author", {}).get("name", "unknown").replace(" ", "_")
    file_key = f"quotes/{author}.json"

    try:
        aws_s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=json.dumps(quote_data, indent=4),
            ContentType="application/json",
        )
        return True, file_key
    except Exception as e:
        return False, str(e)
