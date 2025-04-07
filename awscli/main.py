import typer
import awscli.aws_s3 as s3

app = typer.Typer()


@app.command()
def test_client(env_path: str = typer.Option(".env", help="Path to .env file")):
    client = s3.init_client(env_path)
    if client:
        typer.echo("AWS S3 Client initialized successfully.")
    else:
        typer.echo("Failed to initialize AWS S3 Client.")


@app.command()
def list_s3buckets(env_path: str = typer.Option(".env", help="Path to .env file")):
    client = s3.init_client(env_path)
    if client:
        buckets = s3.list_buckets(client)
        if buckets:
            typer.echo("Buckets available in S3:")
            for bucket in buckets:
                typer.echo(f" - {bucket}")
        else:
            typer.echo("No buckets found or an error occurred.")
    else:
        typer.echo("Failed to initialize AWS S3 Client.")


@app.command()
def create_s3bucket(
    bucket_name: str = typer.Argument(..., help="Name of the bucket to create"),
    region: str = typer.Option("us-west-2", help="AWS region"),
    env_path: str = typer.Option(".env", help="Path to .env file")
):
    client = s3.init_client(env_path)
    if client:
        if s3.create_bucket(client, bucket_name, region):
            typer.echo(f"Bucket '{bucket_name}' created successfully in '{region}'.")
        else:
            typer.echo(f"Failed to create bucket '{bucket_name}'. Check logs for details.")
    else:
        typer.echo("Failed to initialize AWS S3 Client.")


@app.command()
def delete_s3bucket(
    bucket_name: str = typer.Argument(..., help="Name of the bucket to delete"),
    env_path: str = typer.Option(".env", help="Path to .env file")
):
    client = s3.init_client(env_path)
    if client:
        if s3.delete_bucket(client, bucket_name):
            typer.echo(f"Bucket '{bucket_name}' deleted successfully.")
        else:
            typer.echo(f"Failed to delete bucket '{bucket_name}'. Check logs for details.")
    else:
        typer.echo("Failed to initialize AWS S3 Client.")


@app.command()
def bucket_exists(
    bucket_name: str = typer.Argument(..., help="Name of the bucket to check"),
    env_path: str = typer.Option(".env", help="Path to .env file")
):
    client = s3.init_client(env_path)
    if client:
        exists = s3.bucket_exists(client, bucket_name)
        typer.echo(f"Bucket '{bucket_name}' exists: {exists}")
    else:
        typer.echo("Failed to initialize AWS S3 Client.")


@app.command()
def create_bucket_policy(
    bucket_name: str = typer.Argument(..., help="Name of the bucket to set public read policy"),
    env_path: str = typer.Option(".env", help="Path to .env file")
):
    client = s3.init_client(env_path)
    if client:
        try:
            s3.create_bucket_policy(client, bucket_name)
            typer.echo("Bucket policy created successfully.")
        except Exception as e:
            typer.echo(f"Failed to create bucket policy: {e}")
    else:
        typer.echo("Failed to initialize AWS S3 Client.")


@app.command()
def read_bucket_policy(
    bucket_name: str = typer.Argument(..., help="Name of the bucket whose policy you want to read"),
    env_path: str = typer.Option(".env", help="Path to .env file")
):
    client = s3.init_client(env_path)
    if client:
        result = s3.read_bucket_policy(client, bucket_name)
        if not result:
            typer.echo(f"Failed to read bucket policy for '{bucket_name}'.")
    else:
        typer.echo("Failed to initialize AWS S3 Client.")


@app.command()
def set_object_access_policy(
    bucket_name: str = typer.Argument(..., help="Name of the bucket containing the file"),
    file_name: str = typer.Argument(..., help="File name (Key) in the bucket"),
    env_path: str = typer.Option(".env", help="Path to .env file")
):
    client = s3.init_client(env_path)
    if client:
        success = s3.set_object_access_policy(client, bucket_name, file_name)
        if success:
            typer.echo(f"Access policy set to public-read for object '{file_name}' in bucket '{bucket_name}'.")
        else:
            typer.echo(f"Failed to set object access policy for '{file_name}'.")
    else:
        typer.echo("Failed to initialize AWS S3 Client.")


@app.command()
def download_and_upload(
    url: str = typer.Argument(..., help="URL of the file to download"),
    bucket_name: str = typer.Argument(..., help="S3 bucket name where the file will be uploaded"),
    file_name: str = typer.Argument(..., help="Destination file name in S3"),
    keep_local: bool = typer.Option(False, help="Whether to save the downloaded file locally"),
    env_path: str = typer.Option(".env", help="Path to .env file")
):
    client = s3.init_client(env_path)
    if client:
        result = s3.download_file_and_upload_to_s3(client, bucket_name, url, file_name, keep_local)
        if result:
            typer.echo(f"File successfully uploaded to: {result}")
        else:
            typer.echo("Download or upload failed. Check logs for details.")
    else:
        typer.echo("Failed to initialize AWS S3 Client.")


@app.command()
def upload_file(
    filename: str = typer.Argument(..., help="Local file path to upload"),
    bucket_name: str = typer.Argument(..., help="Target S3 bucket name"),
    object_name: str = typer.Argument(..., help="S3 object name (key)"),
    env_path: str = typer.Option(".env", help="Path to .env file")
):
    client = s3.init_client(env_path)
    if client:
        if s3.upload_file(client, filename, bucket_name, object_name):
            typer.echo("File uploaded successfully using upload_file()")
        else:
            typer.echo("Upload failed.")


@app.command()
def upload_file_obj(
    filename: str = typer.Argument(..., help="Local file path to upload"),
    bucket_name: str = typer.Argument(..., help="Target S3 bucket name"),
    object_name: str = typer.Argument(..., help="S3 object name (key)"),
    env_path: str = typer.Option(".env", help="Path to .env file")
):
    client = s3.init_client(env_path)
    if client:
        if s3.upload_file_obj(client, filename, bucket_name, object_name):
            typer.echo("File uploaded successfully using upload_file_obj()")
        else:
            typer.echo("Upload failed.")


@app.command()
def upload_file_put(
    filename: str = typer.Argument(..., help="Local file path to upload"),
    bucket_name: str = typer.Argument(..., help="Target S3 bucket name"),
    object_name: str = typer.Argument(..., help="S3 object name (key)"),
    env_path: str = typer.Option(".env", help="Path to .env file")
):
    client = s3.init_client(env_path)
    if client:
        if s3.upload_file_put(client, filename, bucket_name, object_name):
            typer.echo("File uploaded successfully using upload_file_put()")
        else:
            typer.echo("Upload failed.")

@app.command()
def multipart_upload(
    filename: str = typer.Argument(..., help="Local file to upload"),
    bucket_name: str = typer.Argument(..., help="Target S3 bucket name"),
    object_name: str = typer.Argument(..., help="S3 object name (key)"),
    part_size_mb: int = typer.Option(5, help="Part size in MB (default = 5MB)"),
    env_path: str = typer.Option(".env", help="Path to .env file")
):
    """Perform multipart upload to S3"""
    client = s3.init_client(env_path)
    if client:
        part_size_bytes = part_size_mb * 1024 * 1024
        result = s3.multipart_upload(client, bucket_name, filename, object_name, part_size_bytes)
        if result:
            typer.echo("Multipart upload completed successfully.")
            typer.echo(result)
        else:
            typer.echo("Multipart upload failed. Check logs for details.")
    else:
        typer.echo("Failed to initialize AWS S3 Client.")


@app.command()
def put_lifecycle_policy(
    bucket_name: str = typer.Argument(..., help="Bucket name"),
    expiration_days: int = typer.Option(120, help="Expiration period in days"),
    prefix: str = typer.Option("", help="Prefix to limit objects (optional)"),
    env_path: str = typer.Option(".env", help="Path to .env file")
):
    """Apply a lifecycle policy to automatically delete objects after expiration_days."""
    client = s3.init_client(env_path)
    if client:
        if s3.put_lifecycle_policy(client, bucket_name, prefix, expiration_days):
            typer.echo("Lifecycle policy applied successfully.")
        else:
            typer.echo("Failed to apply lifecycle policy.")
    else:
        typer.echo("Failed to initialize AWS S3 client.")


@app.command()
def delete_object(
    bucket_name: str = typer.Argument(..., help="Name of the bucket"),
    object_key: str = typer.Argument(..., help="Key of the object (file) to delete"),
    env_path: str = typer.Option(".env", help="Path to .env file")
):
    """Delete an object from an S3 bucket."""
    client = s3.init_client(env_path)
    if client:
        if s3.delete_object(client, bucket_name, object_key):
            typer.echo(f"Object '{object_key}' deleted successfully.")
        else:
            typer.echo("Failed to delete object. Check logs.")
    else:
        typer.echo("Failed to initialize AWS S3 client.")


@app.command()
def enable_versioning(
    bucket_name: str = typer.Argument(..., help="Bucket name"),
    env_path: str = typer.Option(".env", help="Path to .env file")
):
    """Enable versioning on a bucket."""
    client = s3.init_client(env_path)
    if client:
        s3.enable_versioning(client, bucket_name)


@app.command()
def check_versioning(
    bucket_name: str = typer.Argument(..., help="Bucket name"),
    env_path: str = typer.Option(".env", help="Path to .env file")
):
    """Check if versioning is enabled on a bucket."""
    client = s3.init_client(env_path)
    if client:
        s3.check_bucket_versioning(client, bucket_name)


@app.command()
def list_versions(
    bucket_name: str = typer.Argument(..., help="Bucket name"),
    object_key: str = typer.Argument(..., help="Object key"),
    env_path: str = typer.Option(".env", help="Path to .env file")
):
    """List all versions of an object."""
    client = s3.init_client(env_path)
    if client:
        s3.list_object_versions(client, bucket_name, object_key)


@app.command()
def restore_previous_version(
    bucket_name: str = typer.Argument(..., help="Bucket name"),
    object_key: str = typer.Argument(..., help="Object key"),
    env_path: str = typer.Option(".env", help="Path to .env file")
):
    """Restore the previous version of an object as the latest version."""
    client = s3.init_client(env_path)
    if client:
        s3.restore_previous_version(client, bucket_name, object_key)


@app.command()
def organize_files(
    bucket_name: str = typer.Argument(..., help="Bucket name to organize"),
    prefix: str = typer.Option("", help="Prefix for filtering files (optional)"),
    env_path: str = typer.Option(".env", help="Path to .env file")
):
    """Organize files by extension into folders and show grouped counts."""
    client = s3.init_client(env_path)
    if client:
        s3.organize_by_extension(client, bucket_name, prefix)


@app.command()
def smart_upload(
    filename: str = typer.Argument(..., help="Local file to upload"),
    bucket_name: str = typer.Argument(..., help="S3 bucket name"),
    env_path: str = typer.Option(".env", help="Path to .env file")
):
    """
    Uploads a file to an S3 bucket into a folder based on its MIME type (e.g., image/, text/, etc.)
    """
    client = s3.init_client(env_path)
    if not client:
        typer.echo("Failed to initialize AWS client.")
        raise typer.Exit(code=1)

    success, result = s3.smart_upload_file_with_mimetype(client, filename, bucket_name)

    if success:
        typer.echo(f"File uploaded successfully to '{result}'")
    else:
        typer.echo(f"Upload failed: {result}")


if __name__ == "__main__":
    app()
