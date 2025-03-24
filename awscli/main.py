import typer
import awscli.aws_s3 as s3  # simplified alias for convenience

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


if __name__ == "__main__":
    app()
