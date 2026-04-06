import argparse
import os
import sys

try:
    from databricks.sdk import WorkspaceClient
except ImportError:
    WorkspaceClient = None


def upload_to_volume(local_file_path: str, volume_file_path: str, overwrite: bool = True) -> None:
    if WorkspaceClient is None:
        raise RuntimeError(
            "databricks-sdk is required. Install it with: pip install databricks-sdk"
        )

    if not os.path.isfile(local_file_path):
        raise FileNotFoundError(f"Local file not found: {local_file_path}")

    if not volume_file_path.startswith("/Volumes/"):
        raise ValueError(
            "Volume path must start with /Volumes/. Example: /Volumes/main/default/my_volume/data.json"
        )

    client = WorkspaceClient()
    with open(local_file_path, "rb") as contents:
        client.files.upload(
            file_path=volume_file_path,
            contents=contents,
            overwrite=overwrite,
        )

    print(f"Uploaded local file {local_file_path} to volume path {volume_file_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Upload a local file to a Databricks Unity Catalog volume."
    )
    parser.add_argument("local_file", help="Path to the local file to upload")
    parser.add_argument(
        "volume_file",
        help="Target volume file path, e.g. /Volumes/main/default/my_volume/data.json",
    )
    parser.add_argument(
        "--no-overwrite",
        dest="overwrite",
        action="store_false",
        help="Do not overwrite the target file if it already exists",
    )

    args = parser.parse_args()

    try:
        upload_to_volume(args.local_file, args.volume_file, overwrite=args.overwrite)
    except Exception as exc:
        print(f"Error uploading file: {exc}", file=sys.stderr)
        sys.exit(1)
