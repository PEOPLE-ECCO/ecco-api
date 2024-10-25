from dataclasses import dataclass
from datetime import timedelta

import rasterio
from minio import Minio
from rio_stac import create_stac_item
from fs_s3fs import S3FS

@dataclass
class UploadRequest:
    file_name: str
    file_type: str
    bucket: str = "ecco"


async def generate_upload_url(s3: Minio, req: UploadRequest):
    # TODO: make file_name unique
    return {
        'data': s3.presigned_put_object(
            req.bucket,
            req.file_name,
            expires=timedelta(hours=1)
        )
    }


async def create_bucket():
    print("TODO: create bucket")



