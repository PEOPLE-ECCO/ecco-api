import json
from dataclasses import dataclass
from datetime import timedelta

from minio import Minio
from quart import current_app


@dataclass(frozen=True)
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


async def generate_stac(event_data: dict):
    print(event_data)

    path = event_data["Key"].split("/")
    filename = path[-1]

    # with rasterio.open(filename, opener=fs.open) as src:
    #     stac = create_stac_item(src, with_proj=True, with_eo=True, with_raster=True)
    #
    #     # create Item JSON
    #     fs.writefile(f"{filename}.stac-catalog.json", io.StringIO(json.dumps(stac.to_dict())), encoding="utf-8")
    #
    #     # update STAC Catalog
    #     # TODO: probably cache this thing locally
    #     catalog_file = "catalog.json"
    #     catalog = json.loads(fs.readtext(catalog_file))
    #     catalog["links"].append({
    #         "rel": "item",
    #         "href": f"./stac/{filename}.stac-catalog.json",
    #         "type": "application/json"
    #     })
    #     fs.writefile(catalog_file, io.StringIO(json.dumps(catalog)), encoding="utf-8")


async def get_object(bucket: str, item: str):
    response = None
    s3 = current_app.config["S3"]
    try:
        response = s3.get_object(
            bucket,
            item
        )
        return json.loads(response.data)
    finally:
        if response:
            response.release_conn()
