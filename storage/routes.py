import io
import json
import tempfile
from dataclasses import dataclass

import pystac
from quart import current_app, request, Blueprint
from pystac import Catalog
from .util import *

storage_bp = Blueprint('upload', __name__)

S3_URL = "localhost:9000"
s3 = Minio(S3_URL,
           access_key='L104ON1uPWWGqluu1ISn',
           secret_key='ETUD4asgDvwxsfC7gxSrQrYwcg0wvkXN6PPPSa7f',
           secure=False
           )


@storage_bp.post('/upload')
async def process_upload():
    content = await request.get_json(force=True)
    # TODO: body validation
    req = UploadRequest(**content)
    return await generate_upload_url(s3, req)


@storage_bp.post('/upload_success')
async def upload_success():
    event_data = await request.json
    current_app.add_background_task(generate_stac, event_data)
    return ""


async def generate_stac(event_data: dict):
    print(event_data)

    path = event_data["Key"].split("/")
    filename = path[-1]
    fs = S3FS(
        endpoint_url="http://localhost:9000",
        bucket_name="ecco",
        dir_path="/".join(path[1:-1]),
        aws_access_key_id='L104ON1uPWWGqluu1ISn',
        aws_secret_access_key='ETUD4asgDvwxsfC7gxSrQrYwcg0wvkXN6PPPSa7f'
    )

    with rasterio.open(filename, opener=fs.open) as src:
        stac = create_stac_item(src, with_proj=True, with_eo=True, with_raster=True)

        # create Item JSON
        fs.writefile(f"{filename}.stac-catalog.json", io.StringIO(json.dumps(stac.to_dict())), encoding="utf-8")

        # update STAC Catalog
        # TODO: probably cache this thing locally
        catalog_file = "catalog.json"
        catalog = json.loads(fs.readtext(catalog_file))
        catalog["links"].append({
            "rel": "item",
            "href": f"./{filename}.stac-catalog.json",
            "type": "application/json"
        })
        fs.writefile(catalog_file, io.StringIO(json.dumps(catalog)), encoding="utf-8")
