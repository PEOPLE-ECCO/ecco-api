from quart import request, Blueprint, redirect

from .util import *

stac_bp = Blueprint('stac', __name__)

# s3fs = S3FS(
#         endpoint_url="http://localhost:9000",
#         bucket_name="ecco",
#         dir_path="/".join(path[1:-1]),
#         aws_access_key_id='L104ON1uPWWGqluu1ISn',
#         aws_secret_access_key='ETUD4asgDvwxsfC7gxSrQrYwcg0wvkXN6PPPSa7f'
#     )

# TODO: get bucket of user dynamically

# We only handle metadata requests here
# Raw observation-data is directly streamed from S3
# @stac_bp.get('/stac')
# async def stac_root():
#    return await get_object(bucket, "catalog.json")


@stac_bp.get('/stac/<path:item>')
async def stac_item(item: str):
    bucket = "ecco"
    obj = await get_object(bucket, item)
    if "assets" in obj:
        s3 = current_app.config["S3"]
        presigned_get_raw_data = s3.presigned_get_object(
            bucket,
            obj["assets"]["asset"]["href"][37:],
            expires=timedelta(minutes=5)
        )
        obj["assets"]["asset"]["href"] = presigned_get_raw_data
        return obj
    
    if "links" in obj:
        obj["links"] = [o for o in obj["links"] if o["rel"] not in ["item", "canonical", "self"]]
    return obj


@stac_bp.get('/stac/data/<path:path>')
async def stac_item_rawdata(path):
    bucket = "ecco"
    s3 = current_app.config["S3"]
    presigned_get_raw_data = s3.presigned_get_object(
        bucket,
        path,
        expires=timedelta(minutes=5)
    )

    return redirect(presigned_get_raw_data, 303)


@stac_bp.post('/stac/upload')
async def process_upload():
    content = await request.get_json(force=True)
    # TODO: body validation
    req = UploadRequest(**content)
    s3 = current_app.config["S3"]
    return await generate_upload_url(s3, req)


@stac_bp.post('/stac/upload_success')
async def upload_success():
    event_data = await request.json
    current_app.add_background_task(generate_stac, event_data)
    return ""
