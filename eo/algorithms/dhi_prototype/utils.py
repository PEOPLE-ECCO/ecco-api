from skimage import segmentation, feature, future
from sklearn.ensemble import RandomForestClassifier
from functools import partial
from openeo.udf import XarrayDataCube
import numpy as np
import pandas as pd
from openeo.udf.debug import inspect
import dask
import xarray as xr
import numpy as np
import pandas as pd
import os
import pickle
from rasterio.transform import from_origin
import rasterio
from tqdm import tqdm
import rioxarray 


def compute_stats(arr, window):
    rolled = arr.rolling(y=window, x=window, center=True)
    return xr.concat([
        rolled.mean().fillna(0),
        # rolled.std().fillna(0).astype('float16'),
        rolled.reduce(np.quantile, q=0.50).fillna(0),
        rolled.max().fillna(0),
        rolled.min().fillna(0),
        rolled.reduce(np.quantile, q=0.25).fillna(0),
        rolled.reduce(np.quantile, q=0.75).fillna(0)
    ], dim='stat').assign_coords(stat=[
        'mean', 'median', 'max', 'min', 'q25', 'q75'
    ])

def add_indices(data):
    blue, green, red, nir = data.sel(band='B02'), data.sel(band='B03'), data.sel(band='B04'), data.sel(band='B08')

    eps = 1e-6 

    # rb_ratio = red / (blue + eps)
    # rg_ratio = red / (green + eps)
    # bg_ratio = green / (blue + eps)
    rdvi = (nir - red) / np.sqrt(nir + red + eps)
    msr = ((nir / (red + eps)) - 1) / (np.sqrt((nir / (red + eps)) + 1) + eps)
    osavi = 1.16 * (nir - red) / (nir + red + 0.16 + eps)
    chlrededge = (nir / (green + eps)) - 1
    orig_band_names = ['red', 'green', 'blue', 'nir']
    mask = (red == 0) | (green == 0) | (blue == 0) | (nir == 0)

    orig_data = [
        data.sel(band=bname).expand_dims(band=[name])
        for bname, name in zip(['B02','B03','B04','B08'],orig_band_names)
    ]

    index_bands = {
        # 'rb_ratio':rb_ratio,
        # 'rg_ratio':rg_ratio,
        # 'bg_ratio':bg_ratio,
        'rdvi': rdvi,
        'msr': msr,
        'osavi': osavi,
        'chlrededge': chlrededge,
    }

    index_data = [
        v.expand_dims(band=[name])
        for name, v in index_bands.items()
    ]
    
    all_data = xr.concat(orig_data + index_data, dim='band')
    all_data = all_data.where(~mask, 0)
    return all_data



def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    inputarray = cube.get_array()

    scl = inputarray.sel(bands=['SCL'])
    cloud_mask = (scl == 2) | (scl == 4) | (scl == 5) | (scl == 6) | (scl == 11)
    scl = scl.where(cloud_mask)

    counts = sorted(
        zip(
            range(inputarray.t.shape[0]),
            scl.count(dim=["x", "y", "bands"]).values.flatten(),
        ),
        key=lambda i: i[1],
        reverse=True
    )

    maxlayers = 3
    pixel_count = scl.sizes.get("x", 0) * scl.sizes.get("y", 0)
    selected_indices = [
        counts[i][0] for i in range(maxlayers)
        if i == 0 or (counts[i][1] / pixel_count > 0.8)
    ]

    resultarray = inputarray.isel(t=selected_indices).sortby("t", ascending=True)
    return XarrayDataCube(resultarray)


def extract_features(image):
    return feature.multiscale_basic_features(
        image,
        intensity=True,
        edges=True,
        texture=True,
        sigma_min=4,
        sigma_max=32,
        channel_axis=0
    )
    

def process_time_slice(year, da, rf_model, scales, base_path, crs=32620):

    da = da['band_data']
    scales = xr.DataArray(scales, dims=["band"])
    da = da / scales
    da = (da.clip(0, 1)*10000).astype('int16')
    
    stats_ds = compute_stats(add_indices(da), 5)
    new_band_names = [f"{b}_{s}" for b in stats_ds.band.values for s in stats_ds.stat.values]
    da_merged = stats_ds.stack(band_stat=('band', 'stat')).transpose('band_stat', 'y', 'x')
    stats_ds = da_merged.drop_vars(['band_stat', 'band', 'stat']).assign_coords(band_stat=new_band_names)
    stats_ds = stats_ds.rename({'band_stat': 'band'})

    conv_ds = xr.apply_ufunc(
        extract_features,
        da,
        input_core_dims=[["band", "y", "x"]],
        output_core_dims=[["y", "x", "feature"]],
        vectorize=True,
        dask="parallelized",
        output_dtypes=[float]
    )
    conv_ds = conv_ds.rename({'feature': 'band'})
    band_names = [f'conv_{i}' for i in range(64)]
    conv_ds = conv_ds.assign_coords(band=("band", band_names))

    max_nir_ds = da[[-1]].rolling(y=150, x=150, center=True).max()
    max_nir_ds = max_nir_ds.assign_coords(band=("band", ['nir_max_150w']))

    s2_data = da.copy()
    s2_data = s2_data.assign_coords(band=("band", ['blue', 'green', 'red', 'nir']))
    all_ds = xr.concat([s2_data, stats_ds, conv_ds, max_nir_ds], dim="band")
    all_ds = all_ds.stack(pixel=("y", "x")).transpose("pixel", "band")

    full_df = pd.DataFrame(all_ds, columns=[band for band in all_ds.band.values], index=all_ds.pixel).reset_index()
    full_df.insert(0, 'y', full_df['index'].map(lambda x: x[0]))
    full_df.insert(0, 'x', full_df['index'].map(lambda x: x[1]))
    full_df.drop(columns='index', inplace=True)

    X_test = full_df.loc[:, list(rf_model.feature_names_in_)]
    X_test.columns = rf_model.feature_names_in_.tolist()
    probs = rf_model.predict_proba(X_test)
    sav_probs = probs[:, 1]
    preds = np.argmax(probs, axis=1)
    probs = np.max(probs, axis=1)

    preds_df = full_df[['x', 'y']].copy()
    preds_df['prob'] = probs
    preds_df['pred'] = preds + 1
    preds_df.loc[full_df['nir_median'] > 2000, 'pred'] = 0
    preds_df.loc[full_df['nir_median'] > 2000, 'prob'] = 1
    preds_df['sav_prob'] = sav_probs
    preds_df.loc[full_df['nir_median'] > 2000, 'sav_prob'] = 0

    unique_x = np.unique(preds_df.x.values)
    unique_y = np.unique(preds_df.y.values)

    preds_df = preds_df.sort_values(by=['y', 'x'], ascending=[False, True])
    pred_values = preds_df.pivot(index='y', columns='x', values='pred').values
    prob_values = preds_df.pivot(index='y', columns='x', values='prob').values
    sav_prob_values = preds_df.pivot(index='y', columns='x', values='sav_prob').values

    ds_preds = xr.DataArray(pred_values, coords=[unique_y, unique_x], dims=["y", "x"])
    ds_probs = xr.DataArray(prob_values * 10000, coords=[unique_y, unique_x], dims=["y", "x"]).astype('int16')
    ds_sav_probs = xr.DataArray(sav_prob_values * 10000, coords=[unique_y, unique_x], dims=["y", "x"]).astype('int16')

    transform = from_origin(unique_x.min(), unique_y.min(), unique_x[1] - unique_x[0], unique_y[0] - unique_y[1])
    
    os.makedirs(base_path, exist_ok=True)

    preds_tif = os.path.join(base_path, f'{year}_preds_openeo.tif')
    probs_tif = os.path.join(base_path, f'{year}_prob_openeo.tif')
    sav_probs_tif = os.path.join(base_path, f'{year}_sav_prob_openeo.tif')

    for path, arr in zip([preds_tif, sav_probs_tif, probs_tif], [ds_preds, ds_sav_probs, ds_probs]):
        with rasterio.open(
            path, 'w', driver='GTiff',
            height=arr.shape[0], width=arr.shape[1],
            count=1, dtype='int16', crs=f'EPSG:{crs}', transform=transform
        ) as dst:
            dst.write(arr.values, 1)

    return ds_probs, ds_sav_probs, ds_preds


