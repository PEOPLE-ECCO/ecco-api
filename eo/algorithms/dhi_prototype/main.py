import tempfile
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Dict

import matplotlib.pyplot as plt
import rioxarray as rio
from openeo.rest.connection import Connection
from osgeo import gdal

from .utils import *


class Algorithm:

    @staticmethod
    def run(conn: Connection, output_dir: Path, parameters: Dict) -> None:
        """
        Entrypoint for all runnable Algorithms.

        :param conn: openEO-Connection, already pre-authenticated
        :param output_dir: Directory for persisting outputs. All files in this directory will be persisted
        :param parameters: User-Supplied parameters.
        :return: None
        """
        os.chdir(Path(__file__).resolve().parent)

        with tempfile.TemporaryDirectory() as work_dir:

            spatial_extent = {
                "west": -63.9068039,
                "south": 46.430161,
                "east": -63.6851986,
                "north": 46.6969723
            }
            aoi_name = 'north_aoi'

            # In[4]:
            with open('rf_model.pkl', 'rb') as f:
                rf_model = pickle.load(f)

            # SAV mapping for single image per year
            run_jobs = False
            input_path = f'{work_dir}/north_aoi2/openeo_s2'
            if not os.path.exists(input_path):
                os.makedirs(input_path, exist_ok=True)
            jobs = []
            for year in range(2017, 2025):
                outputfile = f'{work_dir}/people-ecco/PEI/openeo_s2/{aoi_name}/s2_openeo_{year}.tif'
                if run_jobs:
                    temporal_extent = [f"{year}-08-01", f"{year}-09-30"]
                    bands = ["B02", "B03", "B04", "B08"]

                    s2_data = conn.load_collection(
                        "SENTINEL2_L2A",
                        spatial_extent=spatial_extent,
                        temporal_extent=temporal_extent,
                        bands=bands,
                        max_cloud_cover=5
                    )
                    cloudfree = s2_data.reduce_dimension(dimension="t", reducer="first")
                    job = cloudfree.execute_batch(outputfile=outputfile)
                    jobs.append(job)

            # In[6]:

            gdal.PushErrorHandler('CPLQuietErrorHandler')
            data = []
            for year in range(2017, 2025):
                outputfile = f'{work_dir}/people-ecco/PEI/openeo_s2/{aoi_name}/s2_openeo_{year}.tif'
                ds = xr.open_dataset(outputfile)
                da = ds.assign_coords(band=["scl", 'B02', 'B03', 'B04', 'B08'])[['band_data']]
                da = da.sel(band=['B02', 'B03', 'B04', 'B08'])
                da = da.rio.write_crs(ds.spatial_ref.crs_wkt)
                data.append(da)

            # In[2]:

            # # SAV mapping for median image per year
            # run_jobs = False
            # input_path = f'/home/jovyan/exchange/people-ecco/PEI/data_v6/north_aoi2/median_s2' # output_dir
            # jobs = []
            # data = []
            # # for year in range(2017, 2025):
            # for year in range(2017, 2025):
            #     # outputfile=f'median_preds/north_aoi_{year}.tif'
            #     outputfile = os.path.join(input_path, f's2_openeo_{year}.tif')
            #     if run_jobs:
            #         temporal_extent = [f"{year}-07-15", f"{year}-09-30"]
            #         bands=["B02", "B03", "B04", "B08"]

            #         # Load S2 data
            #         s2_data = conn.load_collection(
            #             "SENTINEL2_L2A",
            #             spatial_extent=spatial_extent,
            #             temporal_extent=temporal_extent,
            #             bands=bands,
            #             max_cloud_cover=20
            #         )

            #         # Get median
            #         median = s2_data.reduce_dimension(dimension="t",reducer="median")
            #         median = median.apply(lambda x: x+1000) # add 1000 to match training data
            #         # Download data
            #         job = median.execute_batch(outputfile=outputfile)
            #         jobs.append(job)

            #     # Load data add 1000 and save it as geotiff
            #     ds = xr.open_dataset(outputfile)
            #     da = ds[['B02', 'B03', 'B04', 'B08']].to_array()
            #     da = da.rename({"variable": "band"})
            #     # da = da+1000 # add 1000 to match training data
            #     da = da.rio.write_crs(ds.crs.crs_wkt )
            #     da.rio.to_raster(os.path.join(work_dir, 'north_aoi2/median_s2' ,f's2_openeo_{year}.tif'))
            #     data.append(da)

            # In[4]:

            # Get preds for single year
            # if work_dir.stem ==  'custom_scales':
            #     scales = [1000., 1200., 2000., 3000.]
            # else:
            #     scales = [1500., 1500., 1500., 1500.]
            # base_path = os.path.join(work_dir, 'preds_openeo')
            # prob, sav_prob, pred = process_time_slice(da=da, year=2024, rf_model=rf_model_v6, compute_stats=compute_stats, scales=scales,
            #                                           add_indices=add_indices, extract_features=extract_features, base_path=base_path)

            # In[8]:

            if work_dir.stem == 'custom_scales':
                scales = [1000., 1200., 2000., 3000.]
            else:
                scales = [1500., 1500., 1500., 1500.]
            base_path = os.path.join(work_dir, 'preds_openeo')

            def run_for_year(year_and_da):
                year, da = year_and_da
                return process_time_slice(
                    year=year,
                    da=da,
                    rf_model=rf_model,
                    compute_stats=compute_stats,
                    scales=scales,
                    add_indices=add_indices,
                    extract_features=extract_features,
                    base_path=base_path
                )

            years = list(range(2017, 2025))
            year_and_da_list = list(zip(years, data))

            probs_all, sav_probs_all, preds_all = [], [], []

            with ProcessPoolExecutor() as executor:
                results = list(tqdm(executor.map(run_for_year, year_and_da_list), total=len(year_and_da_list)))

            for prob, sav_prob, pred in results:
                probs_all.append(prob)
                sav_probs_all.append(sav_prob)
                preds_all.append(pred)

            # In[236]:

            i = 7

            strdate = str(da[:, i].t.values).split('T')[0]
            rgb_ds = (((da[:, i].sel(band=["B04", "B03", "B02"]) - 1000) / 2000).clip(0, 1) * 255).astype('uint8')
            rgb_xr = rgb_ds.transpose("y", "x", "band")

            # probs = freq_ds.transpose("y", "x")
            #
            # fig, axes = plt.subplots(1, 2, figsize=(12, 6))
            # rgb_xr_plot = rgb_xr.transpose("y", "x", "band").values
            # axes[0].imshow(rgb_xr_plot)
            # axes[0].set_title(f"RGB-{strdate}")
            # axes[0].axis("off")
            # probs.plot.imshow(ax=axes[1], )
            # axes[1].set_title("SAV multi-year frequency")
            # axes[1].axis("off")
            # plt.tight_layout()
            # # plt.suptitle(strdate, fontsize=16, y=1.03)
            # plt.show()
            #

            # In[238]:

            i = 7

            strdate = str(da[:, i].t.values).split('T')[0]
            rgb_ds = (((da[:, i].sel(band=["B04", "B03", "B02"]) - 1000) / 2000).clip(0, 1) * 255).astype('uint8')
            rgb_xr = rgb_ds.transpose("y", "x", "band")

            probs = (confident_sav > 5000).transpose("y", "x")

            fig, axes = plt.subplots(1, 2, figsize=(12, 6))
            rgb_xr_plot = rgb_xr.transpose("y", "x", "band").values
            axes[0].imshow(rgb_xr_plot)
            axes[0].set_title(f"RGB-{strdate}")
            axes[0].axis("off")
            probs.plot.imshow(ax=axes[1], cmap='gray', add_colorbar=False)
            axes[1].set_title("SAV aggragated multi-year predictions")
            axes[1].axis("off")

            # In[ ]:

            paths = list(Path(out_path).glob('preds_openeo_20*.tif'))
            arrays = [rio.open_rasterio(p).squeeze("band", drop=True) for p in paths]

            aligned = xr.align(*arrays, join="exact")
            stacked = xr.concat(aligned, dim="stack")

            counts = (stacked == 2).sum(dim="stack")

            counts.rio.write_crs("EPSG:32620", inplace=True)

            frequency = counts
            with rasterio.open(
                    os.path.join(out_path, "frequency.tif"), 'w', driver='GTiff',
                    height=counts.shape[0], width=counts.shape[1],
                    count=1, dtype='int16', crs='EPSG:32620', transform=transform
            ) as dst:
                dst.write(counts.values, 1)

            with rasterio.open(
                    os.path.join(out_path, "frequency.tif"), 'w', driver='GTiff',
                    height=counts.shape[0], width=counts.shape[1],
                    count=1, dtype='int16', crs='EPSG:32620', transform=transform
            ) as dst:
                dst.write(counts.values, 1)
