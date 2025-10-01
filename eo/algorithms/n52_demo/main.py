import os
from pathlib import Path
from typing import Dict

from openeo.rest.connection import Connection


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

        for year in range(parameters["rangestart"], parameters["rangeend"]):
            try:
                print(f"running job for {year}")
                outputfile = f'{output_dir}/s2_openeo_{year}.tif'
                temporal_extent = [f"{year}-08-01", f"{year}-09-30"]
                bands = ["B02", "B03", "B04", "B08"]

                s2_data = conn.load_collection(
                    "SENTINEL2_L2A",
                    spatial_extent=parameters["spatial_extent"],
                    temporal_extent=temporal_extent,
                    bands=bands,
                    max_cloud_cover=5
                )
                cloudfree = s2_data.reduce_dimension(dimension="t", reducer="first")
                # Process handles output-file creation itself.
                cloudfree.execute_batch(outputfile=outputfile)
            except Exception as e:
                # TODO: improve error logging
                print(e)
