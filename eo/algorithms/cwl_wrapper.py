import importlib
import json
import os
import shutil
from abc import ABC
from typing import Dict

import openeo
import pystac
from openeo.rest.connection import Connection
from pystac import Catalog, ItemCollection


class RunnableAlgorithm(ABC):

    @staticmethod
    def run(conn: Connection, catalog: Catalog, parameters: Dict):
        pass


jobs = []


def get_openeo_connection() -> Connection:
    """
    Connects to openEO and configures it for batch job tracking.
    Expects CDSE_CLIENT_ID and CDSE_CLIENT_SECRET in env.
    """
    cdse_client_id = os.getenv("CDSE_CLIENT_ID")
    cdse_client_secret = os.getenv("CDSE_CLIENT_SECRET")

    if not cdse_client_id or not cdse_client_secret:
        raise ValueError("CDSE_CLIENT_ID and CDSE_CLIENT_SECRET must be set as environment variables.")

    conn = openeo.connect("https://openeofed.dataspace.copernicus.eu/").authenticate_oidc_client_credentials(
        client_id=cdse_client_id,
        client_secret=cdse_client_secret,
        provider_id="CDSE"
    )

    def create_job_logged(*args, **kwargs):
        job = conn.create_job_orig(*args, **kwargs)
        print(f"Tracking execution for job: {job.job_id}")
        jobs.append(job)
        return job

    conn.execute = None  # We prevent sync execution until we have it logged
    conn.create_job_orig = conn.create_job
    conn.create_job = create_job_logged  # log all batch jobs to enable tracking

    return conn


def persist_outputs(catalog: Catalog, output_dir: str, run_name: str) -> None:
    """
    Persists the output catalog and assets to a local directory.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    meta = {
        "run_name": run_name
    }
    collection = ItemCollection(items=catalog.get_items(recursive=True), extra_fields=meta)

    # Copy assets
    for item in catalog.get_items(recursive=True):
        for asset in item.get_assets().values():
            if os.path.exists(asset.href):
                dest_path = os.path.join(output_dir, os.path.basename(asset.href))
                shutil.copy(asset.href, dest_path)
                asset.href = item.id

    # Store collection json
    collection_path = os.path.join(output_dir, "collection.json")
    print(f"Storing STAC collection to {collection_path}")
    with open(collection_path, "w+") as f:
        f.write(json.dumps(collection.to_dict()))


def persist_logs(output_dir: str) -> None:
    """
    Writes logs and costs for all tracked openEO jobs to a file.
    """
    log_file_path = os.path.join(output_dir, "openeo_logs.txt")
    costs = 0
    print(f"Writing logs to {log_file_path}")

    try:
        with open(log_file_path, "w+") as f:
            f.write("--- Job Logs ---\n")
            for job in jobs:
                try:
                    log = job.logs()
                    description = job.describe()

                    f.write(f"--- Logs for job {job.job_id} ---\n")
                    for log_entry in log:
                        f.write(f"  {log_entry['level']}: {log_entry['message']}\n")
                    f.write("--- End of logs ---\n\n")

                    f.write(f"--- Description for job {job.job_id} ---\n")
                    f.write(json.dumps(description, indent=2))
                    f.write("\n--- End of description ---\n\n")

                    if "costs" in description and description["costs"] is not None:
                        costs += description["costs"]
                except Exception as e:
                    f.write(f"Could not persist logs for job {job.job_id}! {e}\n")

            f.write("--- End of Job Logs ---\n")
            f.write(f"Total costs: {costs}\n")
    except Exception as e:
        print(e)


def run_algo(algo: RunnableAlgorithm, conn: Connection, catalog: Catalog, parameters: Dict) -> None:
    """
    Runs the algorithm.
    """
    algo.run(conn, catalog, parameters)


def main() -> None:
    """
    Main execution function.
    """
    # Get config from environment variables
    algorithm_module_name = os.getenv("ALGORITHM_BASE")
    if not algorithm_module_name:
        raise ValueError("ALGORITHM_BASE environment variable must be set.")

    run_name = os.getenv("RUN_NAME", "ecco-cwl-run")
    output_dir = os.getenv("OUTPUT_DIR", "./output")
    os.makedirs(output_dir, exist_ok=True)

    parameters_file = os.getenv("PARAMETERS_FILE")
    with open(parameters_file, 'r') as f:
        parameters = json.load(f)

    print(f"Starting algorithm from: {algorithm_module_name}")
    print(f"Run name: {run_name}")
    print(f"Output directory: {output_dir}")
    print(f"Parameters: {json.dumps(parameters, indent=2)}")

    # Dynamically import the algorithm
    try:
        algo_module = importlib.import_module(algorithm_module_name)
        algo = getattr(algo_module, "Algorithm")
    except (ImportError, AttributeError) as e:
        print(f"Error: Could not import Algorithm class from module {algorithm_module_name}.")
        raise e

    # Get openEO connection
    conn = get_openeo_connection()

    try:
        catalog = pystac.Catalog(id=run_name, description=f"Catalog for run: {run_name}")
        run_algo(algo, conn, catalog, parameters)
        persist_outputs(catalog, output_dir, run_name)
    except Exception as e:
        print(f"An error occurred during algorithm execution: {e}")
        # Potentially re-raise or handle as needed
    finally:
        print("Persisting logs...")
        persist_logs(output_dir)
        print("Finished.")


if __name__ == "__main__":
    main()
