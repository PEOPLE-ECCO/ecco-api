# /// script
# dependencies = [
#   "xarray==2023.1.0"
# ]
# ///

import xarray

def apply_datacube(cube: xarray.DataArray, context: dict) -> xarray.DataArray:
    cube.values *= 0.1
    return cube