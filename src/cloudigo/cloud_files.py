from io import BytesIO
from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd
from cloudfiles import CloudFiles


def _path_to_cloudfiles(path: Union[str, Path]):
    """
    Convert a path to a cloudfiles path.
    """
    if isinstance(path, str):
        path = Path(path)
    file_name = str(path.name)
    base_path = path.parent
    base_path = str(base_path)
    base_path = base_path.replace(":/", "://")
    cf = CloudFiles(base_path)
    return cf, file_name


def exists(path: Union[str, Path]):
    """
    Check if a file exists in the cloud.
    """
    if isinstance(path, str):
        path = Path(path)
    cf, file_name = _path_to_cloudfiles(path)
    return cf.exists(file_name)


def put_dataframe(df, path, compression="gzip"):
    path = Path(path)
    folder = path.parent
    filename = str(path.name)
    folder = str(folder)
    folder = folder.replace(":/", "://")
    cf = CloudFiles(folder)
    with BytesIO() as f:
        df.to_csv(f, index=True, compression=compression)
        cf.put(filename, f)


def get_dataframe(path, compression="gzip", **kwargs):
    path = Path(path)
    filename = str(path.name)
    folder = path.parent
    folder = str(folder)
    folder = folder.replace(":/", "://")
    cf = CloudFiles(folder)
    bytes_out = cf.get(filename)
    with BytesIO(bytes_out) as f:
        df = pd.read_csv(f, compression=compression, **kwargs)
    return df


def put_numpy(data, path):
    path = Path(path)
    folder = path.parent
    filename = path.name
    cf = CloudFiles(folder)
    with BytesIO() as f:
        np.savez_compressed(f, data=data)
        cf.put(filename, f)


def get_numpy(path):
    path = Path(path)
    filename = path.name
    cf = CloudFiles(path.parent)
    bytes_out = cf.get(filename)
    with BytesIO(bytes_out) as f:
        data = np.load(f)["data"]
    return data
