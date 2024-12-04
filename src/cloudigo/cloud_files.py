from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
from cloudfiles import CloudFiles


def put_dataframe(df, path):
    path = Path(path)
    folder = path.parent
    filename = path.name
    folder = str(folder)
    folder = folder.replace(":/", "://")
    cf = CloudFiles(folder)
    with BytesIO() as f:
        df.to_csv(f, index=True, compression="gzip")
        cf.put(filename, f)


def get_dataframe(path, **kwargs):
    path = Path(path)
    filename = path.name
    cf = CloudFiles(path.parent)
    bytes_out = cf.get(filename)
    with BytesIO(bytes_out) as f:
        df = pd.read_csv(f, **kwargs)
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
