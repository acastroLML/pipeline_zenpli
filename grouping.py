import multiprocessing
from typing import Iterable
import pandas as pd
import numpy as np
from genpipes import declare, compose
#from sklearn import preprocessing
import math

import multiprocessing as mp
from multiprocessing import Process


@declare.processor(inputs=["date_2"])
def groupByMonth(stream:Iterable[pd.DataFrame], col_to_filter:str):
    for df in stream:
        df_modified=df.copy()
        df_modified["datetime"]=pd.to_datetime(df_modified[col_to_filter])
        df_modified["month"] = df_modified["datetime"].dt.to_period(freq="M")
        df_modified_monthly = df_modified.groupby(["month"])[col_to_filter].count()
        print(df_modified_monthly)
        yield df_modified