import multiprocessing
from typing import Iterable
import pandas as pd
import numpy as np
from genpipes import declare, compose
#from sklearn import preprocessing
import math

import multiprocessing as mp
from multiprocessing import Process


@declare.processor(inputs=["cat_8"])
def filter_by(stream:Iterable[pd.DataFrame],col_to_filter:str,value:str):
    for df in stream:
        dff = df[df[col_to_filter]==value]
        print("df filtered")
        print(dff)
        yield dff