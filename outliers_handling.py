
import multiprocessing as mp
from multiprocessing import Process
import time
import multiprocessing
from typing import Iterable
import pandas as pd
import numpy as np
from genpipes import declare, compose
#from sklearn import preprocessing
import math

def outliers(df, return_dict):
    print("start outliers")
    df["cont_10"] = df["cont_10"].replace("na",np.nan)
    df["disc_6"] = df["disc_6"].replace("na",np.nan)
    df = df.dropna()
    df = df.reset_index(drop=True)
    df["cont_10"] = df["cont_10"].astype(float)
    df["disc_6"] = df["disc_6"].astype(float)

    continuos_features= list(df.describe().columns)
    description_continuos_features = df.describe()

    feature_fences = []
    for f in continuos_features:
        col = description_continuos_features[f].round(4)
        mean = round(col[1],2)
        median = round(col[5],2)
        Q25 = round(col[4],2)
        Q75 = round(col[6],2)
        IQ = round(Q75-Q25,2)
        lower_inner_fence = round(Q25-1.5*(IQ),2)
        upper_inner_fence = round(Q75+1.5*(IQ),2)
        lower_outer_fence = round(Q25-10*(IQ),2)
        upper_outer_fence = round(Q75+10*(IQ),2)
        min = col[3]
        max = col[7]

        if max > upper_outer_fence or min < lower_outer_fence:
            feature_limits={"name":f,"min":min,"max":max, "mean":mean, "std":col[2],"25%":Q25, "median":median,"75%":Q75, "IQ":IQ ,"lower_inner_fence":lower_inner_fence, "upper_inner_fence":upper_inner_fence,"lower_outer_fence":lower_outer_fence,"upper_outer_fence":upper_outer_fence}
            feature_fences.append(feature_limits)

    print("end process outliers")
    return_dict["outliers"]= feature_fences