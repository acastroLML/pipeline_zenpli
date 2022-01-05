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


def data_scheme(df, return_dict):
    print("start data scheme")
    df["cont_10"] = df["cont_10"].replace("na",np.nan)
    df["disc_6"] = df["disc_6"].replace("na",np.nan)
    df = df.dropna()
    df = df.reset_index(drop=True)
    df["cont_10"] = df["cont_10"].astype(float)
    df["disc_6"] = df["disc_6"].astype(float)
    continuos_features= list(df.describe().columns)
    description_continuos_features = df.describe()
    
    dict_ranges = []
    for f in continuos_features:
        col = description_continuos_features[f].round(4)
        dict_feature={"name":f,"min":col[3],"max":col[7], "mean":col[1], "std":col[2],"25%":col[4], "median":col[5],"75%":col[6], "IQ":col[6]-col[4]}
        dict_ranges.append(dict_feature)
    
    categorical_features= ['cat_7','cat_8']
    dict_categorical_values =[]
    for f in categorical_features:
        categories = df[f].value_counts().to_dict()
        categories["name"]=f
        dict_categorical_values.append(categories)

    print("end process data scheme")
    return_dict["scheme"]= [dict_categorical_values,dict_ranges]