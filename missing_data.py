import multiprocessing as mp
from multiprocessing import Process
from typing import Iterable
import pandas as pd
import numpy as np
from genpipes import declare, compose
import math


def imputations(df, return_dict):
    print("start imputations")

    df["cont_10"] = df["cont_10"].replace("na",np.nan)
    df["disc_6"] = df["disc_6"].replace("na",np.nan)
    df = df.dropna()
    df = df.reset_index(drop=True)
    df["cont_10"] = df["cont_10"].astype(float)
    df["disc_6"] = df["disc_6"].astype(float)

    continuos_features= list(df.describe().columns)
    correlation_matriz = np.array(df.corr())
    corr_matriz_size = np.shape(correlation_matriz)
    corr_matriz_size = corr_matriz_size[0]
    
    index_features_correlated = []
    for i in range(0,corr_matriz_size):
        for j in range(0, i):
            if i!=j:
                corr_value = correlation_matriz[i,j]
                corr_index = [i,j]
                if abs(corr_value)>=0.4:
                    index_features_correlated.append(corr_index)

    correlated_features = []
    for n in range(0, len(index_features_correlated)):
        index = index_features_correlated[n]
        first_index = index[0]
        second_index = index[1]

        first_feature= continuos_features[first_index]
        second_feature = continuos_features[second_index]

        features = [first_feature,second_feature]
        correlated_features.append(features)
    
    print("end process imputations")
    return_dict["correlated_features"]= correlated_features