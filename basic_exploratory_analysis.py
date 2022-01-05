import multiprocessing as mp
from multiprocessing import Process
import time
import multiprocessing
from typing import Iterable
import pandas as pd
import numpy as np
from genpipes import declare, compose
import math
from sklearn import linear_model


from scheme import data_scheme
from missing_data import imputations
from outliers_handling import outliers

@declare.generator()
@declare.datasource(inputs=["backend-dev-data-dataset.txt"])
def data_to_be_processed(path:str)-> pd.DataFrame:
    df = pd.read_csv(path)
    
    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    processes = []
    p1 = Process(target=data_scheme, args=(df,return_dict))
    processes.append(p1)
    p1.start()

    p2= Process(target=imputations,args=(df,return_dict))
    processes.append(p2)
    p2.start()

    p3= Process(target=outliers,args=(df,return_dict))
    processes.append(p3)
    p3.start()

    for p in processes:
        p.join()

    print("processes finished")
    print(return_dict)

    # Handling Outliers
    df_no_outliers = df.copy()
    features_with_outliers = return_dict.get('outliers')

    size_outliers = len(features_with_outliers)
    for t in range(0,size_outliers):
        dict_outlier = features_with_outliers[t]
        name_outlier = dict_outlier.get('name')
        df_no_outliers[name_outlier ] = df_no_outliers[name_outlier ].replace("na",np.nan)
        df_no_outliers = df_no_outliers.dropna(subset=[name_outlier])
        df_no_outliers = df_no_outliers.reset_index(drop=True)
        df_no_outliers[name_outlier] = df_no_outliers[name_outlier].astype(float)

        upper_limit = dict_outlier.get('upper_outer_fence')
        df.loc[df[name_outlier] > upper_limit, name_outlier] = upper_limit

    # Handling imputations
    features_with_correlation = return_dict.get('correlated_features')
    size_correlation= len(features_with_correlation)

    for t in range(0,size_correlation):
        dict_corr= features_with_correlation[t]
        feature_one = dict_corr[0]
        feature_two = dict_corr[1]


        df[feature_one] = df[feature_one].replace("na",np.nan)
        df[feature_two] = df[feature_two].replace("na",np.nan)

        df_no_null = df.copy()
        df_no_null = df_no_null.dropna(subset=[feature_one])
        df_no_null = df_no_null.reset_index(drop=True)
        df_no_null[feature_one] = df_no_null[feature_one].astype(float)


        y = df_no_null[feature_one] # dependent variable
        X = df_no_null[[feature_two]] # independent variable

        lm = linear_model.LinearRegression()
        lm.fit(X, y) # fitting the model
        lm.predict(X)
        print("score linear regression: ",lm.score(X, y))
        m = lm.coef_
        b = lm.intercept_

        df['concat'] = df[feature_one].astype(str) + ","+ df[feature_two].astype(str)
        df['concat'] = df['concat'].str.split(",")
        df['cont_10'] = df['concat'].apply(lambda x: round(float(x[1])*m[0]+b,2) if x[0] == 'nan' else round(float(x[0]),2))
        df.drop('concat', inplace=True, axis=1)
    
    df = df.dropna()
    df = df.reset_index(drop=True)
 
    df["disc_6"] = df["disc_6"].replace("na",np.nan)
    nulos_disc_6 = df["disc_6"].isna().value_counts()
    df = df.dropna()
    df = df.reset_index(drop=True)
    df["disc_6"] = df["disc_6"].astype(float)
    return df