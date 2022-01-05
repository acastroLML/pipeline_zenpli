import multiprocessing
from typing import Iterable
import pandas as pd
import numpy as np
from genpipes import declare, compose
#from sklearn import preprocessing
import math

import multiprocessing as mp
from multiprocessing import Process


from basic_exploratory_analysis import data_to_be_processed
from scaling_variables import scaling
from filtering import filter_by
from grouping import groupByMonth


@declare.processor(inputs=[["cont_3","cont_9"]])
def data_transformation(stream:Iterable[pd.DataFrame],col_to_transform):
    for df in stream:
        df_transformed = df.copy()
        df_transformed["x3^3"] = df_transformed[col_to_transform[0]]**3
        df_transformed["exp(x9)"] = df_transformed[col_to_transform[1]].apply(lambda x: math.exp( x))
        df_transformed["x3^3+exp(x9)"] = df_transformed["x3^3"]+df_transformed["exp(x9)"]
        print("data_transformed")
        print(df_transformed)
        yield df_transformed

@declare.processor(inputs=["cat_7"])
def data_aggregation(stream:Iterable[pd.DataFrame], col_to_filter:str):
    for df in stream:
        df_agg= df[col_to_filter].value_counts()
        print(df_agg)
        yield df_agg    

if __name__ == "__main__":
    print("cpu.count:  ",mp.cpu_count())

    list_inputs = ["cont_3", "happy","date_2","cont_3"]
    pipe = compose.Pipeline(steps=[
        ("Step 1 - fetching datasource - parallel",data_to_be_processed,{}),
        ("Step 2 - scaling by feature x",scaling,{}),
        ("Step 2 - filter by categorical value",filter_by,{"value":list_inputs[1]}),
        ("Step 2 - grouping by month", groupByMonth,{}),
        ("Step 3 - data tranformation",data_transformation,{}),
        ("Step 3 - data aggregation", data_aggregation,{})
    ])
    output=pipe.run()
    print(pipe)

