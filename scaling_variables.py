import pandas as pd
from genpipes import declare, compose
from typing import Iterable


def normalize(df, feature_name):
    result = df.copy()
    max_value = df[feature_name].max()
    min_value = df[feature_name].min()
    result[feature_name] = (df[feature_name] - min_value) / (max_value - min_value)
    return result


@declare.processor(inputs=["cont_3"])
def scaling(stream:Iterable[pd.DataFrame],col_to_scale:str):
    for df in stream:
        normalized_df= normalize(df,col_to_scale)
        print("normalized")
        print(normalized_df)
        yield normalized_df
