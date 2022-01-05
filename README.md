# pipeline_zenpli
Pipeline Implementation with Python

This is an initial version of a data pipeline for proccessing a .text file. It defines the pipeline in the main script (main.py) 

pipe = compose.Pipeline(steps=[

        ("Step 1 - fetching datasource - parallel",data_to_be_processed,{}),
        ("Step 2 - scaling by feature x",scaling,{}),
        ("Step 2 - filter by categorical value",filter_by,{"value":list_inputs[1]}),
        ("Step 2 - grouping by month", groupByMonth,{}),
        ("Step 3 - data tranformation",data_transformation,{}),
        ("Step 3 - data aggregation", data_aggregation,{})

    ])

##"Step 1 - fetching datasource - parallel"    
The first step "Step 1 - fetching datasource - parallel" uses 3 parallel proccess that can terminated in any order. This step use the function "data_to_be_processed" that is imported from 
from basic_exploratory_analysis file.

In basic exploratory analysis there are launched the 3 parallel proccess: 
- The first one (data_scheme) verifies the data scheme and makes a description of the columns involved. 
- The second process (imputations) find the imputation rules by means of determining what columns have missed data and if exist some kind of correlation with other column or columns to fit a model 
to get the missing values.
- The third proccess (outliers) get an deviation analysis and find the quantiles and interquartil range for each column and determines the outliers according to the following limits:

lower_inner_fence = round(Q25-1.5*(IQ),2)
upper_inner_fence = round(Q75+1.5*(IQ),2)
lower_outer_fence = round(Q25-10*(IQ),2)
upper_outer_fence = round(Q75+10*(IQ),2)

For example, if the data is out of the outer fence it is considered an extreme outlier and can be modified to the outer fence value.


from scaling_variables import scaling
from filtering import filter_by
from grouping import groupByMonth
When the 3 proccesses are finished
