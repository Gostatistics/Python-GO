##### Library -----------------------------------------------
import numpy as np
import pandas as pd


##### Function -----------------------------------------------

### Check if a value is truly null.
def isnull(x):
  output = False
  if pd.isnull(x):
    output = True
  elif isinstance(x, str):
    if x.lower()=='null':
      outout = True
    if x.lower()=='nan':
      output = True
    if x.lower()=='na':
      output = True
  else:
      output = False
  return output
 
### Output the missing value count for all columns.
from pyspark.sql import DataFrame
def null_report(df):
  dfp = df
  if isinistance(df, DataFrame): dfp = df.toPandas()
  cols = dfp.columns
  nrow = dfp.shape[0]  
  for i in cols:
    print('Missing value count for ', i, ': ',  nrow - dfp[i].apply(lambda l: isnull(l)).notnull().sum())
