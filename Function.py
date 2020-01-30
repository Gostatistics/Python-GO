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
  print('Total entry count: ',nrow)
  for i in cols:
    print('Missing value count for ', i, ': ',  dfp[i].apply(lambda l: isnull(l)).sum())

### Reorder strings in the format to be used as keys
def sorted_title(x):
  x = x.split(' (')[0]
  xl = re.split('\W', x.lower())
  xl = sorted(xl)
  xj = ''.join(xl)
  return xj
