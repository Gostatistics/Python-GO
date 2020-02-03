##### Library -----------------------------------------------
import numpy as np
import pandas as pd


##### Function -----------------------------------------------

### Check if a value is truly null.
def isnull(x):
  output = False
  if pd.isnull(x):
    output = True
  elif isinstance(x, str) and (x.lower() in ['null', 'nan', 'na']):
    outout = True
  else:
    output = False
  return output

### Output the missing value count for all columns.
from pyspark.sql import DataFrame
def null_report(df):
  dfp = df.toPandas()
  dl = []
  cols = dfp.columns
  dl.append(['Total Entry', dfp.shape[0]])
  for i in range(len(cols)):
    dl.append([cols[i], dfp[cols[i]].apply(lambda l: isnull(l)).sum()])
  dl = pd.DataFrame(dl)
  dl.columns = ['Column', 'Null Count']
  return dl

### Reorder strings in the format to be used as keys
def sorted_title(x):
  x = x.split(' (')[0]
  xl = re.split('\W', x.lower())
  xl = sorted(xl)
  xj = ''.join(xl)
  return xj
