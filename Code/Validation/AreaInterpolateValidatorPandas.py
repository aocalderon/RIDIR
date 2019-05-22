#!/usr/bin/env python
# coding: utf-8

#%%
from shapely.geometry import Polygon
from shapely import wkt
import numpy as np
import pandas as pd
import geopandas as gpd
from tobler import area_tables
import sys

#%%
sourceFile = "/home/and/RIDIR/Datasets/phili_2000a.wkt"
data = pd.read_csv(sourceFile, sep = '\t', header = None, names = ["geom", "id", "ext", "int"])
data['geom'] = data['geom'].apply(wkt.loads)
source = gpd.GeoDataFrame(data, geometry='geom')
print(source.head())
print("Source: {}".format(source.shape))

targetFile = "/home/and/RIDIR/Datasets/phili_1990.wkt"
data = pd.read_csv(targetFile, sep = '\t', header = None, names = ["geom", "id"])
data['geom'] = data['geom'].apply(wkt.loads)
target = gpd.GeoDataFrame(data, geometry='geom')
print(target.head())
print("Target: {}".format(target.shape))

#%%
(SU, UT) = area_tables(source, target)

intensive_variable = ["int"]
ST = np.dot(SU, UT)
area = ST.sum(axis=0)
den = np.diag(1./ (area + (area == 0)))
weights = np.dot(ST, den)
intensive = []
for variable in intensive_variables:
    vals = _nan_check(source, variable)
    vals.shape = (len(vals), 1)
    est = (vals * weights).sum(axis=0)
    intensive.append(est)
intensive = np.array(intensive)

print("Done!")


#%%
def _nan_check(df, column):
    """Check if variable has nan values.
    Warn and replace nan with 0.0.
    """
    values = df[column].values
    if np.any(np.isnan(values)):
        wherenan = np.isnan(values)
        values[wherenan] = 0.0
        print('nan values in variable: {var}, replacing with 0.0'.format(var=column))
    return values