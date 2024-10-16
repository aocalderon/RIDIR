#!/usr/bin/env python
# coding: utf-8

from shapely.geometry import Polygon
from shapely import wkt
import numpy as np
import pandas as pd
import geopandas as gpd
from tobler import area_tables
import sys

DATASET_PATH = sys.argv[1]
STATE = sys.argv[2]

data = pd.read_csv("{}/{}_source.wkt".format(DATASET_PATH, STATE), sep = '\t', header = None, names = ["geom", "id"])
data['geom'] = data['geom'].apply(wkt.loads)
source = gpd.GeoDataFrame(data, geometry='geom')
source.head()
print("Source: {}".format(source.count()))

data = pd.read_csv("{}/{}_target.wkt".format(DATASET_PATH, STATE), sep = '\t', header = None, names = ["geom", "id"])
data['geom'] = data['geom'].apply(wkt.loads)
target = gpd.GeoDataFrame(data, geometry='geom')
target.head()
print("Target: {}".format(target.count()))

(SU, UT) = area_tables(source, target)

tobler = np.dot(SU, UT)

count = 1
toblerFile = open("{}/{}_geopandas_test.tsv".format(DATASET_PATH, STATE), "w")
for i in range(tobler.shape[0]):
    for j in range(tobler.shape[1]):
        area = tobler[i,j]
        if area > 0:
            toblerFile.write("{}\t{}\t{}\n".format(target['id'][j], source['id'][i], area))
            count = count + 1
            print("Reading {}...".format(count))
toblerFile.close()
print("Done")
