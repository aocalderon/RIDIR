#!/usr/bin/env python
# coding: utf-8

from shapely.geometry import Polygon
from shapely import wkt
import numpy as np
import pandas as pd
import geopandas as gpd
from tobler import area_tables
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--source", "-s", help="Source polygon file...")
parser.add_argument("--target", "-t", help="Target polygon file...")
parser.add_argument("--output", "-o", default="/tmp/geopandas_area_table.tsv", help="Output file...")
args = parser.parse_args()

data = pd.read_csv(args.source, sep = '\t', header = None, names = ["geom", "id", "ext", "int"])
data['geom'] = data['geom'].apply(wkt.loads)
source = gpd.GeoDataFrame(data, geometry='geom')
print(source.head())
print("Source: {}".format(source.shape()))

data = pd.read_csv(args.target, sep = '\t', header = None, names = ["geom", "id"])
data['geom'] = data['geom'].apply(wkt.loads)
target = gpd.GeoDataFrame(data, geometry='geom')
print(target.head())
print("Target: {}".format(target.shape()))

(SU, UT) = area_tables(source, target)

tobler = np.dot(SU, UT)

toblerFile = open(args.output, "w")
for i in range(tobler.shape[0]):
    for j in range(tobler.shape[1]):
        area = tobler[i,j]
        if area > 0:
            toblerFile.write("{}\t{}\t{}\n".format(source['id'][j], target['id'][i], area))
toblerFile.close()
print("Done!")
