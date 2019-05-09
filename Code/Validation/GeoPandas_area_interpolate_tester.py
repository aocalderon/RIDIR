#!/usr/bin/env python
# coding: utf-8

from shapely.geometry import Polygon
from shapely import wkt
import numpy as np
import pandas as pd
import geopandas as gpd
from tobler import area_tables, area_interpolate
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--source", "-s", help="Source polygon file...")
parser.add_argument("--target", "-t", help="Target polygon file...")
parser.add_argument("--output", "-o", default="/tmp/output.tsv", help="Output file...")
args = parser.parse_args()

data = pd.read_csv(args.source, sep = '\t', header = None, names = ["geometry", "id", "population"])
data['geometry'] = data['geometry'].apply(wkt.loads)
source = gpd.GeoDataFrame(data, geometry='geometry')
print(source.head())
print("Source: {}".format(source.count()))

data = pd.read_csv(args.target, sep = '\t', header = None, names = ["geometry", "id"])
data['geometry'] = data['geometry'].apply(wkt.loads)
target = gpd.GeoDataFrame(data, geometry='geometry')
print(target.head())
print("Target: {}".format(target.count()))

extensive = ["population"]
estimates = area_interpolate(source, target, extensive_variables = extensive)
rextensive = estimates[0]

count = 1
toblerFile = open(args.output, "w")
for x in np.nditer(rextensive):
    toblerFile.write("{}\n".format(x))
    #print(x, end='\n')
    count = count + 1
    print("Reading {}...".format(count))
toblerFile.close()
print("Done")
