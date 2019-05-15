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
parser.add_argument("--outextensive", "-e", default="/tmp/geopandas_extensive.tsv", help="Output extensive file...")
parser.add_argument("--outintensive", "-i", default="/tmp/geopandas_intensive.tsv", help="Output intensive file...")
args = parser.parse_args()

data = pd.read_csv(args.source, sep = '\t', header = None, names = ["geometry", "geoid", "extensive", "intensive"])
data = data[np.isfinite(data['intensive'])]
data['geometry'] = data['geometry'].apply(wkt.loads)
source = gpd.GeoDataFrame(data, geometry='geometry')
print(source.head())
print("Source: {}".format(source.count()))

data = pd.read_csv(args.target, sep = '\t', header  = None, names = ["geometry", "geoid"])
data['geometry'] = data['geometry'].apply(wkt.loads)
target = gpd.GeoDataFrame(data, geometry='geometry')
print(target.head())
print("Target: {}".format(target.count()))

extensive = ["extensive"]
intensive = ["intensive"]
estimates = area_interpolate(source, target, extensive_variables = extensive, intensive_variables = intensive)
rextensive = estimates[0]
rintensive = estimates[1]

count = 0
extensiveFile = open(args.outextensive, "w")
for x in np.nditer(rextensive):
    tid = "T{}".format(target['geoid'][count])
    extensiveFile.write("{}\t{}\n".format(tid, x))
    #print(x, end='\n')
    count = count + 1
    #print("Reading {}...".format(count))
extensiveFile.close()
print("Done extensive. {} records".format(count))

count = 0
intensiveFile = open(args.outintensive, "w")
for x in np.nditer(rintensive):
    tid = "T{}".format(target['geoid'][count])
    intensiveFile.write("{}\t{}\n".format(tid, x))
    #print(x, end='\n')
    count = count + 1
    #print("Reading {}...".format(count))
intensiveFile.close()
print("Done intensive. {} records.".format(count))
