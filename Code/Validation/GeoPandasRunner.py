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
import logging

## Reading arguments...
parser = argparse.ArgumentParser()
parser.add_argument("--source", "-s", default=1, help="Source polygons...")
parser.add_argument("--target", "-t", default=1, help="Target polygons...")
args = parser.parse_args()

data = pd.read_csv(args.source, sep = '\t', header = None, names = ["geom", "id"])
data['geom'] = data['geom'].apply(wkt.loads)
source = gpd.GeoDataFrame(data, geometry='geom')

data = pd.read_csv(args.target, sep = '\t', header = None, names = ["geom", "id"])
data['geom'] = data['geom'].apply(wkt.loads)
target = gpd.GeoDataFrame(data, geometry='geom')

(SU, UT) = area_tables(source, target)
