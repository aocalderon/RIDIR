#!/usr/bin/env python
# coding: utf-8

from shapely.geometry import Polygon
from shapely import wkt
import numpy as np
import pandas as pd
import geopandas as gpd
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--source", "-s", help="Source polygon file...")
parser.add_argument("--target", "-t", help="Target polygon file...")
parser.add_argument("--outextensive", "-e", default="/tmp/geopandas_extensive.tsv", help="Output extensive file...")
parser.add_argument("--outintensive", "-i", default="/tmp/geopandas_intensive.tsv", help="Output intensive file...")
args = parser.parse_args()

data = pd.read_csv(args.source, sep = '\t', header = None, names = ["geometry", "geoid", "extensive", "intensive"])
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

"""
sourceFile = "/home/acald013/RIDIR/Datasets/phili_2010.wkt"
data = pd.read_csv(sourceFile, sep = '\t', header = None, names = ["geometry", "geoid", "ext", "int"])
data['geometry'] = data['geometry'].apply(wkt.loads)
source = gpd.GeoDataFrame(data, geometry='geometry')
print(source.head())
print("Source: {}".format(source.shape))

targetFile = "/home/acald013/RIDIR/Datasets/phili_outliers.wkt"
data = pd.read_csv(targetFile, sep = '\t', header = None, names = ["geometry", "geoid"])
data['geometry'] = data['geometry'].apply(wkt.loads)
target = gpd.GeoDataFrame(data, geometry='geometry')
print(target.head())
print("Target: {}".format(target.shape))

extensive = ["ext"]
intensive = ["int"]
"""

#%%
def area_tables(source_df, target_df):
    if _check_crs(source_df, target_df):
        pass
    else:
        return None

    n_s = source_df.shape[0]
    n_t = target_df.shape[0]
    _left = np.arange(n_s)
    _right = np.arange(n_t)
    source_df.loc[:, '_left'] = _left  # create temporary index for union                                                                                                                                  
    target_df.loc[:, '_right'] = _right # create temporary index for union                                                                                                                                 
    res_union = gpd.overlay(source_df, target_df, how='union')
    n_u, _ = res_union.shape
    SU = np.zeros((n_s, n_u)) # holds area of intersection of source geom with union geom                                                                                                                  
    UT = np.zeros((n_u, n_t)) # binary table mapping union geom to target geom                                                                                                                             
    for index, row in res_union.iterrows():
        # only union polygons that intersect both a source and a target geometry matter                                                                                                                    
        if not np.isnan(row['_left']) and not np.isnan(row['_right']):
            s_id = int(row['_left'])
            t_id = int(row['_right'])
            SU[s_id, index] = row['geometry'].area
            UT[index, t_id] = 1
    source_df.drop(['_left'], axis=1, inplace=True)
    target_df.drop(['_right'], axis=1, inplace=True)
    return SU, UT

def _check_crs(source_df, target_df):
    """check if crs is identical"""
    if not (source_df.crs == target_df.crs):
        print("Source and target dataframes have different crs. Please correct.")
        return False
    return True

def area_interpolate(source_df, target_df, extensive_variables=[], intensive_variables=[], tables=None, allocate_total=True):
    if tables is None:
        SU, UT  = area_tables(source_df, target_df)
    else:
        SU, UT = tables
    den = source_df['geometry'].area.values
    if allocate_total:
        den = SU.sum(axis=1)
    den = den + (den==0)
    weights = np.dot(np.diag(1/den), SU)

    extensive = [] 
    for variable in extensive_variables:
        vals = _nan_check(source_df, variable)
        estimates = np.dot(np.diag(vals), weights)
        estimates = np.dot(estimates, UT)
        estimates = estimates.sum(axis=0)
        extensive.append(estimates)
    extensive = np.array(extensive)

    ST = np.dot(SU, UT)
    area = ST.sum(axis=0)
    den = np.diag(1./ (area + (area == 0)))
    weights = np.dot(ST, den)
    intensive = []
    for variable in intensive_variables:
        vals = _nan_check(source_df, variable)
        vals.shape = (len(vals), 1)
        est = (vals * weights).sum(axis=0)
        intensive.append(est)
    intensive = np.array(intensive)

    return (extensive, intensive)

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

estimates = area_interpolate(source, target, extensive_variables = extensive, intensive_variables = intensive)
rextensive = estimates[0]
rintensive = estimates[1]

print(rintensive)

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
