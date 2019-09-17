#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 16 11:40:56 2019

@author: and
"""

#%%
import geopandas as gpd
from quilt.data.spatialucr import census
from shapely.wkb import loads
import rasterio
from rasterio.plot import show

import matplotlib.pyplot as plt

import geosnap
from geosnap.data.data import store_ltdb

#%%
sample = "/home/and/Datasets/LTDB_Std_All_Sample.zip"
full = "/home/and/Datasets/LTDB_Std_All_fullcount.zip"

store_ltdb(sample = sample, fullcount = full)

#%%
df = geosnap.data.data_store.ltdb

#%%
df['geoid'] = df.index
df['state'] = df['geoid'].str[0:2]
variables = ['geoid', 'n_total_pop']
year = 2000
state = '06'

df_cali_2010 = df[(df['year'] == year) & (df.geoid.str[0:2] == state)][variables]

#%%
aux = census.tracts_2000()
aux['geometry'] = aux.wkb.apply(lambda x: loads(x, hex=True))
aux = aux.drop(['wkb'], axis = 1)
gdf_2000 = gpd.GeoDataFrame(aux)
gdf_2000['year'] = '2000'
gdf_2000.head()

#%%
cali_2000 = gdf_2000[gdf_2000.geoid.str[0:2] == state]
ax = cali_2000.plot(figsize = (10,10), alpha = 0.5, edgecolor = 'black')
ax.set_title("California in 2000", fontsize = 20)
plt.axis('off')

#%%
cali_2000.to_csv("/tmp/cali.wkt", sep="\t", header=False, index=False)
