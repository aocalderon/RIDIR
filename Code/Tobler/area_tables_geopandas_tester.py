#!/usr/bin/env python
# coding: utf-8

#%%
import geosnap
DATASETS_PATH = "/home/and/Datasets"

#%%
sample = "{}/LTDB_Std_All_Sample.zip".format(DATASETS_PATH)
full = "{}/LTDB_Std_All_fullcount.zip".format(DATASETS_PATH)

geosnap.data.read_ltdb(sample=sample, fullcount=full)
geosnap.data.db.ltdb.head()

#%%
df = geosnap.data.db.ltdb[['n_total_pop', 'year']]
df.head()

#%%
df_pop_2000 = df[(df['year'] == 2000)][['n_total_pop']]
df_pop_2000.head()

#%%
import pickle
import pandas as pd

#%%
# This file can be download here: https://drive.google.com/open?id=1gWF0OCn6xuR_WrEj7Ot2jY6KI2t6taIm
with open('/home/and/Datasets/tracts_US.pkl', 'rb') as input: 
    map_gpd = pickle.load(input)

#df = census_2010.loc[(census_2010.state == "PA")]

map_gpd['INTGEOID10'] = pd.to_numeric(map_gpd["GEOID10"])
gdf_2000 = map_gpd.merge(df_pop_2000, left_on = 'GEOID10', right_on = 'geoid')

#%%
gdf_2000.head()

#%%
import geopandas as gpd
from quilt.data.spatialucr import census
from shapely.wkb import loads

#%%
df = census.tracts_1990()
df['geometry'] = df.wkb.apply(lambda x: loads(x, hex=True))
gdf_1990 = gpd.GeoDataFrame(df)

#%%
gdf_2000.plot(figsize = (35, 35), color='blue')
len(gdf_2000)

#%%
gdf_1990.plot(figsize = (35, 35), color='red')
len(gdf_1990)

#%%
gdf_2000.to_file("{}/gdf_2000".format(DATASETS_PATH))
gdf_1990.to_file("{}/gdf_1990".format(DATASETS_PATH))

#%%
# Same function of tobler, but with an additional line that prints which line is processing
import numpy as np
def area_tables(source_df, target_df):
    """
    Construct area allocation and source-target correspondence tables
    Parameters
    ----------
    source_df: geopandas GeoDataFrame with geometry column of polygon type
    source_df: geopandas GeoDataFrame with geometry column of polygon type
    Returns
    -------
    tables: tuple (optional)
            two 2-D numpy arrays
            SU: area of intersection of source geometry i with union geometry j
            UT: binary mapping of union geometry j to target geometry t
    Notes
    -----
    The assumption is both dataframes have the same coordinate reference system.
    Union geometry is a geometry formed by the intersection of a source geometry and a target geometry
    SU Maps source geometry to union geometry, UT maps union geometry to target geometry
    """
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
        
        print('Processing {} polygon out of {}.'.format(index, len(res_union)), end = "\r")
        
        if not np.isnan(row['_left']) and not np.isnan(row['_right']):
            s_id = int(row['_left'])
            t_id = int(row['_right'])
            SU[s_id, index] = row['geometry'].area
            UT[index, t_id] = 1
    source_df.drop(['_left'], axis=1, inplace=True)
    target_df.drop(['_right'], axis=1, inplace=True)
    return SU, UT
#%%
# Really time consuming! Specially because of the inner overlay of the maps.
# resulting_table = area_tables(full_pop_2000, df_1990)
