#!/usr/bin/env python
# coding: utf-8

from shapely.geometry import Polygon
from shapely import wkt
import numpy as np
import pandas as pd
import geopandas as gpd
from tobler import area_tables
import logging
import argparse
import time

clocktime = lambda: int(round(time.time() * 1000))

def log(msg, t, n):
    logging.info("{:30s}|{:10.2f}|{:10}".format(msg, round((clocktime() - t) / 1000.0, 2), n))

def main():
    # Starting script...
    totalStart = clocktime()
    timer = clocktime()
    logging.basicConfig(format='%(asctime)s|%(message)s', level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", "-s", help="Source polygon file...")
    parser.add_argument("--target", "-t", help="Target polygon file...")
    parser.add_argument("--tag", "-g", default="", help="Additional tag for information purposes...")
    parser.add_argument("--format", "-f", default="wkt", help="File format...")
    args = parser.parse_args()
    source = args.source
    target = args.target
    tag = args.tag
    log("Script started", timer, 0)

    # Reading data
    if(args.format == "shp"):
        timer = clocktime()
        sourceGPD = gpd.read_file(source)
        nSourceGPD = len(sourceGPD.index)
        log("Source read", timer, nSourceGPD)

        timer = clocktime()
        targetGPD = gpd.read_file(target)
        nTargetGPD = len(targetGPD.index)
        log("Target read", timer, nTargetGPD)
    if(args.format == "wkt"):
        timer = clocktime()
        data = pd.read_csv(source, sep = '\t', header = None, names = ["geom", "id"])
        data['geom'] = data['geom'].apply(wkt.loads)
        sourceGPD = gpd.GeoDataFrame(data, geometry='geom')
        nSourceGPD = len(sourceGPD.index)
        log("Source read", timer, nSourceGPD)

        timer = clocktime()
        data = pd.read_csv(target, sep = '\t', header = None, names = ["geom", "id"])
        data['geom'] = data['geom'].apply(wkt.loads)
        targetGPD = gpd.GeoDataFrame(data, geometry='geom')
        nTargetGPD = len(targetGPD.index)
        log("Target read", timer, nTargetGPD)

    # Calling area_tables method...
    areaStart = clocktime()
    (SU, UT) = area_tables(sourceGPD, targetGPD)
    areaEnd = clocktime()
    log("GeoPandas area_tables", areaStart, 0)

    # Finishig script...
    areaTime  = (areaEnd - areaStart) / 1000.0
    totalEnd = clocktime()
    totalTime = (totalEnd - totalStart) / 1000.0
    logging.info("GeoPandas|{}|{:.2f}|{:.2f}".format(tag, totalTime, areaTime))
    log("Script finished", totalStart, 0)
    
if __name__ == '__main__':
    main()

