# exec(open('/home/and/RIDIR/Code/PyQGIS/EdgesFilesReader.py'.encode('utf-8')).read())

import subprocess

crs = 2272
#subprocess.run(["scp", "acald013@hn:/tmp/edgesCells.wkt", "/home/and/tmp/edges/edgesCells.wkt"])
#subprocess.run(["scp", "acald013@hn:/tmp/edgesSegments.wkt", "/home/and/tmp/edges/edgesSegments.wkt"])
#subprocess.run(["scp", "acald013@hn:/tmp/edgesHedges.wkt", "/home/and/tmp/edges/edgesHedges.wkt"])
#subprocess.run(["scp", "acald013@hn:/tmp/edgesFaces.wkt", "/home/and/tmp/edges/edgesFaces.wkt"])

names = ["Cells", "Segments", "Vertices", "Hedges"]

for name in names:
  subprocess.run(["scp", "acald013@hn:/tmp/edges{}.wkt".format(name), "/home/and/tmp/edges/edges{}.wkt".format(name)])


instance = QgsProject.instance()

for name in names:
  layers = instance.mapLayersByName(name)
  for layer in layers:
    instance.removeMapLayer(layer)

iface.mapCanvas().refresh()


for name in names:
  uri = "file:///home/and/tmp/edges/edges{}.wkt?delimiter={}&useHeader=no&crs=epsg:{}&wktField={}".format(name, "t", crs, "field_1")
  vCell = iface.addVectorLayer(uri, name, "delimitedtext")

