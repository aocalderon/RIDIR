# exec(open('/home/and/RIDIR/Code/PyQGIS/EdgesFilesReader.py'.encode('utf-8')).read())

import subprocess

crs = 2272
names = ["Cells", "Faces", "Segments", "Hedges", "Vertices"]

for name in names:
  subprocess.run(["scp", "acald013@hn:/tmp/edges{}.wkt".format(name), "/home/and/tmp/edges/edges{}.wkt".format(name)])

instance = QgsProject.instance()
for name in names:
  layers = instance.mapLayersByName(name)
  for layer in layers:
    instance.removeMapLayer(layer)
iface.mapCanvas().refresh()

for name in names:
  uri = "file:///home/and/tmp/edges/edges{}.wkt?delimiter={}&useHeader=no&crs=epsg:{}&wktField={}".format(name, "\\t", crs, "field_1")
  iface.addVectorLayer(uri, name, "delimitedtext")

