# exec(open('/home/and/RIDIR/Code/PyQGIS/EdgesFilesReader.py'.encode('utf-8')).read())

import subprocess

subprocess.run(["scp", "acald013@hn:/tmp/edgesCells.wkt", "/home/and/tmp/edges/edgesCells.wkt"])
subprocess.run(["scp", "acald013@hn:/tmp/edgesFaces.wkt", "/home/and/tmp/edges/edgesFaces.wkt"])

names = ["Cells", "Faces"]

instance = QgsProject.instance()

for name in names:
  layers = instance.mapLayersByName(name)
  for layer in layers:
    instance.removeMapLayer(layer)

iface.mapCanvas().refresh()

uri = "file:///home/and/tmp/edges/edgesCells.wkt?delimiter={}&crs=epsg:2272&wktField={}".format("\\t", "field_2")
vCell = iface.addVectorLayer(uri, "Cells", "delimitedtext")

uri = "file:///home/and/tmp/edges/edgesFaces.wkt?delimiter={}&crs=epsg:2272&wktField={}".format("\\t", "field_3")
vFaces = iface.addVectorLayer(uri, "Faces", "delimitedtext")

