from PyQt5.QtWidgets import QMessageBox
from qgis.core import QgsProject, Qgis, QgsFillSymbol, QgsPalLayerSettings, QgsVectorLayerSimpleLabeling
from qgis.utils import iface
from PyQt5 import uic
import subprocess
import os

DialogBase, DialogType = uic.loadUiType(os.path.join(os.path.dirname(__file__), 'loader.ui'))

class LoaderDialog(DialogType, DialogBase):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setupUi(self)
        with open(os.path.join(os.path.dirname(__file__), 'history.txt')) as f:
            history = f.readlines()
        history = [x.strip() for x in history]
        self.crsText.setPlainText(history.pop())
        self.layersText.setPlainText("\n".join(history))
        self.loadButton.clicked.connect(self.getLayers)

    def getLayers(self):
        text = self.layersText.toPlainText()
        names = text.split("\n")
        crs = self.crsText.toPlainText()
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
            layer = iface.addVectorLayer(uri, name, "delimitedtext")
            if name == "Cells":
                symbol = QgsFillSymbol.createSimple({'color_border': 'blue', 'style': 'no', 'style_border': 'dash'})
                layer.renderer().setSymbol(symbol)
                layer.triggerRepaint()
            if name == "Faces":
                symbol = QgsFillSymbol.createSimple({'color': 'green'})
                layer.renderer().setSymbol(symbol)
                pal_layer = QgsPalLayerSettings()
                pal_layer.fieldName = 'field_2'
                pal_layer.enabled = True
                pal_layer.placement = QgsPalLayerSettings.Free
                labels = QgsVectorLayerSimpleLabeling(pal_layer)
                layer.setLabeling(labels)
                layer.setLabelsEnabled(True)
                layer.triggerRepaint()
        with open(os.path.join(os.path.dirname(__file__), 'history.txt'), 'w') as f:
            f.write("{}\n{}".format(text, crs))
        
