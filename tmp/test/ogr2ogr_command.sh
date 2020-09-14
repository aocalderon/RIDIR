ogr2ogr -f CSV "/vsistdout/" -sql "SELECT * FROM 'Edges'" Intersections.gpkg -lco "GEOMETRY=AS_WKT" -lco "SEPARATOR=TAB" | sed 's/"//g'| cut -f1
