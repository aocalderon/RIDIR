./closer.sh -i file:///home/and/Datasets/Census/CA/CA2000.wkt -o /tmp/CA2000 -k 0 -y -77282.52260068 -x 79968.24050000
scp /tmp/CA2000/part-00000-920698aa-6530-49db-b62e-9485f53c66d6-c000.txt acald013@hn:/home/acald013/Datasets/CA_scaleup/CA2000.wkt

./closer.sh -i file:///home/and/Datasets/Census/CA/CA2010.wkt -o /tmp/CA2010 -k 0 -y -77282.52260068 -x 79968.24050000
scp /tmp/CA2010/part-00000-0bb7c9fb-fb6b-4423-afcd-4e1cda9a3973-c000.txt acald013@hn:/home/acald013/Datasets/CA_scaleup/CA2010.wkt
