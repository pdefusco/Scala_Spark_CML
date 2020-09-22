import os
import time
import json
import requests
import xml.etree.ElementTree as ET
import datetime

#Extracting the correct URL from hive-site.xml
tree = ET.parse('/etc/hadoop/conf/hive-site.xml')
root = tree.getroot()

for prop in root.findall('property'):
    if prop.find('name').text == "hive.metastore.warehouse.dir":
        storage = prop.find('value').text.split("/")[0] + "//" + prop.find('value').text.split("/")[2]

print("The correct Object Store URL is:{}".format(storage))

os.environ['STORAGE'] = storage

#Now some sample CLI commands to create a test dir and upload a file from CML 
#You can run these within a notebook, editor file, or in the session prompt (bottom right) with an exclamation mark
#Or you can run these in the terminal (top right) without the exclamation mark


#Put file in Object Storage

!hdfs dfs -mkdir -p $STORAGE/datalake/
!hdfs dfs -mkdir -p $STORAGE/datalake/spark_scala_demo
!hdfs dfs -copyFromLocal /home/cdsw/data/campaign_conversion.csv $STORAGE/datalake/spark_scala_demo/campaign_conversion.csv
!hdfs dfs -ls $STORAGE/datalake/spark_scala_demo

#Optionally: remove the testdir:
#!hdfs dfs -rm -r $STORAGE/datalake/spark_scala_demo







