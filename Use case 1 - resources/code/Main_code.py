import boto3
import io,os,sys

import pyspark
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("assignment-1").getOrCreate()

for fileObj in getObject(bucket,inPrefix):
	targetFileName = preprocessPrefix+"/"+file.split("/")[-2]+"/"+os.path.basename(file)
    copyFile(bucket,file,bucket,targetFileName)

for fileObj in getObject(bucket,preprocessPrefix):
    PreToLanding(spark,bucket,file)

for fileObj in getFileList(bucket,landingPrefix):
    #LandingToStandard(spark,bucket,file)
    s3path = "s3://"+landingPrefix+"/"+fileObj
    df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep","|".load(s3path)
    df1 = df.withColumnRenamed("Scrip name","Script_Name"),\
            .withColumnRenamed("Adj Close","Adj_Close"),\
            .withColumnRenamed("Script Name","Script_Name")
    filePathToStd = "s3://"+bucket+"/"+stdPrefix+"/"+os.path.basename(fileObj).split(".")[0]
    df.write.mode("overwrite").partitionBy("Script_Name").parquet(filePathToStd)
    
    
    

