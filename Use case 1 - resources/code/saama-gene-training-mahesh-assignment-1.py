import boto3
import sys,io,os
import pyspark
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("assignment-1").getOrCreate()

bucket = "saama-gene-training-data-bucket"
inPrefix = "MaheshWankhede/assignment_1/Inbound"
preprocessPrefix = "MaheshWankhede/assignment_1/Preprocess"
landingPrefix = "MaheshWankhede/assignment_1/Landing"
stdPrefix = "MaheshWankhede/assignment_1/Standardized"
outboundPrefix = "MaheshWankhede/assignment_1/Outbound/archive"

def getObject(bucket,in_prefix):
    s3 = boto3.resource('s3')
    bucketObjectList = s3.Bucket(bucket)
    a_list = []
    for a in bucketObjectList.objects.filter(Prefix=in_prefix):
        if a.key.endswith(".csv") or a.key.endswith(".xls"):
            a_list.append(a.key)
    return a_list      #returns a list of file names with csv and xls extension

def copyFile(sourceBucket,sourceKey,targetBucket,targetKey):
	s3 = boto3.resource('s3')
	copyobj={
		'Bucket': sourceBucket,
		'Key': sourceKey
	}
	s3.meta.client.copy(copyobj,targetBucket,targetKey)
	print(f'file copied: {targetKey}')


# def startCrawler(crawlerName):
# 	session = boto3.session.Session()
# 	glue_client = session.client('glue')
# 	try:
# 		resp = glue_client.start_Crawler(Name = crawler_name)
# 		return resp
# 	except Exception as e:
# 		raise Exception("Error while starting crawler: " + e.__str__())
      
# def readTextFile(bucket,path):
# 	s3_client = boto3.client("s3")
#     s3Obj = s3_client.get_object(Bucket=bucket, Key=path)
#     return s3Obj['Body'].read().decode("utf-8")

def readExcel(spark,bucket,file):
    fileName="s3://"+bucket+"/"+preprocessPrefix+"/"+file
    f_obj = pd.read_excel(fileName)
    return spark.createDataFrame(f_obj)

def readCsv(spark,bucket,file,delimiter=','):
    fileName = "s3://"+bucket+"/"+file
    return spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",delimiter).load(fileName)
    

#def writeCsvFile(bucket,df,file,delimiter=','):
#   file_name = "s3://"+bucket+"/"+file
#    df.toPandas().to_csv(file_name,header=True,index=False,sep=delimiter)

def convertCsv(bucket,df,file,delimiter=','):
    file_name = "s3://"+bucket+"/"+file
    df.toPandas().to_csv(file_name,header=True,index=False,sep=delimiter)

def PreToLanding(spark,bucket,file):    #Files from preprocess folder
    if file.split(".")[-1] == "xls":
        df = readExcel(spark,bucket,file)
    elif file.split(".")[-1]=="csv":
        df = readCsv(spark,bucket,file,",")
    if df:
        fileNameTarget = landingPrefix+"/"+file.split("/")[-2]+"/"+os.path.basename(file).split(".")[0]+".csv"
        convertCsv(bucket,df,fileNameTarget,delimiter = "|")
    else:
        print("Unexpected Error")

for fileObj in getObject(bucket,inPrefix):
    targetFileName = preprocessPrefix+"/"+fileObj.split("/")[-2]+"/"+os.path.basename(fileObj)
    if fileObj.split(".")[-1]=="csv" or fileObj.split(".")[-1]=="xls":
        copyFile(bucket,fileObj,bucket,targetFileName)

for fileObj in getObject(bucket,preprocessPrefix):
    PreToLanding(spark,bucket,fileObj)

for fileObj in getObject(bucket,landingPrefix):
    s3path = "s3://"+landingPrefix+"/"+fileObj
    df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep","|").load(s3path)
    df1 = df.withColumnRenamed("Scrip name","ScriptName"),\
            .withColumnRenamed("Adj Close","Adj_Close"),\
            .withColumnRenamed("Script Name","Script_Name")
    filePathToStd = "s3://"+bucket+"/"+stdPrefix+"/"+os.path.basename(fileObj).split(".")[0]
    df.write.mode("overwrite").partitionBy("Script_Name").parquet(filePathToStd) #write parquet files with partitiona and add them to standardized folder

for file in getObject(bucket, stdPrefix):
	df=readCsv(spark,bucket,file,delimiter='|')
	df1= df.withCoumn("Date",to_date("Date","M/d/yyyy"))\
			.withColumn("week_Number",weekofyear("Date"))\
			.withColumn("year",year("Date"))\
			.withColumnRenamed("Adj Close","Adj_Close").withColumnRenamed("Scrip Name","Script_Name")
	file_path = "s3://"+bucket+"/"+outboundPrefix+"/"+file
	df1.write.format("csv").option("inferSchema","true").option("header","true").save(file_path)