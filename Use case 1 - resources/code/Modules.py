import boto3
import sys,io,os
import pyspark
from pyspark.sql import SparkSession

 
bucket = "saama-gene-training-data-bucket"
inPrefix = "MaheshWankhede/assignment_1/Inbound"
preprocessPrefix = "MaheshWankhede/assignment_1/Preprocess"
landingPrefix = "MaheshWankhede/assignment_1/Landing"
stdPrefix = "MaheshWankhede/assignment_1/Standardized"
outboundPrefix = "MaheshWankhede/assignment_1/Outbound"

def getObject(bucket,in_prefix):
    s3 = boto3.resource('s3')
    bucketObjectList = s3.Bucket(bucket)
    a_list = []
    for a in bucketObjectList.objects.filter(Prefix=in_prefix):
        if a.key.endswith(".csv") or a.key.endswith(".xls")
            a_list.append(a.key)
    return a_list      #returns a list of file names with csv and xls extension

def copyFile(sourceBucket,sourceKey,targetBucket,targetKey):
	s3 = boto3.resource('s3')
	copyobj={
		'Bucket': sourceBucket,
		'Key': sourceKey
	}
	s3.meta.client.copy(copyobj,targetBucket,targetKey)
	print(f'file copied: {targetKey}'

'''
def startCrawler(crawlerName):
	session = boto3.session.Session()
	glue_client = session.client('glue')
	try:
		resp = glue_client.start_Crawler(Name = crawler_name)
		return resp
	except Exception as e:
		raise Exception("Error while starting crawler: " + e.__str__())
 '''       
def readTextFile(bucket,path):
	s3_client = boto3.client("s3")
    s3Obj = s3_client.get_object(Bucket=bucket, Key=path)
    return s3Obj['Body'].read().decode("utf-8")
    
        
def readExcelFile(spark,bucket,file):
	fileName="s3://"+bucket+"/"+file
    f_obj = pd.read_excel(fileName)
    df = spark.createDataFrame(f_obj)
    return df


def readCsvFile(spark,bucket,file,delimiter=','):
    fileName = "s3://"+bucket+"/"+file
    df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",delimiter).load(fileName)
    return df

#def writeCsvFile(bucket,df,file,delimiter=','):
#   file_name = "s3://"+bucket+"/"+file
#    df.toPandas().to_csv(file_name,header=True,index=False,sep=delimiter)
   
def writeParquetFile(bucket,df,file,PartionColNames=""):
    file_name = ""
    
def convertCsv(bucket,df,file,delimiter=','):
    file_name = "s3://"+bucket+"/"+file
    df.toPandas().to_csv(file_name,header=True,index=False,sep=delimiter)
    
    
    
def PreToLanding(spark,bucket,file):    #Files from preprocess folder
    if file.split(".")[-1] == "xls":
        df = readExcel(spark,bucket,file)
    elif file.split(".")[-1]=="csv":
        df = readCsv(spark,bucket,file)
    if df:
        fileNameTarget = landingPrefix+"/"+file.split("/")[-2]+"/"+os.path.basename(file).split(".")[0]+".csv"
        convertCsv(bucket,df,fileNameTarget,delimiter = "|")





