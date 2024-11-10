
import findspark


import pyspark

from pyspark.sql import DataFrame, SparkSession, Window
from typing import List
import pyspark.sql.types as T
import pyspark.sql.functions as F

from pyspark import SparkContext, SparkConf
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, MinHashLSH
from pyspark.sql.types import StructType, StructField, StringType
import numpy as np
import sys
import os

from dotenv import load_dotenv

load_dotenv()

spark = SparkSession \
        .builder \
        .appName('Test')

if os.environ.get('MASTER_MODE') == 'cluster':
    HOST=os.environ.get('HOST')
    MASTER='spark://'+HOST+':7077'
    spark = spark.master(MASTER) \
            .config("spark.driver.host","192.168.56.1")


HDFS='hdfs://'+os.environ.get('HDFS')+':9000' # hdfs luon luon bat buoc

spark = spark.config("spark.kerberos.access.hadoopFileSystems",HDFS) \
        .getOrCreate()
        
headers = np.asarray(['user_id_orginal','item_id_orginal','time','lat','lon'])
history = {}

'''
{
    <userid>:{
        <filename>:{
            rootdf: <pyspark.dataframe>,
            info:{
                name: <dataframe name>,
                html: <html content>
            }
            stagesdf:{
                <no.stage>:{
                    maindf: <pyspark.dataframe>,
                    subdf:[<pyspark.dataframe>,...]
                }
                ...
            }
            stagesinfo:[
                {
                    maindf: {
                        name: <dataframe name>,
                        filename: <saved filename>,
                        html: <html content>
                    },
                    subdf: [{
                        name: <dataframe name>,
                        html: <html content>
                    },...]
                }
            ]
        }
        ...
    }
}
'''

def readdata(filename,header,headermap,user, delimiter=','): # đọc dataset chỉ khi upload
    
    global history

    df = spark.read.csv(user+'/upload/'+filename,sep = delimiter, header=header)
        
    
    headerlist = []

    for map,chosen in headermap.items():
        df = df.withColumnRenamed(df.columns[int(chosen)],headers[int(map)])
        headerlist.append(headers[int(map)])

    df = df.select(headerlist)
    df = df.withColumn('time', F.col('time').cast('timestamp').cast('long')) # dac biet cho gowalla
    
    newfilename = filename.split('.'+filename.split('.')[-1])[0]+'.csv'

    df.write.csv(user+'/init/'+newfilename,header="true", mode="overwrite")
    
    df = spark.read.csv(user+'/init/'+newfilename,header="true")
    delete_location(spark,user+'/upload/'+filename)
    
    history[user][newfilename] = {}
    history[user][newfilename] = {
        'rootdf': df,
        'info':{
            'name': 'Initial dataframe with converted time column',
            'currentstage': {'no': 0,'name':'init'},
            'savedname': newfilename,
            'html': df.limit(10).toPandas().to_html()
        },
        'stagesdf':[None]*4,
        'stagesinfo':[None]*4
    }
    return df, newfilename

'''
def changeDataType():
    test = df.withColumn("time", df["time"].cast("long")).na.drop(subset=("time"))
    test.show(5)
    test = df.withColumn("time", df["time"].cast("timestamp")).na.drop(subset=["time"])
    test.show(5)
    return test.dtypes
    if (timetype == 'String'):
        df = df.withColumn('time', F.col('time').cast('timestamp').cast('long')) # convert to epoch timestamp (seconds format) with specific format -> có thể phải cần input format từ frontend
    if (timetype == 'UNIX'):
        df = df.withColumn('time', F.col('time').cast('long')) # convert to epoch timestamp (seconds format) with specific format -> có thể phải cần input format từ frontend
    if (timetype == 'Timestamp'):
        df = df.withColumn('time', F.col('time').cast('long')) # convert to epoch timestamp (seconds format) with specific format -> có thể phải cần input format từ frontend

    newfilename = filename.split('.'+filename.split('.')[-1])[0]+'.csv'

    df.write.csv(user+'/init/'+newfilename,header="true", mode="overwrite")
    
    df = spark.read.csv(user+'/init/'+newfilename,header="true")
    delete_location(spark,user+'/upload/'+filename)
    
    
    return df, newfilename, dfold

def dropNA():


def fillMA():
'''
def kpercent(df,filename,user,k = 0.01,seed = 3):
    global history
    df = df.sample(fraction=k, seed=seed) #seed chinh la random state -> giữ tập random ko bị thay đổi

    currentfilename = filename.split('.')[0]+'_'+str(k)+'_'+str(seed)+'.csv'
    
    df.write.csv(user+'/ksample/'+currentfilename, header="true", mode="overwrite")
    
    history[user][filename]['stagesdf'][0]={
        'maindf': df
    }
    history[user][filename]['stagesinfo'][0]={
        'maindf': {
            'name': str(k*100)+'% sample from root',
            'currentstage': {'no': 1,'name':'ksample'},
            'savedname': currentfilename,
            'html': df.limit(10).toPandas().to_html()
        }
    }
    
    return df

def interactcount(df, target):
    df = df.groupby(target).count()
    return df

def n_core(dforg, root, currentfilename, user, limitnumber = 10):
    global history
    
    iteminteract = interactcount(dforg, 'item_id_orginal').where('count>10')
    df_joinsemi = dforg.join(iteminteract.withColumnRenamed('count','itemcount'),
                        on = ['item_id_orginal'],how = 'left_semi')


    userinteract = interactcount(df_joinsemi, 'user_id_orginal').where('count>10')
    df_joinsemi = df_joinsemi.join(userinteract.withColumnRenamed('count','usercount'),
                        on = ['user_id_orginal'],how = 'left_semi')
                        
                        
    currentfilename = currentfilename.split('.csv')[0]+'_'+str(limitnumber)+'.csv'

    df_joinsemi.write.csv(user+'/ncore/'+currentfilename, header="true", mode="overwrite")
    
    history[user][root]['stagesdf'][1]={
        'maindf': df_joinsemi,
        'subdf':[iteminteract,userinteract]
    }
    
    history[user][root]['stagesinfo'][1]={
        'maindf': {
            'name': 'Remove items then users less than '+str(limitnumber)+' as outliers',
            'currentstage': {'no': 2,'name':'ncore'},
            'savedname': currentfilename,
            'html': df_joinsemi.limit(10).toPandas().to_html()
        },
        'subdf': [
            {
                'name': 'Items that >'+str(limitnumber)+' interactions',
                'html': iteminteract.limit(10).toPandas().to_html()
            },
            {
                'name': 'Users that >'+str(limitnumber)+' interactions',
                'html': userinteract.limit(10).toPandas().to_html()
            }
        ]
    }
    return df_joinsemi

def remapid(df, orgcol):
    df= df.select(F.col(orgcol)).distinct()\
                        .rdd.zipWithIndex()\
                        .map(lambda x: (x[0][orgcol], x[1] + 1)).toDF(['org_id','remap_id'])
    return df

def mappingid(df, root, currentfilename, user,usermap = False, itemmap = False):
    global history

    if not (usermap or itemmap):
        usermap = remapid(df,'user_id_orginal').withColumnRenamed('remap_id','user_id')\
                                                .withColumnRenamed('org_id','user_id_orginal')
                                                
        
        itemmap = remapid(df,'item_id_orginal').withColumnRenamed('remap_id','item_id')\
                                                .withColumnRenamed('org_id','item_id_orginal')
        
        
    df_merge2ncore = df.join(itemmap, on = ['item_id_orginal'])\
                    .join(usermap, on = ['user_id_orginal'])
    if root != currentfilename:
        df_merge2ncore.write.csv(user+'/ncore/'+currentfilename, header="true", mode="overwrite")
    
    history[user][root]['stagesdf'][2]={
        'maindf': df_merge2ncore,
        'subdf':[itemmap,usermap]
    }
    
    history[user][root]['stagesinfo'][2]={
        'maindf': {
            'name': 'Remapping users and items ID',
            'currentstage': {'no': 3,'name':'mappingid'},
            'savedname': currentfilename,
            'html': df_merge2ncore.limit(10).toPandas().to_html()
        },
        'subdf':[
            {
                'name': 'Items remapped',
                'html': itemmap.limit(10).toPandas().to_html()
            },
            {
                'name': 'Users remapped',
                'html': usermap.limit(10).toPandas().to_html()
            }
        ]
    }
    
    
    return df_merge2ncore

def groupItem(df, modeltype = 'SB'):
    """
    convert to items list
    Args:
        df: dataframe with 3 columns (userid ,itemid, time), no array
        modeltype: type of model input (SB: social based, LB: location based)
    Return:
        pyspark dataframe: time sorted items list of users
    """
    df = (
        df
        .groupby("user_id")
        .agg(
            F.sort_array(F.collect_list(F.struct("time", "item_id")))
            .alias("collected_list")
        )
    )
    
    if modeltype == 'LB':
        df = df.withColumn("interactlist", F.col("collected_list.item_id")) \
        .drop('collected_list')
    
    return df
    
def train_valid_test_split(df, root, currentfilename, user,train=0.5,valid=0.3,test=0.2, modeltype = 'SB'):
    """
    split array of item
    Args:
        df: dataframe from groupItem()
        root: selected file name to refer to history dict
        modeltype: type of model input (SB: social based, LB: location based)
    Return:
        pyspark dataframe: time sorted items list of users
    """
    
    valid_test = valid + test 
    valid = valid / valid_test
    test = test / valid_Test
    
    splitdf = df.select('user_id', F.slice('interactlist', F.lit(1), F.round(F.size('interactlist') * train)).alias('train'),
    F.slice('interactlist', F.round(F.size('interactlist') * train) + 1, F.size('interactlist')).alias('valid_test'))
    
    splitdf = splitdf.withColumns({
        'valid' : F.slice('valid_test', F.lit(1), F.round(F.size('valid_test') * valid)),
        'test' : F.slice('valid_test', F.round(F.size('valid_test') * valid) + 1, F.size('valid_test'))
    })
    
    splitdf = df.select('user_id', F.concat(F.col('user_id'),F.lit(' '),F.concat_ws(" ",F.slice('interactlist', F.lit(1), F.round(F.size('interactlist') * train)))).alias('train'),
    F.concat(F.col('user_id'),F.lit(' '),F.concat_ws(" ",F.slice('interactlist', F.round(F.size('interactlist') * train) + 1, F.size('interactlist'))).alias('valid_test'))
    
    splitdf = splitdf.select('train',F.concat(F.col('user_id'),F.lit(' '),F.concat_ws(" ",F.slice('valid_test', F.lit(1), F.round(F.size('valid_test') * valid)))).alias('valid'),
    F.concat(F.col('user_id'),F.lit(' '),F.concat_ws(" ",F.slice('valid_test', F.round(F.size('valid_test') * valid) + 1, F.size('valid_test'))).alias('test'))
    
    # save train set
    trainpath = currentfilename.split('.csv')[0]+'_train_'+str(train*100)+str(valid*100)+str(test*100)+'.csv'
    splitdf.select('train').write.csv(user+'/split/'+trainpath,header=False,mode='overwrite')
    
    # save valid set
    validpath = currentfilename.split('.csv')[0]+'_validation_'+str(train*100)+str(valid*100)+str(test*100)+'.csv'
    splitdf.select('valid').write.csv(user+'/split/'+validpath,header=False,mode='overwrite')
    
    # save test set
    testpath = currentfilename.split('.csv')[0]+'_test_'+str(train*100)+str(valid*100)+str(test*100)+'.csv'
    splitdf.select('test').write.csv(user+'/split/'+testpath,header=False,mode='overwrite')
    
    
    history[user][root]['stagesdf'][3]={
        'maindf': splitdf
    }
    
    history[user][root]['stagesinfo'][3]={
        'maindf': {
            'name': 'Split data set to train, valid, test to '+" ".join(str(i*100) for i in [train,valid,test]),
            'currentstage': {'no': 4,'name':'split'},
            'savedname': trainpath+"<br/>"+validpath+"<br/>"+testpath,
            'html': splitdf.limit(10).toPandas().to_html()
        }
    }
    
    return splitdf


def jaccardD2(df, root, currentfilename, user, limit = 0.25, numHashTables = 1, numFeatures = False, modeltype = 'SB'):
    global history
    model = ''
    
    if numFeatures:
        model = Pipeline(stages=[
        HashingTF(inputCol="interactlist", outputCol="vectors", numFeatures = numFeatures),
        MinHashLSH(inputCol="vectors", outputCol="lsh", numHashTables= numHashTables)
                        ]).fit(df)
    else:
        model = Pipeline(stages=[
        HashingTF(inputCol="interactlist", outputCol="vectors"),
        MinHashLSH(inputCol="vectors", outputCol="lsh", numHashTables= numHashTables)
                        ]).fit(df)



    dfhash = model.transform(df)
    
    
    dfjaccard = model.stages[-1].approxSimilarityJoin(dfhash,dfhash, limit ,distCol="JaccardDistance")\
                                                        .filter("datasetA.user_id < datasetB.user_id")
    
    
    return dfhash, dfjaccard.select(F.col('datasetA.user_id').alias('userA'),
                    F.col('datasetB.user_id').alias('userB'),
                    F.col('JaccardDistance'))



from typing import List

from py4j.java_gateway import JavaObject
# access hadoop filesystem api through pyspark

def configure_hadoop(spark: SparkSession):
    hadoop = spark.sparkContext._jvm.org.apache.hadoop  # type: ignore
    conf = hadoop.conf.Configuration()
    fs = hadoop.fs.FileSystem.get(conf)
    return hadoop, conf, fs


def ensure_exists(spark: SparkSession, file: str):
    hadoop, _, fs = configure_hadoop(spark)
    if not fs.exists(hadoop.fs.Path(file)):
        out_stream = fs.create(hadoop.fs.Path(file, False))
        out_stream.close()


def delete_location(spark: SparkSession, location: str):
    hadoop, _, fs = configure_hadoop(spark)
    if fs.exists(hadoop.fs.Path(location)):
        fs.delete(hadoop.fs.Path(location), True)


def get_files(spark: SparkSession, src_dir: str) -> List[JavaObject]:
    """Get list of files in HDFS directory"""
    hadoop, _, fs = configure_hadoop(spark)
    ensure_exists(spark, src_dir)
    files = []
    for f in fs.listStatus(hadoop.fs.Path(src_dir)):
        if f.isFile():
            files.append(f.getPath())
    if not files:
        raise ValueError("Source directory {} is empty".format(src_dir))

    return files


def copy_merge_into(
    spark: SparkSession, src_dir: str, dst_file: str, delete_source: bool = True, overwrite = False
):
    """Merge files from HDFS source directory into single destination file

    Args:
        spark: SparkSession
        src_dir: path to the directory where dataframe was saved in multiple parts
        dst_file: path to single file to merge the src_dir contents into
        delete_source: flag for deleting src_dir and contents after merging

    """
    hadoop, conf, fs = configure_hadoop(spark)

    # 1. Get list of files in the source directory
    files = get_files(spark, src_dir)

    # 2. Set up the 'output stream' for the final merged output file
    # if destination file already exists, add contents of that file to the output stream
    if fs.exists(hadoop.fs.Path(dst_file)):
        tmp_dst_file = dst_file + ".tmp"
        tmp_in_stream = fs.open(hadoop.fs.Path(dst_file))
        tmp_out_stream = fs.create(hadoop.fs.Path(tmp_dst_file), True)
        try:
            hadoop.io.IOUtils.copyBytes(
                tmp_in_stream, tmp_out_stream, conf, False
            )  # False means don't close out_stream
        finally:
            tmp_in_stream.close()
            tmp_out_stream.close()

        tmp_in_stream = fs.open(hadoop.fs.Path(tmp_dst_file))
        out_stream = fs.create(hadoop.fs.Path(dst_file), True)
        try:
            hadoop.io.IOUtils.copyBytes(tmp_in_stream, out_stream, conf, False)
        finally:
            tmp_in_stream.close()
            fs.delete(hadoop.fs.Path(tmp_dst_file), False)
    # if file doesn't already exist, create a new empty file
    else:
        out_stream = fs.create(hadoop.fs.Path(dst_file), False)

    # 3. Merge files from source directory into the merged file 'output stream'
    try:
        for file in files:
            in_stream = fs.open(file)
            try:
                hadoop.io.IOUtils.copyBytes(
                    in_stream, out_stream, conf, False
                )  # False means don't close out_stream
            finally:
                in_stream.close()
    finally:
        out_stream.close()
    
    fs.moveToLocalFile(hadoop.fs.Path(dst_file), hadoop.fs.Path(dst_file))
    
    # 4. Tidy up - delete the original source directory
    if delete_source:
        delete_location(spark, src_dir)
