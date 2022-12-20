


import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans, BisectingKMeans

import sklearn
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, Tokenizer
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from pyspark.ml.linalg import Vectors
from pyspark.sql import functions as F
import datetime
from datetime import datetime
from sklearn.preprocessing import LabelEncoder
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressionModel
from sklearn.preprocessing import StandardScaler
from pyspark.ml.tuning import TrainValidationSplitModel
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import StringIndexer
from pyspark.sql.types import IntegerType
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.pipeline import PipelineModel
from pyspark.streaming import StreamingContext
import requests

KAFKA_TOPIC_NAME = "test2"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
# CHECKPOINT_LOCATION = "LOCAL DIRECTORY LOCATION (FOR DEBUGGING PURPOSES)"
CHECKPOINT_LOCATION = "/content/sample_data"

if __name__ == "__main__":

    # Creating spark session 

    spark = (
        SparkSession.builder
        .appName("Kaf")
        .master("local[2]")
        .getOrCreate()
        # .config("spark.jars", "postgresql-42.4.0.jar")
        

    )
    spark.sparkContext.setLogLevel("ERROR")
    ssc = StreamingContext(spark, 1)
    # Reading Incoming data from Kafka Topic

    sampleDataframe = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_NAME)
        .load()
    )

    base_df = sampleDataframe.selectExpr("CAST(value as STRING)")
    base_df.printSchema()

    # Describing the Schema for the data

    sample_schema = (
        StructType()
        .add("id", StringType())
        .add("speed", StringType())
        .add("st", StringType())
        .add("name", StringType())
        .add("date", StringType())


    )

    info_dataframe = base_df.select(
        from_json(col("value"), sample_schema).alias("info"))
    # Printing the shcema
    info_dataframe.printSchema()
    info_df_fin = info_dataframe.select("info.*")
    #Describing the format for the data
    format = "yyyy-MM-dd'T'HH:mm:ss.SSSZ"

    #Converting all the data to correct DataType
    info_df_fin = info_df_fin.withColumn(
        "id", info_df_fin["id"].cast(IntegerType()))
    info_df_fin = info_df_fin.withColumn(
        "st", info_df_fin["st"].cast(IntegerType()))
    info_df_fin = info_df_fin.withColumn(
        "speed", info_df_fin["speed"].cast(IntegerType()))
    info_df_fin = info_df_fin.withColumn(
        "name", info_df_fin["name"].cast(IntegerType()))
    #Converting DAteTime data to Timestamp format
    info_df_fin = info_df_fin.withColumn(
        'timestamp', F.unix_timestamp('date', format).cast('timestamp'))
    #Extracting Date
    info_df_fin = info_df_fin.withColumn("hour", hour("date")+1)
    #Extracting min
    info_df_fin = info_df_fin.withColumn("min", minute("date"))
    ##Extracting Day of Week
    info_df_fin = info_df_fin.withColumn("day", dayofweek("date"))
    #verify the data types
    info_df_fin.printSchema()

    #dropping unwanted columns
    info_df_fin = info_df_fin.drop("timestamp", 'date')

    #Vectorize the data 
    assembler = VectorAssembler().setInputCols(
        info_df_fin.columns[1:]).setOutputCol('features').setHandleInvalid("keep")
    info_df_fin = assembler.transform(info_df_fin)
    #dropping unwanted columns
    info_df_fin = info_df_fin.drop(
        "timestamp", "date", "id", "st", "speed", "name", "hour", "min", "day")

    #Load the Pre trained model 
    model = DecisionTreeRegressionModel.load(
        'C:/Users/vikra/OneDrive/Documents/model')
    #Predict on current data
    df = model.transform(info_df_fin)
#--------------------------------*****************************************----------------------------------

    def foreach_batch_function(df,epoch_id):
    
        df=df.select('prediction')
       # print(df.collect()[0])
       # print(df.toJSON().first())
        x = requests.post('http://localhost:3000/pred', json = {'id': df.collect()[0] } )

        # df.write\
        #     .outputMode("complete") \
        #     .format("console") \
        #     .start() \
        #     .awaitTermination()






#--------------------------------*****************************************----------------------------------

    # For Each batch write to Postgres
    # def foreach_batch_function(df, epoch_id):
    #     df = df.select('prediction')
    #     df.write \
    #         .format("jdbc") \
    #         .mode('append') \
    #         .option("url", "jdbc:postgresql://localhost:1998/sprkl")\
    #         .option("driver", "org.postgresql.Driver") \
    #         .option("dbtable", "data") \
    #         .option("user", "postgres")\
    #         .option("password", "parmar98") \
    #         .save()

    # for Each Batch send to function
    df.writeStream.foreachBatch(
        foreach_batch_function).start().awaitTermination()
