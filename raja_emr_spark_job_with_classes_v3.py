#!/usr/bin/env python
# coding: utf-8

# In[42]:

from asyncore import socket_map
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType
from pyspark.sql import functions as f
from pyspark.context import SparkContext
import sys
import pyspark.sql.utils

# In[43]:

spark = SparkSession.builder.appName("Masking").getOrCreate()
spark.sparkContext.addPyFile("s3://raja-s3-ohio-landingzone/Files/Delta/delta-core_2.12-0.8.0.jar")
from delta.tables import *

# In[44]:


class Config:
    def __init__(self):
        self.jsonData = self.read_config()
        self.ingest_actives_source = self.jsonData['ingest-actives']['source']['data-location']
        self.ingest_actives_dest = self.jsonData['ingest-actives']['destination']['data-location']
        self.ingest_viewership_source = self.jsonData['ingest-viewership']['source']['data-location']
        self.ingest_viewership_dest = self.jsonData['ingest-viewership']['destination']['data-location']
        self.transformation_cols_actives = self.jsonData['masked-actives']['transformation-cols']
        self.transformation_cols_viewership = self.jsonData['masked-viewership']['transformation-cols']
        self.ingest_raw_actives_source = self.jsonData['masked-actives']['source']['data-location']
        self.ingest_raw_viewership_source = self.jsonData['masked-viewership']['source']['data-location']
        self.masking_col_actives= self.jsonData['masked-actives']['masking-cols']
        self.masking_col_viewership= self.jsonData['masked-viewership']['masking-cols']
        self.dest_staging_actives= self.jsonData['masked-actives']['destination']['data-location']
        self.dest_staging_viewership= self.jsonData['masked-viewership']['destination']['data-location']
        self.actives_partition_cols= self.jsonData['masked-actives']['partition-cols']
        self.viewership_partition_cols= self.jsonData['masked-viewership']['partition-cols']
        self.lookupDataLocation= self.jsonData['lookup-dataset']['data-location']
        self.piiColumns= self.jsonData['lookup-dataset']['pii-cols']
        
    def read_config(self):
        configData = spark.sparkContext.textFile("s3://raja-s3-ohio-landingzone/Config/app-config.json").collect()
        dataString = ''.join(configData)
        jsonData = json.loads(dataString)
        return jsonData
        
    


# In[45]:


class Transform:
    #Function for Reading the data from source
    def read_data(self,path,format):
        df=spark.read.parquet(path)
        return df
    
    # Function for writting the data to destination
    def write_data(self,df,path,format):
        df.write.mode("overwrite").parquet(path)
        
    def writeDataByPartition(self,df,path,format,colName=[]):
        df.write.mode("overwrite").partitionBy(colName[0],colName[1]).parquet(path)
    
    #Function for transform and type casting 
    def transform(self,df,cast_dict):
        key_list = []
        for key in cast_dict.keys():
            key_list.append(key)

        for column in key_list:
            if cast_dict[column].split(",")[0] == "DecimalType":
                df = df.withColumn(column,df[column].cast(DecimalType(scale=int(cast_dict[column].split(",")[1]))))
            elif cast_dict[column] == "ArrayType-StringType":
                df = df.withColumn(column,f.concat_ws(",",f.col(column)))
        return df
    
    #Funstion for masking
    def masking_data(self,df,column_list):
        for column in column_list:
            df = df.withColumn("Masked_"+column,f.sha2(f.col(column),256))
        return df
    
    def scdDataset(self,df,lookupDataLocation,piiColumns,datasetName):
        df_source = df.withColumn("begin_date",f.current_date())
        df_source = df_source.withColumn("update_date",f.lit("null"))
        pii_cols = [i for i in piiColumns if i in df.columns]
        columns_needed = []
        insert_dict = {}
        
        for col in pii_cols:
            if col in df.columns:
                columns_needed += [col,"Masked_"+col]
        
        source_columns_used = columns_needed + ['begin_date','update_date']
        df_source = df_source.select(*source_columns_used)
        try:
            targetTable = DeltaTable.forPath(spark,lookupDataLocation+datasetName)
            delta_df = targetTable.toDF()
        except pyspark.sql.utils.AnalysisException:
            print('Table does not exist')
            df_source = df_source.withColumn("flag_active",f.lit("true"))
            df_source.write.format("delta").mode("overwrite").save(lookupDataLocation+datasetName)
            print('Table Created Sucessfully!')
            targetTable = DeltaTable.forPath(spark,lookupDataLocation+datasetName)
            delta_df = targetTable.toDF()
            delta_df.show(100)
        
        for i in columns_needed:
            insert_dict[i] = "updates."+i
        
        insert_dict['begin_date'] = f.current_date()
        insert_dict['flag_active'] = "True" 
        insert_dict['update_date'] = "null"
        
        _condition ="dtName.flag_active == true AND "+" OR ".join(["updates."+i+" <> "+ "dtName."+i for i in [x for x in columns_needed if x.startswith("Masked_")]])
        column = ",".join(["dtName"+"."+i for i in [x for x in pii_cols]])
        print('..._condition',_condition)
        
        updatedColumnsToInsert = df_source.alias("updates").join(targetTable.toDF().alias("dtName"), pii_cols).where(_condition)
        
        stagedUpdates = (
          updatedColumnsToInsert.selectExpr('NULL as mergeKey',*[f"updates.{i}" for i in df_source.columns]).union(df_source.selectExpr("concat("+','.join([x for x in pii_cols])+") as mergeKey", "*")))
        
        targetTable.alias("dtName").merge(stagedUpdates.alias("updates"),"concat("+str(column)+") = mergeKey").whenMatchedUpdate(
            condition = _condition,
            set = {
                # Set current to false and endDate to source's effective date."flag_active" : "False",
                "update_date" : f.current_date()
            }
            ).whenNotMatchedInsert(values = insert_dict).execute()
        for i in pii_cols:
            df = df.drop(i).withColumnRenamed("Masked_"+i, i)
        
        return df
    


if __name__ == "__main__":
    config_obj=Config()
    trans_obj=Transform()
    
    activeFN = config_obj.ingest_actives_source.split("/")[-1]+".parquet"
    viewerFN = config_obj.ingest_viewership_source.split("/")[-1]+".parquet"
    
    #Reading the data of actives & viewership  parquet files from landing zone
    Data_actives=trans_obj.read_data(config_obj.ingest_actives_source,"parquet")
    Data_viewership=trans_obj.read_data(config_obj.ingest_viewership_source,"parquet")
    
    #Write the data to raw zone
    trans_obj.write_data(Data_actives,config_obj.ingest_actives_dest,"parquet")
    trans_obj.write_data(Data_viewership,config_obj.ingest_viewership_dest,"parquet")

    #Reading the data of actives and viewership parquet files from raw  zone
    Data_raw_actives=trans_obj.read_data(config_obj.ingest_raw_actives_source ,"parquet")
    Data_raw_viewership=trans_obj.read_data(config_obj.ingest_raw_viewership_source ,"parquet")

    #Transformation on actives & viewership file
    Data_staging_actives=trans_obj.transform(Data_raw_actives,config_obj.transformation_cols_actives)
    Data_staging_viewership=trans_obj.transform(Data_raw_viewership,config_obj.transformation_cols_viewership)

    #Masking on actives & viewership file
    masked_actives=trans_obj.masking_data(Data_staging_actives,config_obj.masking_col_actives)
    masked_viewership=trans_obj.masking_data(Data_staging_viewership,config_obj.masking_col_viewership)
    
    dfActives = trans_obj.scdDataset(masked_actives,config_obj.lookupDataLocation,config_obj.piiColumns,activeFN)
    dfViewer = trans_obj.scdDataset(masked_viewership,config_obj.lookupDataLocation,config_obj.piiColumns,viewerFN)
    
    #Write the data of masked actives & viewership data  into staging zone
    trans_obj.writeDataByPartition(dfActives,config_obj.dest_staging_actives,"parquet",config_obj.actives_partition_cols)
    trans_obj.writeDataByPartition(dfViewer,config_obj.dest_staging_viewership,"parquet",config_obj.viewership_partition_cols)
    
    spark.stop()





