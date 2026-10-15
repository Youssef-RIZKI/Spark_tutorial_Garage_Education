
from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
import time 
#create static data 
data =   [("Brooke", 20), ("Denny", 31), ("Jules", 30), 
("TD", 35), ("Brooke", 25)]


#define the data schema
schema ="`name` STRING, `age` INT"





# main program

if __name__ == '__main__' : 

    print("before creating a session : ")
    ## create a spark session 
    spark = SparkSession\
            .builder\
            .appName('Manipulating dataframes : ')\
            .master('spark://172.22.0.2:7077')\
            .getOrCreate()
    

    print('creating a data frame from static data ...\n\n')
    df_blogs = spark.createDataFrame(data, schema)
    time.sleep(2)
    print(f'the dataframe is created successfully !!\n\n')
    df_blogs.show()
    
    

    print('Aggregating (average)...\n\n')
    df_blogs_aggregated = df_blogs.groupBy('name').agg(F.avg('age').alias('age_avg'))
    time.sleep(2)
    print(f'the dataframe aggregation is the following : \n')
    df_blogs_aggregated.show()
    
    print('Ispecting the spark execution plan ...\n\n')
    df_blogs_aggregated.explain()
    time.sleep(60)

    spark.stop()


