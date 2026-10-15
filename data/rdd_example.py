
from pyspark.sql import SparkSession
import time 
#create static data 
data =  [("Brooke", 20), ("Denny", 31), ("Jules", 30), 
("TD", 35), ("Brooke", 25)]

#define the data schema
# schema ="`Name` STRING, `Age` INT"

#  No schema is defined for rdds !!!



# main program

if __name__ == '__main__' : 

    
    ## create a spark session 
    spark = SparkSession\
            .builder\
            .appName('Manipulating RDDs : ')\
            .master('spark://172.22.0.2:7077')\
            .getOrCreate()
    

    print('creating an RDD from static data ...\n\n')
    rdd_data = spark.sparkContext.parallelize(data)
    time.sleep(2)
    print(f'the {rdd_data.collect()} rdd is created successfully !!\n\n')

    
    print('Mapping ...\n\n')
    rdd_mapped = rdd_data.map(lambda x : ( x[0], (x[1], 1) ))
    time.sleep(2)
    print(f'the result of mapping is : {rdd_mapped.collect()} \n\n')


    print('Reducing ...\n\n')
    rdd_reduced = rdd_mapped.reduceByKey(lambda x, y : ( x[0] + y[0], x[1] + y[1] ))
    time.sleep(2)
    print(f'the reduced data is {rdd_reduced.collect()} \n\n')

    print('Aggregating ...\n\n')
    rdd_aggregated = rdd_reduced.map(lambda x : ( x[0], x[1][0]/x[1][1] ))
    time.sleep(2)
    print(f'the aggregated (avg) data is {rdd_aggregated.collect()} \n\n')
    
    # rdd_reduced = rdd_reduced.map(lambda x : )

    time.sleep(60)

    

    spark.stop()


