
from pyspark.sql import SparkSession
from pyspark.sql.types import * 
import pyspark.sql.functions as f 

#create static data 
data =  [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter","LinkedIn"]],
[2, "Brooke","Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter",
"LinkedIn"]],
[3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web",
"twitter", "FB", "LinkedIn"]],
[4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568, 
["twitter", "FB"]],
[5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web",
"twitter", "FB", "LinkedIn"]],
[6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, 
["twitter", "LinkedIn"]]
]

#define the data schema

# schema ="`Id` INT, `First` STRING, `Last` STRING, `Url` STRING, `Published` STRING, `Hits` INT, `Campaigns` ARRAY<STRING>"

schema = StructType(
    [
        StructField('Id', IntegerType(), False),
        StructField('First', StringType(), False),
        StructField('Last', StringType(), False),
        StructField('Url', StringType(), False),
        StructField('Published', StringType(), False),
        StructField('Hits', IntegerType(), False),
        StructField('Campaigns', ArrayType(StringType(), True), False),
    ]
) 
# main program

if __name__ == '__main__' : 

    
    ## create a spark session 
    spark = SparkSession\
            .builder\
            .appName('First Example')\
            .master('spark://172.22.0.2:7077')\
            .getOrCreate()

    df = spark.createDataFrame(data, schema)

    df.show(truncate= False)


    print('--- Dataframe Sorted by "Hits" DESC')

    df.sort(f.col("Hits").desc()).show(truncate= False)

    print('--- Dataframe Sorted by "Hits" ASC')
    
    df.sort(f.col("Hits").asc()).show(truncate= False)
    print(df.schema)   # could be used to create another dataframe within the same schema 

    print('--- FAmous actors ---')
    
    df.withColumn('IsFamous',f.expr("Hits > 10000")).show(truncate= False)

    df.printSchema()   # display the dataframe schema 

    spark.stop()


# import sys


# if __name__ == '__main__' : 
        
#         print(f"hello {sys.argv[1]} !!!")
