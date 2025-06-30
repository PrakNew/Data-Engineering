#importing relevant libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#creating user-defined functions
# 1. Calculate total cost
def calc_total_cost(items,t_type):
    total_cost=0
    for item in items:
        total_cost+=item['unit_price']*item['quantity']
    if t_type=='RETURN':
        return total_cost*-1
    else:
        return total_cost

# 2. Calculate total items
def calc_total_items(items):
    item_count=0
    for item in items:
        item_count+=item['quantity']
    return item_count

# 3. Check if the transaction is an ORDER
def is_order(t_type):
    if t_type=='ORDER':
        return 1
    else:
        return 0

# 4. Check if the transaction is a RETURN
def is_return(t_type):
    if t_type=='RETURN':
        return 1
    else:
        return 0

#Starting Spark Session
spark=SparkSession.builder.appName("spark-streaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

#Fetching data from kafka
lines=spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers","18.211.252.152:9092")\
    .option("startingOffsets","earliest")\
    .option("failOnDataLoss","false")\
    .option("subscribe","real-time-project")\
    .load()

#defining schema for incoming messages
jsonSchema = StructType() \
    .add("invoice_no", LongType()) \
    .add("country", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("type", StringType()) \
    .add("items", ArrayType(StructType([
        StructField("SKU", StringType()),
        StructField("title", StringType()),
        StructField("unit_price", FloatType()),
        StructField("quantity", IntegerType())
    ])))

#converting the json stream messages to Spark Dataframe
orders = lines.select(from_json(col("value").cast("string"), 
                                    jsonSchema).alias("data")).select("data.*")

#Creating UDFs for applying on the dataframe
total_cost_udf = udf(calc_total_cost, DoubleType())
total_items_udf = udf(calc_total_items, IntegerType())
is_order_udf = udf(is_order, IntegerType())
is_return_udf = udf(is_return, IntegerType())

#apply UDFs on Spark Dataframe 'orders'
orders_df = orders \
    .withColumn("total_cost", total_cost_udf(orders.items, orders.type)) \
    .withColumn("total_items", total_items_udf(orders.items)) \
    .withColumn("is_order", is_order_udf(orders.type)) \
    .withColumn("is_return", is_return_udf(orders.type))

#writing the data to console
streaming_query = orders_df \
    .select("invoice_no", "country", "timestamp", "total_cost", "total_items", "is_order", "is_return") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime = "1 minute") \
    .start()

#aggregating by time
time_agg = orders_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp", "1 minute", "1 minute"))\
    .agg(sum("total_cost").alias("total_sales_volume"),
         count("invoice_no").alias("OPM"),
         avg("is_return").alias("rate_of_return"),
         avg("total_cost").alias("average_transaction_size"))\
    .select("window", "OPM", "total_sales_volume", "average_transaction_size","rate_of_return")

#writing the time-aggregated data to HDFS
agg_time_stream_query = time_agg.writeStream \
    .format('json')\
    .outputMode("append")\
    .option("truncate","false")\
    .option("path","/user/ec2-user/Timebased-KPI")\
    .trigger(processingTime="1 minute")\
    .start()

#aggragating by time and country
time_country_agg = orders_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp", "1 minute", "1 minute"), "country")\
    .agg(sum("total_cost").alias("total_sales_volume"),
         count("invoice_no").alias("OPM"),
         avg("is_return").alias("rate_of_return")) \
    .select("window", "country", "OPM", "total_sales_volume", "rate_of_return")

#writing time and country aggregated data to HDFS
agg_country_stream_query = time_country_agg.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate","false") \
    .option("path","/user/ec2-user/Country-and-timebased-KPI") \
    .trigger(processingTime="1 minute") \
    .start()

#waiting for termination
streaming_query.awaitTermination()
agg_time_stream_query.awaitTermination()
agg_country_stream_query.awaitTermination()