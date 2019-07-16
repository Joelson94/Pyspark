#DATA420-19S1 Assignment 1
#Import the required classes

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql import functions as F

#Get the default configurations
spark.sparkContext._conf.getAll()

#Update the default configurations
conf = spark.sparkContext._conf.setAll([('spark.executor.memory', '4g'), ('spark.app.name', 'Spark Updated Conf'), ('spark.executor.cores', '2'), ('spark.executor.instances', '4'), ('spark.driver.memory','4g')])

N = int(conf.get("spark.executor.instances"))
M = int(conf.get("spark.executor.cores"))
partitions = 4 * N * M

#Create a Spark Session
sc = SparkSession.builder.config(conf=conf).getOrCreate()
#2b
schema_daily = StructType([
    StructField('id', StringType()),
    StructField('date', StringType()),
    StructField('element', StringType()),
    StructField('value', IntegerType()),
    StructField('measurement flag', StringType()),
    StructField('quality flag', StringType()),
    StructField('source flag', StringType()),
    StructField('observation time', StringType()),
])

daily = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2017.csv.gz")
    .limit(1000)
)
daily.cache()
daily.show(10, False)
'''
df4 = spark.sparkContext.textFile("/data/ghcnd/daily/2017.csv.gz")
parts = df4.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(id=p[0],date=p[1],element=p[2],value=p[3],measurement_flag=p[4],quality_flag=p[5],source_flag=p[6],observation_time=p[7]))
daily_2017 = spark.createDataFrame(people)
daily_2017 = daily_2017.limit(1000)

daily_2017 =daily_2017.select(to_date($"date", "dd-MMM-yyyy"))
daily_2017_u = daily_2017.select('date', from_unixtime(unix_timestamp('date', 'dd-MMM-yyyy')))
'''
#2c
df = spark.read.text('/data/ghcnd/stations')
stations = df.select(
    df.value.substr(1,11).alias('id'),
    df.value.substr(13,8).cast(DoubleType()).alias('latitude'),
    df.value.substr(22,9).cast(DoubleType()).alias('longitude'),
    df.value.substr(32,6).cast(DoubleType()).alias('elevation'),
    df.value.substr(39,2).alias('state'),
    df.value.substr(42,30).alias('name'),
    df.value.substr(73,3).alias('gsn_flag'),
    df.value.substr(77,3).alias('hcn_crn_flag'),
    df.value.substr(81,5).alias('wmo_id'),
)

df1 = spark.read.text('/data/ghcnd/countries')
countries = df1.select(
	df1.value.substr(1,2).alias('code'),
	df1.value.substr(4,47).alias('name'),
)

df2 = spark.read.text('/data/ghcnd/states')
states = df2.select(
	df2.value.substr(1,2).alias('code'),
	df2.value.substr(4,47).alias('name'),
)

df3 = spark.read.text('/data/ghcnd/inventory')
inventory = df3.select(
	df3.value.substr(1,11).alias('id'),
    df3.value.substr(13,8).cast(DoubleType()).alias('latitude'),
    df3.value.substr(22,9).cast(DoubleType()).alias('longitude'),
    df3.value.substr(32,4).alias('element'),
    df3.value.substr(37,4).cast('integer').alias('firstyear'),
    df3.value.substr(42,4).cast('integer').alias('lastyear'),
)
stations.where(F.col("wmo_id").cast(IntegerType()).isNull()).count()
#95595
#3a
stations = df.select(
    df.value.substr(1,2).alias('country_code'),
    df.value.substr(1,11).alias('id'),
    df.value.substr(13,8).cast(DoubleType()).alias('latitude'),
    df.value.substr(22,9).cast(DoubleType()).alias('longitude'),
    df.value.substr(32,6).cast(DoubleType()).alias('elevation'),
    df.value.substr(39,2).alias('state'),
    df.value.substr(42,30).alias('name'),
    df.value.substr(73,3).alias('gsn_flag'),
    df.value.substr(77,3).alias('hcn_crn_flag'),
    df.value.substr(81,5).alias('wmo_id'),
)

##stations.rdd.saveAsHadoopFile("//user/srs122/outputs/ghcnd/stations",'org.apache.hadoop.mapred.TextOutputFormat')
#stations.rdd.saveAsTextFile("/user/srs122/outputs/ghcnd/stations")
#3b
stations.registerTempTable('stations_tbl')
countries.registerTempTable('countries_tbl')
join1_sql = """ SELECT * from stations_tbl LEFT JOIN countries_tbl ON stations_tbl.country_code = countries_tbl.code"""
join1 = spark.sql(join1_sql)
join1.show()
#3c      
stations_US = stations.filter("country_code =='US'")
states.registerTempTable('states_tbl')
stations_US.registerTempTable('stations_US_tbl')
join2_sql = """ SELECT * from stations_US_tbl LEFT JOIN states_tbl ON stations_US_tbl.state = states_tbl.code"""
join2 = spark.sql(join2_sql)
join2.show()
#3d
from pyspark.sql import functions as F

counts = (
    inventory
    # Select only the columns that are needed
    .select(['id','firstyear','lastyear'])
    # Group by source and count destinations
    .groupBy('id')
    .agg({'lastyear': 'max','firstyear': 'min'})
    .orderBy('id', ascending=True)
    .select(
        F.col('id'),
        F.col('min(firstyear)').alias('firstyear'),
        F.col('max(lastyear)').alias('lastyear')
    )
)

counts2 = (
    inventory
    # Select only the columns that are needed
    .select(['id','element'])
    # Group by source and count destinations
    .groupBy('id')
    .agg({'element': 'count'})
    .orderBy('count(element)', ascending=False)
    .select(
        F.col('id'),
        F.col('count(element)').alias('element_count')
    )
)
# By adding 'element' to the groupby we can determine that there are no duplicates for each element of each inventory

inventory2 = inventory.withColumn("core_flag", F.rand())
inventory2 = inventory.withColumn("prcp_flag", F.rand())
inventory2.show()

inventory2 = inventory2.withColumn("core_flag",
                           F.when(
                               (F.col('element') == "TMAX")
                                | (F.col('element') == "TMIN")
                                | (F.col('element') == "PRCP")
                                | (F.col('element') == "SNOW")
                                | (F.col('element') == "SNWD"),
                               1
                           ).otherwise(0)
                           )

inventory2 = inventory2.withColumn("prcp_flag",
                           F.when(
                               (F.col('element') == "PRCP"),
                               1
                           ).otherwise(0)
                           )

counts3 = (
    inventory2
    # Select only the columns that are needed
    .select(['id','element','core_flag','prcp_flag'])
    # Group by source and count destinations
    .groupBy('id')
    .agg({'element': 'count','core_flag':'sum','prcp_flag':'sum'})
    .orderBy('id', ascending=True)
    .select(
        F.col('id'),
        F.col('count(element)').alias('element_count'),
        F.col('sum(core_flag)').alias('core_elements'),
        F.col('sum(prcp_flag)').alias('prcp_elements')
    )
)


counts3 = counts3.withColumn("other_elements",F.col('element_count')-F.col('core_elements'))

counts3.filter(counts3["core_elements"] == 5).count()
counts3.filter(counts3["prcp_elements"] == 1).count()

counts4 = inventory2.groupby("id").agg(F.collect_set("element").alias("element_collection"))

counts3.registerTempTable('counts3_tbl')
counts4.registerTempTable('counts4_tbl')
join3_sql = """ SELECT counts3_tbl.*,counts4_tbl.element_collection from counts3_tbl JOIN counts4_tbl ON counts3_tbl.id = counts4_tbl.id"""
join3 = spark.sql(join3_sql)
join3.show()
#3e
join3.registerTempTable('join3_tbl')
join4_sql = """ SELECT stations_tbl.*,join3_tbl.element_count,join3_tbl.core_elements,join3_tbl.prcp_elements,join3_tbl.other_elements,join3_tbl.element_collection from stations_tbl LEFT JOIN join3_tbl ON stations_tbl.id = join3_tbl.id"""
join4 = spark.sql(join4_sql)
join4.show()
join4.write.parquet("/user/jth133/outputs/ghcnd/Station_join_4e.parquet")
#3f
join4.registerTempTable('join4_tbl')
daily.registerTempTable('daily_tbl')
join5_sql = """ SELECT daily_tbl.*,
    join4_tbl.latitude,join4_tbl.longitude,join4_tbl.elevation,join4_tbl.state,join4_tbl.name,
    join4_tbl.gsn_flag,join4_tbl.hcn_crn_flag,join4_tbl.wmo_id,join4_tbl.element_count,join4_tbl.core_elements,
    join4_tbl.prcp_elements,join4_tbl.other_elements,join4_tbl.element_collection 
    from daily_tbl
    LEFT JOIN join4_tbl ON daily_tbl.id = join4_tbl.id"""
join5 = spark.sql(join5_sql)
join5.show()


#Analysis
#q1 a)


b = (
    daily
    .select(["id","date"])
    .filter(daily.date
    .startswith("2017")))

b.distinct().count()

stations.distinct().count()

#part a
gsnStations = (
    stations
    .select(['id','gsn_flag'])
    .filter(stations["gsn_flag"].rlike("GSN"))
    )
gsnStations.count()
#991

hcnStations = (
    stations
    .select(['id','hcn_crn_flag'])
    .filter(stations["hcn_crn_flag"].rlike("HCN"))
    )
hcnStations.count()
#1218

crnStations = (
    stations
    .select(['id','hcn_crn_flag'])
    .filter(stations["hcn_crn_flag"].rlike("CRN"))
    )
crnStations.count()
#230

#commonStations = (
#    stations
#    .select(['id','gsn_flag','hcn_crn_flag'])
#    .filter(stations["id"].isin[gsnStations,hcnStations,crnStations])
#    )
#commonStations.count()

gsnStations.registerTempTable('gsnStations')
hcnStations.registerTempTable('hcnStations')
join7_sql = """ SELECT gsnStations.id from gsnStations JOIN hcnStations ON gsnStations.id = hcnStations.id"""
join7 = spark.sql(join7_sql)
join7.count()


crnStations.registerTempTable('crnStations')
join7.registerTempTable('join7_tbl')
join8_sql = """ SELECT * from join7_tbl JOIN crnStations ON join7_tbl.id = crnStations.id"""
join8 = spark.sql(join8_sql)
join8.count()
#0
#None of the stations exist in all 3 networks



#q1 b)

c = (
    stations
    .select(["country_code","id","name"])
    .groupby("country_code")
    .agg({'country_code': 'count'})
    .orderBy('count(country_code)', ascending=False)
    .withColumnRenamed('country_code', 'countries')
    )


d = (
    stations
    .select(["id","name","state"])
    .filter(stations.state!='')
    .groupby("state")
    .agg({'state': 'count'})
    .orderBy('count(state)', ascending=False)
    .withColumnRenamed('state', 'States')
    )

c.write.format("com.databricks.spark.csv").save("/user/jth133/outputs/ghcnd/countries_count")
d.write.format("com.databricks.spark.csv").save("/user/jth133/outputs/ghcnd/states_count")

#q1 c)

e = (
    stations
    .select(["name","latitude"])
    .filter(stations.latitude < 0)
    .agg({'name': 'count'})
    )

f = (
    stations
    .select(["id","name","state"])
    .groupby("state")
    .agg({'state': 'count'})
    )

f.count()

#Question 3
#part a
#The default block size is 134217728. 
#Size of daily summaries for the year 2017 - 119.5 M   477.8 M   /data/ghcnd/daily/2017.csv.gz
#Therefore, 3.56 blocks are required to store the daily summaries of 2017
#Size of daily summaries for the year 2010 - 197.6 M   790.3 M   /data/ghcnd/daily/2010.csv.gz
#Therefore, 5.88 ~ 6 blocks will be required to store the daily summaries of year 2010

#part b
daily2010 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2010.csv.gz")
)
daily2010.count()
#36946080 number of rows

daily2017 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2017.csv.gz")
)
daily2017.count()
#21904999



#part c
daily2010to2015 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/201[0-5].csv.gz")
)
daily2010to2015.count()
#207716098
#the number of tasks corresponds to the number of files as input to load

#another way to use regular expressions
daily2010to2015 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/201{0,1,2,3,4,5}.csv.gz")
)
daily2010to2015.count()
#207716098

#The files in gz format are very common and fast to load but cannot be split. 

#q2

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf, struct
from math import radians, cos, sin, asin, sqrt
import pandas as pd
import geopandas as gp
from shapely.geometry import Point

#nz_stations =（stations.filter(stations.country_code=="NZ").select("id","name","latitude", "longitude"）

nz_stations = (
    stations
    .filter(stations.country_code=="NZ")
    .select("id","name","latitude", "longitude")
    )

nz_station_pairs = (nz_stations.crossJoin(nz_stations).toDF(
    "id_a","name_a","latitude_a","longitude_a","id_b","name_b","latitude_b","longitude_b"))

nz_station_pairs =(nz_station_pairs.filter(
    nz_station_pairs.id_a != nz_station_pairs.id_b))

def get_distance(lon1,lat1,lon2,lat2):
    lon1, lat1, lon2, lat2 = map(radians,[lon1,lat1, lon2, lat2])
    radius =6371
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    area = sin(dlat / 2)**2 + cos(lat1) * sin(dlon/2)**2
    central_angle = 2 * asin(sqrt(area))
    distance = central_angle * radius
    return abs(round(distance,2))

udf_get_distance = F.udf(get_distance)

nz_pairs_distance = (nz_station_pairs.withColumn("abs_distance",udf_get_distance(
    nz_station_pairs.longitude_a,nz_station_pairs.latitude_a,
    nz_station_pairs.longitude_b,nz_station_pairs.latitude_b)))

nz_pairs_distance = nz_pairs_distance.withColumn('abs_distance',nz_pairs_distance['abs_distance'].cast('double'))

shortest_distance = (
    nz_pairs_distance
    .select(["name_a","name_b","abs_distance"])
    .groupby("name_a","name_b")
    .agg({'abs_distance': 'min'})
    .orderBy('min(abs_distance)', ascending=True)
    .withColumnRenamed('min(abs_distance)', 'distance')
    )
shortest_distance.show(10)

shortest_distance.write.format("com.databricks.spark.csv").save("/user/jth133/outputs/ghcnd/shortest_distance")
#4a
daily = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/*.csv.gz")
    .withColumn("Year",F.input_file_name().substr(36,4))
    .repartition(partitions)
)
daily = daily.withColumn('country_code',daily['id'].substr(1,2))
#daily.cache()
daily.count()
#2624027105
#4b
daily_new = daily.filter(
    "element =='PRCP' or element =='SNOW' or element =='SNWD' or element =='TMAX' or element =='TMIN'")

element_count = (
    daily_new
    .select(["id","element"])
    .groupby("element")
    .agg({'element': 'count'})
    .orderBy('count(element)', ascending=False)
    .withColumnRenamed('count(element)', 'count')
    .repartition(1)
    )
element_count.show(10)

#PRCP|918490401|
#TMAX|362528096|
#TMIN|360656728|
#SNOW|322003304|
#SNWD|261455306|


#Precipitation(PRCP) element has the most number of observations.

inventory_element_count = (
    inventory2
    # Select only the columns that are needed
    .select(['id','element'])
    # Group by source and count destinations
    .groupBy('element')
    .agg({'element': 'count'})
    .orderBy('count(element)', ascending=False)
    .select(
        F.col('element'),
        F.col('count(element)').alias('element_count')
    )
    .repartition(1)
)
inventory_element_count = inventory_element_count.filter(
    "element =='PRCP' or element =='SNOW' or element =='SNWD' or element =='TMAX' or element =='TMIN'")
inventory_element_count.show()


#PRCP|       101614|
#SNOW|        61756|
#SNWD|        53307|
#TMAX|        34109|
#TMIN|        34012|

#The inventory data is not consistent with the daily data as the SNOW & SNWD elements are more than TMAX & TMIN elements in inventory.

#4c
daily_element = daily_new.groupby("id").agg(F.collect_set("element").alias("element_collection"))
daily_no_tmax = daily_element.filter(daily_element["element_collection"].getItem(0).contains("TMAX")==False)
daily_tmin = daily_no_tmax.filter(daily_element["element_collection"].getItem(0).contains("TMIN"))  
#daily_tmin.count()
#141
daily_tmin.registerTempTable('daily_tmin_tbl')
join_tmin_sql = """ SELECT stations_tbl.*,daily_tmin_tbl.element_collection from stations_tbl JOIN daily_tmin_tbl ON stations_tbl.id = daily_tmin_tbl.id"""
join_tmin = spark.sql(join_tmin_sql)
join_tmin.show()
join_tmin.count()
daily_tmin_distinct = daily_tmin.groupby("id").agg(F.countDistinct("element_collection"))
daily_tmin_distinct.count()
join_tmin.filter(join_tmin["gsn_flag"] == "GSN").count()
join_tmin.filter("hcn_crn_flag == 'HCN' or hcn_crn_flag == 'CRN'").count()
join_tmin.where(F.col("wmo_id").cast(IntegerType()).isNull()==False).count()
#141 different stations contributed to these observations
#4d
daily_nz = daily.filter(daily['country_code']=="NZ")
daily_tmax_tmin = daily_nz.filter("element =='TMAX' or element =='TMIN'")
daily_tmax_tmin.write.format("com.databricks.spark.csv").save("/user/jth133/outputs/ghcnd/daily_tmax_tmin")
daily_tmax_tmin.count() 
#447017 # number of observations with tmax and tmin values
stations_nz = (
    daily_tmax_tmin
    .select(["id","element","value","year"])
    .groupby("id","element","year")
    .agg({'value': 'avg'})
    .orderBy('avg(value)', ascending=False)
    .withColumnRenamed('avg(value)', 'count')
    .repartition(1)
    )
stations_nz.count()
nz_tmax_ts = stations_nz.filter(stations_nz['element']=="TMAX").drop(stations_nz['element'])
nz_tmin_ts = stations_nz.filter(stations_nz['element']=="TMIN").drop(stations_nz['element'])
nz_tmax_ts.write.format("com.databricks.spark.csv").save("/user/jth133/outputs/ghcnd/nz_tmax_ts")
nz_tmin_ts.write.format("com.databricks.spark.csv").save("/user/jth133/outputs/ghcnd/nz_tmin_ts")

value_count = (
    daily_tmax_tmin
    .select(["year","element","value"])
    .groupby("year","element")
    .agg({'value': 'avg'})
    .orderBy('avg(value)', ascending=False)
    .withColumnRenamed('avg(value)', 'count')
    .repartition(1)
    )
#value_count.count()
#156
tmax_ts = value_count.filter(value_count['element']=="TMAX").drop(value_count['element'])
tmin_ts = value_count.filter(value_count['element']=="TMIN").drop(value_count['element'])
tmax_ts.write.format("com.databricks.spark.csv").save("/user/jth133/outputs/ghcnd/tmax_ts")
tmin_ts.write.format("com.databricks.spark.csv").save("/user/jth133/outputs/ghcnd/tmin_ts")

#4e
daily_prcp = daily.filter("element =='PRCP'")
average_prcp = (
    daily_prcp
    .select(["year","country_code","value"])
    .groupby("year","country_code")
    .agg({'value': 'avg'})
    .orderBy('avg(value)', ascending=False)
    .withColumnRenamed('avg(value)', 'avg_rainfall')
    .repartition(1)
    )
average_prcp.registerTempTable('average_prcp_tbl')
average_rainfall_sql = """ SELECT average_prcp_tbl.*,countries_tbl.name from average_prcp_tbl JOIN countries_tbl ON average_prcp_tbl.country_code = countries_tbl.code"""
average_rainfall = spark.sql(average_rainfall_sql)
average_rainfall.show()

cum_prcp = (
    daily_prcp
    .select(["year","country_code","value"])
    .groupby("year","country_code")
    .agg({'value': 'sum'})
    .orderBy('sum(value)', ascending=False)
    .withColumnRenamed('sum(value)', 'rainfall')
    .repartition(1)
    )
cum_prcp.registerTempTable('average_prcp_tbl')
cum_rainfall_sql = """ SELECT average_prcp_tbl.*,countries_tbl.name from average_prcp_tbl JOIN countries_tbl ON average_prcp_tbl.country_code = countries_tbl.code"""
cum_rainfall = spark.sql(cum_rainfall_sql)
cum_rainfall.show()


cum_rainfall.write.format("com.databricks.spark.csv").save("/user/jth133/outputs/ghcnd/cum_rainfall")
average_rainfall.write.format("com.databricks.spark.csv").save("/user/jth133/outputs/ghcnd/average_rainfall")

daily_temporal = (
    daily
    .select(["year","element","value"])
    .groupby("year","element")
    .agg({'value': 'avg'})
    .orderBy('avg(value)', ascending=False)
    .withColumnRenamed('avg(value)', 'count')
    .repartition(1)
    )
daily_temporal.write.format("com.databricks.spark.csv").save("/user/jth133/outputs/ghcnd/daily_temporal")

daily_global = (
    daily
    .select(["country_code","element","value"])
    .groupby("country_code","element")
    .agg({'value': 'sum'})
    .orderBy('sum(value)', ascending=False)
    .withColumnRenamed('sum(value)', 'count')
    .repartition(1)
    )
daily_global.write.format("com.databricks.spark.csv").save("/user/jth133/outputs/ghcnd/daily_global")

### How many rows in daily failed  

daily_quality = (
    daily
    .select(['id','date','element','quality flag'])
    .orderBy('id', ascending=False)
    .filter(daily["quality flag"] != "null")
    )
daily_quality.show()
daily_quality.count()
# The total number of quality failures is 46713


###  quality flags count for us states
quality_us_states = (
    daily
    .select(["id","quality flag"])
    .groupby("id")
    .agg({'quality flag': 'count'})
    .withColumnRenamed('count(quality flag)', 'qual_count')
    )
daily = daily.filter(daily["quality flag"] != "null")

quality_us_states_fil = (
    daily
    .select(["id","quality flag"])
    .groupby("id","quality flag")
    .agg({'quality flag': 'count'})
    .withColumnRenamed('count(quality flag)', 'qual_count')
    )

d = stations_US.filter(stations_US.state!='')

us_state = quality_us_states.join(d,quality_us_states.id == d.id, "inner").drop(d.id)
us_state_fil = quality_us_states_fil.join(d,quality_us_states_fil.id == d.id, "inner").drop(d.id)

us_states = (
    us_state
    .select(["id","qual_count","state"])
    .groupby("state")
    .agg({'qual_count': 'sum'})
    .withColumnRenamed('sum(qual_count)', 'total')
    .repartition(1)
    )

us_states_fil = (
    us_state_fil
    .select(["id","qual_count","state"])
    .groupby("state")
    .agg({'qual_count': 'sum'})
    .withColumnRenamed('sum(qual_count)', 'total')
    .repartition(1)
    )

us_states.write.format("com.databricks.spark.csv").save("/user/jth133/outputs/ghcnd/us_states")
us_states_fil.write.format("com.databricks.spark.csv").save("/user/jth133/outputs/ghcnd/us_states_fil")
#Stop the current Spark Session
spark.sparkContext.stop()


