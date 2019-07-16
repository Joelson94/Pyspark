#DATA420-19S1 Assignment 1
#Processing Q1
#structure of data in /data/ghcnd/
hdfs dfs -ls /data/ghcnd/
hdfs dfs -ls /data/ghcnd/daily | head
hdfs dfs -ls /data/ghcnd/daily | tail

# /data/ghcnd/
# ├─ countries
# ├─ daily
# │  ├─ 1763.csv.gz
# │  ├─ 1764.csv.gz
# │  ├─ ...
# │  └─ 2017.csv.gz
# ├─ inventory
# ├─ states
# └─ stations

#number of files in daily
hdfs dfs -ls /data/ghcnd/daily | wc -l

#size of all of the data and the size of daily specifically
hdfs dfs -du -h /data/ghcnd/
hdfs dfs -du -h /data/ghcnd/daily

#peek at the top of each data file to check the schema is as described
hdfs dfs -cat /data/ghcnd/countries | head
hdfs dfs -cat /data/ghcnd/inventory | head
hdfs dfs -cat /data/ghcnd/states | head
hdfs dfs -cat /data/ghcnd/stations | head

hdfs dfs -cat /data/ghcnd/daily/2017.csv.gz | gunzip | head

#Analysis Q3a
#default block size
hdfs getconf -confKey dfs.blocksize

#file size of daily 2017
hdfs dfs -du -h /data/ghcnd/daily/2017.csv.gz

#file size of daily 2010
hdfs dfs -du -h /data/ghcnd/daily/2010.csv.gz

#individual block size for daily 2017
hdfs fsck /data/ghcnd/daily/2017.csv.gz -files -blocks

#individual block size for daily 2010
hdfs fsck /data/ghcnd/daily/2010.csv.gz -files -blocks

#Analysis 4
#copying results from HDFS output directory to local
hdfs dfs -copyToLocal /user/psc87/outputs/ghcnd/dailyTmaxTmin1/part-*.csv ./dailyTmaxTmin1.csv
hdfs dfs -copyToLocal /user/jth133/outputs/ghcnd/tmax_ts/part-*.csv ./tmax_ts.csv
hdfs dfs -copyToLocal /user/jth133/outputs/ghcnd/tmin_ts/part-*.csv ./tmin_ts.csv
hdfs dfs -copyToLocal /user/jth133/outputs/ghcnd/nz_tmax_ts/part-*.csv ./nz_tmax_ts.csv
hdfs dfs -copyToLocal /user/jth133/outputs/ghcnd/nz_tmin_ts/part-*.csv ./nz_tmin_ts.csv
hdfs dfs -copyToLocal /user/jth133/outputs/ghcnd/cum_rainfall/part-*.csv ./cum_rainfall.csv
hdfs dfs -copyToLocal /user/jth133/outputs/ghcnd/average_rainfall/part-*.csv ./average_rainfall.csv



#Challenge 1
#copying results from HDFS output directory to local
hdfs dfs -copyToLocal /user/jth133/outputs/ghcnd/daily_global/part-*.csv ./daily_global.csv
hdfs dfs -copyToLocal /user/jth133/outputs/ghcnd/daily_temporal/part-*.csv ./daily_temporal.csv