
rm -rf opt/data/hdfs/namenode/
rm -rf opt/data/hdfs/datanode/
mkdir opt/data/hdfs/namenode/
mkdir opt/data/hdfs/datanode/ 
hdfs namenode -format
start-dfs.sh
start-yarn.sh
echo "===Hadoop started===="
jps
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/user
hdfs dfs -mkdir /user/user/data
hdfs dfs -mkdir /spark.eventLog
echo "====Hadoop initialized===="
hdfs dfs -put advDatabases/data/*.csv hdfs://master:9000/user/user/data
echo "====Data uploaded to HDFS===="
hdfs dfs -ls /user/user/data
$SPARK_HOME/sbin/starn-master.sh
$SPARK_HOME/sbin/start-worker.sh spark://master:7077
echo "====Spark started===="
jps