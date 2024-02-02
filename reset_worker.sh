rm -rf opt/data/hdfs/namenode/
rm -rf opt/data/hdfs/datanode/
mkdir opt/data/hdfs/namenode/
mkdir opt/data/hdfs/datanode/ 
echo "===Hadoop started===="
jps
echo "====Data uploaded to HDFS===="
hdfs dfs -ls /user/user/data
$SPARK_HOME/sbin/start-worker.sh spark://master:7077
echo "====Spark started===="
jps