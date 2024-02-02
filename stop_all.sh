$SPARK_HOME/sbin/stop-workers.sh
$SPARK_HOME/sbin/stop-master.sh
echo "Spark stopped"
stop-yarn.sh
echo "Yarn stopped"
stop-dfs.sh
echo "HDFS stopped"
jps
