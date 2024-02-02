# LAPD Crimes
Term project for the course 'Advanced Databases' during $9^{th}$ semester at ECE NTUA. <br>
The project objective was to process a large dataset on Apache Hadoop's distributed storage using the Apache Spark processing engine. The dataset contains information about crimes recorded by the Los Angeles Police Department (LAPD) from 2010 to 2024. It contains information about the date and time of the crime, the type of crime, the area where the crime was committed, the type of location (e.g. street, residence).


## How to start
1. Install Hadoop + Spark ( instruction @ [installation](https://colab.research.google.com/drive/1eE5FXf78Vz0KmBK5W8d4EUvEFATrVLmr?usp=drive_link#scrollTo=AVipleZma-DY))
    - Follow the same installation steps for setting the VMs locally, by ignoring the okeanos part and using a virtualization software like VirtualBox.
    - Remember to set the VMs under the same network.
2. Download the data executing the `download_data.sh` script
3. Start the services by 
    - `start-dfs.sh` 
    - `start-yarn.sh`
    - `start-master.sh`
    - `start-worker.sh spark://master:7077`
5. Import the data into HDFS with `hdfs dfs -put advDatabases/data/*.csv hdfs://master:9000/user/user/data`
6. Execute `spark-submit --master spark://master:7077 advDatabases/Config_data.py` to create the complete dataset.
7. Execute each query with `spark-submit --master spark://master:7077 advDatabases/q{i}_{DF,SQL,RDD}.py`
(Or execute `run_all.sh` to run all queries)

:exclamation: Steps 3 and 4 can be skipped by executing `reset_master.sh` script at master node but MUST execute `reset_worker.sh` at worker node,too.
## Implemented Queries
1. Find, for each year, the 3 months with the highest number of recorded crimes and print the total number of criminal activities recorded at that time, and the position of that month in the ranking within the corresponding year.
2. Sort the time of the day  according to the number of crimes recorded on the street (STREET).The day is divided in:
    - Morning $\rightarrow$  05:00-11:59
    - Afternoon $\rightarrow$ 12:00-16:59
    - Evening $\rightarrow$ 17:00-20:59
    - Night $\rightarrow$ 21:00-04:59
3. Find the descent of recorded crime victims in Los Angeles for the year 2015 in the 3 areas (ZIP Codes) with the highest and the 3 areas (ZIP Codes) with the lowest income per household.

## Techologies
- Apache Spark v3.5.0
- Apache Hadoop v3.3.6
- Python3 v3.10.12
- OpenJDK v11.0.21
## Cluster
- 1 Master Node : 192.168.64.9
    - 1 Master (Spark)
    - 1 Worker (Spark)
    - 1 Namenode (HDFS)
    - 1 Datanode (HDFS)
- 1 Worker Node : 192.168.64.10
    - 1 Worker (Spark)
    - 1 Datanode (HDFS)

## Resource Manager
To monitor cluster's health and the apps' execution :
- Apache Spark : [http://192.168.64.9:8080](http://192.168.64.9:8080/)
- Apache Hadoop : [http://192.168.64.9:9870](http://192.168.64.9:9870/)
- Apache Hadoop YARN : [http://192.168.64.9:8088](http://192.168.64.9:8088/)
