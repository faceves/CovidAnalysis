# Project 2: CovidAnalysis


## Project Description
Create a Spark Application that process Covid data. Your project should involve some 
analysis of covid data (Every concept of spark from rdd, dataframes, sql, dataset and 
optimization methods should be included, persistence also). The final expected output 
is different trends that you have observed as part of data collectivley and how can WHO
make use of these trends to make some useful decisions.

## Technology Stack
- Apache Spark 3.1.1
- Spark SQL
- YARN
- HDFS
- Scala 2.12.10
- Git + GitHub


## Features
List of features ready and TODO's for future development.

### Features:
- First Occur Relationship
- Local Spike Instance Detector
- \Tim Feature/

### TODO:
- First Occur improvements for future
  - Optimizing by implementing a broadcast join for joins.
- Local Spike improvements for future
  - include an iterator whose index values contain dates of spikes
  - develop a more independent schema assessment
- \Tim feature improvements for future/  

## Getting Started
GitHub clone URL: https://github.com/faceves/CovidAnalysis.git

- Enable WSL and update to WSL2 on Windows 10 
- Install Java JDK 1.8 on Windows 10
- Install Ubuntu 18+
- Install Java JDK 1.8 on Ubuntu
- Install Hadoop on Ubuntu
- Install Apache-Hive on Ubuntu
- Install Apache-Spark on ubuntu
- install Intellij Community Edition 2021
- Open Ubuntu Terminal:
    - ssh localhost
    - ~HADOOP_HOME/sbin/start-dfs.sh
    - ~HADOOP_HOME/sbin/start-yarn.sh
    - cd ~
    - hdfs dfs -mkdir /user/<username>/project2
    - hdfs dfs -mkdir /user/<username>/project2/Datasets  
    - hdfs dfs -chmod 777 /user/<username>/project2
    - hdfs dfs -cp /mnt/<path to files>/covid_19_data.csv /user/project2/covid_19_data.csv
    - hdfs dfs -cp /mnt/<path to files>/time_series_covid_19_confirmed.csv /user/<username>/project2/Datasetstime_series_covid_19_confirmed.csv
    - hdfs dfs -cp /mnt/<path to files>/time_series_covid_19_confirmed_US.csv /user/<username>/project2/Datasetsime_series_covid_19_confirmed_US.csv
    - hdfs dfs -cp /mnt/<path to files>/time_series_covid_19_deaths.csv /user/<username>/project2/Datasetstime_series_covid_19_deaths.csv
    - hdfs dfs -cp /mnt/<path to files>/time_series_covid_19_deaths_US.csv /user/<username>/project2/Datasetstime_series_covid_19_deaths_US.csv
    - hdfs dfs -cp /mnt/<path to files>/time_series_covid_19_recovered.csv /user/<username>/project2/Datasetstime_series_covid_19_recovered.csv 
- Clone project into IntelliJ
    
## Contributors
- Francisco Aceves
  - https://github.com/faceves
- George Kotzabassis
  - https://github.com/PaxImpetus
- Timothy Miller
  - https://github.com/Tim-J-Miller
    
## License
