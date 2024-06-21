hdfs dfs -mkdir -p /kolos/avro_data/
hdfs dfs -mkdir -p /kolos/csv_data/
hdfs dfs -copyFromLocal /home/vagrant/kolos/input/WIFI.csv /kolos/csv_data/

