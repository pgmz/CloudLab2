mvn clean
mvn package
rm -r output
/opt/hadoop-2.8.4/bin/hadoop jar target/cloud_lab_2_jar-1.0-SNAPSHOT.jar dataset output city_list.txt
