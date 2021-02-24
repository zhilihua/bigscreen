spark-submit  \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/root/client/log4j.properties" \
--class com.gkgd.bigscreen.construction.realtime.ods.OdsTrackJT8080Stream  \
--master yarn  \
--deploy-mode cluster  \
--driver-memory 1g  \
--executor-memory 1g  \
--executor-cores 2  \
--jars /home/hdfs/application_client/gd-common-1.0.0.jar,/home/hdfs/application_client/kafka-clients-0.10.0.0.jar,/home/hdfs/application_client/spark-streaming-kafka-0-10_2.11-2.2.0.jar,/home/hdfs/application_client/cglib-2.2.2.jar,/home/hdfs/application_client/mysql-connector-java-5.1.45.jar,/home/hdfs/application_client/fastjson-1.2.60.jar  \
/home/hdfs/application_client/gd-construction-waste-1.0.0.jar




spark-submit  \
--class com.gkgd.bigscreen.construction.realtime.dwd.DdwDataBus2EsStream  \
--master yarn  \
--deploy-mode cluster  \
--driver-memory 1g  \
--executor-memory 1g  \
--executor-cores 2  \
--jars /home/hdfs/application_client/gd-common-1.0.0.jar,/home/hdfs/application_client/kafka-clients-0.10.0.0.jar,/home/hdfs/application_client/spark-streaming-kafka-0-10_2.11-2.2.0.jar,/home/hdfs/application_client/cglib-2.2.2.jar,/home/hdfs/application_client/mysql-connector-java-5.1.45.jar,/home/hdfs/application_client/fastjson-1.2.60.jar,/home/hdfs/application_client/jest-common-5.3.3.jar,/home/hdfs/application_client/jest-5.3.3.jar,/home/hdfs/application_client/httpcore-nio-4.4.6.jar,/home/hdfs/application_client/httpasyncclient-4.1.3.jar  \
/home/hdfs/application_client/gd-construction-waste-1.0.0.jar

