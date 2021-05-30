#!/bin/bash
if [[ $# -eq 0 ]] ; then
    echo 'You should specify length array and count of arrays!'
    exit 1
fi

#export PATH=$PATH:/usr/local/hadoop/bin/
hdfs dfs -rm -r /user/root/input
hdfs dfs -rm -r /user/root/out
hdfs dfs -mkdir /user/root/input

# Генерируем данные
python3 generate.py --length $1 --count $2

# Скачиваем kafka
if [ ! -f kafka_2.13-2.8.0.tgz ]; then
    wget https://apache-mirror.rbc.ru/pub/apache/kafka/2.8.0/kafka_2.13-2.8.0.tgz
    tar xvzf kafka_2.13-2.8.0.tgz
else
    echo "Kafka already exists, skipping..."
fi

# Скачиваем Spark
if [ ! -f spark-2.3.1-bin-hadoop2.7.tgz ]; then
    wget https://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz
    tar xvzf spark-2.3.1-bin-hadoop2.7.tgz
else
    echo "Spark already exists, skipping..."
fi

#export SPARK_HOME=/spark-2.3.1-bin-hadoop2.7
#export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop
#export PATH=$PATH:/spark-2.3.1-bin-hadoop2.7/bin

# Запускаем zookeeper-server и kafka-server
#kafka_2.13-2.8.0/bin/zookeeper-server-start.sh config/zookeeper.properties&
#kafka_2.13-2.8.0/bin/kafka-server-start.sh config/server.properties&

# Создаем новый топик
#kafka_2.13-2.8.0/bin/kafka-topics.sh --create --topic spark-event --bootstrap-server localhost:9092

# Пишем данные из сгенерированного файла в kafka
cat data | kafka_2.13-2.8.0/bin/kafka-console-producer.sh --topic spark-event --bootstrap-server localhost:9092

# Подсчитываем необходимое количество последних сообщений из kafka
ALL=$(kafka_2.13-2.8.0/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic spark-event --time -1 | awk '{split($0,a,":"); print a[3]}')
let NUMBER=ALL-$2

# Читаем данные из kafka и записываем в hdfs
#kafka_2.13-2.8.0/bin/kafka-console-consumer.sh --topic test-events --from-beginning --bootstrap-server localhost:9092 --timeout-ms 1000 > hdfs://localhost:9000/user/root/input/
kafka_2.13-2.8.0/bin/kafka-console-consumer.sh --topic spark-event --partition 0 --offset $NUMBER --bootstrap-server localhost:9092 --timeout-ms 1000 > consumed
hdfs dfs -put consumed /user/root/input
hdfs dfs -get /user/root/input/consumed fromHdfs

cat fromHdfs

./spark-2.3.1-bin-hadoop2.7/bin/spark-submit --class bdtc.lab2.SparkRddApplication --master local --deploy-mode client --executor-memory 1g --name intensive --conf "spark.app.id=SparkRddApplication" /tmp/lab2-1.0-SNAPSHOT-jar-with-dependencies.jar fromHdfs out

rm -f data
rm -f consumed
rm -f fromHdfs

echo "DONE! RESULT IS: "

cat out/part-00000
rm -rf out
#hdfs fs -cat /user/root/out/part-00000
