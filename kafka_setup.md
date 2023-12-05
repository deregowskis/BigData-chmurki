<h1>Kafka setup</h1>

Here's a step-by-step solution of how we setup Apache Kafka on Google Cloud Platform (GCP).

<h3> Creating VM instance </h3>
We created a VM instance on Google Compute Engine. Equivalent code to recreate it can be found in *nifi_setup.md* file (only name changes).

<h3>Kafka configuration</h3>
After connecting SSH to the instance:

```
sudo apt update
sudo apt install openjdk-8-jdk
cd ~
wget https://downloads.apache.org/kafka/3.6.0/kafka-3.6.0-src.tgz
tar -xzf kafka-3.6.0-src.tgz
mv kafka-3.6.0 kafka
vim ~/kafka/config/server.properties
```

Then correct following parameters:

```
advertised.listeners=PLAINTEXT://34.118.106.96:9092
listeners=PLAINTEXT://0.0.0.0:9092
```
<h3> Zookeeper and Kafka servers setup</h3>

First start Zookeeper server:

```
cd ~/kafka
./gradlew jar -PscalaVersion=2.13.11
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Then in new terminal start Kafka server:

```
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

Finally in new terminal we can create topics:

```
bin/kafka-topics.sh --create --topic weather-nifi --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic warsaw-nifi --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Lists kafka topics:

```
bin/kafka-topics.sh --bootstrap-server=localhost:9092 --list
```

Run Zookeeper and Kafka as deamons

```
nohup bin/zookeeper-server-start.sh config/zookeeper.properties &
nohup bin/kafka-server-start.sh config/server.properties &
```
