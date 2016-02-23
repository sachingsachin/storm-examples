# storm-examples


## Examples in this repository

Each example is completely contained in a single file in **examples/storm/storm\_hello\_world/**

1. HelloWorld.java - Basic example where a bold just adds an exclamation mark on its input word.
2. ExclamationWithMetrics.java - Shows the usage of metrics.
3. KafkaReader.java - Shows how to use the storm-kafka "plugin" with kafka.


## Installing Storm

As per the guide at [Setting-up-a-Storm-cluster](http://storm.apache.org/documentation/Setting-up-a-Storm-cluster.html), first step is to [install zookeeper](http://zookeeper.apache.org/doc/r3.3.3/zookeeperStarted.html#sc_InstallingSingleMode) which is simply downloading, unzipping, setting up some properties and running the ZK server.

You can then run the ZK command-line-interface to interact with your ZK:

```bash

$ bin/zkCli.sh -server 127.0.0.1:2181
  help
  ls /

```

Once ZK is running, play with some of the ZK four letter commands:

```bash

$ echo ruok | nc 127.0.0.1 2181
imok

```

Make copies of the downloaded storm:

```bash

mv storm-0.10.0    storm-0.10.0.nimbus
cp -rf storm-0.10.0.nimbus    storm-0.10.0.supervisor1
cp -rf storm-0.10.0.nimbus    storm-0.10.0.supervisor2

```

Start nimbus in one shell

```bash

cd storm-0.10.0.nimbus/
bin/storm nimbus

```
 
Start supervisor and the UI in the other shell

```bash

cd  storm-0.10.0.supervisor1
bin/storm supervisor &
bin/storm ui &

```

 
Then navigate to http://localhost:8080/index.html to see that storm is up and running.



## Building an uber jar with maven shade plugin

This examples code uses the maven-shaden-plugin to build an uber jar (i.e. a jar with all the dependency jars).

That is required because the regular storm distribution does not contain the storm-kafka jars.
And we need to either put them in storm's lib/ directory or build an uber jar.

But storm does complain about "multiple defaults.yaml resources" when it sees the Storm jars bundled in the topology jar file. Hence we remove it by using scope "provided" for storm-core libraries.

maven-assembly-plugin might be useful too.



## Running the examples

1. Build jar using `mvn clean package`
2. Copy target/storm\_hello\_world-0.0.1-SNAPSHOT.jar to a convienient location.
3. Run storm as `bin/storm jar ../\*.jar examples.storm.storm\_hello\_world.HelloWorld`

