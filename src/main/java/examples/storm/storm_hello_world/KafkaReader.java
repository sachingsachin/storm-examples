package examples.storm.storm_hello_world;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <pre>
 * Example showing how to read from Kafka and print to the console.
 * Usage:
 *   bin/storm jar \
 *       [path-to-jar] \
 *       examples.storm.storm_hello_world.KafkaReader
 *       [ZK-Connection-String]
 *       [Kafka-Topic-Name]
 *       [ZK-Root-For-Consumer-Offsets]
 * </pre>
 */
public class KafkaReader
{
	@SuppressWarnings("serial")
	public static class PrintlnBolt extends BaseRichBolt
	{
		OutputCollector _collector;
		private static final Logger logger = LoggerFactory.getLogger(PrintlnBolt.class);
		
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
		{
			_collector = collector;
		}

		public void execute(Tuple tuple) {
			//_collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
			_collector.ack(tuple);
			System.out.println("Printing tuple with toString(): " + tuple.toString());
		    System.out.println("Printing tuple with getString(): " + tuple.getString(0));
		    logger.info("Logging tuple with logger: " + tuple.getString(0));
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {}
	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException
    {
		if (args == null || args.length != 3)
		{
			System.err.println ("Wrong args. \n" +
					"Usage: bin/storm jar [path-to-jar] \n" + 
					"examples.storm.storm_hello_world.KafkaReader \n" +
					"[ZK-Connection-String] \n" +
					"[Kafka-Topic-Name] \n" +
					"[ZK-Root-For-Consumer-Offsets]");
			return;
		}
		String _zkConnString = args[0];
		String _topicName = args[1];
		String _zkRootForConsumerOffsets = args[2];
		
    	BrokerHosts zkHosts = new ZkHosts(_zkConnString);
    	SpoutConfig spoutConfig = new SpoutConfig(zkHosts, _topicName, _zkRootForConsumerOffsets, UUID.randomUUID().toString());
    	spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    	KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

    	TopologyBuilder builder = new TopologyBuilder();
    	// For every setSpout() / setBolt() methods, there is an integer argument
    	// called 'parallelism_hint' which denotes the number of tasks that 'should' be assigned to
    	// execute this spout. Each task will run on a thread in a process somewhere around the cluster.
        builder.setSpout("kafkaSpout", kafkaSpout, 1);
        builder.setBolt("exclaim1", new PrintlnBolt(), 3).shuffleGrouping("kafkaSpout");

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(20000);
        cluster.killTopology("test");
        cluster.shutdown();
    }
}
