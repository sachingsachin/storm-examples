package examples.storm.storm_hello_world;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.elasticsearch.hadoop.serialization.Generator;
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter;
import org.elasticsearch.storm.EsBolt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 * <pre>
 * Example showing how to read from Kafka and write to Elastic-Search (ES running on localhost:9200) 
 * Usage:
 *   bin/storm jar \
 *       [path-to-jar] \
 *       examples.storm.storm_hello_world.KafkaToElasticSearch \
 *       [Kafka-ZK-Connection-String] \
 *       [Kafka-Topic-Name] \
 *       [Kafka-ZK-Root-For-Consumer-Offsets] \
 *       [es.nodes eg. 10.11.12.13] \
 *       [es.port eg. 9200]
 * </pre>
 */
public class KafkaToElasticSearch
{
	public static class KafkaMsgToJson extends JdkValueWriter
	{
		private final ObjectMapper mapper = new ObjectMapper();

		public KafkaMsgToJson() {}
		public KafkaMsgToJson(boolean writeUnknownTypes) {super(writeUnknownTypes);}

		@Override
	    public Result write(Object value, Generator generator)
		{
			String msg = null;
			if (value instanceof Tuple)
			{
				Tuple tuple = (Tuple)value;
				Fields fields = tuple.getFields();
				if (fields.size() == 1)
				{
					msg = tuple.getString(0);
				}
			}
			else if (value instanceof String)
			{
				msg = (String)value;
			}

			if (msg != null)
			{
				// Assume tab-separated fields in the same message
				String [] msgFields = msg.split("\t");
				int i=0;
				Map<String, Object> fieldMap = new HashMap<String, Object>();
				for (String msgField : msgFields)
				{
					++i;
					fieldMap.put("fld" + i, msgField);
				}
				try
				{
					String json = mapper.writeValueAsString(fieldMap);
					System.out.println("Successfully made JSON: " + json);
					return super.write(fieldMap, generator);
				}
				catch (JsonProcessingException e)
				{
					e.printStackTrace();
					return Result.FAILED(value);
				}
			}
			System.out.println(">>>>>>>>>>>>>>>>>>> Object read from Kafka is not a string. Cannot convert to JSON.\nType = "
					+ value.getClass().getName() + "\nValue = " + value);
	        return doWrite(value, generator, null);
	    }
	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException
    {
		if (args == null || args.length != 5)
		{
			System.err.println ("Wrong args. \n" +
					"Usage: bin/storm jar [path-to-jar] \n" + 
					"examples.storm.storm_hello_world.KafkaReader \n" +
					"[Kafka-ZK-Connection-String] \n" +
					"[Kafka-Topic-Name] \n" +
					"[Kafka-ZK-Root-For-Consumer-Offsets]\n"+
					"[es.nodes eg. 10.11.12.13]\n"+
					"[es.port eg 9200]\n");
			return;
		}
		String kafkaZkConnString = args[0];
		String kafkaTopicName = args[1];
		String kafkaZkRootForConsumerOffsets = args[2];
		String esNodes = args[3];
		String esPort = args[4];
		
    	BrokerHosts zkHosts = new ZkHosts(kafkaZkConnString);
    	SpoutConfig spoutConfig = new SpoutConfig(zkHosts, kafkaTopicName, kafkaZkRootForConsumerOffsets, UUID.randomUUID().toString());
    	spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    	// spoutConfig.metricsTimeBucketSizeInSecs sets the interval when the inbuilt Kafka metrics runs to gather stats.
    	// be careful while keeping this value too low as it can affect the performance a lot.
    	// (On a local setup, saw 18k docs consumed with default value of 60 seconds and only 300 docs consumed with 5 seconds) 
    	//spoutConfig.metricsTimeBucketSizeInSecs = 5;
    	KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

    	TopologyBuilder builder = new TopologyBuilder();
    	// For every setSpout() / setBolt() methods, there is an integer argument
    	// called 'parallelism_hint' which denotes the number of tasks that 'should' be assigned to
    	// execute this spout. Each task will run on a thread in a process somewhere around the cluster.
        builder.setSpout("kafkaSpout", kafkaSpout, 1);

        // Set Elastic-Search bolt.
        // Also see the bolt-specific options at:
        // https://www.elastic.co/guide/en/elasticsearch/hadoop/current/storm.html#storm-write
        // And at:
        // https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html
        // Some interesting defaults:
        // es.storm.bolt.write.ack (default false)
        //    If true, it acknowledges the Tuple after it was written to Elastic-Search and hence more reliable
        // es.resource.write = my-collection/{@timestamp:YYYY.MM.dd}
        //    Shows an example of a dynamic target for writing. (Same target as used below)
        //    Assuming a field '@timestamp' in the input document, '@timestamp:YYYY.MM.dd' extracts the day from it.
        //    And as the day changes, the target written to will also change.
        //    This makes for a great automatic index change strategy on a daily basis.
        // es.nodes (default localhost)
        // es.port (default 9200)
        // es.index.auto.create (default yes)
        // es.nodes.discovery (default true)
        // es.nodes.client.only (default false)
        //    Note this typically significantly reduces the node parallelism and thus it is disabled by default.
        //    Enabling it also disables es.nodes.data.only (since a client node is a non-data node).
        //    Apparently, this option does not turn the storm-node into a client node but instead sends the data to client nodes only.
        //    That's why parallelism is reduced because number of client-nodes are typically lesser than data nodes.
        // es.nodes.data.only (default true)
        //     The purpose of this configuration setting is to avoid overwhelming non-data nodes as these tend to be "smaller" nodes.
        // es.http.timeout (default 1m)
        // es.http.retries (default 3)
        // Flushing options
        // es.storm.bolt.flush.entries.size (default 1000)
        // es.storm.bolt.tick.tuple.flush (default true)
        //    Flushes the data to Elastic-Search if the bolt receives storm's heart-beat Tuple called 'Tick'
        //    If true, it creates a time-based as well as size-based (as defined by es.storm.bolt.flush.entries.size) flushing.
        // es.batch.size.bytes (default 1mb)
        //    Size per task instance. Total bulk-size written to Elastic-Search = this setting * no of tasks
        // es.batch.size.entries (default 1000)
        //    Companion to es.batch.size.bytes. Update is executed if any of these options becomes true.
        // es.batch.write.refresh (default true)
        // Plus many more options at the above links
        HashMap<String, String> esConf = new HashMap<String, String>();
        esConf.put("es.batch.size.entries", "100");
        esConf.put("es.input.json", "false");
        esConf.put("es.nodes", esNodes);
        esConf.put("es.port", esPort);
        // Option "es.ser.writer.value.class" is defined by the option ES_SERIALIZATION_WRITER_VALUE_CLASS in ConfigurationOptions class
        // And that is set to JdkValueWriter.class.getName() in org.elasticsearch.hadoop.integration.rest.AbstractRestQueryTest
        // Hence we too do a something similar for writing our own message to JSON
        esConf.put("es.ser.writer.value.class", KafkaMsgToJson.class.getName());
        String target = "storm/json-trips"; // format: <index>/<type>
        builder.setBolt("es-bolt", new EsBolt(target, esConf)).shuffleGrouping("kafkaSpout");

        Config conf = new Config();
        conf.setDebug(true);

        /* LoggingMetricsConsumer will show up as a Bolt in the Storm web UI and it will also
         * add to the file "logs/metrics.log" along with writing to INFO logs on the console */
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 2);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(30000);
        cluster.killTopology("test");
        cluster.shutdown();
    }
}
