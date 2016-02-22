package examples.storm.storm_hello_world;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 * <pre>
 * Creates a storm topology that looks like this:
 *   "word" ---> "exclaim1" ---> "exclaim2"
 * where:
 *    "word" is a spout of type TestWordSpout
 *    "exclaim1" is a bolt of type ExclamationBolt
 *    "exclaim2" is another bolt of the same type ExclamationBolt
 *
 * TestWordSpout emits a random word from a static collection of words.
 * ExclamationBolt is a very simple bolt that just adds some exclamation marks to its input.
 * </pre>
 */
public class HelloWorld 
{
	@SuppressWarnings("serial")
	public static class ExclamationBolt extends BaseRichBolt
	{
		OutputCollector _collector;
		
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
		{
			_collector = collector;
		}

		public void execute(Tuple tuple) {
			_collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
			_collector.ack(tuple);
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer)
		{
			declarer.declare(new Fields("word"));
		}
	}

	public static void main( String[] args ) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException
    {
    	TopologyBuilder builder = new TopologyBuilder();

    	// TestWordSpout emits a random word from a static collection of words.
    	//
    	// For every setSpout() / setBolt() methods, there is an integer argument
    	// called 'parallelism_hint' which denotes the number of tasks that 'should' be assigned to
    	// execute this spout. Each task will run on a thread in a process somewhere around the cluster.
        builder.setSpout("word", new TestWordSpout(), 1);
        builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("word");
        builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0)
        {
          conf.setNumWorkers(3);
          StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else
        {
          LocalCluster cluster = new LocalCluster();
          cluster.submitTopology("test", conf, builder.createTopology());
          Utils.sleep(10000);
          cluster.killTopology("test");
          cluster.shutdown();
        }
    }
}
