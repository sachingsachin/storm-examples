package examples.storm.storm_hello_world;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.metric.api.ReducedMetric;
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
public class ExclamationWithMetrics
{
	@SuppressWarnings("serial")
	public static class ExclamationBolt extends BaseRichBolt
	{
		private OutputCollector _collector;
		private CountMetric _countMetric;
		private MultiCountMetric _wordCountMetric;
		private ReducedMetric _wordLengthMeanMetric;
		
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
		{
			_collector = collector;
			initMetrics(context);
		}

		/* Step 1: Initialize metrics and register with storm */
		void initMetrics(TopologyContext context)
		{
			_countMetric = new CountMetric();
			_wordCountMetric = new MultiCountMetric();
			_wordLengthMeanMetric = new ReducedMetric(new MeanReducer());
			context.registerMetric("execute_count", _countMetric, 1);
			context.registerMetric("word_count", _wordCountMetric, 1);
			context.registerMetric("word_length", _wordLengthMeanMetric, 1);
		}

		public void execute(Tuple tuple) {
			_collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
			_collector.ack(tuple);
			updateMetrics(tuple.getString(0));
		}

		/* Step 2: Update counts in the metrics-objects for each emission of the tuple. */
		void updateMetrics(String word)
		{
			_countMetric.incr();
			_wordCountMetric.scope(word).incr();
			_wordLengthMeanMetric.update(word.length());
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

        /* Step 3: Put in a consumer for our metrics.
         * LoggingMetricsConsumer will show up as a Bolt in the Storm web UI and it will also
         * add to the file "logs/metrics.log" along with writing to INFO logs on the console */
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 2);

        if (args != null && args.length > 0)
        {
          conf.setNumWorkers(3);
          System.out.println("------------ Not using local-cluster ------------");
          StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else
        {
          System.out.println("------------ Using local-cluster ------------");
          LocalCluster cluster = new LocalCluster();
          cluster.submitTopology("test", conf, builder.createTopology());
          Utils.sleep(10000);
          System.out.println("------------ Killing topology test  ------------");
          cluster.killTopology("test");
          cluster.shutdown();
        }
    }
}
