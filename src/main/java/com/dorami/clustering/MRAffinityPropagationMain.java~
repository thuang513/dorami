package com.dorami.clustering;


import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.ParseException;


public class MRAffinityPropagation {

	/** 
	 *  Setup the logger 
	 */
	private static final Logger LOGGER = 
		Logger.getLogger(MRAffinityPropagation.class.getName());

	private static final Options options = new Options();

	private static final String HELP = "help";

	private static final String FLAG_NUM_NODES = "n";

	private static final String FLAG_OUTPUT_PATH = "output_path";

	private static final String FLAG_INPUT_PATH = "input_path";

	static {
		Option FLAG_NUM_NODES = 
			OptionBuilder.withArgName(FLAG_NUM_NODES)
			             .withDescription("The number of nodes to use.")
           			   .isRequired()
			             .hasArg()
			             .create(FLAG_NUM_NODES);

		Option inputPath = 
			OptionBuilder.withArgName(FLAG_INPUT_PATH)
			             .withDescription("Directory for the input path.")
           			   .isRequired()
			             .hasArg()
			             .create(FLAG_INPUT_PATH);

		Option FLAG_OUTPUT_PATH = 
			OptionBuilder.withArgName(FLAG_OUTPUT_PATH)
			             .withDescription("Directory for the output path.")
           			   .isRequired()
			             .hasArg()
			             .create(FLAG_OUTPUT_PATH);
		
		Option help = 
			OptionBuilder.withArgName(HELP)
			             .create(HELP);

		options.addOption(FLAG_NUM_NODES);
		options.addOption(FLAG_INPUT_PATH);
		options.addOption(FLAG_OUTPUT_PATH);
		options.addOption(help);
	}

	public static void main(String[] args) {
		CommandLineParser parser = new GnuParser();
		CommandLine line = null;

		try {
			// parse the command line arguments
			line = parser.parse(options, args);
		} catch (ParseException pe) {
			LOGGER.warning("Parser failed: " + pe.getMessage());
			System.exit(-1);
		}

		String inputPath = line.getOptionValue(FLAG_INPUT_PATH);
		String outputPath = line.getOptionValue(FLAG_OUTPUT_PATH);
		int numNodes = 10000;

		if (args.length > 3) {
			numNodes = Integer.parseInt(args[3]);
		}

		int numReducers = 7;
		if (args.length > 4) {
			numReducers = Integer.parseInt(args[4]);
		}


		// Setup the jobs
		JobConf conf0 = new JobConf();
	}
}

import org.apache.hadoop.examples.textpair.FirstPartitioner;
import org.apache.hadoop.examples.textpair.GroupComparator;
import org.apache.hadoop.examples.textpair.KeyComparator;
import org.apache.hadoop.examples.textpair.TextPair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PageRankNew {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];
		int specIteration = 0;
		if (args.length > 2) {
			specIteration = Integer.parseInt(args[2]);
		}

		int numNodes = 100000;
		if (args.length > 3) {
			numNodes = Integer.parseInt(args[3]);
		}

		int numReducers = 7;
		if (args.length > 4) {
			numReducers = Integer.parseInt(args[4]);
		}

		boolean fixpoint = false;
		if (args.length > 5) {
			fixpoint = Boolean.parseBoolean(args[5]);
		}

		long start = System.currentTimeMillis();

		/*
		 * count job, to count out-going urls for each url
		 */
		JobConf confc = new JobConf(NaivePageRank.class);
		confc.setJobName("PageRank-Count");
		confc.setOutputKeyClass(Text.class);
		confc.setOutputValueClass(Text.class);
		confc.setMapperClass(NaivePageRank.CountMapper.class);
		confc.setReducerClass(NaivePageRank.CountReducer.class);
		confc.setInputFormat(TextInputFormat.class);
		confc.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(confc, new Path(inputPath));
		FileOutputFormat.setOutputPath(confc, new Path(outputPath + "/count"));
		confc.setNumReduceTasks(numReducers);
		JobClient.runJob(confc);

		/*
		 * Initial Rank Assignment Job
		 */
		JobConf conf0 = new JobConf(NaivePageRank.class);
		conf0.setJobName("PageRank-Initialize");
		conf0.setOutputKeyClass(Text.class);
		conf0.setOutputValueClass(Text.class);
		conf0.setMapperClass(NaivePageRank.InitialRankAssignmentMapper.class);
		conf0.setReducerClass(NaivePageRank.InitialRankAssignmentReducer.class);
		conf0.setInputFormat(TextInputFormat.class);
		conf0.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf0, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf0, new Path(outputPath + "/i-1"));
		conf0.setNumReduceTasks(numReducers);
		JobClient.runJob(conf0);

		/**
		 * Join Job, to populate rank value from source url to destination url
		 */
		JobConf conf1 = new JobConf();
		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(Text.class);
		conf1.setMapOutputKeyClass(TextPair.class);
		conf1.setMapperClass(NaivePageRank.ComputeRankMap.class);
		conf1.setReducerClass(NaivePageRank.ComputeRankReduce.class);
		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);
		conf1.setPartitionerClass(FirstPartitioner.class);
		conf1.setOutputKeyComparatorClass(KeyComparator.class);
		conf1.setOutputValueGroupingComparator(GroupComparator.class);

		/**
		 * Aggregate map/reduce step, to aggregate rank value for each unique
		 * url
		 */
		JobConf conf2 = new JobConf();
		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(Text.class);
		conf2.setMapOutputKeyClass(Text.class);
		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);
		conf2.setMapperClass(NaivePageRank.RankAggregateMapper.class);
		conf2.setCombinerClass(NaivePageRank.RankAggregateReducer.class);
		conf2.setReducerClass(NaivePageRank.RankAggregateReducer.class);

		/**
		 * set the as-a-whole iterative job conf
		 */
		JobConf conf = new JobConf(NaivePageRank.class);
		conf.setJobName("PageRank");
		conf.setNumReduceTasks(numReducers);
		conf.setLoopInputOutput(RankLoopInputOutput.class);
		conf.setLoopReduceCacheSwitch(RankReduceCacheSwitch.class);
		conf.setLoopReduceCacheFilter(RankReduceCacheFilter.class);
		conf.setLoopStepHook(PageRankStepHook.class);
		conf.setInputPath(inputPath);
		conf.setOutputPath(outputPath);
		// set up the m-r step pipeline
		conf.setStepConf(0, conf1);
		conf.setStepConf(1, conf2);
		conf.setIterative(true);
		conf.setNumIterations(specIteration);
		conf.setInt("haloop.num.nodes", numNodes);

		if (fixpoint) {
			// set fix point logics
			conf.setFixpointCheck(true);
			conf.setDistanceMeasure(RankDistanceMeasure.class);
			conf.setLoopReduceOutputCacheSwitch(RankReduceOutputCacheSwitch.class);
			conf.setDistanceThreshold(1.0f);
		}

		JobClient.runJob(conf);
		long end = System.currentTimeMillis();
		System.out.println("running time " + (end - start) / 1000 + "s");
	}

}