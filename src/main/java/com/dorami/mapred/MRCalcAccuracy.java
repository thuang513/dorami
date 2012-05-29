package com.dorami.mapred;

import com.dorami.data.SNPDataProtos.Accuracy;
import com.dorami.data.SNPDataProtos.ModelResults;


import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MRCalcAccuracy {

		/** 
		 *  Setup the logger 
		 */
		private static final Logger LOGGER = 
			Logger.getLogger(MRCalcAccuracy.class.getName());

	public static class AccuracyMapper extends TableMapper<Text, ImmutableBytesWritable> {
		public void map(ImmutableBytesWritable row, 
										Result value, 
										Mapper.Context context) {
			final byte[] COOKED_FAMILY = Bytes.toBytes("cooked");
			final byte[] DATA = Bytes.toBytes("data");
			byte[] rawData = value.getValue(COOKED_FAMILY, DATA);
			

			if (rawData == null) {
				return;
			}
			ModelResults results = null;
			try {
				results = ModelResults.parseFrom(rawData);
			} catch (InvalidProtocolBufferException ipbe) {
				ipbe.printStackTrace();
				return;
			}
			
			int correct = results.getTotalCorrect();
			int total =  results.getTotalExperiments();
			
			Accuracy.Builder accuracy = Accuracy.newBuilder();
			accuracy.setCorrect(correct);
			accuracy.setTotal(total);
			
			Text key = new Text(row.get());

			try {
			context.write(key,
										new ImmutableBytesWritable(accuracy.build().toByteArray()));
			} catch (IOException ioe) {
				ioe.printStackTrace();
				return;
			} catch (InterruptedException ie) {
				ie.printStackTrace();
			}
		}
	}

	public static class AccuracyReducer 
		extends Reducer<Text, ImmutableBytesWritable, Text, Text> {
		
		public void reduce(Text key,
											 Iterable<ImmutableBytesWritable> values,
											 Reducer.Context context)
			throws IOException, InterruptedException {
			int correct = 0;
			int total = 0;
			StringBuilder result = new StringBuilder();
			for (ImmutableBytesWritable val : values) {
				try {
					Accuracy a = Accuracy.parseFrom(val.get());
					correct += a.getCorrect();
					total += a.getTotal();
				} catch (InvalidProtocolBufferException ipbe) {
					ipbe.printStackTrace();
				}
			}

			double accuracy = correct/total;
			Text output = new Text(result.append(correct)
				                    .append(" ")
				                    .append(total)
				                    .append(" ")
				                    .append(accuracy)
														 .toString());

			//			context.write(key, output);
			context.write(key, new Text("HELLO"));
		}
	}

	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();

		try {
			Job job = new Job(conf, "MRCalcAccuracy");
			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			TableMapReduceUtil.initTableMapperJob("gene",
																						scan,
																						AccuracyMapper.class,
																						Text.class,
																						ImmutableBytesWritable.class,
																						job);
			job.setReducerClass(AccuracyReducer.class);
			job.setNumReduceTasks(1);

			job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			TextOutputFormat.setOutputPath(job, new Path("accuracy.dat"));  

			boolean b = job.waitForCompletion(true);
			if (!b) {
				LOGGER.warning("MRCalcAccuracy job failed!");
			}
		} catch (IOException ioe) {
			LOGGER.warning("Could not setup the mapper and reducer jobs!");
		} catch (InterruptedException ie) {
			LOGGER.warning("MRGenotypeCalling was interrupted!");
		} catch (ClassNotFoundException notFound) {
			LOGGER.warning("Could not find the class: MRCalcAccuracy.class");
		}
	}

}