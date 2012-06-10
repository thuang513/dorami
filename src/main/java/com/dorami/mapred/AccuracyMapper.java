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
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AccuracyMapper extends TableMapper<Text, Text> {
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
			double ratio = correct/total;

			Accuracy.Builder accuracy = Accuracy.newBuilder();
			accuracy.setCorrect(correct);
			accuracy.setTotal(total);
			
			Text key = new Text(row.get());
			
			StringBuilder result = new StringBuilder();
			Text output = new Text(result.append(correct)
														 .append(" ")
														 .append(total)
														 .append(" ")
														 .append(ratio)
														 .toString());

			try {
				//			context.write(key,
				//					new ImmutableBytesWritable(accuracy.build().toByteArray()));
				context.write(key, output);
			} catch (IOException ioe) {
				ioe.printStackTrace();
				return;
			} catch (InterruptedException ie) {
				ie.printStackTrace();
			}
		}
	}

