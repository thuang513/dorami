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

public class AccuracyReducer 
	extends TableReducer<Text, ImmutableBytesWritable, ImmutableBytesWritable> {
	
	protected void setup() {
		System.out.println("Running reducer!");
	}
	
	protected void reduce(Text key, 
												Iterable<ImmutableBytesWritable> values,
												Reducer.Context context) throws IOException, InterruptedException {
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
		
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(Bytes.toBytes("output"),
						Bytes.toBytes("accuracy"),
						output.getBytes());
		context.write(null, put);

		// context.write(key, output);
		//context.write(new Text("HELLO"), new IntWritable(1));
	}
}





