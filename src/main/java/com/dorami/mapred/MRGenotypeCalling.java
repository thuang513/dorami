package com.dorami.mapred;


import com.dorami.data.SNPDataProtos.SNPData;
import com.dorami.data.SNPDataProtos.Answers;
import com.dorami.data.SNPDataProtos.ModelResults;
import com.dorami.clustering.GenotypeCallingMM;
import com.dorami.util.RUtil;
import com.dorami.util.SNPAnswerMap;

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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MRGenotypeCalling extends TableMapper<ImmutableBytesWritable, Put> {

	/** 
	 *  Setup the logger 
	 */
	private static final Logger LOGGER = 
		Logger.getLogger(MRGenotypeCalling.class.getName());
	public void map(ImmutableBytesWritable row, 
									Result value, 
									Context context) {
		// Read the data from HTable.
		final byte[] CONTENT_FAMILY = Bytes.toBytes("raw");
		final byte[] INTENSITY = Bytes.toBytes("intensity-proto");
		final byte[] ANSWERS = Bytes.toBytes("answer-proto");
		byte[] rawIntensityData = value.getValue(CONTENT_FAMILY, INTENSITY);
		byte[] rawAnswers = value.getValue(CONTENT_FAMILY, ANSWERS);
		// Only want to deal with interesting data that have results...
		if (rawIntensityData == null || rawAnswers == null) {
			return;
		}
		if (rawIntensityData.length == 0 || rawAnswers.length == 0) {
			return;
		}
		// Parse the results from binary to protobuf forms.
		SNPData intensityData = null;
		Answers genotypeAnswers = null;
		try {
			intensityData = SNPData.parseFrom(rawIntensityData);
			genotypeAnswers = Answers.parseFrom(rawAnswers);
		} catch (InvalidProtocolBufferException ipbf) {
			LOGGER.warning("Error in parsing the protobufs!");
			return;
		}

		// Cluster and predict.
		SNPAnswerMap answerMap = new SNPAnswerMap(genotypeAnswers);
		GenotypeCallingMM model = new GenotypeCallingMM(intensityData, answerMap);
		ModelResults runResults = model.clusterAndAnalyze();

		// Write the results to HTable.
		final byte[] COOKED_FAMILY = Bytes.toBytes("cooked");
		final byte[] DATA_QUALIFIER = Bytes.toBytes("data");
		try {
			Put addingRow = new Put(row.get());
			addingRow.add(COOKED_FAMILY,
										DATA_QUALIFIER,
										runResults.toByteArray());
			context.write(row, addingRow); 
		} catch (IOException ioe) {
			LOGGER.warning("Couldn't write out the summary to HTable!");
		} catch (InterruptedException ie) {
			LOGGER.warning("Writing to HTable was interrupted!");
		}
	}

	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();

		// Setup what parts of the HTable do we want to map over.
		try {
			Job job = new Job(conf, "MRGenotypeCalling");
			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);

			TableMapReduceUtil.initTableMapperJob("gene",
																						scan,
																						MRGenotypeCalling.class,
																						null,
																						null,
																						job);
			
			TableMapReduceUtil.initTableReducerJob("gene",
																						 null,             // reducer class
																						 job);
			job.setNumReduceTasks(0);
			boolean b = job.waitForCompletion(true);
			if (!b) {
				LOGGER.warning("MRGenotypeCalling job failed!");
			}
		} catch (IOException ioe) {
			LOGGER.warning("Could not setup the mapper and reducer jobs!");
		} catch (InterruptedException ie) {
			LOGGER.warning("MRGenotypeCalling was interrupted!");
		} catch (ClassNotFoundException notFound) {
			LOGGER.warning("Could not find the class: MRGenotypeCalling.class");
		}
	}
}