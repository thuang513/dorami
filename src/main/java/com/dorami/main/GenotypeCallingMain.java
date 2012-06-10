package com.dorami.main;

import com.dorami.data.SNPDataProtos.SNPData;
import com.dorami.data.SNPDataProtos.Answers;
import com.dorami.data.SNPDataProtos.ModelResults;
import com.dorami.data.SNPDataProtos.ModelResults.GenotypeModel;
import com.dorami.clustering.GenotypeCallingMM;
import com.dorami.util.RUtil;
import com.dorami.util.SNPAnswerMap;

import java.io.FileWriter;
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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;

public class GenotypeCallingMain {
	public static void main(String[] args) {
		final byte[] CONTENT_FAMILY = Bytes.toBytes("raw");
		final byte[] INTENSITY = Bytes.toBytes("intensity-proto");
		final byte[] ANSWERS = Bytes.toBytes("answer-proto");

		Get get = new Get(Bytes.toBytes(args[0]));
		get.addColumn(CONTENT_FAMILY, INTENSITY);
		get.addColumn(CONTENT_FAMILY, ANSWERS);

		Configuration config = HBaseConfiguration.create();
		HTable table = null;
    final String TABLE_NAME = "gene";
		try {
			table = new HTable(config, TABLE_NAME);
		} catch (IOException ioe) {
			System.err.println("Error in connecting to the network!");
			return;
		}

		Result result = null;
		try {
			result = table.get(get);
		} catch (IOException ioe) {
			System.err.println("Couldn't search for results!");
			ioe.printStackTrace();
			return;
		}

		byte[] rawIntensityData = result.getValue(CONTENT_FAMILY, INTENSITY);
		byte[] rawAnswers = result.getValue(CONTENT_FAMILY, ANSWERS);

		// Parse the results from binary to protobuf forms.
		SNPData intensityData = null;
		Answers genotypeAnswers = null;
		try {
			intensityData = SNPData.parseFrom(rawIntensityData);
			genotypeAnswers = Answers.parseFrom(rawAnswers);
		} catch (InvalidProtocolBufferException ipbf) {
			System.err.println("Error in parsing the protobufs!");
			return;
		}

		SNPAnswerMap answerMap = new SNPAnswerMap(genotypeAnswers);
		GenotypeCallingMM gmm = new GenotypeCallingMM(intensityData, answerMap);
		ModelResults runResults = gmm.clusterAndAnalyze();

		try {
			FileWriter r = new FileWriter("debug.R");
			r.write(runResults.getROutput());
			r.flush();
			r.close();
			
			FileWriter data = new FileWriter("debug.dat");
			StringBuilder output = new StringBuilder();
			for (GenotypeModel gm : runResults.getResultList()) {
				output.append(gm.getSnpData().getPersonId())
				.append(" ")
					.append(gm.getSnpData().getIntensityA())
					.append(" ")
					.append(gm.getSnpData().getIntensityB())
					.append(" ")
					.append(gm.getGuess())
					.append("\n");
				
			}
			data.write(output.toString());
			data.flush();
			data.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		System.out.println("# Accuracy for " + args[0] + ": " + runResults.getTotalCorrect());
	}
}