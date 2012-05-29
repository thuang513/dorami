package com.dorami.mapred;


import com.dorami.data.TwoDimDataPoint;
import com.dorami.data.SNPDataProtos.SNPData;
import com.dorami.data.SNPDataProtos.Answers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.ImmutablePair;
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

public class MRConvertToProto extends TableMapper<ImmutableBytesWritable, Put> {

	/** 
	 *  Setup the logger 
	 */
	private static final Logger LOGGER = 
		Logger.getLogger(MRConvertToProto.class.getName());

	private static Pair<List<TwoDimDataPoint>, List<String>> 
		parseIntensityData(String intensityData) {

		BufferedReader reader = null;
    String buffer = null;
		List<TwoDimDataPoint> points = new ArrayList<TwoDimDataPoint>();
		List<String> personId = new ArrayList<String>();
    final int PERSON = 0;
    final int ALLELE_A = 1;
    final int ALLELE_B = 2;
		
    try {
      reader = new BufferedReader(new StringReader(intensityData));
      while ((buffer = reader.readLine()) != null) {
        String[] rowData = buffer.split(" ");
        double intensityA = Double.valueOf(rowData[ALLELE_A]);
        double intensityB = Double.valueOf(rowData[ALLELE_B]);
        TwoDimDataPoint point = new TwoDimDataPoint(intensityA,
                                                    intensityB);
        points.add(point);
        personId.add(rowData[PERSON]);
      }
    } catch (IOException ioe) {
			LOGGER.severe("Reading error!");
      return null;
    } 

		ImmutablePair<List<TwoDimDataPoint>, List<String>> results = 
			new ImmutablePair<List<TwoDimDataPoint>, List<String>>(points,
																														 personId);
		return results;
	}

	private Answers loadAnswers(String answers) throws IOException {
		BufferedReader read = new BufferedReader(new StringReader(answers));
		String buffer = null;

		final String SEPERATOR = " ";
		final int PERSON_ID = 0;
		final int ANSWER = 1;

		Answers.Builder newAnswers = Answers.newBuilder();
		while ((buffer = read.readLine()) != null) {
			String[] data = buffer.split(SEPERATOR);
			String genotype = data[ANSWER].trim();
			Answers.GenotypeObs observed = 
				Answers.GenotypeObs.newBuilder()
				                   .setPersonId(data[PERSON_ID])
				                   .setGenotype(genotype)
				                   .build();
			newAnswers.addObserved(observed);
		}

		return newAnswers.build();
	}

	public void map(ImmutableBytesWritable row, 
									Result value, 
									Context context) {
		final byte[] RAW_FAMILY = Bytes.toBytes("raw");
		final byte[] INTENSITY_STRING = Bytes.toBytes("intensity-data");
		final byte[] ANSWER_STRING = Bytes.toBytes("answers");

		final byte[] INTENSITY_PROTO = Bytes.toBytes("intensity-proto");
		final byte[] ANSWER_PROTO = Bytes.toBytes("answer-proto");

		final byte[] rawIntensityData = value.getValue(RAW_FAMILY, 
																									 INTENSITY_STRING);
		final byte[] rawAnswers = value.getValue(RAW_FAMILY,
																						 ANSWER_STRING);

		// Only want to deal with interesting data that have results...
		if (rawIntensityData == null || rawAnswers == null) {
			return;
		}

		if (rawIntensityData.equals("") || rawAnswers.equals("")) {
			return;
		}

		String intensityString =
			Bytes.toString(rawIntensityData);
		
		String answersString = 
			Bytes.toString(rawAnswers);

		Pair<List<TwoDimDataPoint>, List<String>> intensityDataPair = 
			parseIntensityData(intensityString);

		List<TwoDimDataPoint> intensityPoints = intensityDataPair.getLeft();
		List<String> personId = intensityDataPair.getRight();

		SNPData.Builder newSNP = SNPData.newBuilder();
		for (int i = 0; i < intensityPoints.size(); ++i) {
			TwoDimDataPoint point = intensityPoints.get(i);

			SNPData.PersonSNP.Builder newPerson = 
				SNPData.PersonSNP.newBuilder();

			newPerson.setIntensityA(point.getX());
			newPerson.setIntensityB(point.getY());

			newPerson.setPersonId(personId.get(i));
			newSNP.addPeople(newPerson);
		}

		Answers answers = null;

		try {
			answers = loadAnswers(answersString);

		} catch (IOException ioe) {
			System.err.println("Couldn't figure out how to read answers!");
		}

		// Write to the HTable row.
		try {
			Put addingRow = new Put(row.get());
			addingRow.add(RAW_FAMILY,
										INTENSITY_PROTO,
										newSNP.build().toByteArray());
			
			addingRow.add(RAW_FAMILY,
										ANSWER_PROTO,
										answers.toByteArray());

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
			Job job = new Job(conf, "MRConvertToProto");
			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);

			TableMapReduceUtil.initTableMapperJob("gene",
																						scan,
																						MRConvertToProto.class,
																						null,
																						null,
																						job);
			
			TableMapReduceUtil.initTableReducerJob("gene",
																						 null,             // reducer class
																						 job);
			job.setNumReduceTasks(0);
			boolean b = job.waitForCompletion(true);
			if (!b) {
				LOGGER.warning("MRConvertToProto job failed!");
			}
		} catch (IOException ioe) {
			LOGGER.warning("Could not setup the mapper and reducer jobs!");
		} catch (InterruptedException ie) {
			LOGGER.warning("MRConvertToProto was interrupted!");
		} catch (ClassNotFoundException notFound) {
			LOGGER.warning("Could not find the class: MRConvertToProto.class");
		}
	}
}