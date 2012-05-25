package com.dorami.mapred;


import com.dorami.data.TwoDimDataPoint;
import com.dorami.data.SNPDataProtos.SNPData;
import com.dorami.data.SNPDataProtos.Answers;
import com.dorami.clustering.GaussianMixtureModel;
import com.dorami.util.RUtil;
import com.dorami.util.SNPAnswerMap;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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

public class MRGenotypeCalling extends TableMapper<ImmutableBytesWritable, Put> {

	/** 
	 *  Setup the logger 
	 */
	private static final Logger LOGGER = 
		Logger.getLogger(MRGenotypeCalling.class.getName());

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



  private static GaussianMixtureModel gaussianClustering(SNPData data,
																												 RUtil r) {
		final int NUM_CLUSTERS = 3;
		GaussianMixtureModel gmm = GaussianMixtureModel.createWithSNPData(intensityData,
																																			NUM_CLUSTERS,
																																			r);
		
		return gmm;
	}

  private static GaussianMixtureModel gaussianClustering(List<TwoDimDataPoint> data) {
    final int NUM_CLUSTERS = 3;
    GaussianMixtureModel mixtures = 
      new GaussianMixtureModel(data, 
                               NUM_CLUSTERS,
                               new RUtil());
    int i = 0;
    while (true) {
      mixtures.expectationStep();
      mixtures.maximizationStep();

      // TODO: Figure out the right convergence method.
      //       Currently does this by running some iteration.
      if (i >= 50) {
        break;
      }
      ++i;
    }
    return mixtures;
  }

	public void map(ImmutableBytesWritable row, 
									Result value, 
									Context context) {
		final byte[] CONTENT_FAMILY = Bytes.toBytes("raw");
		final byte[] INTENSITY = Bytes.toBytes("intensity-data");
		final byte[] ANSWERS = Bytes.toBytes("answers");

		byte[] rawIntensityData = value.getValue(CONTENT_FAMILY, INTENSITY);
		byte[] rawAnswers = value.getValue(CONTENT_FAMILY, ANSWERS);

		// Only want to deal with interesting data that have results...
		if (rawIntensityData == null || rawAnswers == null) {
			return;
		}

		if (rawIntensityData.length == 0 || rawAnswers.length == 0) {
			return;
		}

		SNPData intensityData = SNPData.parseFrom(rawIntensityData);
		Answers genotypeAnswers = Answers.parseFrom(rawAnswers);

		GaussianMixtureModel gmm = GaussianMixtureModel.createWithSNPData(intensityData, 















		String intensityData =
			Bytes.toString(rawIntensityData);
		
		String answers = 
			Bytes.toString(rawAnswers);

		Pair<List<TwoDimDataPoint>, List<String>> intensityDataPair = 
			parseIntensityData(intensityData);
		List<TwoDimDataPoint> intensityPoints = intensityDataPair.getLeft();

		GaussianMixtureModel weights = gaussianClustering(intensityPoints);
		List<String> personId = intensityDataPair.getRight();

		SNPAnswerMap answerMap = new SNPAnswerMap(answers);

		int correct = 0;

		final char SPACE = ' ';
		StringBuilder report = new StringBuilder();
		for (int i = 0; i < personId.size(); ++i) {
			TwoDimDataPoint point = intensityPoints.get(i);
			String person = personId.get(i);
			report.append(person)
				.append(SPACE)
				.append(point.getX())
				.append(SPACE)
				.append(point.getY())
				.append(SPACE);
			
			// Write the probabilities for cluster 0,1,2
			int clusterGuess = 0;
			double highestWeight = Double.MIN_VALUE;
			for (int c = 0; c < weights.getNumClusters(); ++c) {
				double currentWeight = weights.getWeight(i,c);
				if (currentWeight > highestWeight) {
					highestWeight = currentWeight;
					clusterGuess = c;
				}
				report.append(currentWeight).append(SPACE);
			}
			
			// Write the guess for this person
			report.append(clusterGuess).append(SPACE).append("\n");

			if (answerMap.checkAnswer(person, clusterGuess, false) == 1) {
				++correct;
			}
		}

		final byte[] COOKED_FAMILY = Bytes.toBytes("cooked");
		final byte[] DATA_QUALIFIER = Bytes.toBytes("data");
		final byte[] ACCURACY_QUALIFIER = Bytes.toBytes("accuracy");

		final byte[] TOTAL_QUALIFIER = Bytes.toBytes("total");

		try {
			Put addingRow = new Put(row.get());
			addingRow.add(COOKED_FAMILY,
										DATA_QUALIFIER,
										Bytes.toBytes(report.toString()));

			addingRow.add(COOKED_FAMILY,
										ACCURACY_QUALIFIER,
										Bytes.toBytes(correct));

			addingRow.add(COOKED_FAMILY,
										TOTAL_QUALIFIER,
										Bytes.toBytes(personId.size()));

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