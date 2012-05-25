package com.dorami.main;


import com.dorami.data.TwoDimDataPoint;
import com.dorami.clustering.GaussianMixtureModel;
import com.dorami.util.RUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.ParseException;

public class EMClustering {

  /** 
	 *  Setup the logger 
	 */
	private static final Logger LOGGER = 
		Logger.getLogger(EMClustering.class.getName());

	private static final Options options = new Options();
	private static final String HELP = "help";

	static {
		Option inputFile = 
			OptionBuilder.withArgName("i")
			             .withDescription("The input file")
           			   .isRequired()
			             .hasArg()
			             .create("i");
		
		Option outputFile = 
			OptionBuilder.withArgName("o")
			             .withDescription("The output file")
			             .isRequired()
			             .hasArg()
			             .create("o");

		Option help = 
			OptionBuilder.withArgName(HELP)
			             .create(HELP);

		options.addOption(inputFile);
		options.addOption(outputFile);
		options.addOption(help);
	}
  
  private static GaussianMixtureModel gaussianClustering(List<TwoDimDataPoint> data) {
    final int NUM_CLUSTERS = 3;

		RUtil output = new RUtil();
    GaussianMixtureModel mixtures = 
      new GaussianMixtureModel(data, 
                               NUM_CLUSTERS,
                               output);
    
		System.out.println(output);
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

  private static boolean loadData(String filepath, 
                                  List<TwoDimDataPoint> data,
                                  List<String> personId) {
    final int PERSON = 0;
    final int ALLELE_A = 1;
    final int ALLELE_B = 2;

    BufferedReader file = null;
    String buffer = "";

    try {
      file = new BufferedReader(new FileReader(filepath));
      while ((buffer = file.readLine()) != null) {
        String[] rowData = buffer.split(" ");
        double intensityA = Double.valueOf(rowData[ALLELE_A]);
        double intensityB = Double.valueOf(rowData[ALLELE_B]);
        TwoDimDataPoint point = new TwoDimDataPoint(intensityA,
                                                    intensityB);
        data.add(point);
        personId.add(rowData[PERSON]);
      }
    } catch (FileNotFoundException fnfe) {
      LOGGER.severe("File: " + filepath + " NOT FOUND!");
      return false;
    } catch (IOException ioe) {
      LOGGER.severe("Reading error!");
      return false;
    } 
    
    return true;
  }

  private static void writeData(List<String> personIds,
                                List<TwoDimDataPoint> data,
                                GaussianMixtureModel weights,
                                String outFilePath) {
    try {
      FileWriter writer = new FileWriter(outFilePath);
      for (int i = 0; i < personIds.size(); ++i) {
        TwoDimDataPoint point = data.get(i);
        String person = personIds.get(i);

        // Write the person's name
        writer.write(person + " ");
        
        // Write the intensity values
        writer.write(point.getX() + " " + point.getY() + " ");

        // Write the probabilities for cluster 0,1,2
        int clusterGuess = 0;
        double highestWeight = Double.MIN_VALUE;
        for (int c = 0; c < weights.getNumClusters(); ++c) {
          double currentWeight = weights.getWeight(i,c);
          if (currentWeight > highestWeight) {
            highestWeight = currentWeight;
            clusterGuess = c;
          }
          writer.write(currentWeight + " ");
        }
        // Write the guess for this person
        writer.write(clusterGuess + " " + weights.getNumClusters() + "\n");

        // Write the correctness of the guess (0 = incorrect, 1 = correct).
      }

      writer.flush();
      writer.close();
    } catch (IOException ioe) {
      LOGGER.severe("I/O Error!");
      return;
    }
  }

  /**
   *  Expects file format as:
   *  [PERSON] [Allele A Intensity] [Allele B Intensity]
   */
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

    String inputFilePath = line.getOptionValue("i");
    String outputFilePath = line.getOptionValue("o");

    List<TwoDimDataPoint> data = new ArrayList();
    List<String> personId = new ArrayList();
    loadData(inputFilePath, data, personId);
    GaussianMixtureModel results = gaussianClustering(data);
    writeData(personId, data, results, outputFilePath);
  }
}