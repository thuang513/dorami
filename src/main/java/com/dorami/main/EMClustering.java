package com.dorami.main;


import com.dorami.data.TwoDimDataPoint;
import com.dorami.clustering.GaussianMixtureModel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
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
  
  private void gaussianClustering(List<TwoDimDataPoint> data) {
    final int NUM_CLUSTERS = 3;
    GaussianMixtureModel mixtures = new GaussianMixtureModel(data, NUM_CLUSTERS);
    mixtures.expectationStep();
    mixtures.maximizationStep();
  }

  private static boolean loadData(String filepath, 
                              List<TwoDimDataPoint> data,
                              List<String> personId) {
    final int PERSON = 0;
    final int ALLELE_A = 0;
    final int ALLELE_B = 0;

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
    
  public static void usage() {
    
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
  }
}