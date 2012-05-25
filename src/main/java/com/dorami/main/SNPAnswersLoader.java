package com.dorami.main;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class SNPAnswersLoader {
 
  /** 
	 *  Setup the logger 
	 */
	private static final Logger LOGGER = 
		Logger.getLogger(SNPDataLoader.class.getName());
  
	private static final Options options = new Options();
	private static final String HELP = "help";

	static {
		Option chrFile = 
			OptionBuilder.withArgName("c")
			.withDescription("Chromosome file.")
			.isRequired()
			.hasArg()
			.create("c");

    Option populationCode = 
			OptionBuilder.withArgName("p")
			.withDescription("Population code.")
			.isRequired()
			.hasArg()
			.create("p");

		options.addOption(chrFile);
		options.addOption(populationCode);
	}

	private static Put addAnswerToSNP(String popCode,
																		String rsid,
																		String snpAlleles,
																		List<String> names,
																		List<String> answers) {
		if (names.size() != answers.size()) {
			LOGGER.warning("Mismatch in answers and names!");
			System.exit(-1);
		}

		StringBuilder finalData = new StringBuilder();
		for (int i = 0; i < names.size(); ++i) {
			finalData.append(popCode + "_")
				.append(names.get(i))
				.append(" ")
				.append(answers.get(i))
				.append("\n");
		}

		final byte[] rsidBytes = Bytes.toBytes(rsid);
		final byte[] COLUMN_FAMILY = Bytes.toBytes("raw");
		final byte[] QUALIFIER = Bytes.toBytes("answers");
		final byte[] data = Bytes.toBytes(finalData.toString());
		
		Put newPut = new Put(rsidBytes);
		newPut.add(COLUMN_FAMILY, QUALIFIER, data);

		System.out.println("Adding " + rsid);
		return newPut;
	}

  private static void processPuts(HTable table,
																	List<Put> puts) {
    if (table == null) {
      LOGGER.warning("Couldn't get HTable!");
      return;
    }

    if (puts == null) {
      LOGGER.warning("Nothing to put!");
      return;
    }

    // Add the rows to the HTable.
		try {
			table.put(puts);
		} catch (IOException ioe) {
			LOGGER.warning("Erroring in putting row to HTable!");
			ioe.printStackTrace();
      return;
		}

		LOGGER.info("Added " + puts.size() + "SNPs to table.");
  }

  private static void readData(String filePath, 
															 String popCode) throws IOException {

    BufferedReader reader = new BufferedReader(new FileReader(filePath));

    // Read the header and grab the personIDs.
    String buffer = reader.readLine();
    String[] header = buffer.split(" ");
    List<String> headerList = Arrays.asList(header);
    List<String> names = headerList.subList(11, headerList.size());
		final int RSID = 0;
		final int SNP_ALLELES = 1;

		List<Put> puts = new ArrayList<Put>();
	  while ((buffer = reader.readLine()) != null) {
			List<String> data = Arrays.asList(buffer.split(" "));
			List<String> answers = data.subList(11, data.size());

			Put newPut = addAnswerToSNP(popCode,
																	data.get(RSID),
																	data.get(SNP_ALLELES),
																	names,
																	answers);
			puts.add(newPut);
		}
		
		// Open up an HTable connection.
		Configuration config = HBaseConfiguration.create();
		HTable table = null;
    final String TABLE_NAME = "gene";
		try {
			table = new HTable(config, TABLE_NAME);
		} catch (IOException ioe) {
			LOGGER.warning("Error in connecting to the network!");
			return;
		}

		processPuts(table, puts);
  }
  
  public static void main(String[] args) {
    CommandLineParser parser = new GnuParser();
    CommandLine line = null;
    
    try {
			// parse the command line arguments
			line = parser.parse(options, args);
		} catch (ParseException pe) {
      System.err.println("SNPAnswersLoader -c CHROMOSOME_FILE -p POPULATION_CODE");
			System.exit(-1);
		}
    
    String chromoFile = line.getOptionValue("c");
    String popCode = line.getOptionValue("p");

		try {
			readData(chromoFile, popCode);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
  }
}