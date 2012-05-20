package com.dorami.main;


import com.dorami.util.FileMap;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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

/**
 *  A quick/hacked-up class used to load SNP intensity data into an HTable.
 *
 *  @author Terry Huang (thuang513@gmail.com)
 */
public class SNPDataLoader {

  /** 
	 *  Setup the logger 
	 */
	private static final Logger LOGGER = 
		Logger.getLogger(SNPDataLoader.class.getName());
  
	private static final Options options = new Options();
	private static final String HELP = "help";

	static {
		Option snpFolder = 
			OptionBuilder.withArgName("snp")
                   .withDescription("Folder containing the SNP data.")
                   .isRequired()
			             .hasArg()
			             .create("snp");

    Option rsidMapFile = 
			OptionBuilder.withArgName("rsid")
                   .withDescription("The file contianing the RSID to SNP id data.")
                   .isRequired()
			             .hasArg()
			             .create("rsid");

		Option help = 
			OptionBuilder.withArgName(HELP)
			             .create(HELP);

		options.addOption(snpFolder);
		options.addOption(help);
	}



  private static Put addSNP(String rsid,
                            String data) {
    final byte[] QUALIFIER = Bytes.toBytes("intensity-data");
    byte[] rsidBytes = Bytes.toBytes(rsid);
    byte[] dataBytes = Bytes.toBytes(data);
    Put result = new Put(rsidBytes);
    result.add(rsidBytes,
               QUALIFIER,
               dataBytes);
    return result;
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
      return;
		}
  }

  private static void readData(String domFile, 
                               String weakFile,
                               FileMap rsidMap) throws IOException {
    // Open up the dom and weak dat files.
    BufferedReader dom = new BufferedReader(new FileReader(domFile));
    BufferedReader weak = new BufferedReader(new FileReader(weakFile));

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

    final int BATCH_LIMIT = 10000;
    List<Put> puts = new ArrayList<Put>(BATCH_LIMIT);


    // 0. read the headers to get the person id.
    //    also make sure the headers match.
    String domBuffer = dom.readLine();
    String weakBuffer = weak.readLine();
    if (domBuffer != weakBuffer) {
      LOGGER.severe("The headers do not match!");
      return;
    }

    String[] person_id = domBuffer.replace("\"\'", "").split(" ");

    final int SNP_ID = 0;
    while ((domBuffer = dom.readLine()) != null &&
           (weakBuffer = weak.readLine()) != null) {

      // 1. translate snp to rsid
      String domData[] = domBuffer.split(" ");
      String weakData[] = domBuffer.split(" ");
      String rsid = rsidMap.getValue(domData[SNP_ID]);

      // 2. print out a list of the individual and its A and B intensity
      //    values.
      StringBuilder intensityData = new StringBuilder();
      for (int i = 1; i < domData.length; ++i) {
        intensityData.append(person_id[i])
                     .append(" ")
                     .append(domData[i])
                     .append(" ")
                     .append(weakData[i])
                     .append('\n');
      }

      // 3. Put this SNP in HTable.
      Put newPut = addSNP(domData[0], intensityData.toString());
      puts.add(newPut);

      if (puts.size() >= BATCH_LIMIT) {
        processPuts(table, puts);
      }
    }
  }

  public static void main(String[] args) {
    CommandLineParser parser = new GnuParser();
		CommandLine line = null;

		try {
			// parse the command line arguments
			line = parser.parse(options, args);
		} catch (ParseException pe) {
      System.err.println("SNPDataLoader -snp FOLDER -rsid RSID_FILE");
			System.exit(-1);
		}

    String snpFolder = line.getOptionValue("snp");
    String rsidMapFile = line.getOptionValue("rsid");
    FileMap rsidMap = FileMap.createFileMapFromPath(rsidMapFile);
    
    if (rsidMap == null) {
      System.err.println("Invalid RSID map.");
      System.exit(-1);
    }

    try {
      for (int i = 0; i < 8; ++i) {
        readData(snpFolder + "/" + i + ".dom.dat",
                 snpFolder + "/" + i + ".weak.dat",
                 rsidMap);
      }
    } catch (IOException ioe) {
      LOGGER.warning("IO Error in reading data!");
      System.exit(-1);
    }
  }
}