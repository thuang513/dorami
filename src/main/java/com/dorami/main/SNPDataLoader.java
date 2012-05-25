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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
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
		options.addOption(rsidMapFile);
		options.addOption(help);
	}



  private static Put addSNP(String rsid,
														String data,
														HTable table) {
		final byte[] COLUMN_FAMILY = Bytes.toBytes("raw");
    final byte[] QUALIFIER = Bytes.toBytes("intensity-data");
    byte[] rsidBytes = Bytes.toBytes(rsid);

		// Check to see if the row already exists
		Get get = new Get(rsidBytes);
		get.addColumn(COLUMN_FAMILY, QUALIFIER);

		Result result = null;
		try {
			result = table.get(get);
		} catch (IOException ioe) {
			System.err.println("Couldn't search for results!");
			ioe.printStackTrace();
			return null;
		}

		StringBuilder dataToAdd = new StringBuilder();
		if (!result.isEmpty()) {
			byte[] previousData = result.getValue(COLUMN_FAMILY, QUALIFIER);
			dataToAdd.append(Bytes.toString(previousData));
		}

		byte[] newData = Bytes.toBytes(dataToAdd.append(data).toString());
		Put put = new Put(rsidBytes);
		put.add(COLUMN_FAMILY, QUALIFIER,  newData);
    return put;
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
    if (!domBuffer.equals(weakBuffer)) {
      LOGGER.severe("The headers do not match!");
			System.err.println("dom = " + domBuffer);
			System.err.println("weakBuffer = " + weakBuffer);
      return;
    }

		System.out.println(domBuffer.replaceAll("\"",""));
    String[] person_id = domBuffer.replaceAll("\"", "").split(" ");
		
    final int SNP_ID = 0;
    while ((domBuffer = dom.readLine()) != null &&
           (weakBuffer = weak.readLine()) != null) {

      // 1. translate snp to rsid
      String domData[] = domBuffer.replaceAll("\"","").split(" ");
      String weakData[] = weakBuffer.replaceAll("\"","").split(" ");
      String rsid = rsidMap.getValue(domData[SNP_ID]);

      // 2. print out a list of the individual and its A and B intensity
      //    values.
      StringBuilder intensityData = new StringBuilder();
      for (int i = 1; i < domData.length; ++i) {
        intensityData.append(person_id[i-1].substring(0, 11))
                     .append(" ")
                     .append(domData[i])
                     .append(" ")
                     .append(weakData[i])
                     .append('\n');
      }

      // 3. Append this SNP in HTable.
			//			System.out.println("Adding " + rsid);
      Put newPut = addSNP(rsid, intensityData.toString(), table);
			puts.add(newPut);

			if (puts.size() >= BATCH_LIMIT) {
				processPuts(table, puts);
				puts.clear();
				
				if (!puts.isEmpty()) {
					System.err.println("Didn't clear puts batch!");
					System.exit(-1);
				}
			}
		}
		
		dom.close();
		weak.close();

		if (!puts.isEmpty()) {
			processPuts(table, puts);
			puts.clear();
		}
	}

  public static void main(String[] args) {
    CommandLineParser parser = new GnuParser();
		CommandLine line = null;

		try {
			// parse the command line arguments
			line = parser.parse(options, args);
		} catch (ParseException pe) {
      System.err.println("Usage: SNPDataLoader -snp FOLDER -rsid RSID_FILE");
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
				String domFile = snpFolder + "/" + i + ".dom.dat";
				String weakFile = snpFolder + "/" + i + ".weak.dat";
				System.out.println("================= Processing " + domFile + "=======================");
        readData(domFile, weakFile, rsidMap);
      }
    } catch (IOException ioe) {
      LOGGER.warning("IO Error in reading data!");
      System.exit(-1);
    }
  }
}