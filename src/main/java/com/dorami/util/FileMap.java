package com.dorami.util;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.HashMap;

public class FileMap {

  /** 
	 *  Setup the logger 
	 */
	private static final Logger LOGGER = 
		Logger.getLogger(FileMap.class.getName());

  private HashMap<String, String> map;

  private File file;

  public static FileMap createFileMapFromPath(String fileLoc) {
    if (fileLoc == null || fileLoc == "") {
      LOGGER.severe("File location is null!");
      return null;
    }

    return new FileMap(new File(fileLoc));
  }

  public FileMap(File file) {
    map = new HashMap<String, String>();
    this.file = file;
    loadIntoMap();
  }

  private boolean loadIntoMap() {
    try {
      BufferedReader read = new BufferedReader(new FileReader(file));
      String buffer = null;
      final String SEPERATOR = " ";
      final String REMOVE_CHAR = "\"\'";
      final int SNP_ID = 0;
      final int RSID = 1;
      
      while((buffer = read.readLine()) != null) {
        String[] rowData = buffer.replace(REMOVE_CHAR, "").split(" ");
        map.put(rowData[SNP_ID], rowData[RSID]);
      }
    } catch (FileNotFoundException fnfe) {
      LOGGER.severe("File: " + file.getAbsolutePath() + " NOT FOUND!");
      return false;
    } catch (IOException ioe) {
      LOGGER.severe("Reading error in the mapper!");
      return false;
    }
    return true;
  }

  public String getValue(String key) {
    return map.get(key);
  }
}

