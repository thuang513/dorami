package com.dorami.util;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SNPAnswerMap {

  /** 
	 *  Setup the logger 
	 */
	private static final Logger LOGGER = 
		Logger.getLogger(SNPAnswerMap.class.getName());

  private HashMap<String, String> map;

  private Set<String> genotypes;

	private String answers;

	private Map<Integer, String> regularGene;
	
	private Map<Integer, String> invertedGene;

  public SNPAnswerMap(String answers) {
    map = new HashMap<String, String>();
		genotypes = new HashSet<String>();
		this.answers = answers;

		try {
			loadAnswers();
		} catch (IOException ioe) {
			System.err.println("Error in loading answers!");
			ioe.printStackTrace();
		}
  }

	private boolean isHomozygous(String gene) {
		if (gene.length() != 2) {
			LOGGER.warning("Gene size is not 2!");
		}
		return gene.charAt(0) == gene.charAt(1);
	}

	private boolean isHeterzygous(String gene) {
		if (gene.length() != 2) {
			LOGGER.warning("Gene size is not 2!");
		}
		return gene.charAt(0) != gene.charAt(1);
	}

	private Map<Integer, String> createGenotypeMap(boolean flip) {
		Map<Integer, String> clusterToGene = new HashMap<Integer, String>();

		final Integer NO_DATA = Integer.valueOf(-1);
		final Integer DOM_CODE = Integer.valueOf(0);
		final Integer HETER = Integer.valueOf(1);
		final Integer WEAK_CODE = Integer.valueOf(2);

		String heterzygous = null;
		String dominant = null;
		String weak = null;
		String no_data = null;

		for (String gene : genotypes) {
			if (gene.equals("NN")) {
				no_data = gene;
				continue;
			}

			if (isHeterzygous(gene)) {
				heterzygous = gene;
				continue;
			}

			if (isHomozygous(gene) && dominant == null) {
				dominant = gene;
				continue;
			}

			if (isHomozygous(gene) && dominant != null) {
				weak = gene;
				continue;
			}
		}

		clusterToGene.put(NO_DATA, no_data);
		clusterToGene.put(HETER, heterzygous);

		if (!flip) {
			clusterToGene.put(DOM_CODE, dominant);
			clusterToGene.put(WEAK_CODE, weak);
		}

		if (flip) {
			clusterToGene.put(DOM_CODE, weak);
			clusterToGene.put(WEAK_CODE, dominant);
		}

		return clusterToGene;
	}

	private void loadAnswers() throws IOException {
		BufferedReader read = new BufferedReader(new StringReader(answers));
		String buffer = null;

		final String SEPERATOR = " ";
		final int PERSON_ID = 0;
		final int ANSWER = 1;

		while ((buffer = read.readLine()) != null) {
			String[] data = buffer.split(SEPERATOR);
			String answer = data[ANSWER].trim();
			genotypes.add(answer);
			map.put(data[PERSON_ID], answer);
		}

		regularGene = createGenotypeMap(false);
		invertedGene = createGenotypeMap(true);
	}

	/**
	 *  Returns -1 = no data.
	 *           0 = wrong
	 *           1 = correct
	 */
	 public int checkAnswer(String person, int cluster, boolean inverted) {
		 String correctGene = map.get(person);
		 String guessedGene = null;

		 if (!inverted) {
			 guessedGene = regularGene.get(Integer.valueOf(cluster));
		 }

		 if (inverted) {
			 guessedGene = invertedGene.get(Integer.valueOf(cluster));

		 }

		 if (guessedGene == null) {
			 return 0;
		 }

		 if (guessedGene.equals("NN")) {
			 return -1;
		 }

		 if (guessedGene.equals(correctGene)) {
			 return 1;
		 }

		 if (!guessedGene.equals(correctGene)) {
			 return 0;
		 }

		 return -2;
	}
}
