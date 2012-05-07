package com.dorami.clustering;


import java.util.logging.Logger;
import java.util.regex.PatternSyntaxException;

public class MRAffinityPropagation {
	
	/** 
	 *  Setup the logger 
	 */
	private static final Logger LOGGER = 
		Logger.getLogger(MRAffinityPropagation.class.getName());

	private static final char MESSAGE_DELIM_CHAR = ' ';

	public static class RespPrepMapper extends MapReduceBase
		implements Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, 
										Text value,
										OutputCollector<Text, Text> output,
										Reporter reporter) {
			// Parse the value.
			String line = value.toString();
			String[] messageData = null;

			try {
				messageData = line.split(MESSAGE_DELIM_CHAR);
			} catch (PatternSyntaxException pse) {
				LOGGER.error("Couldn't read the map value!");
				return;
			}
			
			final int ROW = 0;
			final int COLUMN = 1;
			final int SCORE = 2;

			Double scoreValue = Double.valueOf(messageData[SCORE]);
			String combinedValue =
				new StringBuilder().append(messageData[COLUMN])
				                   .append(MESSAGE_DELIM_CHAR)
				                   .append(messageData[SCORE])
				                   .toString();

			// Output the value.
			output.collect(messageData[ROW], combinedValue);
		}
	}

	public static class RespCalcReducer extends MapReduceBase
		implements Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key,
											 Iterator<Text> values,
											 OutputCollector<Text, Text> output,
											 Reporter reporter) {
			// Place all the values in a hash.
			HashMap<Integer, List<ASMessageScore>> scoresByColumn = new HashMap();
			while (values.hasNext()) {
				ASMessageScore msgScore = new ASMessageScore(values.next());
				if (scoresByColumn.containsKey(msgScore.getColumn())) {
					List<ASMessageScore> col = scoresByColumn.get(msgScore.getColumn());
					col.add(msgScore);
				}

				if (!scoresByColumn.containsKey(msgScore.getColumn())) {
					List<ASMessageScore> col = new ArrayList<ASMessageScore>();
					col.add(msgScore);
					scoresByColumn.put(msgScore.getColumn(), col);
				}
			}

			// Combine the scores and find the top two values.
			double max = Double.MIN_VALUE;
			double second_max = Double.MIN_VALUE;
			Integer maxCol = null;
			for (Integer col : scoresByColumn.getKeySet()) {
				List<ASMessageScore> scores = scoresByColumn.get(col);
				
			}


			// Output resp[i,k] for every k.
		}
	}
}


class TopTwoValues {
	
	private Double topValue;

	private Double secondTopValue;

	public TopTwoValues() {
		topValue = Double.MIN_VALUE;
		secondTopValue = Double.MIN_VALUE;
	}

	public void checkTopValue(Double c, int originColumn) {
		if (c.compareTo(topValue) > 0) {
			topValue = c;
			return;
		}

		if (c.compareTo(secondTopValue) > 0) {
			topValue = c;
			return;
		}
	}
	
	public Double getTopValue() {
		return topValue;
	}

	public Double getSecondTopValue() {
		return secondTopValue;
	}
}
