package com.dorami.clustering;


import com.dorami.clustering.GaussianMixtureModel;
import com.dorami.data.SNPDataProtos.SNPData;
import com.dorami.data.SNPDataProtos.SNPData.PersonSNP;
import com.dorami.data.SNPDataProtos.Answers;
import com.dorami.data.SNPDataProtos.ModelResults;
import com.dorami.data.SNPDataProtos.ModelResults.GenotypeModel;
import com.dorami.data.SNPDataProtos.ModelResults.Outcome;
import com.dorami.data.TwoDimDataPoint;
import com.dorami.util.RUtil;
import com.dorami.util.SNPAnswerMap;

import java.util.ArrayList;
import java.util.List;

public class GenotypeCallingMM {

	private GaussianMixtureModel gmm;

	private RUtil r;

	private SNPData data;

	private SNPAnswerMap answerMap;

	public GenotypeCallingMM(SNPData data,
													 SNPAnswerMap answerMap) {
		List<PersonSNP> snpData = data.getPeopleList();
		List<TwoDimDataPoint> dataPoints = new ArrayList<TwoDimDataPoint>();
		for (PersonSNP person : snpData) {
			dataPoints.add(new TwoDimDataPoint(person));
		}

		this.data = data;
		this.answerMap = answerMap;

		r = new RUtil();
		final int NUM_CLUSTERS = 3;
		gmm = new GaussianMixtureModel(dataPoints, NUM_CLUSTERS, r);
	}

	private static GenotypeModel.Builder createGenotypeModel(PersonSNP person,
																														 int clusterGuess) {
		// Set the guess, correctness, and r output.
		GenotypeModel.Builder personResult = GenotypeModel.newBuilder();
		personResult.setSnpData(person);
		personResult.setGuess(clusterGuess);

		return personResult;
	}

	private static Outcome interpretGuess(int guess) {
		if (guess == -1) {
			return Outcome.NODATA;
		}

		if (guess == 0) {
			return Outcome.INCORRECT;
		}

		if (guess == 1) {
			return Outcome.CORRECT;
		}

		return null;
	}

	private int guessCluster(int point) {
		int clusterGuess = -1;
		double highestWeight = Double.MIN_VALUE;
		for (int c = 0; c < gmm.getNumClusters(); ++c) {
			double currentWeight = gmm.getWeight(point, c);
			if (currentWeight > highestWeight) {
				highestWeight = currentWeight;
				clusterGuess = c;
			}
		}

		return clusterGuess;
	}

	private Outcome checkGuess(String personId, int guess, boolean flip) {
		int flipGuess = answerMap.checkAnswer(personId,
																					guess,
																					flip);
		
		return interpretGuess(flipGuess);
	}

	public ModelResults clusterAndAnalyze() {
		gmm.clusterData();

		// We want to check two versions, flipped and not flipped.
		// Then we will pick the one with the better accuracy score.
		ModelResults.Builder flip = ModelResults.newBuilder();
		ModelResults.Builder noFlip = ModelResults.newBuilder();
		int flipCount = 0;
		int noFlipCount = 0;

		// We want to figure out which configuration has a better accuracy.
		// Also keep count of how many actual experiements were done
		// (i.e. no data not counted).
		int experimentCount = 0;

		boolean FLIP = true;
		boolean NO_FLIP = false;

		// Go through each person, make your prediction, and check the prediction
		// with the actual results.
		for (int i = 0; i < data.getPeopleList().size(); ++i) {
			PersonSNP person = data.getPeople(i);
			int guess = guessCluster(i);
			Outcome flipOutcome = checkGuess(person.getPersonId(), guess, FLIP);
			Outcome noFlipOutcome = checkGuess(person.getPersonId(), guess, NO_FLIP);

			if (flipOutcome == Outcome.NODATA || 
					noFlipOutcome == Outcome.NODATA) {
				continue;
			}
			++experimentCount;
			
			if (flipOutcome == Outcome.CORRECT) {
				++flipCount;
			}

			if (noFlipOutcome == Outcome.CORRECT) {
				++noFlipCount;
			}

			// Build a GenotypeModel entry.
			GenotypeModel.Builder model = createGenotypeModel(person, guess);
			
			// Add to both flip and no flip.
			model.setOutcome(flipOutcome);
			flip.addResult(model);

			model.setOutcome(noFlipOutcome);
			noFlip.addResult(model);
		}

		
		// return the Model with better accuracy (more accuracy counts).
		if (flipCount > noFlipCount) {
			return finalizeResults(flip, flipCount, experimentCount, r);
		}

		return finalizeResults(noFlip, noFlipCount, experimentCount, r);
	}

	private ModelResults finalizeResults(ModelResults.Builder builder,
																			 int count,
																			 int experimentCount,
																			 RUtil r) {
		builder.setTotalCorrect(count);
		builder.setTotalExperiments(experimentCount);
		builder.setROutput(r.getCommands());

		return builder.build();
	}
}
