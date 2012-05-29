package com.dorami.clustering;


import com.dorami.data.TwoDimDataPoint;
import com.dorami.data.SNPDataProtos.ModelResults.Outcome;
import com.dorami.data.SNPDataProtos.SNPData;
import com.dorami.data.SNPDataProtos.SNPData.PersonSNP;
import com.dorami.data.SNPDataProtos.Answers;
import com.dorami.data.SNPDataProtos.ModelResults;
import com.dorami.data.SNPDataProtos.ModelResults.GenotypeModel;
import com.dorami.data.SNPDataProtos.SNPData;
import com.dorami.util.Distributions;
import com.dorami.util.RUtil;
import com.dorami.util.SNPAnswerMap;
import com.dorami.vector.TwoDimMean;
import com.dorami.vector.TwoDimVariance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import org.apache.commons.math.stat.StatUtils;

public class GaussianMixtureModel {

  /** 
	 *  Setup the logger 
	 */
	private static final Logger LOGGER = 
		Logger.getLogger(GaussianMixtureModel.class.getName());

  private List<TwoDimDataPoint> data;

  private double[][] weights;

  private double[] clusterTotals;

  private List<TwoDimMean> gaussianMeans;

  private List<TwoDimVariance> gaussianVar;

  private int numClusters;

	private RUtil r;

  /**
   *  numClusters (or number of gaussian clusters).
   */
  public GaussianMixtureModel(List<TwoDimDataPoint> data, 
                              int numClusters, 
                              RUtil r) {
    this.data = Collections.unmodifiableList(data);
    this.numClusters = numClusters;
    this.r = r;
    weights = new double[data.size()][numClusters];
    clusterTotals = new double[numClusters];

    setupInitialMeans();
    setupInitialVariances();

    // Setup the default scores for the cluster totals.
    // Initially, every cluster has an equal probability.
    final double INITIAL_CLUSTER_TOTAL = (1.0/(double)numClusters);
    for (int i = 0; i < numClusters; ++i) {
      clusterTotals[i] = INITIAL_CLUSTER_TOTAL;
    }
  }

  private void setupInitialMeans() {
    List<TwoDimDataPoint> initialMeans = generateInitialMeans();
    gaussianMeans = new ArrayList(numClusters);
    for (int i = 0; i < numClusters; ++i) {
      TwoDimDataPoint mean = initialMeans.get(i);
      TwoDimMean newMean = new TwoDimMean(data,
                                          mean.getX(),
                                          mean.getY(),
                                          this,
                                          i);
      gaussianMeans.add(newMean);
    }
  }
  
  private void setupInitialVariances() {
    gaussianVar = new ArrayList(numClusters);
    for (int i = 0; i < numClusters; ++i) {
      TwoDimMean mean = gaussianMeans.get(i);
      TwoDimVariance var = new TwoDimVariance(data, this, i, mean);
      gaussianVar.add(var);

      // Draw out the curves
      if (r != null) {
        r.drawLevelCurve(i, mean, var);
      }
    }
	}

  private List<TwoDimDataPoint> generateInitialMeans() {
    double[] xValues = new double[data.size()];
    double[] yValues = new double[data.size()];

    for (int i = 0; i < data.size(); ++i) {
      TwoDimDataPoint point = data.get(i);
      xValues[i] = point.getX();
      yValues[i] = point.getY();
    }

    double xMax = StatUtils.max(xValues);
    double xMin = StatUtils.min(xValues);

    double yMax = StatUtils.max(yValues);    
    double yMin = StatUtils.min(yValues);

    final double PERCENT_INCREMENT = 1.0/(numClusters + 1.0);
    double xPercent = 0.0 + PERCENT_INCREMENT;
    double yPercent = 1.0 - PERCENT_INCREMENT;
    double xRange = xMax - xMin;
    double yRange = yMax - yMin;
    List<TwoDimDataPoint> result = new ArrayList<TwoDimDataPoint>(numClusters);
    for (int i = 0; i < numClusters; ++i) {
      double newX = xPercent * xRange + xMin;
      double newY = yPercent * yRange + yMin;
      
      xPercent += PERCENT_INCREMENT;
      yPercent -= PERCENT_INCREMENT;
      result.add(new TwoDimDataPoint(newX, newY));

      if (r != null) {
        r.printComment("# INIT MEAN = " + newX + "," + newY);
      }
    }
    return result;
	}

  public int getNumClusters() {
    return numClusters;
  }

  public void setWeight(int dataPoint, int cluster, double value) {
    weights[dataPoint][cluster] = value;
  }

  public double getWeight(int dataPoint, int cluster) {
    return weights[dataPoint][cluster];
  }

  /**
   *  @returns the total weights for a cluster.
   */
  public double getClusterTotal(int cluster) {
    return clusterTotals[cluster];
  }

  /**
   *  Calculates the cluster total, or the sum of a column.
   */
  private void calculateClusterTotal() {
    for (int cluster = 0; cluster < clusterTotals.length; ++cluster) {
      clusterTotals[cluster] = sumOfClusterProb(cluster);
    }
  }

  public boolean checkForConvergence() {
    // TODO: actually check for convergence.
    return true;
  }

  private double sumOfClusterProb(int cluster) {
    double sum = 0.0;
    for (int i = 0; i < data.size(); ++i) {
      sum += weights[i][cluster];
    }
    return sum;
  }

  public double getProbOfCluster(int cluster) {
    double allClusters = 0.0;
    for (int i = 0; i < clusterTotals.length; ++i) {
      allClusters += clusterTotals[i];
    }
    return (clusterTotals[cluster] / allClusters);
  }

  public double calculateGaussian(int dataPoint,
                                  int cluster) {
    TwoDimDataPoint point = data.get(dataPoint);
    TwoDimMean mean = gaussianMeans.get(cluster);
    TwoDimVariance var = gaussianVar.get(cluster);

    return Distributions.bivariateGaussian(point.getX(),
                                           point.getY(),
                                           mean.getMeanX(),
                                           mean.getMeanY(),
                                           var.getStdX(),
                                           var.getStdY(),
                                           var.getCorrelation());
  }
  
  /**
   *  Expectation step is to figure out the probabilistic weights for
   *  every point being in a certain cluster.
   *
   *  This step figures out what are the chances that a point i is in 
   *  a cluster j.
   */
  public void expectationStep() {
    for (int point = 0; point < data.size(); ++point) {
      double normalizer = 0.0;
      for (int cluster = 0; cluster < clusterTotals.length; ++cluster) {
        double clusterProb = getProbOfCluster(cluster);
        double probInCluster = clusterProb * calculateGaussian(point, cluster);
        normalizer += probInCluster;
      }
      
      for (int cluster = 0; cluster < clusterTotals.length; ++cluster) {
        double clusterProb = getProbOfCluster(cluster);
        double probInCluster = (1/normalizer) * clusterProb * calculateGaussian(point, cluster);
        setWeight(point, cluster, probInCluster);
      }
    }

    calculateClusterTotal();
  }

  /**
   *  In the maximization step, we recalculate the mean, variance, and
   *  cluster probabilities, given our estimates in the expectation step.
   *
   */
  public void maximizationStep() {
		for (TwoDimMean mean : gaussianMeans) {
      mean.calculateMean();
    }

    for (TwoDimVariance var : gaussianVar) {
      var.calculateVarAndCov();
    }

    // Print out the level curves
    for (int i = 0; i < numClusters; ++i) {
      TwoDimMean mean = gaussianMeans.get(i);
      TwoDimVariance var = gaussianVar.get(i);
      r.drawLevelCurve(i, mean, var);
    }
  }

	public void clusterData() {
		// TODO: Figure out the right convergence method.
		//       Currently does this by running some iteration.
		for (int i = 0; i < 50; ++i) {
      expectationStep();
      maximizationStep();
    }
	}
}




