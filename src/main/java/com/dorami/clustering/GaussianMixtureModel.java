package com.dorami.clustering;


import java.util.Collections;

public class GaussianMixtureModel {

  private List<TwoDimDataPoint> data;

  private double[][] weights;

  private double[] clusterTotals;

  private List<TwoDimMean> gaussianMeans;

  private List<TwoDimVariance> gaussianVar;

  private double lastConvergenceCount = 0.0;

  private static final double CONVERGENCE_MARGIN = 0.001;
  /**
   *  numGaussians (or number of gaussian clusters).
   */
  public GaussianMixture(List<TwoDimDataPoint> data, int numGaussians) {
    this.data = Collections.unmodifiableList(data);
    this.numClusters = numClusters;
    weights = new double[data.size()][numGaussians];

    gaussianMeans = new ArrayList(numGaussians);
    for (int i = 0; i < numGaussians; ++i) {
      gaussianMeans.add(new TwoDimMean(data, this, i));
    }

    gaussianVar = new ArrayList(numGaussians);
    for (int i = 0; i < numGaussians; ++i) {
      // TODO: figure this out
      gaussianVar.add(new TwoDimVariance());
    }

    clusterTotals = new double[numGaussians];
    final double INITIAL_CLUSTER_TOTAL = (1.0/(double)numGaussians);
    for (int i = 0; i < numGaussians; ++i) {
      clusterTotals[i] = INITIAL_CLUSTER_TOTAL;
    }
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
    return clusterTotal[cluster];
  }

  /**
   *  Calculates the cluster total, or the sum of a column.
   */
  private void calculateClusterTotal() {
    for (int cluster = 0; cluster < clusterTotals.length; ++cluster) {
      clusterTotals[cluster] = sumOfClusterProb(cluster);
    }
  }

  private void checkForConvergence() {
    // Check for convergence
    double convergenceCount = 0.0;
    for (int i = 0; i < clusterTotals.length; ++i) {
      convergenceCount += clusterTotals[i];
    }

    double diff = lastConvergenceCount - convergenceCount;
    System.out.println("Convergence difference = " + diff);
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

  public void calculateGaussian(int dataPoint,
                                int cluster) {
    TwoDimDataPoint point = data.get(dataPoint);
    calculateGaussian(point.getX(),
                      point.getY(), 
                      gaussianMeans.get(cluster),
                      gaussianVar.get(cluster));
  }

  private double square(double x) {
    return Math.pow(x, 2);
  }

  /**
   *  Calculates the probability of (x,y) from a bivariate normal
   *  distribution.
   */
  private double calculateGaussian(double x,
                                   double y,
                                   TwoDimMean mean,
                                   TwoDimVariance var) {
    double two_pi = (2*Math.PI);
    double rho = var.getCorrelation();
    double rhoSquare = square(rho);
    double correlationScore = Math.sqrt(1.0 - rhoSquare);
    double normalizer =
      1/(two_pi * var.getStdX() * var.getStdY() * correlationScore);

    double exponentCoeff = -1.0/(2.0*correlationScore);
    double xMeanDist = x - mean.getMeanX();
    double yMeanDist = y - mean.getMeanY();

    double xDist = square(xMeanDist/var.getStdX());
    double yDist = square(yMeanDist/var.getStdY());

    double xyDist = 
      (2 * rho * xMeanDist * yMeanDist) / (var.getStdX() * var.getStdY());
    
    double expoTerm = exponentCoeff * (xDist - xyDist + yDist);
    double result = normalizer * Math.exp(expoTerm);
    return result;
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
      for (int cluster = 0; cluster < NUM_CLUSTERS; ++cluster) {
        double probInCluster = calculateGaussian(point, cluster);
        setWeight(point, cluster, probInCluster);
      }
    }
  }

  /**
   *  In the maximization step, we recalculate the mean, variance, and
   *  cluster probabilities, given our estimates in the expectation step.
   *
   */
  public void maximizationStep() {
    // Calculate the mean, variance and P(i = cluster).
    for (TwoDimMean mean : gaussianMeans) {
      mean.calculateMean();
    }

    for (TwoDimVariance var : gaussianVar) {
      var.calculateVarAndCov();
    }
    calculateClusterTotal();
    checkForConvergence();
  }
}