package com.dorami.vector;


import com.dorami.clustering.GaussianMixtureModel;
import com.dorami.data.TwoDimDataPoint;
import com.dorami.vector.TwoDimMean;

import java.util.List;

public class TwoDimVariance {
  
  private List<TwoDimDataPoint> data; 

  private double varX;
  
  private double varY;

  private double covXY;

  private TwoDimMean mean;

  private GaussianMixtureModel weights;
  
  private int cluster;

  public TwoDimVariance(List<TwoDimDataPoint> data,
                        GaussianMixtureModel weights,
                        int cluster,
                        TwoDimMean mean) {
    // TODO: figure out what to init.
    //       what is the initial variance?
    // Setting the default covariance matrix as
    // var(x) * I (the identity matrix).
    //
    // var(x) = 1.0.
    this(data, 1.0, 1.0, 0.0, weights, cluster, mean);
  }

  public TwoDimVariance(List<TwoDimDataPoint> data, 
                        double initialVarX,
                        double initialVarY,
                        double initialCovXY,
                        GaussianMixtureModel weights,
                        int cluster,
                        TwoDimMean mean) {
    this.data = data;
    this.varX = initialVarX;
    this.varY = initialVarY;
    this.covXY = initialCovXY;
    this.weights = weights;
    this.cluster = cluster;
    this.mean = mean;

    // DEBUG
    calculateInitVarAndCov();
    System.out.println("# Initial VarX = " + varX + ", Initial VarY = " + varY + ", initialCovXY= " + covXY);
  }

  public int getCluster() {
    return cluster;
  }

  public double getStdX() {
    return Math.sqrt(getVarX());
  }

  public double getStdY() {
    return Math.sqrt(getVarY());
  }

  public double getVarX() {
    return varX;
  }

  public double getVarY() {
    return varY;
  }

  public double getCorrelation() {
    return (covXY / (getStdY() * getStdX()));
  }

  public double getCovariance() {
    return covXY;
  }

  private double getWeightForDataPoint(int dataPoint) {
    return weights.getWeight(dataPoint, cluster);
  }

  private double getClusterTotal() {
    return weights.getClusterTotal(cluster);
  }

  private void calculateInitVarAndCov() {
    // Calculate the mean
    double sumX = 0.0;
    double sumY = 0.0;
    for (int i = 0; i < data.size(); ++i) {
      TwoDimDataPoint point = data.get(i);
      sumX += point.getX();
      sumY += point.getY();
    }

    double meanX =  sumX / data.size();
    double meanY =  sumY / data.size();

    // Calculate the variance estimator
    double sumXDiff = 0.0;
    double sumYDiff = 0.0;
    double sumXYDiff = 0.0;
    for (int i = 0; i < data.size(); ++i) {
      TwoDimDataPoint point = data.get(i);
      sumXDiff += Math.pow(point.getX() - meanX, 2);
      sumYDiff += Math.pow(point.getY() - meanY, 2);
      sumXYDiff += (point.getX() - meanX) * (point.getY() - meanY);
    }
    /*
    varX = sumXDiff / data.size();
    varY = sumYDiff /data.size();
    covXY = sumXYDiff / data.size();

    */

    varX = 1.0;
    varY = 1.0;
    covXY = 0.9;
  }

  public void calculateVarAndCov() {
    double meanX = mean.getMeanX();
    double meanY = mean.getMeanY();
    double xSquareSum = 0.0;
    double ySquareSum = 0.0;
    double XYDistSum = 0.0;

    for (int i = 0; i < data.size(); ++i) {
      TwoDimDataPoint point = data.get(i);
      double distFromMeanX = point.getX() - meanX;
      double distFromMeanY = point.getY() - meanY;
      double weight = getWeightForDataPoint(i);
      xSquareSum += weight * distFromMeanX * distFromMeanX;
      ySquareSum += weight * distFromMeanY * distFromMeanY;
      XYDistSum += weight * distFromMeanX * distFromMeanY;
    }
    
    double clusterTotal = getClusterTotal();
    varX = xSquareSum / clusterTotal;
    varY = ySquareSum / clusterTotal;
    covXY = XYDistSum / clusterTotal;

    //    System.out.println("NEW varX = " + varX + " NEW varY= " + varY + " NEW covXY= " +covXY);
    //    System.out.println("xSquareSum= " + xSquareSum + " ySquareSum= " + ySquareSum + " | clusterTotal= " +clusterTotal);
  }
}
