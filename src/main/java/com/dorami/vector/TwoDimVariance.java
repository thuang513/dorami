package com.dorami.vector;


import com.dorami.clustering.ClusterScores;
import com.dorami.data.TwoDimDataPoint;

public class TwoDimVariance {
  
  private List<TwoDimDataPoint> data; 

  private double varX;
  
  private double varY;

  private ClusterScores weights;
  
  private int cluster;

  public TwoDimVariance(List<TwoDimDataPoint> data, 
                        double varX, 
                        double varY,
                        ClusterScores weights,
                        int cluster) {
    this.data = data;
    this.varX = varX;
    this.varY = varY;
    this.weights = weights;
    this.cluster = cluster;
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

  public void calculateCovariance() {
    return covXY;
  }

  private double getWeightForDataPoint(int dataPoint) {
    return weights.getWeight(dataPoint, cluster);
  }

  private double getClusterTotal() {
    return weights.getClusterTotal(cluster);
  }

  public void calculateVarAndCov() {
    double meanX = ;
    double meanY = ;
    
    double xSquareSum = 0.0;
    double ySquareSum = 0.0;
    double XYDistSum = 0.0;
    for (int i = 0; i < data.size(); ++i) {
      TwoDimDataPoint point = data.get(i);
      double distFromMeanX = point.getX() - meanX;
      double distFromMeanY = point.getY() - meanY;
      double weight = getWeightForDataPoint(i);
      xSquareSum += weight * Math.pow(distFromMeanX, 2);
      ySquareSum += weight * Math.pow(distFromMeanY, 2);
      XYDistSum += weight * distFromMeanX * distFromMeanY;
    }

    varX = xSquareSum / getClusterTotal();
    varY = ySquareSum / getClusterTotal();
    covXY = XYDistSum / getClusterTotal();
  }
}
