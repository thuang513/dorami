package com.dorami.vector;


import com.dorami.clustering.ClusterScores;
import com.dorami.data.TwoDimDataPoint;

public class TwoDimMean {
  
  private List<TwoDimDataPoint> data; 
  
  private double meanX;
  
  private double meanY;

  private ClusterScores weights;
  
  private int cluster;

  public TwoDimMean(List<TwoDimDataPoint> data, 
                    ClusteringScores weights,
                    int cluster) {
    this(data, 0.0, 0.0, weights, cluster);
  }

  public TwoDimMean(List<TwoDimDataPoint> data, 
                    double meanX, 
                    double meanY,
                    ClusteringScores weights,
                    int cluster) {
    this.data = data;
    this.meanX = meanX;
    this.meanY = meanY;
    this.weights = weights;
    this.cluster = cluster;
  }

  private double getWeightForData(int dataPoint) {
    return weights.getWeight(dataPoint, cluster);
  }

  public void calculateMean() {
    double sumOfX = 0.0;
    double sumOfY = 0.0;

    if (weights.length != data.size()) {
      System.err.println("ERROR! Size mismatch in mean!");
    }

    for (int i = 0; i < weights.length; ++i) {
      TwoDimDataPoint d = data.get(point);
      double weight = weights.getWeight(i, cluster);
      sumOfX += weights * d.getX();
      sumOfY += weights * d.getY();
    }

    double clusterTotal = weights.getClusterTotal(cluster);
    meanX = sumOfX/clusterTotal;
    meanY = sumOfY/clusterTotal;
  }
  
  public getMeanX() {
    return meanX;
  }

  public getMeanY() {
    return meanY;
  }
}