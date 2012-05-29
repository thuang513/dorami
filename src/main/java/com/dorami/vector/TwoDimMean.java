package com.dorami.vector;


import com.dorami.clustering.GaussianMixtureModel;
import com.dorami.data.TwoDimDataPoint;

import java.util.List;
import java.util.logging.Logger;
import java.util.Random;

public class TwoDimMean {
  
  /** 
	 *  Setup the logger 
	 */
	private static final Logger LOGGER = 
		Logger.getLogger(TwoDimMean.class.getName());

  private List<TwoDimDataPoint> data; 
  
  private double meanX;
  
  private double meanY;

  private GaussianMixtureModel weights;
  
  private int cluster;

  public TwoDimMean(List<TwoDimDataPoint> data, 
                    GaussianMixtureModel weights,
                    int cluster) {
    this(data, 0.0, 0.0, weights, cluster);
    // Randomly grab initial meanX and meanY from some data point.
    Random r = new Random();
    int randomPoint = r.nextInt(data.size());
    TwoDimDataPoint random = data.get(randomPoint);
    meanX = random.getX();
    meanY = random.getY();

    // DEBUG
    System.out.println("# Initial mean = " + meanX + "," + meanY);
  }

  public TwoDimMean(List<TwoDimDataPoint> data, 
                    double meanX, 
                    double meanY,
                    GaussianMixtureModel weights,
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

    for (int i = 0; i < data.size(); ++i) {
      TwoDimDataPoint d = data.get(i);
      double dataWeight = weights.getWeight(i, cluster);
      sumOfX += dataWeight * d.getX();
      sumOfY += dataWeight * d.getY();
    }

    double clusterTotal = weights.getClusterTotal(cluster);

    // DEBUG
    //    LOGGER.info("cluster= " + cluster + " clusterTotal= " + clusterTotal);
    //    System.out.println("sumOfX = " + sumOfX + " sumOfY = " + sumOfY + " | Cluster total= " +clusterTotal);
    meanX = sumOfX/clusterTotal;
    meanY = sumOfY/clusterTotal;

    //    System.out.println("" + meanX + "," + meanY);

  }
  public double getMeanX() {
    return meanX;
  }

  public double getMeanY() {
    return meanY;
  }
}