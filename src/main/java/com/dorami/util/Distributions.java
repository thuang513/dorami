package com.dorami.util;

public class Distributions {
  private Distributions() {

  }

  /**
   *  Calculates the probability of (x,y) from a bivariate normal
   *  distribution.
   */
  public static double bivariateGaussian(double x,
                                         double y,
                                         double meanX,
                                         double meanY,
                                         double stdX,
                                         double stdY,
                                         double rho) {

    /*
    double two_pi = 2*Math.PI;
    double rhoSquare = rho*rho;
    double correlationScore = 1.0 - rhoSquare;
    double normalizer =
      1/(two_pi * stdX * stdY * Math.sqrt(correlationScore));

    double exponentCoeff = -1.0/(2.0*correlationScore);
    double xMeanDist = x - meanX;
    double yMeanDist = y - meanY;
    double xDistSquare = Math.pow(xMeanDist/stdX, 2);
    double yDistSquare = Math.pow(yMeanDist/stdY, 2);

    double xyDist = 
      (2 * rho * xMeanDist * yMeanDist) / (stdX * stdY);
    
    double expoTerm = exponentCoeff * (xDistSquare - xyDist + yDistSquare);
    double result = normalizer * Math.exp(expoTerm);
    return result;
    */

    double rhoSquare = rho*rho;
    double normalizer = 1/(2*Math.PI * stdX * stdY * Math.sqrt(1.0 - rhoSquare));
    double xDistSquare = Math.pow( ((x - meanX)/stdX), 2);
    double yDistSquare = Math.pow( ((y - meanY)/stdY), 2);
    double xyDist = 2 * rho * ((x - meanX)/stdX) * ((y - meanY)/stdY);

    double expoCoeff = -1.0/(2* (1-rhoSquare));
    double expoTerm = expoCoeff * (xDistSquare + yDistSquare - xyDist);
    double result = normalizer * Math.exp(expoTerm);
    return result;
  }
}