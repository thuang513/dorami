package com.dorami.util;


import com.dorami.vector.TwoDimMean;
import com.dorami.vector.TwoDimVariance;
import java.io.PrintStream;

public class RUtil {
  
  private PrintStream out;

  public RUtil(PrintStream out) {
    this.out = out;
  }

  public static RUtil getSystemOutWriter() {
    return new RUtil(System.out);
  }
  
  public void drawLevelCurve(int cluster,
                             TwoDimMean mean,
                             TwoDimVariance var) {
    double color = cluster + 2;
    drawLevelCurve(cluster, 
                   mean.getMeanX(), 
                   mean.getMeanY(), 
                   var.getVarX(), 
                   var.getVarY(), 
                   var.getCovariance(),
                   color);
  }
  
  public void drawLevelCurve(int cluster, 
                             double meanX,
                             double meanY,
                             double varX,
                             double varY,
                             double covXY,
                             double color) {
    out.println("center" + cluster + " <- " +
                "c(" + meanX + "," + meanY +")");
    out.println("mcorr" + cluster + " <- " +
                "matrix(c(" + varX +"," + covXY + "," + covXY +"," + varY + "), 2,2)");

    out.println("ellipse(center" + cluster + "," + 
                "mcorr" + cluster + ", " +
                "sqrt(qchisq(.5,2)), col = " + color + ")");
  }

  public void printComment(String comment) {
    out.println("# " + comment);
  }
}