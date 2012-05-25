package com.dorami.util;


import com.dorami.vector.TwoDimMean;
import com.dorami.vector.TwoDimVariance;

public class RUtil {

	private StringBuilder content;
	
  public RUtil() {
		content = new StringBuilder();
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
    content.append("center")
			.append(cluster)
			.append(" <- ")
			.append("c(")
			.append(meanX)
			.append(",")
			.append(meanY)
			.append(")")
			.append("\n");

    content.append("mcorr")
			.append(cluster)
			.append(" <- " )
			.append("matrix(c(")
			.append(varX)
			.append(",")
			.append(covXY)
			.append(",")
			.append(covXY)
			.append(",")
			.append(varY)
			.append("), 2,2)")
			.append("\n");

    content.append("ellipse(center")
			.append(cluster)
			.append(",")
			.append("mcorr")
			.append(cluster)
			.append(", ")
			.append("sqrt(qchisq(.5,2)), col = ")
			.append(color)
			.append(")")
			.append("\n");
  }

  public void printComment(String comment) {
    content.append("# " + comment).append("\n");
  }

	public String getCommands() {
		return content.toString();
	}
}