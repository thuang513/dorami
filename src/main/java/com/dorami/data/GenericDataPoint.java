package com.dorami.data;


import com.dorami.data.DataPoint;

public class GenericDataPoint implements DataPoint {

	private double[] internals;
	
	private int numDimensions;

	public GenericDataPoint(int numDimensions) {
		internals = new double[numDimensions];
	}

	public int numDimensions() {
		return numDimensions;
	}

	public double getDimension(int d) {
		return internals[d];
	}

	public void setDimension(int dim, double value) {
		internals[dim] = value;
	}
}