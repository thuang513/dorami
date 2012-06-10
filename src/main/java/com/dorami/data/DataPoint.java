package com.dorami.data;


public interface DataPoint {
	int numDimensions();

	double getDimension(int dim);
}