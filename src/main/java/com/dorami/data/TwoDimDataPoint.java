package com.dorami.data;


import com.dorami.util.Hashing;

public class TwoDimDataPoint {
	
	private int x;

	private int y;

	public TwoDimDataPoint(int x, int y) {
		this.x = x;
		this.y = y;
	}

	public int getX() {
		return x;
	}

	public int getY() {
		return y;
	}

	public Double getEuclideanDistance(TwoDimDataPoint other) {
		if (!(other instanceof TwoDimDataPoint)) {
			throw new
				IllegalStateException("Can't compare different types of DataPoints!");
		}

		TwoDimDataPoint otherEuclideanPoint = (TwoDimDataPoint)other;

		double x_dist = Math.pow((double)x - otherEuclideanPoint.getX(), 2.0);
		double y_dist = Math.pow((double)y - otherEuclideanPoint.getY(), 2.0);
		return new Double((x_dist+y_dist)*-1.0);
	}

	public boolean equals(Object other) {
		if (!(other instanceof TwoDimDataPoint)) {
			return false;
		}

		TwoDimDataPoint otherPoint = (TwoDimDataPoint)other;
		return x == otherPoint.getX() && y == otherPoint.getY();
	}
	
	public int hashCode() {
    return Hashing.multiplyHash(x) + Hashing.multiplyHash(y);
	}

	public String toString() {
		return "" + x + ", " + y;
	}
}
