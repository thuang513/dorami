package com.dorami.data;


import com.dorami.util.Hashing;
import com.dorami.data.SNPDataProtos.SNPData.PersonSNP;

public class TwoDimDataPoint extends GenericDataPoint {

	public TwoDimDataPoint(PersonSNP snp) {
		this(snp.getIntensityA(), snp.getIntensityB());
	}

	public TwoDimDataPoint(double x, double y) {
		super(2);
		setDimension(0, x);
		setDimension(1, y);
	}

	public double getX() {
		return getDimension(0);
	}

	public double getY() {
		return getDimension(1);
	}

	public boolean equals(Object other) {
		if (!(other instanceof TwoDimDataPoint)) {
			return false;
		}

		TwoDimDataPoint otherPoint = (TwoDimDataPoint)other;
		return getX() == otherPoint.getX() && getY() == otherPoint.getY();
	}
	
	public int hashCode() {
    return Hashing.multiplyHash(getX()) + Hashing.multiplyHash(getY());
	}

	public String toString() {
		return "" + getX() + ", " + getY();
	}
}
