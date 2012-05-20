package com.dorami.util;


public class Hashing {

  /**
   *  Can't instantiate me! Nah nah nah...
   */
  private Hashing() {
  }
  
  private static final double FACTOR = (Math.sqrt(5) - 1.0)/2.0;

  private static final int MULTIPLY_FACTOR = (int)Math.pow(2, 3);

  /**
   *  Multiply hashing function is based on the method described by Cormen,
   *  Leiserson, Rivest, and Stein in Introductions to Algorithms (2008).
   */
  public static int multiplyHash(int k) {
    return ((int)Math.floor(MULTIPLY_FACTOR * (k * FACTOR % 1)));
  }

  /**
   *  Multiply hashing function is based on the method described by Cormen,
   *  Leiserson, Rivest, and Stein in Introductions to Algorithms (2008).
   */
  public static int multiplyHash(double k) {
    return ((int)Math.floor(MULTIPLY_FACTOR * (k * FACTOR % 1)));
  }
}