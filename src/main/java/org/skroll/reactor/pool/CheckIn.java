package org.skroll.reactor.pool;

/**
 * Represents an object that can be checked back in.
 */
public interface CheckIn {
  /**
   * Return this object.
   */
  void checkIn();
}
