package org.skroll.reactor.pool;

/**
 * A wrapped member of a {@link ReactivePool}.
 * @param <T> the wrapped type
 */
public interface Member<T> extends CheckIn {
  /**
   * The wrapped object.
   *
   * @return the wrapped object
   */
  T value();

  /**
   * This method should not throw. Feel free to add logging so that you are aware
   * of a problem with disposal.
   */
  void disposeValue();
}
