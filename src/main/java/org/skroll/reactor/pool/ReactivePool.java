package org.skroll.reactor.pool;

import reactor.core.publisher.Mono;

/**
 * A reactive object pool.
 * @param <T> the type of pooled objects
 */
public interface ReactivePool<T> extends AutoCloseable {
  /**
   * A reusable {@link Mono} that takes items from the pool.
   * @return a {@link Mono} for {@link Member} objects
   */
  Mono<Member<T>> member();
}
