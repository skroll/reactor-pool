package org.skroll.reactor.pool;

import reactor.core.publisher.Mono;

public interface ReactivePool<T> extends AutoCloseable {
  Mono<Member<T>> member();
}
