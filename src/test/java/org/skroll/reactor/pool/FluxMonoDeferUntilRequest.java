package org.skroll.reactor.pool;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class FluxMonoDeferUntilRequest<T> extends Flux<T> {
  private final Mono<T> mono;

  public FluxMonoDeferUntilRequest(final Mono<T> mono) {
    this.mono = mono;
  }

  @Override
  public void subscribe(final CoreSubscriber<? super T> actual) {
    final MonoSubscription<T> sub = new MonoSubscription<>(actual, mono);
    actual.onSubscribe(sub);
  }

  private static final class MonoSubscription<T> extends Operators.MonoSubscriber<T, T> {
    private final Mono<T> mono;
    private final AtomicReference<Subscription> subscription = new AtomicReference<>();
    private final AtomicBoolean gate = new AtomicBoolean();

    MonoSubscription(final CoreSubscriber<? super T> actual, final Mono<T> mono) {
      super(actual);
      this.mono = mono;
    }

    @Override
    public void request(final long n) {
      super.request(n);
      if (n > 0 && gate.compareAndSet(false, true)) {
        final Subscription d = subscription.get();
        if (d == null) {
          mono.subscribe(this);
        }
      }
    }

    @Override
    public void cancel() {
      if (!subscription.compareAndSet(null, null)) {
        subscription.get().cancel();
        subscription.set(null);
      }
    }

    @Override
    public void onNext(final T t) {
      actual.onNext(t);
    }

    @Override
    public void onSubscribe(final Subscription s) {
      if (!subscription.compareAndSet(null, s)) {
        s.cancel();
        subscription.set(null);
      }
    }
  }
}
