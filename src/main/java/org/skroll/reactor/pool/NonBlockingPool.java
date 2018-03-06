package org.skroll.reactor.pool;
;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class NonBlockingPool<T> implements ReactivePool<T> {
  final Callable<? extends T> factory;
  final Predicate<? super T> healthCheck;
  final long idleTimeBeforeHealthCheckMs;
  final Consumer<? super T> disposer;
  final int maxSize;
  final long maxIdleTimeMs;
  final long createRetryIntervalMs;
  final BiFunction<? super T, ? super CheckIn, ? extends T> checkInDecorator;
  final Scheduler scheduler;
  final Callable<Void> closeAction;

  private final AtomicReference<MemberMono<T>> member = new AtomicReference<>();
  private volatile boolean closed;

  /* package */NonBlockingPool(
      final Callable<? extends T> factory, final Predicate<? super T> healthCheck, final Consumer<? super T> disposer,
      final int maxSize, final long idleTimeBeforeHealthCheckMs, final long maxIdleTimeMs, final long createRetryIntervalMs,
      final BiFunction<? super T, ? super CheckIn, ? extends T> checkInDecorator, final Scheduler scheduler, final Callable<Void> closeAction
  ) {
    this.factory = Objects.requireNonNull(factory);
    this.healthCheck = Objects.requireNonNull(healthCheck);
    this.disposer = Objects.requireNonNull(disposer);
    if (maxSize <= 0) {
      throw new IllegalArgumentException("maxSize must be at least 1");
    }
    this.maxSize = maxSize;
    this.idleTimeBeforeHealthCheckMs = idleTimeBeforeHealthCheckMs;
    this.checkInDecorator = Objects.requireNonNull(checkInDecorator);
    this.scheduler = Objects.requireNonNull(scheduler);
    if (createRetryIntervalMs < 0) {
      throw new IllegalArgumentException("createRetryIntervalMs must be >= 0");
    }
    this.createRetryIntervalMs = createRetryIntervalMs;
    this.closeAction = Objects.requireNonNull(closeAction);
    if (maxIdleTimeMs < 0) {
      throw new IllegalArgumentException("maxIdleTimeMs must be >= 0");
    }
    this.maxIdleTimeMs = maxIdleTimeMs;
  }

  private MemberMono<T> createMember() {
    return new MemberMono<>(this);
  }

  @Override
  public Mono<Member<T>> member() {
    while (true) {
      MemberMono<T> m = member.get();
      if (m != null) {
        return m;
      } else {
        m = createMember();
        if (member.compareAndSet(null, m)) {
          return m;
        }
      }
    }
  }

  public void checkIn(final Member<T> m) {
    final MemberMono<T> mem = member.get();
    if (mem != null) {
      mem.checkIn(m);
    }
  }

  @Override
  public void close() {
    closed = true;
    while (true) {
      MemberMono<T> m = member.get();
      if (m == null) {
        return;
      } else if (member.compareAndSet(m, null)) {
        m.close();
        break;
      }
    }
    try {
      closeAction.call();
    } catch (Exception e) {
      Exceptions.throwIfFatal(e);
    }
  }

  boolean isClosed() {
    return closed;
  }

  public static <T> Builder<T> factory(Callable<T> factory) {
    return new Builder<T>().factory(factory);
  }

  public static class Builder<T> {
    private static final Predicate<Object> ALWAYS_TRUE = o -> true;
    private static final BiFunction<Object, CheckIn, Object> DEFAULT_CHECKIN_DECORATOR = (x, y) -> x;

    private Callable<? extends T> factory;
    private Predicate<? super T> healthCheck = ALWAYS_TRUE;
    private long idleTimeBeforeHealthCheckMs = 1000;
    private Consumer<? super T> disposer = ignored -> {};
    private int maxSize = 10;
    private long createRetryIntervalMs = 30000;
    private Scheduler scheduler = Schedulers.elastic(); // TODO: Change?
    private long maxIdleTimeMs;
    @SuppressWarnings("unchecked")
    private BiFunction<? super T, ? super CheckIn, ? extends T> checkInDecorator = (BiFunction<T, CheckIn, T>) DEFAULT_CHECKIN_DECORATOR;
    private Callable<Void> closeAction = () -> null;

    private Builder() {}

    public Builder<T> factory(final Callable<? extends T> factory) {
      this.factory = Objects.requireNonNull(factory);
      return this;
    }

    public Builder<T> healthCheck(final Predicate<? super T> healthCheck) {
      this.healthCheck = Objects.requireNonNull(healthCheck);
      return this;
    }

    public Builder<T> idleTimeBeforeHealthCheck(final long duration, final TimeUnit unit) {
      if (duration < 0) {
        throw new IllegalArgumentException("duration must be >= 0");
      }
      this.idleTimeBeforeHealthCheckMs = unit.toMillis(duration);
      return this;
    }

    public Builder<T> maxIdleTime(final long value, final TimeUnit unit) {
      this.maxIdleTimeMs = unit.toMillis(value);
      return this;
    }

    public Builder<T> createRetryInterval(final long duration, final TimeUnit unit) {
      this.createRetryIntervalMs = unit.toMillis(duration);
      return this;
    }

    public Builder<T> disposer(final Consumer<? super T> disposer) {
      this.disposer = Objects.requireNonNull(disposer);
      return this;
    }

    public Builder<T> maxSize(final int maxSize) {
      if (maxSize <= 0) {
        throw new IllegalArgumentException("maxSize must be > 0");
      }
      this.maxSize = maxSize;
      return this;
    }

    public Builder<T> scheduler(final Scheduler scheduler) {
      this.scheduler = Objects.requireNonNull(scheduler);
      return this;
    }

    public Builder<T> checkinDecorator(final BiFunction<? super T, ? super CheckIn, ? extends T> f) {
      this.checkInDecorator = f;
      return this;
    }

    public Builder<T> onClose(final Callable<Void> closeAction) {
      this.closeAction = closeAction;
      return this;
    }

    public NonBlockingPool<T> build() {
      return new NonBlockingPool<>(factory, healthCheck, disposer, maxSize, idleTimeBeforeHealthCheckMs,
          maxIdleTimeMs, createRetryIntervalMs, checkInDecorator, scheduler, closeAction);
    }
  }
}
