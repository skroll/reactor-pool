package org.skroll.reactor.pool;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

final class DecoratingMember<T> implements Member<T>, Runnable {
  private static final Logger log = Loggers.getLogger(DecoratingMember.class);

  private volatile T value;
  private final MemberMono<T> memberMono;
  private final BiFunction<? super T, ? super CheckIn, ? extends T> checkInDecorator;

  private Disposable scheduled;
  private boolean releasing;
  private boolean checking;
  private long lastCheckTime;

  DecoratingMember(@Nullable final T value,
                   final BiFunction<? super T, ? super CheckIn, ? extends T> checkInDecorator,
                   final MemberMono<T> memberMono) {
    this.checkInDecorator = checkInDecorator;
    this.memberMono = memberMono;
    this.value = value;
  }

  @Override
  public T value() {
    return checkInDecorator.apply(value, this);
  }

  @Override
  public void checkIn() {
    memberMono.pool.checkIn(this);
  }

  public void markAsReleasing() {
    this.releasing = true;
  }

  public boolean isReleasing() {
    return releasing;
  }

  public void markAsChecking() {
    this.checking = true;
  }

  public boolean isChecking() {
    return checking;
  }

  @Override
  public void disposeValue() {
    try {
      if (scheduled != null) {
        scheduled.dispose();
        scheduled = null;
      }
      log.debug("disposing value {}", value);
      memberMono.pool.disposer.accept(value);
    } catch (Throwable e) {
      // make action configurable
      Exceptions.throwIfFatal(e);
    } finally {
      value = null;
      checking = false;
    }
  }

  public void setValueAndClearReleasingFlag(final T value) {
    this.value = value;
    this.releasing = false;
    this.lastCheckTime = now();
  }

  void scheduleRelease() {
    if (scheduled != null) {
      scheduled.dispose();
      log.debug("cancelled scheduled release of {}", this);
    }

    long maxIdleTimeMs = memberMono.pool.maxIdleTimeMs;

    if (maxIdleTimeMs > 0) {
      scheduled = memberMono.pool.scheduler.schedule(
          this, maxIdleTimeMs, TimeUnit.MILLISECONDS);

      log.debug("scheduled release in {}ms of {}", maxIdleTimeMs, this);
    }
  }

  @Override
  public void run() {
    memberMono.addToBeReleased(this);
  }

  public void markAsChecked() {
    checking = false;
    lastCheckTime = now();
  }

  private long now() {
    return memberMono.pool.scheduler.now(TimeUnit.MILLISECONDS);
  }

  public long lastCheckTime() {
    return lastCheckTime;
  }
}
