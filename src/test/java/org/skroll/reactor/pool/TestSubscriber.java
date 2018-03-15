package org.skroll.reactor.pool;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.Operators;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class TestSubscriber<T>
    extends BaseTestConsumer<T, TestSubscriber<T>>
    implements Subscriber<T>, Subscription, Disposable {
  private final Subscriber<? super T> actual;

  private volatile boolean cancelled;

  private final AtomicReference<Subscription> subscription;

  private final AtomicLong missedRequested;

  public TestSubscriber() {
    this(Operators.emptySubscriber(), Long.MAX_VALUE);
  }

  public TestSubscriber(final long initialRequest) {
    this(Operators.emptySubscriber(), initialRequest);
  }

  public TestSubscriber(final Subscriber<? super T> actual) {
    this(actual, Long.MAX_VALUE);
  }

  public TestSubscriber(final Subscriber<? super T> actual, final long initialRequest) {
    if (initialRequest < 0) {
      throw new IllegalArgumentException("Negative initial request not allowed");
    }
    this.actual = actual;
    this.subscription = new AtomicReference<>();
    this.missedRequested = new AtomicLong(initialRequest);
  }

  @Override
  public void onSubscribe(final Subscription s) {
    lastThread = Thread.currentThread();

    if (s == null) {
      errors.add(new NullPointerException("onSubscribe received a null Subscription"));
      return;
    }
    if (!subscription.compareAndSet(null, s)) {
      s.cancel();
      if (subscription.get() != Operators.cancelledSubscription()) {
        errors.add(new IllegalStateException("onSubscribe received multiple subscriptions: " + s));
      }
      return;
    }

    if (actual != Operators.emptySubscriber()) {
      actual.onSubscribe(s);
    }

    long mr = missedRequested.getAndSet(0L);
    if (mr != 0L) {
      s.request(mr);
    }

    onStart();
  }

  /**
   * Called after the onSubscribe is called and handled.
   */
  protected void onStart() {

  }

  @Override
  public void onNext(final T t) {
    if (!checkSubscriptionOnce) {
      checkSubscriptionOnce = true;
      if (subscription.get() == null) {
        errors.add(new IllegalStateException("onSubscribe not called in proper order"));
      }
    }

    lastThread = Thread.currentThread();
    values.add(t);

    if (t == null) {
      errors.add(new NullPointerException("onNext received a null value"));
    }

    if (actual != Operators.emptySubscriber()) {
      actual.onNext(t);
    }
  }

  @Override
  public void onError(final Throwable t) {
    if (!checkSubscriptionOnce) {
      checkSubscriptionOnce = true;
      if (subscription.get() == null) {
        errors.add(new NullPointerException("onSubscribe not called in proper order"));
      }
    }
    try {
      lastThread = Thread.currentThread();
      errors.add(t);

      if (t == null) {
        errors.add(new IllegalStateException("onError received a null Throwable"));
      }

      if (actual != Operators.emptySubscriber()) {
        actual.onError(t);
      }
    } finally {
      done.countDown();
    }
  }

  @Override
  public void onComplete() {
    if (!checkSubscriptionOnce) {
      checkSubscriptionOnce = true;
      if (subscription.get() == null) {
        errors.add(new IllegalStateException("onSubscribe not called in proper order"));
      }
    }
    try {
      lastThread = Thread.currentThread();
      completions++;

      if (actual != Operators.emptySubscriber()) {
        actual.onComplete();
      }
    } finally {
      done.countDown();
    }
  }

  @Override
  public void request(final long n) {
    deferredRequest(subscription, missedRequested, n);
  }

  @Override
  public void cancel() {
    if (!cancelled) {
      cancelled = true;
      cancel(subscription);
    }
  }

  public final boolean isCancelled() {
    return cancelled;
  }

  @Override
  public void dispose() {
    cancel();
  }

  @Override
  public boolean isDisposed() {
    return cancelled;
  }

  public final boolean hasSubscription() {
    return subscription.get() != null;
  }

  public final TestSubscriber<T> requestMore(long n) {
    request(n);
    return this;
  }

  private static void deferredRequest(AtomicReference<Subscription> field, AtomicLong requested, long n) {
    Subscription s = field.get();
    if (s != null) {
      s.request(n);
    } else {
      if (Operators.validate(n)) {
        addCap(requested, n);

        s = field.get();
        if (s != null) {
          long r = requested.getAndSet(0L);
          if (r != 0L) {
            s.request(r);
          }
        }
      }
    }
  }

  private static long addCap(AtomicLong requested, long n) {
    for (;;) {
      long r = requested.get();
      if (r == Long.MAX_VALUE) {
        return Long.MAX_VALUE;
      }
      long u = Operators.addCap(r, n);
      if (requested.compareAndSet(r, u)) {
        return r;
      }
    }
  }

  private static boolean cancel(AtomicReference<Subscription> field) {
    Subscription current = field.get();
    if (current != Operators.cancelledSubscription()) {
      current = field.getAndSet(Operators.cancelledSubscription());
      if (current != Operators.cancelledSubscription()) {
        if (current != null) {
          current.cancel();
        }
        return true;
      }
    }
    return false;
  }

  public static <T> TestSubscriber<T> from(Publisher<? extends T> publisher) {
    return from(0, publisher);
  }

  public static <T> TestSubscriber<T> from(final long initialRequest, Publisher<? extends T> publisher) {
    final TestSubscriber<T> ts = new TestSubscriber<>(initialRequest);
    publisher.subscribe(ts);
    return ts;
  }
}
