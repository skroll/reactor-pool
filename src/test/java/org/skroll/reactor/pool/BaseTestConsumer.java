package org.skroll.reactor.pool;

import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Signal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public abstract class BaseTestConsumer<T, U extends BaseTestConsumer<T, U>> implements Disposable {
  protected final CountDownLatch done;
  protected final List<T> values;
  protected final List<Throwable> errors;
  protected long completions;
  protected Thread lastThread;

  protected boolean checkSubscriptionOnce;

  protected CharSequence tag;

  public BaseTestConsumer() {
    this.values = new ArrayList<>();
    this.errors = new ArrayList<>();
    this.done = new CountDownLatch(1);
  }

  public final Thread lastThread() {
    return lastThread;
  }

  public final List<T> values() {
    return values;
  }

  public final List<Throwable> errors() {
    return errors;
  }

  public final long completions() {
    return completions;
  }

  public final boolean isTerminated() {
    return done.getCount() == 0;
  }

  public final int valueCount() {
    return values.size();
  }

  public final int errorCount() {
    return errors.size();
  }

  protected final AssertionError fail(String message) {
    StringBuilder b = new StringBuilder(64 + message.length());
    b.append(message);

    b.append(" (")
      .append("latch = ").append(done.getCount()).append(", ")
      .append("values = ").append(values.size()).append(", ")
      .append("errors = ").append(errors.size()).append(", ")
      .append("completions = ").append(completions)
    ;

//    if (timeout) {
//      b.append(", timeout!");
//    }

    if (isDisposed()) {
      b.append(", disposed!");
    }

    CharSequence tag = this.tag;
    if (tag != null) {
      b.append(", tag = ")
        .append(tag);
    }

    b
      .append(')')
    ;

    AssertionError ae = new AssertionError(b.toString());
    if (!errors.isEmpty()) {
      if (errors.size() == 1) {
        ae.initCause(errors.get(0));
      } else {
        final RuntimeException ce = Exceptions.multiple(errors);
        ae.initCause(ce);
      }
    }
    return ae;
  }

  @SuppressWarnings("unchecked")
  public final U assertValueCount(int count) {
    int s = values.size();
    if (s != count) {
      throw fail("Value counts differ; Expected: " + count + ", Actual: " + s);
    }
    return (U)this;
  }

  @SuppressWarnings("unchecked")
  public final U assertNotTerminated() {
    if (done.getCount() == 0) {
      throw fail("Subscriber terminated!");
    }
    return (U)this;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public final List<List<Object>> getEvents() {
    List<List<Object>> result = new ArrayList<List<Object>>();

    result.add((List)values());

    result.add((List)errors());

    List<Object> completeList = new ArrayList<Object>();
    for (long i = 0; i < completions; i++) {
      completeList.add(Signal.complete());
    }
    result.add(completeList);

    return result;
  }
}
