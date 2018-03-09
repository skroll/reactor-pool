package org.skroll.reactor.pool;

import org.junit.Test;
import org.reactivestreams.Publisher;
import org.skroll.reactor.test.TestSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class NonBlockingPoolTest {
  private static final Logger log = Loggers.getLogger(NonBlockingPoolTest.class);

  @Test
  public void testCloseCallsOnClose() throws Exception {
    VirtualTimeScheduler s = VirtualTimeScheduler.create();
    AtomicInteger count = new AtomicInteger();
    AtomicBoolean closed = new AtomicBoolean();

    final ReactivePool<Integer> pool = NonBlockingPool
      .factory(count::incrementAndGet)
      .scheduler(s)
      .onClose(() -> { closed.set(true); return null; })
      .build();

    pool.member().subscribe();

    pool.close();
    assertTrue(closed.get());
  }

  @Test
  public void testMaxIdleTime() {
    VirtualTimeScheduler s = VirtualTimeScheduler.create();
    AtomicInteger count = new AtomicInteger();
    AtomicInteger disposed = new AtomicInteger();

    final ReactivePool<Integer> pool = NonBlockingPool
      .factory(count::incrementAndGet)
      .healthCheck(n -> true)
      .maxSize(3)
      .maxIdleTime(1, TimeUnit.MINUTES)
      .disposer(n -> disposed.incrementAndGet())
      .scheduler(s)
      .build();

    TestSubscriber<Member<Integer>> ts = makeTestSubscriber(1,
      deferred(pool.member())
        .doOnNext(Member::checkIn)
        .doOnNext(n -> log.debug(n.toString()))
        .doOnRequest(t -> log.debug("test request=" + t)));

    s.advanceTime();
    ts.assertValueCount(1);
    assertEquals(0, disposed.get());
    s.advanceTimeBy(Duration.ofMinutes(1));
    assertEquals(1, disposed.get());
  }

  @Test
  public void testReleasedMemberIsRecreated() throws Exception {
    VirtualTimeScheduler s = VirtualTimeScheduler.create();
    AtomicInteger count = new AtomicInteger();
    AtomicInteger disposed = new AtomicInteger();

    ReactivePool<Integer> pool = NonBlockingPool
      .factory(count::incrementAndGet)
      .healthCheck(n -> true)
      .maxSize(1)
      .maxIdleTime(1, TimeUnit.MINUTES)
      .disposer(n -> disposed.incrementAndGet())
      .scheduler(s)
      .build();

    {
      TestSubscriber<Member<Integer>> ts = makeTestSubscriber(1,
        deferred(pool.member())
          .doOnNext(Member::checkIn)
          .doOnNext(n -> log.debug(n.toString()))
          .doOnRequest(t -> log.debug("test request=" + t))
      );

      s.advanceTime();
      ts.assertValueCount(1);
      assertEquals(0, disposed.get());
      s.advanceTimeBy(Duration.ofMinutes(1));
      assertEquals(1, disposed.get());
      ts.cancel();
      assertEquals(1, disposed.get());
    }

    {
      TestSubscriber<Member<Integer>> ts = makeTestSubscriber(1,
        deferred(pool.member())
          .repeat()
          .doOnNext(Member::checkIn)
          .doOnNext(n -> log.debug(n.toString()))
          .doOnRequest(t -> log.debug("test request=" + t))
      );

      s.advanceTime();
      ts.assertValueCount(1);
      assertEquals(1, disposed.get());
      s.advanceTimeBy(Duration.ofMinutes(1));
      assertEquals(2, disposed.get());
    }

    {
      TestSubscriber<Member<Integer>> ts = makeTestSubscriber(1,
        deferred(pool.member())
          .repeat()
          .doOnNext(Member::checkIn)
          .doOnNext(n -> log.debug(n.toString()))
          .doOnRequest(t -> log.debug("test request=" + t))
      );

      s.advanceTime();
      ts.assertValueCount(1);
      assertEquals(2, disposed.get());
    }

    pool.close();
    assertEquals(3, disposed.get());
  }

  @Test
  public void testConnectionPoolRecyclesAlternating() {
    VirtualTimeScheduler s = VirtualTimeScheduler.create();
    AtomicInteger count = new AtomicInteger();
    ReactivePool<Integer> pool = NonBlockingPool
      .factory(count::incrementAndGet)
      .healthCheck(n -> true)
      .maxSize(2)
      .maxIdleTime(1, TimeUnit.MINUTES)
      .scheduler(s)
      .build();

    final TestSubscriber<Integer> ts = makeTestSubscriber(4,
      deferred(pool.member())
        .repeat()
        .doOnNext(Member::checkIn)
        .map(Member::value)
    );

    s.advanceTime();
    ts.assertValueCount(4)
      .assertNotTerminated();

    final List<Object> list = ts.getEvents().get(0);

    assertTrue(list.get(0) == list.get(1));
    assertTrue(list.get(1) == list.get(2));
    assertTrue(list.get(2) == list.get(3));
  }

  @Test
  public void testConnectionPoolRecyclesMany() throws Exception {
    VirtualTimeScheduler s = VirtualTimeScheduler.create();
    AtomicInteger count = new AtomicInteger();
    ReactivePool<Integer> pool = NonBlockingPool
      .factory(count::incrementAndGet)
      .healthCheck(n -> true)
      .maxSize(2)
      .maxIdleTime(1, TimeUnit.MINUTES)
      .scheduler(s)
      .build();

    TestSubscriber<Member<Integer>> ts = makeTestSubscriber(4,
      deferred(pool.member())
          .repeat()
      );
    s.advanceTime();
    ts.assertNoErrors()
      .assertValueCount(2)
      .assertNotTerminated();
    List<Member<Integer>> list = new ArrayList<>(ts.values());
    list.get(1).checkIn(); // should release a connection
    s.advanceTime();
    {
      List<Object> values = ts.assertValueCount(3)
        .assertNotTerminated()
        .getEvents().get(0);
      assertEquals(list.get(0).hashCode(), values.get(0).hashCode());
      assertEquals(list.get(1).hashCode(), values.get(1).hashCode());
      assertEquals(list.get(1).hashCode(), values.get(2).hashCode());
    }
    list.get(0).checkIn();
    s.advanceTime();

    {
      List<Object> values = ts.assertValueCount(4)
        .assertNotTerminated()
        .getEvents().get(0);
      assertEquals(list.get(0), values.get(0));
      assertEquals(list.get(1), values.get(1));
      assertEquals(list.get(1), values.get(2));
      assertEquals(list.get(0), values.get(3));
    }
  }

  @Test
  public void testHealthCheckWhenFails() throws Exception {
    VirtualTimeScheduler s = VirtualTimeScheduler.create();
    AtomicInteger count = new AtomicInteger();
    AtomicInteger disposed = new AtomicInteger();
    AtomicInteger healthChecks = new AtomicInteger();
    final ReactivePool<Integer> pool = NonBlockingPool
      .factory(count::incrementAndGet)
      .healthCheck(n -> {
        healthChecks.incrementAndGet();
        return false;
      })
      .createRetryInterval(10, TimeUnit.MINUTES)
      .idleTimeBeforeHealthCheck(1, TimeUnit.MILLISECONDS)
      .maxSize(1)
      .maxIdleTime(1, TimeUnit.HOURS)
      .disposer(n -> disposed.incrementAndGet())
      .scheduler(s)
      .build();

    {
      final TestSubscriber<Member<Integer>> ts = makeTestSubscriber(1,
        deferred(pool.member())
          .repeat()
          .doOnNext(Member::checkIn)
      );

      s.advanceTime();
      ts.assertValueCount(1);
      assertEquals(0, disposed.get());
      assertEquals(0, healthChecks.get());

      ts.request(1);
      s.advanceTime();
      ts.assertValueCount(2);
      assertEquals(0, disposed.get());
      assertEquals(0, healthChecks.get());

      s.advanceTimeBy(Duration.ofMillis(1));
      ts.request(1);
      s.advanceTime();
      ts.assertValueCount(2);
      assertEquals(1, disposed.get());
      assertEquals(1, healthChecks.get());

      s.advanceTimeBy(Duration.ofMinutes(10));
      ts.assertValueCount(3);

      ts.cancel();
      assertEquals(1, disposed.get());
    }
  }

  @Test
  public void testMemberAvailableAfterCreationScheduledIsUsedImmediately() throws Exception {
    VirtualTimeScheduler ts = VirtualTimeScheduler.create();
    Scheduler s = createSchedulerToDelayCreation(ts);
    AtomicInteger count = new AtomicInteger();
    ReactivePool<Integer> pool = NonBlockingPool
      .factory(count::incrementAndGet)
      .createRetryInterval(10, TimeUnit.MINUTES)
      .maxSize(2)
      .maxIdleTime(1, TimeUnit.HOURS)
      .scheduler(s)
      .build();
    List<Member<Integer>> list = new ArrayList<>();
    pool.member().doOnSuccess(list::add).subscribe();
    assertEquals(0, list.size());
    ts.advanceTimeBy(Duration.ofMinutes(1));
    assertEquals(1, list.size());
    pool.member().doOnSuccess(list::add).subscribe();
    list.get(0).checkIn();
    ts.advanceTime();
    assertEquals(2, list.size());
  }

  @Test
  public void testClosedPoolSubscriptionThrowsException() throws Exception {
    VirtualTimeScheduler s = VirtualTimeScheduler.create();
    TestSubscriber<Member<Integer>> ts = TestSubscriber.create(0);
    AtomicInteger count = new AtomicInteger();

    NonBlockingPool<Integer> pool = NonBlockingPool
      .factory(count::incrementAndGet)
      .createRetryInterval(10, TimeUnit.MINUTES)
      .maxSize(2)
      .maxIdleTime(1, TimeUnit.HOURS)
      .scheduler(s)
      .build();

    assertFalse(pool.isClosed());
    pool.close();
    assertTrue(pool.isClosed());
    pool.member().subscribe(ts);
    ts.assertError(PoolClosedException.class);
  }

  private static <T> Flux<T> deferred(final Mono<T> mono) {
    return new FluxMonoDeferUntilRequest<>(mono);
  }

  private static <T> TestSubscriber<T> makeTestSubscriber(final long initialRequest, final Publisher<T> publisher) {
    final TestSubscriber<T> ts = TestSubscriber.create(initialRequest);
    publisher.subscribe(ts);
    return ts;
  }

  private static Scheduler createSchedulerToDelayCreation(final VirtualTimeScheduler ts) {
    return new Scheduler() {
      @Override
      public Disposable schedule(final Runnable task) {
        return schedule(task, 0, TimeUnit.NANOSECONDS);
      }

      @Override
      public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
        final Worker w = createWorker();
        return w.schedule(task, delay, unit);
      }

      @Override
      public Worker createWorker() {
        final Worker w = ts.createWorker();
        return new Worker() {
          @Override
          public Disposable schedule(final Runnable task) {
            return schedule(task, 0, TimeUnit.NANOSECONDS);
          }

          @Override
          public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
            if (task instanceof MemberMono.Initializer && delay == 0) {
              return w.schedule(task, 1, TimeUnit.MINUTES);
            } else {
              return w.schedule(task, delay, unit);
            }
          }

          @Override
          public void dispose() {
            w.dispose();
          }

          @Override
          public boolean isDisposed() {
            return w.isDisposed();
          }
        };
      }
    };
  }
}
