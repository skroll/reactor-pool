package org.skroll.reactor.pool;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class NonBlockingPoolTest {
  private static final Logger log = Loggers.getLogger(NonBlockingPoolTest.class);

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

    TestSubscriber<Member<Integer>> ts = TestSubscriber.from(1,
      deferred(pool.member())
        .doOnNext(Member::checkIn)
        .doOnNext(n -> log.debug(n.toString()))
        .doOnRequest(t -> log.debug("test request=" + t)));

    s.advanceTime();
    ts.assertValueCount(1);
    Assert.assertEquals(0, disposed.get());
    s.advanceTimeBy(Duration.ofMinutes(1));
    Assert.assertEquals(1, disposed.get());
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
      TestSubscriber<Member<Integer>> ts = TestSubscriber.from(1,
        deferred(pool.member())
          .doOnNext(Member::checkIn)
          .doOnNext(n -> log.debug(n.toString()))
          .doOnRequest(t -> log.debug("test request=" + t))
      );

      s.advanceTime();
      ts.assertValueCount(1);
      Assert.assertEquals(0, disposed.get());
      s.advanceTimeBy(Duration.ofMinutes(1));
      Assert.assertEquals(1, disposed.get());
      ts.cancel();
      Assert.assertEquals(1, disposed.get());
    }

    {
      TestSubscriber<Member<Integer>> ts = TestSubscriber.from(1,
        deferred(pool.member())
          .repeat()
          .doOnNext(Member::checkIn)
          .doOnNext(n -> log.debug(n.toString()))
          .doOnRequest(t -> log.debug("test request=" + t))
      );

      s.advanceTime();
      ts.assertValueCount(1);
      Assert.assertEquals(1, disposed.get());
      s.advanceTimeBy(Duration.ofMinutes(1));
      Assert.assertEquals(2, disposed.get());
    }

    {
      TestSubscriber<Member<Integer>> ts = TestSubscriber.from(1,
        deferred(pool.member())
          .repeat()
          .doOnNext(Member::checkIn)
          .doOnNext(n -> log.debug(n.toString()))
          .doOnRequest(t -> log.debug("test request=" + t))
      );

      s.advanceTime();
      ts.assertValueCount(1);
      Assert.assertEquals(2, disposed.get());
    }

    pool.close();
    Assert.assertEquals(3, disposed.get());
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

    final TestSubscriber<Integer> ts = TestSubscriber.from(4,
      deferred(pool.member())
        .repeat()
        .doOnNext(Member::checkIn)
        .map(Member::value)
    );

    s.advanceTime();
    ts.assertValueCount(4)
      .assertNotTerminated();

    final List<Object> list = ts.getEvents().get(0);

    Assert.assertTrue(list.get(0) == list.get(1));
    Assert.assertTrue(list.get(1) == list.get(2));
    Assert.assertTrue(list.get(2) == list.get(3));
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
      final TestSubscriber<Member<Integer>> ts = TestSubscriber.from(1,
        deferred(pool.member())
          .repeat()
          .doOnNext(Member::checkIn)
      );

      s.advanceTime();
      ts.assertValueCount(1);
      Assert.assertEquals(0, disposed.get());
      Assert.assertEquals(0, healthChecks.get());

      ts.request(1);
      s.advanceTime();
      ts.assertValueCount(2);
      Assert.assertEquals(0, disposed.get());
      Assert.assertEquals(0, healthChecks.get());

      s.advanceTimeBy(Duration.ofMillis(1));
      ts.request(1);
      s.advanceTime();
      ts.assertValueCount(2);
      Assert.assertEquals(1, disposed.get());
      Assert.assertEquals(1, healthChecks.get());

      s.advanceTimeBy(Duration.ofMinutes(10));
      ts.assertValueCount(3);

      ts.cancel();
      Assert.assertEquals(1, disposed.get());
    }
  }

  private static <T> Flux<T> deferred(final Mono<T> mono) {
    return new FluxMonoDeferUntilRequest<>(mono);
  }
}
