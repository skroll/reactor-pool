package org.skroll.reactor.pool;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class NonBlockingPoolTest {
  @Test
  public void testDummy() {
    VirtualTimeScheduler s = VirtualTimeScheduler.create();
    AtomicInteger count = new AtomicInteger();
    AtomicInteger disposed = new AtomicInteger();

    final ReactivePool<Integer> pool = NonBlockingPool
      .factory(count::incrementAndGet)
      .healthCheck(n -> true)
      .maxSize(1)
      .scheduler(s)
      .maxIdleTime(1, TimeUnit.MINUTES)
      .disposer(n -> disposed.incrementAndGet())
      .build();

    for (int i = 0; i < 1000; i++) {
      final Disposable d = pool.member()
        .doOnNext(m -> System.out.println("doOnNext1"))
        .doOnRequest(t -> System.out.println("test1 request=" + t))
        .subscribe(m -> {
          System.out.println("value1=" + m.value());
          m.checkIn();
        });
      d.dispose();
    }

    final Disposable d2 = pool.member()
      .doOnNext(m -> System.out.println("doOnNext2"))
      .doOnRequest(t -> System.out.println("test2 request=" + t))
      .subscribe(m -> {
        System.out.println("value2=" + m.value());
        m.checkIn();
      });

    Assert.assertEquals(0, count.get());
    // d.dispose();
    s.advanceTime();
//    Assert.assertEquals(1, count.get());
//    Assert.assertEquals(0, disposed.get());
  }
  @Test
  public void testMaxIdleTime() {
    VirtualTimeScheduler.getOrSet();
    AtomicInteger count = new AtomicInteger();
    AtomicInteger disposed = new AtomicInteger();

    final ReactivePool<Integer> pool = NonBlockingPool
      .factory(count::incrementAndGet)
      .healthCheck(n -> true)
      .maxSize(3)
      .maxIdleTime(1, TimeUnit.MINUTES)
      .disposer(n -> disposed.incrementAndGet())
      .build();

    final Mono<Member<Integer>> ts = pool.member()
      .doOnNext(Member::checkIn)
      .doOnNext(System.out::println)
      .doOnRequest(t -> System.out.println("test request=" + t));

    StepVerifier.withVirtualTime(() -> ts)
      .expectSubscription()
      .then(() -> Assert.assertEquals(0, disposed.get()))
      .thenAwait(Duration.ofMinutes(1))
      .then(() -> Assert.assertEquals(1, disposed.get()))
      .expectNextCount(1)
      .verifyComplete();
  }

  @Test
  public void testReleasedMemberIsRecreated() throws Exception {
    final VirtualTimeScheduler s = VirtualTimeScheduler.create();
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

    {
      final Mono<Member<Integer>> ts = pool.member()
        .doOnNext(Member::checkIn)
        .doOnNext(System.out::println)
        .doOnRequest(t -> System.out.println("test request=" + t));

      StepVerifier.create(ts)
        .expectSubscription()
        .then(() -> Assert.assertEquals(0, disposed.get()))
        .then(s::advanceTime)
        .expectNextCount(1)
        .then(() -> s.advanceTimeBy(Duration.ofMinutes(1)))
        .then(() -> Assert.assertEquals(1, disposed.get()))
        .then(s::advanceTime)
        .thenCancel()
        .verify();

      Assert.assertEquals(1, disposed.get());
    }

    {
      final Mono<Member<Integer>> ts = pool.member()
        .doOnNext(Member::checkIn)
        .doOnNext(System.out::println)
        .doOnRequest(t -> System.out.println("test request=" + t));

      StepVerifier.create(ts)
        .expectSubscription()
        .then(() -> Assert.assertEquals(1, disposed.get()))
        .then(s::advanceTime)
        .expectNextCount(1)
        .then(() -> s.advanceTimeBy(Duration.ofMinutes(1)))
        .verifyComplete();

      Assert.assertEquals(2, disposed.get());
    }

    {
      final Mono<Member<Integer>> ts = pool.member()
        .doOnNext(Member::checkIn)
        .doOnNext(System.out::println)
        .doOnRequest(t -> System.out.println("test request=" + t));

      StepVerifier.create(ts)
        .expectSubscription()
        .then(() -> Assert.assertEquals(2, disposed.get()))
        .then(s::advanceTime)
        .expectNextCount(1)
        .verifyComplete();

      Assert.assertEquals(2, disposed.get());
    }

    pool.close();
    Assert.assertEquals(3, disposed.get());
  }
}
