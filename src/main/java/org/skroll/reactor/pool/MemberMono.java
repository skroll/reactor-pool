package org.skroll.reactor.pool;

import java.io.Closeable;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Operators.MonoSubscriber;
import reactor.core.scheduler.Scheduler;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;

final class MemberMono<T> extends Mono<Member<T>> implements Subscription, Closeable, Runnable {
  private static final Logger log = Loggers.getLogger(MemberMono.class);

  final AtomicReference<Subscribers<T>> subscribers;

  @SuppressWarnings({ "rawtypes", "unchecked" })
  static final Subscribers EMPTY =
      new Subscribers(new MemberMonoSubscriber[0], new boolean[0], 0, 0);

  private final Queue<DecoratingMember<T>> initializedAvailable;
  private final Queue<DecoratingMember<T>> notInitialized;
  private final Queue<DecoratingMember<T>> toBeReleased;
  private final Queue<DecoratingMember<T>> toBeChecked;

  private final AtomicInteger wip = new AtomicInteger();
  private final DecoratingMember<T>[] members;
  private final Scheduler scheduler;
  private final long createRetryIntervalMs;

  // synchronized by 'wip'
  private final Disposable.Composite scheduled = Disposables.composite();

  final NonBlockingPool<T> pool;

  // represents the number of outstanding member requests.
  // the number is decremented when a new member value is
  // initialized (a scheduled action with a subsequent drain call)
  // or an existing value is available from the pool (queue) (and
  // is then emitted)
  private final AtomicLong requested = new AtomicLong();

  private final AtomicLong initializeScheduled = new AtomicLong();

  private volatile boolean cancelled;

  @SuppressWarnings("unchecked")
  MemberMono(final NonBlockingPool<T> pool) {
    final Supplier<Queue<DecoratingMember<T>>> queueSupplier = Queues.unboundedMultiproducer();
    this.initializedAvailable = queueSupplier.get();
    this.notInitialized = queueSupplier.get();
    this.toBeReleased = queueSupplier.get();
    this.toBeChecked = queueSupplier.get();
    this.members = createMembersArray(pool.maxSize, pool.checkInDecorator);
    for (final DecoratingMember<T> m : members) {
      notInitialized.offer(m);
    }
    this.scheduler = pool.scheduler;
    this.createRetryIntervalMs = pool.createRetryIntervalMs;
    this.subscribers = new AtomicReference<>(EMPTY);
    this.pool = Objects.requireNonNull(pool);
  }

  private DecoratingMember<T>[] createMembersArray(
      final int poolMaxSize,
      final BiFunction<? super T, ? super CheckIn, ? extends T> checkInDecorator
  ) {
    @SuppressWarnings("unchecked")
    final DecoratingMember<T>[] m = new DecoratingMember[poolMaxSize];

    for (int i = 0; i < m.length; i++) {
      m[i] = new DecoratingMember<>(null, checkInDecorator, this);
    }

    return m;
  }

  @Override
  public void subscribe(final CoreSubscriber<? super Member<T>> actual) {
    // the action of checking out a member from the pool is implemented
    // as a subscription to the singleton MemberMono
    final MemberMonoSubscriber<T> s = new MemberMonoSubscriber<>(actual, this);
    actual.onSubscribe(s);

    if (pool.isClosed()) {
      actual.onError(new PoolClosedException());
      return;
    }
    add(s);
    if (s.isDisposed()) {
      remove(s);
    }
    requested.incrementAndGet();
    log.debug("subscribed");
    drain();
  }

  public void checkIn(final Member<T> member) {
    checkIn(member, false);
  }

  public void checkIn(final Member<T> member, final boolean decrementInitializeScheduled) {
    log.debug("checking in {}", member);
    final DecoratingMember<T> d = ((DecoratingMember<T>) member);
    d.scheduleRelease();
    d.markAsChecked();
    initializedAvailable.offer((DecoratingMember<T>) member);
    if (decrementInitializeScheduled) {
      initializeScheduled.decrementAndGet();
    }
    drain();
  }

  public void addToBeReleased(final DecoratingMember<T> member) {
    toBeReleased.offer(member);
    drain();
  }

  @Override
  public void request(final long l) {
    drain();
  }

  @Override
  public void cancel() {
    this.cancelled = true;
  }

  @Override
  public void run() {
    try {
      drain();
    } catch (final Throwable t) {
      Exceptions.throwIfFatal(t);
    }
  }

  private void drain() {
    log.debug("drain called");
    if (wip.getAndIncrement() == 0) {
      log.debug("drain loop starting");
      int missed = 1;
      while (true) {
        // we schedule release of members even if no requests exist
        scheduleReleases();
        scheduleChecks();

        final long numRequested = requested.get();
        log.debug("requested={}", numRequested);

        long numEmitted = 0; // emitted
        while (numEmitted != numRequested) {
          if (cancelled) {
            disposeAll();
            return;
          }
          final Subscribers<T> subs = subscribers.get();
          // the check below is required so a tryEmit that returns false doesn't bring
          // abouts a spin on this loop
          final int activeSubsCount = subs.activeCount;
          // if there have been some cancellations then adjust the requested amount by
          // increasing emitted e
          numEmitted += Math.max(0, numRequested - numEmitted - activeSubsCount);
          if (activeSubsCount == 0) {
            // if no observers then we break the loop
            break;
          }
          // check for an already initialized available member
          final DecoratingMember<T> m = initializedAvailable.poll();
          log.debug("poll of available members returns {}", String.valueOf(m));

          if (m == null) {
            // no members available, check for a released member (that needs to be
            // reinitialized before use)
            final DecoratingMember<T> m2 = notInitialized.poll();
            if (m2 == null) {
              break;
            } else {
              // only schedule member initialization if there is enough demand,
              boolean used = trySchedulingInitialization(numRequested, numEmitted, m2);
              if (!used) {
                break;
              }
            }
          } else if (!m.isReleasing() && !m.isChecking()) {
            log.debug("trying to emit member");
            if (shouldPerformHealthCheck(m)) {
              log.debug("queueing member for health check {}", m);
              toBeChecked.offer(m);
            } else {
              log.debug("no health check required for {}", m);
              // this should not block because it just schedules emissions to observers
              if (tryEmit(subs, m)) {
                numEmitted++;
              } else {
                log.debug("no active subscribers");
              }
            }
          }

          // schedule release immediately of any member
          // queued for releasing
          scheduleReleases();
          // schedule check of any member queued for checking
          scheduleChecks();

        }
        // normally we don't reduce requested if it is Long.MAX_VALUE
        // but given that the only way to increase requested is by subscribing
        // (which increases it by one only) then requested will never be Long.MAX_VALUE
        if (numEmitted != 0L) {
          requested.addAndGet(-numEmitted);
        }
        missed = wip.addAndGet(-missed);
        if (missed == 0) {
          return;
        }
      }
    }
  }

  private boolean trySchedulingInitialization(final long r,
                                              final long e,
                                              final DecoratingMember<T> m) {
    // check initializeScheduled using a CAS loop
    while (true) {
      final long cs = initializeScheduled.get();
      if (e + cs < r) {
        if (initializeScheduled.compareAndSet(cs, cs + 1)) {
          log.debug("scheduling member creation");
          scheduled.add(scheduler.schedule(new Initializer(m)));
          return true;
        }
      } else {
        log.debug("insufficient demand to initialize {}", m);
        // don't need to initialize more so put back on queue and exit the loop
        notInitialized.offer(m);
        return false;
      }
    }
  }

  private boolean shouldPerformHealthCheck(final DecoratingMember<T> m) {
    long now = scheduler.now(TimeUnit.MILLISECONDS);
    return pool.idleTimeBeforeHealthCheckMs > 0
        && now - m.lastCheckTime() >= pool.idleTimeBeforeHealthCheckMs;
  }

  private void scheduleChecks() {
    DecoratingMember<T> m;
    while ((m = toBeChecked.poll()) != null) {
      if (!m.isReleasing()) {
        // we mark as checking so that we can ignore it if already in the
        // initializedAvailable queue after concurrent checkin
        m.markAsChecking();
        scheduled.add(scheduler.schedule(new Checker(m)));
      }
    }
  }

  private void scheduleReleases() {
    DecoratingMember<T> m;
    while ((m = toBeReleased.poll()) != null) {
      // we mark as releasing so that we can ignore it if already in the
      // initializedAvailable queue after concurrent checkin
      m.markAsReleasing();
      scheduled.add(scheduler.schedule(new Releaser(m)));
    }
  }

  private boolean tryEmit(final Subscribers<T> subs, final DecoratingMember<T> m) {
    final int index = subs.index;
    final MemberMonoSubscriber<T> sub = subs.subscribers[index];
    MemberMonoSubscriber<T> subNext = sub;

    // atomically bump up the index (if that entry has not been deleted in
    // the meantime by disposal)
    while (true) {
      final Subscribers<T> x = subscribers.get();
      if (x.index == index && x.subscribers[index] == sub) {
        final boolean[] active = new boolean[x.active.length];
        System.arraycopy(x.active, 0, active, 0, active.length);

        int nextIndex = (index + 1) % active.length;
        while (nextIndex != index && !active[nextIndex]) {
          nextIndex = (nextIndex + 1) % active.length;
        }

        active[nextIndex] = false;

        if (subscribers.compareAndSet(
            x,
            new Subscribers<>(x.subscribers, active, x.activeCount - 1, nextIndex))
        ) {
          subNext = x.subscribers[nextIndex];
          break;
        }
      } else {
        m.checkIn();
        return false;
      }
    }

    final Scheduler.Worker worker = scheduler.createWorker();
    worker.schedule(new Emitter<>(worker, subNext, m));
    return true;
  }

  final class Initializer implements Runnable {
    private final DecoratingMember<T> member;

    Initializer(final DecoratingMember<T> member) {
      this.member = member;
    }

    @Override
    public void run() {
      if (!cancelled) {
        try {
          final T value = pool.factory.call();
          member.setValueAndClearReleasingFlag(value);
          requested.incrementAndGet();
          checkIn(member, true);
        } catch (final Throwable t) {
          Exceptions.throwIfFatal(t);

          if (!cancelled) {
            scheduled.add(scheduler.schedule(this, createRetryIntervalMs, TimeUnit.MILLISECONDS));
          }
        }
      }
    }
  }

  final class Releaser implements Runnable {
    private DecoratingMember<T> member;

    Releaser(final DecoratingMember<T> member) {
      this.member = member;
    }

    @Override
    public void run() {
      try {
        member.disposeValue();
        release(member);
      } catch (final Throwable t) {
        Exceptions.throwIfFatal(t);
      }
    }
  }

  final class Checker implements Runnable {
    private final DecoratingMember<T> member;

    public Checker(final DecoratingMember<T> member) {
      this.member = member;
    }

    @Override
    public void run() {
      try {
        if (!pool.healthCheck.test(member.value())) {
          member.disposeValue();
          scheduled.add(scheduler.schedule(() -> {
            notInitialized.offer(member);
            drain();
          }, pool.createRetryIntervalMs, TimeUnit.MILLISECONDS));
        } else {
          member.markAsChecked();
          initializedAvailable.offer(member);
          drain();
        }
      } catch (Throwable t) {
        Exceptions.throwIfFatal(t);
      }
    }
  }

  @Override
  public void close() {
    cancel();
  }

  private void disposeAll() {
    initializedAvailable.clear();
    toBeReleased.clear();
    notInitialized.clear();
    disposeValues();
    removeAllSubscribers();
  }

  private void disposeValues() {
    scheduled.dispose();
    for (final DecoratingMember<T> member : members) {
      member.disposeValue();
    }
  }

  void add(MemberMonoSubscriber<T> inner) {
    while (true) {
      Subscribers<T> a = subscribers.get();
      int n = a.subscribers.length;
      @SuppressWarnings("unchecked")
      MemberMonoSubscriber<T>[] b = new MemberMonoSubscriber[n + 1];
      System.arraycopy(a.subscribers, 0, b, 0, n);
      b[n] = inner;
      boolean[] active = new boolean[n + 1];
      System.arraycopy(a.active, 0, active, 0, n);
      active[n] = true;
      if (subscribers.compareAndSet(a, new Subscribers<>(b, active, a.activeCount + 1, a.index))) {
        return;
      }
    }
  }


  @SuppressWarnings("unchecked")
  private void removeAllSubscribers() {
    while (true) {
      final Subscribers<T> a = subscribers.get();
      if (subscribers.compareAndSet(a, EMPTY)) {
        return;
      }
    }
  }

  @SuppressWarnings("unchecked")
  void remove(final MemberMonoSubscriber<T> inner) {
    while (true) {
      Subscribers<T> a = subscribers.get();
      int n = a.subscribers.length;
      if (n == 0) {
        return;
      }

      int j = -1;

      for (int i = 0; i < n; i++) {
        if (a.subscribers[i] == inner) {
          j = i;
          break;
        }
      }

      if (j < 0) {
        return;
      }

      Subscribers<T> next;
      if (n == 1) {
        next = EMPTY;
      } else {
        MemberMonoSubscriber<T>[] b = new MemberMonoSubscriber[n - 1];
        System.arraycopy(a.subscribers, 0, b, 0, j);
        System.arraycopy(a.subscribers, j + 1, b, j, n - j - 1);
        boolean[] active = new boolean[n - 1];
        System.arraycopy(a.active, 0, active, 0, j);
        System.arraycopy(a.active, j + 1, active, j, n - j - 1);
        int nextActiveCount = a.active[j] ? a.activeCount - 1 : a.activeCount;
        if (a.index >= j && a.index > 0) {
          next = new Subscribers<>(b, active, nextActiveCount, a.index - 1);
        } else {
          next = new Subscribers<>(b, active, nextActiveCount, a.index);
        }
      }

      if (subscribers.compareAndSet(a, next)) {
        return;
      }
    }
  }


  private static final class Subscribers<T> {
    final MemberMonoSubscriber<T>[] subscribers;
    final boolean[] active;
    final int activeCount;
    final int index;

    Subscribers(final MemberMonoSubscriber<T>[] subscribers,
                final boolean[] active,
                final int activeCount,
                final int index) {
      // TODO: check subscribers length
      this.subscribers = subscribers;
      this.index = index;
      this.active = active;
      this.activeCount = activeCount;
    }
  }

  private static final class Emitter<T> implements Runnable {
    private final Scheduler.Worker worker;
    private final MemberMonoSubscriber<T> subscriber;
    private final Member<T> member;

    public Emitter(final Scheduler.Worker worker,
                   final MemberMonoSubscriber<T> subscriber,
                   final Member<T> member) {
      this.worker = worker;
      this.subscriber = subscriber;
      this.member = member;
    }

    @Override
    public void run() {
      worker.dispose();
      try {
        subscriber.onNext(member);
        subscriber.onComplete();
      } catch (final Throwable e) {
        Exceptions.throwIfFatal(e);
      } finally {
        subscriber.dispose();
      }
    }
  }

  public void release(DecoratingMember<T> m) {
    notInitialized.offer(m);
    drain();
  }

  static final class MemberMonoSubscriber<T> extends MonoSubscriber<Member<T>, Member<T>> {
    final AtomicReference<MemberMono<T>> parent = new AtomicReference<>();

    public MemberMonoSubscriber(final CoreSubscriber<? super Member<T>> actual,
                                final MemberMono<T> parent) {
      super(actual);
      this.parent.lazySet(parent);
    }

    @Override
    public void onNext(final Member<T> t) {
      actual.onNext(t);
    }

    @Override
    public void cancel() {
      if (!isCancelled()) {
        super.cancel();
        dispose();
      }
    }

    void dispose() {
      final MemberMono<T> p = parent.getAndSet(null);
      if (p != null) {
        p.remove(this);
      }
    }

    boolean isDisposed() {
      return parent.get() == null;
    }
  }
}
