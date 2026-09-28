from app.detection.window import SlidingWindow


def test_window_evicts_old_events():
    w = SlidingWindow(10)
    w.add(100.0, True, now=100.0)
    w.add(105.0, False, now=105.0)
    assert (w.total, w.errors) == (2, 1)
    w.evict(now=110.5)                      # first event is 10.5 s old
    assert (w.total, w.errors) == (1, 0)


def test_window_boundary_event_is_kept():
    w = SlidingWindow(10)
    w.add(100.0, True, now=100.0)
    w.evict(now=110.0)                      # exactly W old -> still inside
    assert w.total == 1
    w.evict(now=110.001)                    # just past -> gone
    assert w.total == 0 and w.errors == 0


def test_error_rate_arithmetic():
    w = SlidingWindow(60)
    for i in range(100):
        w.add(1000.0, is_error=i < 50, now=1000.0)
    assert w.error_rate == 0.5


def test_empty_window_rate_is_zero():
    assert SlidingWindow(10).error_rate == 0.0


def test_silent_service_decays_on_tick_eviction():
    w = SlidingWindow(10)
    for _ in range(20):
        w.add(100.0, True, now=100.0)
    assert w.total == 20
    w.evict(now=200.0)                      # no new events, but the clock moved (the tick does this)
    assert (w.total, w.errors, w.error_rate) == (0, 0, 0.0)


def test_stale_event_is_evicted_on_arrival():
    w = SlidingWindow(10)
    w.add(50.0, True, now=100.0)            # arrives 50 s late (e.g. replayed backlog)
    assert w.total == 0
