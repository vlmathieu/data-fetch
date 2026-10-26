"""Progress display for the rules that loop over API calls.

Separate module for the same reason comtrade_polars.py is one: the ENVIRONMENT
that can import it. `tqdm` lives in the rule's conda env, not in the base env
where the Snakefile imports comtrade_common. Progress also has nothing to do
with polars, whose module is long enough already.

Why a clock and not just a counter
----------------------------------
Measured on 2026-10-26: ~940 rows/s, so the default config pulls 539,660 rows
in 9 calls of roughly 64 seconds each. A bar driven only by completed calls
would move 9 times in ten minutes and sit frozen in between — which reads as a
crash, not as a missing bar. So the display has two parts with two jobs:

  line 1  the bar.    Advances by the rows ACTUALLY RECEIVED when a call
                      lands. Never extrapolated.
  line 2  the ticker. Redrawn by a thread every half second, so something is
                      always moving while line 1 has nothing honest to say.

Line 1 is deliberately not extrapolated during a call: a bar that creeps
forward on an estimate keeps creeping when the call is stuck, which is exactly
when you need it to be truthful.

Not a `script:` file but an imported module, so it carries a runnable
self-check:

    python workflow/scripts/comtrade_progress.py
"""

from __future__ import annotations

import sys
import threading
import time

TICK_SECONDS = 0.5

# tqdm's bar needs a terminal. Piped to a file or a pipe it emits one LINE per
# redraw, which on a ten-minute run is well over a thousand lines of noise, so
# the whole thing switches off instead. The per-call lines already written to
# the rule's `log:` stay the record either way.
#
# The bar NEVER goes to the log file: a carriage return in a log is unreadable.


def _elapsed(seconds: float) -> str:
    """Seconds as mm:ss, for the ticker line.

    Parameters
    ----------
    seconds : float
        Elapsed time.

    Returns
    -------
    str
        'MM:SS', minutes not wrapped at 60 so a long call stays readable
        ('74:03' rather than '1:14:03').
    """
    return f"{int(seconds) // 60:02d}:{int(seconds) % 60:02d}"


class Progress:
    """Two-line progress display around a loop of blocking API calls.

    Use as a context manager: the ticker runs in a daemon thread and must be
    stopped even when the loop raises, or a failed download leaves a live
    thread and a terminal full of half-drawn bars.

    Parameters
    ----------
    total : int
        What 100% means. Rows for the download (from the plan's
        expected_rows_total), periods for the preflight.
    desc : str
        Label at the head of the bar, e.g. 'Comtrade 1996-2023'.
    unit : str, optional
        Unit shown by the bar, ' rows' by default. The leading space is tqdm's
        convention so a scaled count reads '540k rows' and not '540krows'.
    unit_scale : bool, optional
        Abbreviate large counts (540000 -> '540k'), true by default. Turn it
        off for small counts: '4/4 periods' beats '4.00/4.00 periods'. The
        unit then wants no leading space, because tqdm inverts the rate below
        one unit per second and 's/ period' reads badly.
    enabled : bool, optional
        Whether to draw anything. Defaults to "stderr is a terminal". Passed
        explicitly by the self-check, which runs with no tqdm installed.

    Attributes
    ----------
    enabled : bool
        False means every method is a no-op and tqdm is never imported.
    received : int
        Running total of what `done()` was given, which is what the bar shows.
    """

    def __init__(self, total: int, desc: str, unit: str = " rows",
                 enabled: bool | None = None, unit_scale: bool = True) -> None:
        self.total = total
        self.desc = desc
        self.unit = unit
        self.unit_scale = unit_scale
        self.enabled = (sys.stderr.isatty() if enabled is None else enabled)
        self.received = 0

        self._bar = None
        self._status = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        # What the ticker reads. Guarded because the thread reads it while the
        # main thread writes it.
        self._lock = threading.Lock()
        self._label = ""
        self._note = ""
        self._started = time.monotonic()

    # --- lifecycle ---------------------------------------------------------

    def __enter__(self) -> Progress:
        if not self.enabled:
            return self
        # Imported HERE and not at module level: the disabled path must need
        # nothing, so that the self-check runs without tqdm and a
        # non-interactive run cannot fail on a missing dependency.
        from tqdm import tqdm  # noqa: PLC0415

        self._bar = tqdm(
            total=self.total, desc=self.desc, unit=self.unit,
            unit_scale=self.unit_scale, position=0, file=sys.stderr,
            leave=True,
        )
        # A second bar used as a plain text line: bar_format '{desc}' draws
        # nothing but the description.
        self._status = tqdm(
            total=0, position=1, file=sys.stderr, bar_format="{desc}",
            leave=True,
        )
        self._thread = threading.Thread(target=self._tick, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2 * TICK_SECONDS)
        for bar in (self._status, self._bar):
            if bar is not None:
                bar.close()
        self._thread = None

    # --- what the caller drives -------------------------------------------

    def step(self, label: str, expected: int | None = None) -> None:
        """Announce a call BEFORE making it.

        The half that matters most: without it the display sits on the figure
        reached by the previous call for the whole minute the next one takes,
        and nothing says which request is in flight.

        Parameters
        ----------
        label : str
            What is being fetched, e.g. 'pack 5/9  2011-2013'.
        expected : int, optional
            Rows the plan expects from this call, shown as context.

        Returns
        -------
        None
        """
        with self._lock:
            self._label = (f"{label}   {expected:,} expected" if expected
                           else label)
            self._note = ""
            self._started = time.monotonic()
        self._draw()

    def done(self, received: int) -> None:
        """Record a finished call and advance the bar.

        Advances by what was RECEIVED, not by what the plan expected: if
        Comtrade revised upstream, the bar shows the truth and the counts check
        reports the gap. tqdm copes with overshooting its total.

        Parameters
        ----------
        received : int
            Units actually obtained — rows for the download, 1 per period for
            the preflight.

        Returns
        -------
        None
        """
        self.received += received
        if self._bar is not None:
            self._bar.update(received)

    def note(self, text: str) -> None:
        """Add a line of context to the ticker, kept until the next `step`.

        Carries what would otherwise be invisible: a retry and its backoff, or
        a phase with no bar of its own such as the vintage batches.

        Parameters
        ----------
        text : str
            Short, appended after the label.

        Returns
        -------
        None
        """
        with self._lock:
            self._note = text
        self._draw()

    # --- the ticker --------------------------------------------------------

    def _line(self) -> str:
        """The ticker line as it stands now."""
        with self._lock:
            label, note = self._label, self._note
            elapsed = time.monotonic() - self._started
        parts = [f"  ↳ {label}" if label else "  ↳"]
        if note:
            parts.append(note)
        parts.append(f"{_elapsed(elapsed)} elapsed")
        return "   ".join(parts)

    def _draw(self) -> None:
        if self._status is not None:
            self._status.set_description_str(self._line())

    def _tick(self) -> None:
        """Redraws the ticker until __exit__ sets the stop event.

        `Event.wait` rather than `sleep`, so stopping is immediate instead of
        waiting out the current interval.
        """
        while not self._stop.wait(TICK_SECONDS):
            self._draw()


def retry_note(attempt: int, attempts: int, delay: float, detail: str) -> str:
    """Formats a call() retry for the ticker.

    This is the only moment the workflow genuinely looks hung: call() sleeps
    5+10+20+40 = 75 seconds on a 429 and used to say nothing at all, so the
    ticker would have counted up while implying a download was in progress.

    Parameters
    ----------
    attempt : int
        Which attempt just failed, 1-based.
    attempts : int
        How many will be made in total.
    delay : float
        Seconds before the next one.
    detail : str
        Why it failed, from call().

    Returns
    -------
    str
        One line, e.g. 'FAILED attempt 3/5, retrying in 20s (HTTP 429)'.
    """
    return (f"FAILED attempt {attempt}/{attempts}, retrying in "
            f"{delay:.0f}s ({detail[:60]})")


def retry_reporter(progress: Progress, label: str, log):
    """Builds the `on_retry` callback for comtrade_common.call().

    Sends a retry to both places it belongs: the ticker, so the screen says
    why it is waiting, and the log, because a retry is a fact about the run
    and not just something to look at.

    Parameters
    ----------
    progress : Progress
        The display to write the note on. A disabled one is a no-op, so the
        caller never has to branch.
    label : str
        Which call is retrying, e.g. 'count 2014'.
    log : callable
        A logging function taking a format string and arguments, typically
        logging.warning.

    Returns
    -------
    callable
        report(attempt, attempts, delay, detail), to pass as
        call(..., on_retry=report).
    """
    def report(attempt: int, attempts: int, delay: float, detail: str) -> None:
        note = retry_note(attempt, attempts, delay, detail)
        log("[%s] %s", label, note)
        progress.note(note)

    return report


# --- check ------------------------------------------------------------------

def _demo() -> None:
    """Runnable check: python workflow/scripts/comtrade_progress.py

    Runs with tqdm DISABLED throughout, on purpose: the repo's self-checks must
    work without conda and without the network, and tqdm only exists in the
    rule env. The drawing is not testable; the lifecycle and the arithmetic
    are, and those are where a bug would actually hurt — a leaked thread or a
    bar that counts the wrong thing.

    Returns
    -------
    None
        Prints 'comtrade_progress: checks OK', or raises.
    """
    # --- elapsed formatting ---
    assert _elapsed(0) == "00:00"
    assert _elapsed(72.9) == "01:12"
    # Minutes are NOT wrapped at 60: a call stuck for over an hour must read as
    # a big number, not as a small one.
    assert _elapsed(4443) == "74:03", _elapsed(4443)

    # --- disabled: no tqdm, no thread, no output ---
    with Progress(total=100, desc="off", enabled=False) as off:
        off.step("pack 1/1", 100)
        off.note("something")
        off.done(100)
        assert off._thread is None, "no thread when disabled"
        assert off._bar is None and off._status is None
    # The accounting still works, so a caller can read `received` either way.
    assert off.received == 100

    # --- the bar counts what was RECEIVED, not what was expected ---
    p = Progress(total=1000, desc="x", enabled=False)
    p.step("pack 1/2", expected=600)
    p.done(550)                       # upstream revision: 50 fewer
    p.step("pack 2/2", expected=400)
    p.done(400)
    assert p.received == 950, p.received

    # --- the ticker line ---
    p = Progress(total=10, desc="x", enabled=False)
    p.step("pack 3/9  2011-2013", expected=61_208)
    line = p._line()
    assert "pack 3/9  2011-2013" in line and "61,208 expected" in line, line
    assert "elapsed" in line
    p.note("FAILED attempt 2/5")
    assert "FAILED attempt 2/5" in p._line()
    # A note lasts until the next step and no longer.
    p.step("pack 4/9  2014-2016")
    assert "FAILED" not in p._line()

    # --- lifecycle with the thread actually running ---
    # Forced on with a fake tqdm, so the thread is exercised without the real
    # dependency and without writing to the terminal.
    import types                                      # noqa: PLC0415

    drawn: list[str] = []

    class FakeBar:
        def __init__(self, **kwargs):
            self.n = 0
            self.closed = False

        def update(self, n):
            self.n += n

        def set_description_str(self, text):
            drawn.append(text)

        def close(self):
            self.closed = True

    sys.modules["tqdm"] = types.SimpleNamespace(tqdm=FakeBar)
    try:
        before = threading.active_count()
        with Progress(total=10, desc="live", enabled=True) as live:
            live.step("pack 1/1", 10)
            assert live._thread is not None and live._thread.is_alive()
            # Long enough for at least two ticks.
            time.sleep(3 * TICK_SECONDS)
            live.done(10)
        assert not live._thread, "the thread handle must be dropped"
        assert live._bar.closed and live._status.closed, "both bars closed"
        assert live._bar.n == 10, live._bar.n
        assert len(drawn) >= 2, f"the ticker drew {len(drawn)} time(s)"
        assert threading.active_count() == before, "a thread was leaked"

        # The thread must also stop when the loop RAISES, which is the case
        # that would otherwise leave a daemon running and the terminal broken.
        try:
            with Progress(total=10, desc="boom", enabled=True) as bad:
                bad.step("pack 1/1", 10)
                raise RuntimeError("the download failed")
        except RuntimeError:
            pass
        assert bad._bar.closed and bad._status.closed
        assert threading.active_count() == before, "a thread leaked on error"
    finally:
        del sys.modules["tqdm"]

    # --- the retry line, and that it reaches BOTH destinations ---
    note = retry_note(3, 5, 20.0, "None response (HTTP error swallowed)")
    assert "attempt 3/5" in note and "20s" in note, note

    logged: list[tuple] = []
    p = Progress(total=1, desc="x", enabled=False)
    report = retry_reporter(p, "count 2014", lambda *args: logged.append(args))
    report(2, 5, 10.0, "HTTP 429")
    assert len(logged) == 1 and logged[0][1] == "count 2014", logged
    assert "attempt 2/5" in logged[0][2], logged[0]
    # Disabled display, so nothing was DRAWN -- but the log still got it, and
    # the state is tracked all the same, as `received` is. That split is the
    # point: turning the bar off must never cost information.
    assert p._status is None, "nothing drawn when disabled"
    assert "attempt 2/5" in p._note, p._note

    print("comtrade_progress: checks OK")


if __name__ == "__main__":
    _demo()
