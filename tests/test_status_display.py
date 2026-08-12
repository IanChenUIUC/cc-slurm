"""spec.md §11 `status` rendering: the stacked bar, the task histogram, and the two
places where slurm's own vocabulary is involved (unknown states, duration compare)."""
from specs import CHAIN, SPLIT


def _strip(s):
    import re
    return re.sub(r"\033\[\d+m", "", s)


def row(r, unit):
    return next(ln for ln in r.stdout.splitlines() if ln.startswith(unit + " "))


def test_a_single_unit_array_shows_its_tasks(mock_run):
    """It used to render exactly like a one-job recipe, which is how a 9-task array
    could sit there looking like a single pending job."""
    r = mock_run(CHAIN, {"root": "COMPLETED"}, "status")
    assert r.ok, r.stderr
    assert "[1 array]" in row(r, "root") or "[1 array," in row(r, "root")
    assert "1 COMPLETED" in row(r, "root")


def test_absent_work_is_counted_and_uppercased(mock_run):
    r = mock_run(CHAIN, {"root": "COMPLETED"}, "status")
    assert "1 ABSENT" in row(r, "mid")
    assert "absent" not in r.stdout


def test_invalidated_reads_as_absent(mock_run):
    """Both mean "no valid result here"; which one it was is what `history` is for."""
    r = mock_run(CHAIN, {"root": "INVALIDATED"}, "status")
    assert "1 ABSENT" in row(r, "root")
    assert "INVALIDATED" not in r.stdout


def test_one_bad_task_among_many_keeps_a_cell(engine):
    """The whole point of the bar: a 1-in-72 failure must not round away."""
    bar = engine.Engine({})._bar({"COMPLETED": 71, "TIMEOUT": 1})
    assert len(bar) == engine.BAR_WIDTH
    assert bar.count(engine.GLYPH["failed"]) == 1
    assert bar.count(engine.GLYPH["completed"]) == engine.BAR_WIDTH - 1


def test_a_colored_bar_is_solid_and_a_plain_one_is_shaded(engine):
    """A shade glyph blends with the background, so the same color code renders
    visibly dimmer than the histogram text beside it. Colored, the hue carries the
    distinction; uncolored, the shades are all there is."""
    e = engine.Engine({})
    counts = {"COMPLETED": 10, "PENDING": 10}
    assert set(_strip(e._bar(counts, color=True))) == {engine.SOLID}
    assert set(e._bar(counts)) == {engine.GLYPH["completed"], engine.GLYPH["waiting"]}


def test_bar_classes_and_width(engine):
    e = engine.Engine({})
    assert e._bar({}) == ""
    assert e._bar({"COMPLETED": 4}) == engine.GLYPH["completed"] * engine.BAR_WIDTH
    mixed = e._bar({"COMPLETED": 131, "PENDING": 259, "RUNNING": 30})
    assert len(mixed) == engine.BAR_WIDTH
    assert set(mixed) == {engine.GLYPH[c] for c in ("completed", "running", "waiting")}


def test_color_is_off_when_not_a_tty(mock_run):
    """`just status | tee` has to stay diffable, so the escape codes are TTY-only."""
    r = mock_run(SPLIT, {}, "status")
    assert "\033[" not in r.stdout


def test_elapsed_compares_as_a_duration(engine):
    """A string compare puts a 26-hour job below a 23-hour one."""
    assert engine._seconds("1-02:00:00") > engine._seconds("23:00:00")
    assert max(["23:00:00", "1-02:00:00"], key=engine._seconds) == "1-02:00:00"
    assert engine._seconds("-") == -1


def test_an_unknown_state_reads_as_live(engine, mock_run):
    """slurm owns this vocabulary. A state we have never seen is likelier to be a
    live job -- resubmitting underneath one is the expensive mistake."""
    assert engine.is_live("SOME_NEW_STATE")
    assert not engine.is_live("CANCELLED")
    assert not engine.is_live(None)

    r = mock_run(CHAIN, {"root": "SOME_NEW_STATE"}, "submit")
    assert r.ok, r.stderr
    assert "root" not in r.submitted


def test_each_bar_segment_paints_its_own_class(engine):
    """The bar is built from class names while the histogram is built from states, so
    the painter has to take a class -- feeding it a state (or vice versa) silently
    painted every segment with the fallback color."""
    e = engine.Engine({})
    bar = e._bar({"COMPLETED": 10, "RUNNING": 10}, color=True)
    for cls_name in ("completed", "running"):
        assert f"\033[{engine.COLORS[cls_name]}m{engine.SOLID}" in bar
    assert f"\033[{engine.COLORS['failed']}m" not in bar

    hist = e._hist({"COMPLETED": 10, "TIMEOUT": 1}, color=True)
    assert f"\033[{engine.COLORS['completed']}m10 COMPLETED" in hist
    assert f"\033[{engine.COLORS['failed']}m1 TIMEOUT" in hist


def test_an_unrecognized_state_paints_as_a_failure(engine):
    """It reads as *live* for submission, but on screen it must stand out, not hide
    in the green."""
    assert engine.state_class("SOME_NEW_STATE") == "failed"
    assert engine.state_class("PENDING") == "waiting"
