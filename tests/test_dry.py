"""spec.md §11 `submit --dry`: the same decisions as a real submit, printed instead
of issued. `dry` used to be a separate walk that listed every unit unconditionally,
never reconciling and never computing what actually needed to run.
"""
from specs import FANIN

UP, SIDE = "700", "800"


def test_dry_submits_nothing_and_logs_no_submission(mock_run):
    r = mock_run(FANIN, {}, "submit", "--dry")
    assert r.ok, r.stderr
    assert r.submitted == {}                    # the runner was never invoked
    assert [rec["unit"] for rec in r.log] == []  # nothing seeded, nothing appended
    # ...but the scripts are on disk to read, which is the point of a dry run.
    assert (r.workdir / ".pipeline" / "scripts" / "up.tasks").is_dir()


def test_dry_plans_only_what_needs_running(mock_run):
    """The old `dry` printed all three units regardless of state."""
    state = {"up": ("COMPLETED", UP), "side": ("COMPLETED", SIDE)}
    r = mock_run(FANIN, state, "submit", "--dry")
    assert r.ok, r.stderr
    assert set(r.planned) == {"down"}
    assert [u for u, _ in r.skipped] == ["up", "side"]


def test_dry_matches_a_real_submit(mock_run):
    """Same state, same scope -> same units and same dependency tokens. A live
    parent's real id appears in both; only in-wave parents differ (placeholders)."""
    state = {"up": ("RUNNING", UP), "side": ("COMPLETED", SIDE)}
    dry = mock_run(FANIN, state, "submit", "--dry", "--only", "down*")
    wet = mock_run(FANIN, state, "submit", "--only", "down*")
    assert dry.ok and wet.ok, dry.stderr + wet.stderr

    assert set(dry.planned) == set(wet.submitted) == {"down"}
    assert dry.planned["down"].afterok == wet.submitted["down"].afterok == [UP]
    assert dry.planned["down"].flags == wet.submitted["down"].flags
    assert dry.planned["down"].kind == wet.submitted["down"].kind


def test_dry_renders_in_wave_parents_as_placeholders(mock_run):
    """An in-wave parent has no job id yet, so it renders as <unit> rather than
    being silently dropped from the dependency."""
    r = mock_run(FANIN, {}, "submit", "--dry")
    assert r.ok, r.stderr
    assert set(r.planned) == {"up", "side", "down"}
    assert set(r.planned["down"].afterok) == {"<up>", "<side>"}
    assert r.planned["up"].afterok == []


def test_dry_enforces_only_dependencies(mock_run):
    """A dry run is not a way to bypass the --only precondition."""
    r = mock_run(FANIN, {"side": ("COMPLETED", SIDE)}, "submit", "--dry", "--only", "down*")
    assert not r.ok
    assert "down needs up (absent)" in (r.error or "")


def test_dry_honors_rerun(mock_run):
    """--rerun re-plans a COMPLETED unit and, unscoped, its downstream too."""
    state = {"up": ("COMPLETED", UP), "side": ("COMPLETED", SIDE),
             "down": ("COMPLETED", "900")}
    r = mock_run(FANIN, state, "submit", "--dry", "--rerun", "up*")
    assert r.ok, r.stderr
    assert set(r.planned) == {"up", "down"}     # down is downstream of a rerun
