"""spec.md §11 read-only / state verbs: `cancel-ids`, `dag` globbing, and the
strict validation of the valueless slurm flags.
"""
from specs import FANIN, SPLIT

UP, SIDE = "700", "800"


def ids(r):
    return [ln.strip() for ln in r.stdout.splitlines() if ln.strip()]


def test_cancel_ids_omits_terminal_units(mock_run):
    r = mock_run(FANIN, {"up": ("RUNNING", UP), "side": ("COMPLETED", SIDE)},
                 "cancel-ids", "*")
    assert r.ok, r.stderr
    assert ids(r) == [UP]                       # a COMPLETED unit is not cancellable


def test_cancel_ids_honors_glob(mock_run):
    state = {"up": ("RUNNING", UP), "side": ("PENDING", SIDE)}
    assert ids(mock_run(FANIN, state, "cancel-ids", "up*")) == [UP]
    assert ids(mock_run(FANIN, state, "cancel-ids", "side*")) == [SIDE]
    assert sorted(ids(mock_run(FANIN, state, "cancel-ids", "*"))) == sorted([UP, SIDE])


def test_cancel_ids_reaches_units_no_longer_in_the_spec(mock_run):
    """`cancel` is the emergency stop, so it matches the run log, not the spec: a
    live job whose recipe was since renamed or deleted must still be cancellable."""
    r = mock_run(FANIN, {"ghost": ("RUNNING", "999")}, "cancel-ids", "*")
    assert r.ok, r.stderr
    assert ids(r) == ["999"]


def test_dag_glob_restricts_units(mock_run):
    r = mock_run(FANIN, {}, "dag", "up*")
    assert r.ok, r.stderr
    assert "up" in r.stdout
    assert "side" not in r.stdout and "down" not in r.stdout


def test_dag_glob_still_shows_edges_to_units_outside_it(mock_run):
    r = mock_run(FANIN, {}, "dag", "down*")
    assert r.ok, r.stderr
    assert "aftercorr" in r.stdout or "afterok" in r.stdout
    assert "up" in r.stdout                     # the parent is named by the edge


def test_dag_rolls_arrays_up_and_verbose_expands(mock_run):
    """Three levels: recipes, then units, then tasks. On a real spec the per-unit
    lines outnumber the recipes ~2.5:1 and the per-task lines ~50:1, so both are
    opt-in and each level is a strict expansion of the one above."""
    rolled = mock_run(FANIN, {}, "dag")
    assert rolled.ok, rolled.stderr
    assert "[array 6]" in rolled.stdout and "down" in rolled.stdout
    assert "task" not in rolled.stdout
    assert "afterok:" not in rolled.stdout          # edges are inline now
    assert "<- side, up" in rolled.stdout

    units = mock_run(FANIN, {}, "dag", "-v")
    assert "[array 6] down" in units.stdout
    assert "    afterok:   side, up" in units.stdout
    assert "task" not in units.stdout

    tasks = mock_run(FANIN, {}, "dag", "-vv")
    assert "task 0: down-a-0" in tasks.stdout
    assert [ln for ln in tasks.stdout.splitlines()
            if not ln.startswith("    task")] == units.stdout.splitlines()


def test_status_glob_restricts_rows(mock_run):
    r = mock_run(FANIN, {"up": ("COMPLETED", UP), "side": ("FAILED", SIDE)},
                 "status", "up*")
    assert r.ok, r.stderr
    assert "up" in r.stdout
    assert "side" not in r.stdout and "down" not in r.stdout


def test_status_glob_keeps_the_whole_array_row(mock_run):
    """A unit is the display grain: a glob matching one task of an array must not
    hide the rest of it, or the roll-up counts would contradict the row."""
    r = mock_run(SPLIT, {}, "status", "wide-a-0")
    assert r.ok, r.stderr
    assert "wide" in r.stdout


def test_status_local_never_consults_sacct(mock_run):
    """A pipeline run through the synchronous runner has no cluster to ask, and an
    interrupted one leaves a live-looking SUBMITTED record that would reach for it."""
    r = mock_run(FANIN, {"up": ("RUNNING", UP)}, "status", "--local")
    assert r.ok, r.stderr
    # sacct is what turns the seeded SUBMITTED into RUNNING, so the raw log state
    # surviving is the proof it was never queried.
    assert "SUBMITTED" in r.stdout and "RUNNING" not in r.stdout


BAD_BOOL = """
[recipe.solo]
params  = { dataset = ["a"] }
command = "echo ${dataset}"
slurm   = { exclusive = "ture" }
"""


def test_non_boolean_bool_flag_is_a_hard_error(mock_run):
    """A typo in a valueless slurm flag must fail loudly, not silently mean 'off'
    (which would drop `exclusive` and quietly share the node)."""
    r = mock_run(BAD_BOOL, {}, "dag")
    assert not r.ok
    assert "slurm.exclusive" in (r.error or "") and "boolean" in r.error


GOOD_BOOL = """
[recipe.solo]
params  = { dataset = ["a"] }
command = "echo ${dataset}"
slurm   = { exclusive = true }
"""


def test_boolean_true_emits_the_valueless_flag(mock_run):
    r = mock_run(GOOD_BOOL, {}, "submit", "--dry")
    assert r.ok, r.stderr
    assert r.planned["solo-a"].flags.get("-x") is True
