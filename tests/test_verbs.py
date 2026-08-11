"""spec.md §11 read-only / state verbs: `cancel-ids`, `dag` globbing, and the
strict validation of the valueless slurm flags.
"""
from specs import FANIN

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
