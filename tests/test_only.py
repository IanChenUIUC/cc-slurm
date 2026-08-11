"""spec.md §11 --only: a matched node's parents must be already COMPLETED, still
live (running parent -> depended on via afterok), or themselves in the run;
otherwise submit runs nothing and errors. Pins the live-dependency fix
(commit "submit job can depend on live jobs now").
"""
import pytest

from specs import FANIN

# `side` is always COMPLETED, so its edge is dropped; we vary `up`. Explicit ids
# make the afterok assertion readable.
UP, SIDE = "700", "800"


@pytest.mark.parametrize("up_state, ok, afterok", [
    ("RUNNING",   True,  [UP]),     # live -> depended on via afterok (the fix)
    ("PENDING",   True,  [UP]),     # any RUNNINGISH state counts as live
    ("COMPLETED", True,  []),       # completed -> satisfied, edge dropped
    ("FAILED",    False, None),     # terminal non-completed -> unsatisfied, error
])
def test_only_parent_state(mock_run, up_state, ok, afterok):
    state = {"up": (up_state, UP), "side": ("COMPLETED", SIDE)}
    r = mock_run(FANIN, state, "submit", "--only", "down*")
    assert r.ok is ok, r.stderr
    if ok:
        d = r.submitted["down"]
        assert d.afterok == afterok
        assert SIDE not in d.afterok            # completed parent never targeted
    else:
        assert "unsatisfied dependencies" in (r.error or "")
        assert "down needs up" in r.error


def test_only_absent_parent_errors(mock_run):
    # `up` never seeded -> absent (no log record) -> unsatisfied.
    r = mock_run(FANIN, {"side": ("COMPLETED", SIDE)}, "submit", "--only", "down*")
    assert not r.ok
    assert "down needs up (absent)" in r.error


def test_only_both_parents_live(mock_run):
    r = mock_run(FANIN, {"up": ("RUNNING", UP), "side": ("RUNNING", SIDE)},
                 "submit", "--only", "down*")
    assert r.ok, r.stderr
    assert set(r.submitted["down"].afterok) == {UP, SIDE}


def test_only_local_rejects_live_parent(mock_run):
    # Under --local nothing is genuinely running (cc-local is synchronous), so a
    # live parent does NOT count as satisfied.
    r = mock_run(FANIN, {"up": ("RUNNING", UP), "side": ("COMPLETED", SIDE)},
                 "submit", "--only", "down*", "--local")
    assert not r.ok
    assert "unsatisfied dependencies" in (r.error or "")
    assert "down needs up" in r.error


def test_only_parent_in_run_is_satisfied(mock_run):
    # `up` absent -> in scope and (re)run this wave; `down` then depends on up's
    # fresh id from this submission.
    r = mock_run(FANIN, {"side": ("COMPLETED", SIDE)},
                 "submit", "--only", "up*", "--only", "down*")
    assert r.ok, r.stderr
    up_id = r.submitted["up"].job_id
    assert up_id is not None
    assert r.submitted["down"].afterok == [up_id]
