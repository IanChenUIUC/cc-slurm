"""spec.md §11 --only: a matched node's parents must be already COMPLETED, still
live (running parent -> depended on via afterok), or themselves in the run;
otherwise submit runs nothing and errors. Pins the live-dependency fix
(commit "submit job can depend on live jobs now").
"""
import pytest

from specs import CHAIN, FANIN

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


def test_deps_pulls_in_the_whole_upstream_chain(mock_run):
    """--deps is the upstream counterpart of downstream propagation: what the scope
    needs, transitively, not just its immediate parents."""
    r = mock_run(CHAIN, {}, "submit", "--only", "leaf*", "--deps")
    assert r.ok, r.stderr + (r.error or "")
    assert set(r.submitted) == {"root", "mid", "leaf"}
    # CHAIN's arrays share a grid, so the edges are element-wise.
    assert r.submitted["mid"].aftercorr == [r.submitted["root"].job_id]
    assert r.submitted["leaf"].aftercorr == [r.submitted["mid"].job_id]


def test_deps_stops_at_a_completed_ancestor(mock_run):
    """A COMPLETED parent is already satisfiable, so nothing above it is pulled in --
    and it is not re-run just for being upstream."""
    r = mock_run(CHAIN, {"mid": ("COMPLETED", "800")}, "submit", "--only", "leaf*", "--deps")
    assert r.ok, r.stderr + (r.error or "")
    assert set(r.submitted) == {"leaf"}
    assert r.submitted["leaf"].aftercorr == []    # completed parent never targeted


def test_deps_stops_at_a_live_ancestor(mock_run):
    """A live parent is depended on via afterok rather than resubmitted, which is
    the same rule --only already applies."""
    r = mock_run(CHAIN, {"mid": ("RUNNING", "800")}, "submit", "--only", "leaf*", "--deps")
    assert r.ok, r.stderr + (r.error or "")
    assert set(r.submitted) == {"leaf"}
    assert r.submitted["leaf"].aftercorr == ["800"]


def test_deps_does_not_propagate_downstream(mock_run):
    """Upstream expansion must not quietly become a full run: `down` is absent but
    is not a prerequisite of `up`."""
    r = mock_run(FANIN, {}, "submit", "--only", "up*", "--deps")
    assert r.ok, r.stderr + (r.error or "")
    assert set(r.submitted) == {"up"}


def test_deps_without_only_is_a_noop(mock_run):
    """Unscoped, the scope is already everything, so --deps can change nothing --
    including the downstream propagation that reruns the COMPLETED `mid`."""
    state = {"mid": ("COMPLETED", "800")}
    plain = mock_run(CHAIN, state, "submit")
    with_deps = mock_run(CHAIN, state, "submit", "--deps")
    assert plain.ok and with_deps.ok
    assert set(plain.submitted) == set(with_deps.submitted) == {"root", "mid", "leaf"}


def test_only_parent_in_run_is_satisfied(mock_run):
    # `up` absent -> in scope and (re)run this wave; `down` then depends on up's
    # fresh id from this submission.
    r = mock_run(FANIN, {"side": ("COMPLETED", SIDE)},
                 "submit", "--only", "up*", "--only", "down*")
    assert r.ok, r.stderr
    up_id = r.submitted["up"].job_id
    assert up_id is not None
    assert r.submitted["down"].afterok == [up_id]
