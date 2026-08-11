"""D2: `history` over the append-only run.jsonl. `status` shows only the latest
record, so a unit that timed out and was then rerun is indistinguishable from one
that always succeeded; this is the view that keeps both attempts.
"""
from specs import SPLIT

MB = 1024 ** 2


def rec(unit, event, state, jid: str | None = "700", t=0, **kw):
    return {"unit": unit, "kind": "array", "job_id": jid, "state": state,
            "event": event, "time": t, **kw}


def blocks(stdout):
    """{unit: [event rows]} — a unit's name starts at column 0, its rows are indented."""
    out, cur = {}, None
    for ln in stdout.splitlines():
        if ln.startswith("  "):
            out[cur].append(ln.strip())
        else:
            cur = ln.strip()
            out[cur] = []
    return out


def test_every_attempt_appears_in_order(mock_run):
    log = [rec("wide:a", "submit", "SUBMITTED", "700"),
           rec("wide:a", "reconcile", "TIMEOUT", "700", elapsed="04:00:02"),
           rec("wide:a", "force", "INVALIDATED", "700"),
           rec("wide:a", "submit", "SUBMITTED", "900"),
           rec("wide:a", "reconcile", "COMPLETED", "900", elapsed="03:51:20")]
    r = mock_run(SPLIT, {}, "history", "wide-a*", extra_log=log)
    assert r.ok, r.stderr
    rows = blocks(r.stdout)["wide:a"]
    assert [x.split()[2] for x in rows] == ["submit", "reconcile", "force",
                                            "submit", "reconcile"]
    assert [x.split()[3] for x in rows] == ["700", "700", "700", "900", "900"]
    # The timeout survives the successful retry, which is the whole point.
    assert "TIMEOUT" in rows[1] and "COMPLETED" in rows[4]


def test_never_submitted_unit_is_visibly_distinct(mock_run):
    """`absent` in `status` conflates 'never ran' with 'no record yet'. R7's
    collectors need never-submitted to be its own answer."""
    r = mock_run(SPLIT, {}, "history", extra_log=[rec("wide:a", "submit", "SUBMITTED")])
    assert blocks(r.stdout)["wide:b"] == ["(no history)"]
    assert blocks(r.stdout)["wide:a"] != ["(no history)"]


def test_glob_restricts_to_matching_units(mock_run):
    log = [rec("wide:a", "submit", "SUBMITTED"), rec("wide:b", "submit", "SUBMITTED")]
    r = mock_run(SPLIT, {}, "history", "wide-b*", extra_log=log)
    assert set(blocks(r.stdout)) == {"wide:b"}


def test_no_glob_covers_every_unit(mock_run):
    r = mock_run(SPLIT, {}, "history")
    assert set(blocks(r.stdout)) == {"wide:a", "wide:b"}


def test_task_histogram_shown_once_recorded(mock_run):
    log = [rec("wide:a", "reconcile", "FAILED", elapsed="00:10:00", max_rss=200 * MB,
               tasks={"0": {"state": "COMPLETED"}, "1": {"state": "FAILED"},
                      "2": {"state": "COMPLETED"}})]
    r = mock_run(SPLIT, {}, "history", "wide-a*", extra_log=log)
    row = blocks(r.stdout)["wide:a"][0]
    assert "2 COMPLETED" in row and "1 FAILED" in row
    assert "00:10:00" in row and "200M" in row


def test_event_is_inferred_for_pre_r5_records(mock_run):
    """Records written before the explicit `event` field distinguished themselves
    only by a `reconcile` flag or by carrying a forced state."""
    log = [{"unit": "wide:a", "job_id": "700", "state": "SUBMITTED", "time": 0},
           {"unit": "wide:a", "job_id": "700", "state": "FAILED", "time": 0,
            "reconcile": True},
           {"unit": "wide:a", "job_id": "700", "state": "INVALIDATED", "time": 0}]
    r = mock_run(SPLIT, {}, "history", "wide-a*", extra_log=log)
    assert r.ok, r.stderr
    rows = blocks(r.stdout)["wide:a"]
    assert [x.split()[2] for x in rows] == ["submit", "reconcile", "force"]


def test_local_run_history_has_no_sacct_columns(mock_run):
    # A local run is synchronous: two submit records, no elapsed/RSS to show.
    log = [rec("wide:a", "submit", "SUBMITTED", jid=None),
           rec("wide:a", "submit", "COMPLETED", jid=None)]
    r = mock_run(SPLIT, {}, "history", "wide-a*", extra_log=log)
    rows = blocks(r.stdout)["wide:a"]
    assert [x.split()[3] for x in rows] == ["-", "-"]
    assert all(x.split()[-1] in ("SUBMITTED", "COMPLETED") for x in rows)
