#!/usr/bin/env python3
"""Mock a cc-slurm pipeline's state, run a `pipeline.py` action against it, and
report what the engine decides — without a cluster.

Dual-use:

  * **By hand** (the "is this fixed?" checker), pointed at the real spec::

        python3 tests/mockpipe.py \\
            --spec /models/ianchen3/community_search/slurm/pipeline.toml \\
            --state genquery=RUNNING --state ib-core-decomp=COMPLETED \\
            --run "submit --only testing-csk*"

  * **In tests**, via the same library::

        from mockpipe import mock_run
        r = mock_run(SPEC, {"genquery": "RUNNING"}, "submit", "--only", "testing-csk*")
        assert r.submitted["testing-csk"].afterok == ["90001"]

Everything runs in an isolated workdir (a fresh temp dir by default); the user's
real `.pipeline/` is never touched.

**Reconcile-aware state translation.** `reconcile` only trusts `sacct` for live
jobs, so a *live* state can't just be dropped into the log. `mock_run` seeds a
live unit (`RUNNING`/`PENDING`/...) as `SUBMITTED` **with an id** *and* registers
that id in the fake `sacct` under the requested state, so `reconcile` resolves it
exactly as the cluster would. Terminal states (`COMPLETED`/`FAILED`/...) seed
directly — `reconcile` skips them.
"""
import argparse
import dataclasses
import json
import os
import pathlib
import shlex
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parent
PIPELINE = REPO / "pipeline.py"
FAKE_CC = HERE / "fakes" / "cc-submit"
FAKE_SACCT = HERE / "fakes" / "sacct"

# States that only `sacct` can confirm (mirror pipeline.py's RUNNINGISH). A unit
# asked for one of these is seeded SUBMITTED+id and surfaced through fake sacct.
RUNNINGISH = {"RUNNING", "PENDING", "REQUEUED", "SUSPENDED",
              "COMPLETING", "CONFIGURING", "RESIZING"}
SEED_ID_BASE = 90001


@dataclasses.dataclass
class Submit:
    unit: str
    kind: str                       # "sbatch" | "array"
    afterok: list                   # ids from `-d`
    aftercorr: list                 # ids from `-C`
    flags: dict                     # {"-c": "1", "-m": "128GB", ...}
    job_id: str | None = None       # id the fake runner returned for this unit
    array_indices: str | None = None  # -A: the task subset, None when the whole array

    @property
    def indices(self):
        """`array_indices` expanded to the sorted int list it names, or None."""
        if self.array_indices is None:
            return None
        out = []
        for part in self.array_indices.split(","):
            lo, _, hi = part.partition("-")
            out.extend(range(int(lo), int(hi) + 1) if hi else [int(lo)])
        return sorted(out)


@dataclasses.dataclass
class Result:
    returncode: int
    stdout: str
    stderr: str
    submitted: dict                 # unit -> Submit (parsed from the fake cc-submit capture)
    planned: dict                   # unit -> Submit (--dry only: what it would have run)
    skipped: list                   # (unit, state)
    log: list                       # parsed run.jsonl records
    error: str | None               # the `pipeline: error: ...` message, if any
    workdir: pathlib.Path           # isolated run dir (inspect .pipeline/scripts, etc.)

    @property
    def ok(self):
        return self.returncode == 0

    @property
    def latest(self):
        """unit -> its last log record, the same fold the engine's state read does."""
        return {r["unit"]: r for r in self.log}

    def tasks(self, unit):
        """{index: state} the engine folded out of sacct for one unit's array tasks,
        or {} if the unit has no per-task detail."""
        rec = self.latest.get(unit) or {}
        return {i: t["state"] for i, t in (rec.get("tasks") or {}).items()}


def _norm_state(entry, idx):
    """(state, job_id) for a `state` value that is either a bare STATE or
    (STATE, id). Live units get a deterministic id if none supplied."""
    if isinstance(entry, (tuple, list)):
        state, jid = entry[0], str(entry[1])
    else:
        state, jid = entry, str(SEED_ID_BASE + idx)
    return state.upper(), jid


def _seed(state):
    """Translate {unit: SEED} into (log_records, sacct_states), where SEED is a bare
    STATE, a (STATE, id) pair, or — for an array whose tasks differ — a dict

        {"tasks": {0: "COMPLETED", 17: "FAILED", ...}, "id": "90500"}

    A task dict is always seeded *live* (SUBMITTED + id, registered with the fake
    sacct), because the whole point is to let the engine's own fold decide the
    unit-level state from the per-task rows rather than asserting it here."""
    records, sacct = [], {}
    for idx, unit in enumerate(sorted(state)):
        entry = state[unit]
        if isinstance(entry, dict):
            jid = str(entry.get("id", SEED_ID_BASE + idx))
            records.append({"unit": unit, "kind": "array", "job_id": jid,
                            "state": "SUBMITTED"})
            sacct[jid] = {k: v for k, v in entry.items() if k != "id"}
            sacct[jid]["tasks"] = {str(k): v for k, v in entry["tasks"].items()}
            continue
        st, jid = _norm_state(entry, idx)
        if st in RUNNINGISH:
            records.append({"unit": unit, "kind": "array", "job_id": jid,
                            "state": "SUBMITTED"})
            sacct[jid] = {"state": st}
        elif st == "SUBMITTED":
            records.append({"unit": unit, "kind": "array", "job_id": jid,
                            "state": "SUBMITTED"})
        else:                                   # terminal: reconcile trusts the log
            records.append({"unit": unit, "kind": "array", "job_id": jid,
                            "state": st})
    return records, sacct


def _parse_argv(argv):
    """A runner argv (from `kind` onward) -> Submit, or None if unnamed."""
    kind = argv[0]                              # "sbatch" | "array"
    name, afterok, aftercorr, flags = None, [], [], {}
    indices = None
    i = 1
    while i < len(argv):
        tok = argv[i]
        if tok == "-j":
            name = argv[i + 1]; i += 2
        elif tok == "-d":
            afterok.append(argv[i + 1]); i += 2
        elif tok == "-C":
            aftercorr.append(argv[i + 1]); i += 2
        elif tok == "-A":
            indices = argv[i + 1]; i += 2
        elif tok in ("-c", "-m", "-p", "-t"):
            flags[tok] = argv[i + 1]; i += 2
        elif tok in ("-x",):
            flags[tok] = True; i += 1
        else:
            i += 1
    return (Submit(name, kind, afterok, aftercorr, flags, array_indices=indices)
            if name is not None else None)


def _parse_capture(capture):
    """Fake cc-submit argv lines -> {unit: Submit}."""
    out = {}
    if not capture.exists():
        return out
    for line in capture.read_text().splitlines():
        if line.strip():
            s = _parse_argv(json.loads(line))
            if s:
                out[s.unit] = s
    return out


def _parse_planned(stdout):
    """`--dry` prints the runner argv it would have invoked; parse those into the
    same {unit: Submit} shape as a real submit, so the two can be compared."""
    out = {}
    for line in stdout.splitlines():
        argv = shlex.split(line)
        kinds = [i for i, t in enumerate(argv) if t in ("sbatch", "array")]
        if not kinds:
            continue
        s = _parse_argv(argv[kinds[0]:])
        if s:
            out[s.unit] = s
    return out


def _parse_skips(stdout):
    skips = []
    for line in stdout.splitlines():
        parts = line.split(None, 1)
        if parts and parts[0] == "skip" and len(parts) > 1:
            name, _, rest = parts[1].partition("\t")
            skips.append((name.strip(), rest.strip().strip("()")))
    return skips


def _parse_jobids(stdout):
    """`submit <jid>\\t<name>` / `done <jid>\\t<name>` lines -> {name: jid}."""
    ids = {}
    for line in stdout.splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[0] in ("submit", "done"):
            ids[parts[2]] = parts[1]
    return ids


def mock_run(spec, state, action, *args, idmap=None, workdir=None, extra_log=()):
    """Run `pipeline.py <action> <args>` against a mocked state. `spec` is TOML
    text; `state` is {unit: SEED} (see `_seed`). `extra_log` records are appended
    after the seeds, for cases that need a specific *prior* log — an earlier attempt
    to show in `history`, or an already-observed state to test dedup against.
    Returns a Result."""
    wd = pathlib.Path(workdir) if workdir else pathlib.Path(tempfile.mkdtemp(prefix="mockpipe-"))
    (wd / ".pipeline").mkdir(parents=True, exist_ok=True)
    (wd / "spec.toml").write_text(spec)

    records, sacct = _seed(state)
    (wd / ".pipeline" / "run.jsonl").write_text(
        "".join(json.dumps(r) + "\n" for r in [*records, *extra_log]))

    capture = wd / ".pipeline" / "cc_capture.jsonl"
    sacct_path = wd / ".pipeline" / "sacct_states.json"
    sacct_path.write_text(json.dumps(sacct))

    env = dict(os.environ,
               CC_CAPTURE=str(capture),
               SACCT_STATES=str(sacct_path))
    if idmap:
        idmap_path = wd / ".pipeline" / "cc_idmap.json"
        idmap_path.write_text(json.dumps(idmap))
        env["CC_IDMAP"] = str(idmap_path)

    cmd = [sys.executable, str(PIPELINE), action, "spec.toml", *args,
           "--cc-submit", f"{sys.executable} {FAKE_CC}",
           "--sacct", f"{sys.executable} {FAKE_SACCT}"]
    proc = subprocess.run(cmd, cwd=wd, capture_output=True, text=True, env=env)

    log_path = wd / ".pipeline" / "run.jsonl"
    log = [json.loads(ln) for ln in log_path.read_text().splitlines() if ln.strip()]
    error = None
    for ln in proc.stderr.splitlines():
        if ln.startswith("pipeline: error:"):
            error = ln[len("pipeline: error:"):].strip()

    submitted = _parse_capture(capture)
    jobids = _parse_jobids(proc.stdout)
    for name, jid in jobids.items():
        if name in submitted:
            submitted[name].job_id = jid
    return Result(proc.returncode, proc.stdout, proc.stderr, submitted,
                  _parse_planned(proc.stdout), _parse_skips(proc.stdout), log, error, wd)


# ---- CLI ----------------------------------------------------------------

def _fmt(r):
    lines = []
    if r.error:
        lines.append(f"error: {r.error}")
    for unit, s in r.submitted.items():
        deps = []
        if s.afterok:
            deps.append("afterok:" + ",".join(s.afterok))
        if s.aftercorr:
            deps.append("aftercorr:" + ",".join(s.aftercorr))
        dep = ("  deps: " + " ".join(deps)) if deps else "  (no deps)"
        lines.append(f"submit  {unit}{dep}")
    for unit, st in r.skipped:
        lines.append(f"skip    {unit}  ({st})")
    if not lines:
        lines.append("(nothing submitted, skipped, or errored)")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--spec", required=True, help="path to a pipeline .toml")
    ap.add_argument("--state", action="append", default=[], metavar="UNIT=STATE",
                    help="seed a unit's state, e.g. genquery=RUNNING (repeatable). "
                         "Optional id: genquery=RUNNING:99999")
    ap.add_argument("--run", required=True, metavar="'ACTION ARGS'",
                    help="the pipeline.py invocation, e.g. \"submit --only testing-csk*\"")
    ap.add_argument("--workdir", help="isolated workdir (default: fresh temp dir)")
    ap.add_argument("--raw", action="store_true", help="also print engine stdout/stderr")
    args = ap.parse_args()

    state = {}
    for item in args.state:
        unit, _, val = item.partition("=")
        st, sep, jid = val.partition(":")
        state[unit] = (st, jid) if sep else st

    action, *rest = shlex.split(args.run)
    r = mock_run(pathlib.Path(args.spec).read_text(), state, action, *rest,
                 workdir=args.workdir)
    print(_fmt(r))
    if args.raw:
        print("\n--- stdout ---\n" + r.stdout, file=sys.stderr)
        print("--- stderr ---\n" + r.stderr, file=sys.stderr)
    sys.exit(0 if r.ok else 1)


if __name__ == "__main__":
    main()
