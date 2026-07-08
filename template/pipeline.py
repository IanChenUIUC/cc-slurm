#!/usr/bin/env python3
"""Pipeline engine: resolve a TOML spec (see SPEC.md) into a SLURM job DAG and
emit cc-submit calls.

    pipeline.py dag    spec.toml     # print resolved nodes + edges + dep types
    pipeline.py dry    spec.toml     # print exact cc-submit commands (placeholder ids)
    pipeline.py submit spec.toml     # materialize scripts, submit, log job ids

The resolution pipeline (SPEC.md Sec.9): expand params -> nodes; wire deps by
capture matching; toposort; resolve aliases (topo order); render command + slurm;
group into submission units (individual jobs / arrays); translate dependencies
(afterok / aftercorr) and submit.
"""
import argparse
import fnmatch
import itertools
import json
import pathlib
import re
import shlex
import subprocess
import sys
import time
import tomllib
from collections import defaultdict

RESERVED = {"params", "deps", "command", "array", "slurm"}
SLURM_FLAGS = {"cpus": "-c", "mem": "-m", "partition": "-p", "time": "-t"}
# Job states. Only COMPLETED is success; everything else terminal is a failure
# (and thus resubmit-eligible). NON_TERMINAL states are still live: skip them.
RUNNINGISH = {"RUNNING", "PENDING", "REQUEUED", "SUSPENDED",
              "COMPLETING", "CONFIGURING", "RESIZING"}
NON_TERMINAL = RUNNINGISH | {"SUBMITTED", "UNKNOWN"}
VAR = re.compile(r"\$\{([^}]+)\}")
CAP = re.compile(r"^(\w+)\s*\((.*)\)\s*$")


class PipelineError(Exception):
    pass


class NotReady(Exception):
    """Raised mid-substitution when a sibling alias isn't resolved yet."""


def scalar_items(binding):
    return [(k, v) for k, v in binding.items() if not isinstance(v, list)]


class Node:
    def __init__(self, recipe, binding, slurm_override, rdef):
        self.recipe = recipe
        self.binding = binding
        self.slurm_override = slurm_override
        self.rdef = rdef
        vals = [str(v) for _, v in scalar_items(binding)]
        self.ident = "-".join([recipe] + vals)
        self.parents = []          # list[Node]
        self.aliases = {}          # resolved
        self.alias_defs = {}       # name -> template
        self.slurm = {}            # resolved flags
        self.command = None
        self.array_index = None
        self.job_id = None         # assigned at submit


class Engine:
    def __init__(self, spec):
        self.spec = spec
        self.defaults = spec.get("defaults", {})
        self.default_slurm = self.defaults.get("slurm", {})
        self.default_aliases = {k: v for k, v in self.defaults.items() if k != "slurm"}
        self.recipes = spec.get("recipe", {})
        self.nodes = []
        self.by_recipe = {}
        self._build()

    # ---- Sec.9 step 2-3: expand params into identified nodes ----
    def _build(self):
        for name, rdef in self.recipes.items():
            raw = rdef.get("params", None)
            records = self._records(raw)
            recipe_aliases = {k: v for k, v in rdef.items() if k not in RESERVED}
            for rec in records:
                rec = dict(rec)
                slurm_override = rec.pop("slurm", {})
                node = Node(name, rec, slurm_override, rdef)
                node.alias_defs = {**self.default_aliases, **recipe_aliases}
                self.nodes.append(node)
                self.by_recipe.setdefault(name, []).append(node)

        seen = {}
        for n in self.nodes:
            if n.ident in seen:
                raise PipelineError(f"duplicate node identity: {n.ident}")
            seen[n.ident] = n

        # index arrays deterministically
        for name in self.recipes:
            if self._is_array(name):
                for i, n in enumerate(sorted(self.by_recipe.get(name, []), key=self._sortkey)):
                    n.array_index = i

        self._wire()                 # step 4
        self.order = self._toposort()  # step 5
        self._resolve_aliases()      # step 6
        self._resolve_slurm_command()  # step 7
        self._build_units()          # grouping for submission
        self._check_arrays()         # Sec.9 eligibility

    @staticmethod
    def _sortkey(n):
        return tuple(str(v) for _, v in scalar_items(n.binding))

    def _records(self, raw):
        if raw is None:
            return [{}]
        if isinstance(raw, dict):        # product sugar
            keys = list(raw)
            return [dict(zip(keys, combo))
                    for combo in itertools.product(*(raw[k] for k in keys))]
        if isinstance(raw, list):        # explicit record list
            return raw
        raise PipelineError("params must be a table (product) or a list of records")

    def _is_array(self, recipe):
        return bool(self.recipes[recipe].get("array", False))

    # ---- Sec.9 step 4: wire edges via capture matching ----
    def _wire(self):
        for n in self.nodes:
            for cap in self._expand_deps(n):
                for p in self._match(cap):
                    if p not in n.parents:
                        n.parents.append(p)

    def _expand_deps(self, node):
        out = []
        for entry in node.rdef.get("deps", []):
            s = entry.strip()
            m = re.fullmatch(r"\$\{(\w+)\}", s)
            if m and isinstance(node.binding.get(m.group(1)), list):
                out.extend(node.binding[m.group(1)])          # splice list of captures
            else:
                out.append(self._subst_binding(entry, node))
        return out

    def _subst_binding(self, tmpl, node):
        def repl(m):
            name = m.group(1).strip()
            if name in node.binding and not isinstance(node.binding[name], list):
                return str(node.binding[name])
            raise PipelineError(f"{node.ident}: deps may only use scalar binding "
                                f"vars; bad reference ${{{name}}}")
        return VAR.sub(repl, tmpl)

    def _match(self, cap):
        recipe, constraints = self._parse_capture(cap)
        if recipe not in self.by_recipe:
            raise PipelineError(f"capture references unknown recipe: {recipe!r}")
        out = []
        for n in self.by_recipe[recipe]:
            nkeys = [k for k, _ in scalar_items(n.binding)]
            if any(k not in constraints for k in nkeys):    # rule 2: mention every key
                continue
            ok = True
            for k, v in constraints.items():                # rule 1: constraints hold
                if v == "*":
                    continue
                if str(n.binding.get(k)) != v:
                    ok = False
                    break
            if ok:
                out.append(n)
        if not out:
            raise PipelineError(f"capture matched zero nodes: {cap!r}")
        return out

    @staticmethod
    def _parse_capture(cap):
        cap = cap.strip()
        m = CAP.match(cap)
        if not m:
            if re.fullmatch(r"\w+", cap):
                return cap, {}
            raise PipelineError(f"malformed capture: {cap!r}")
        recipe, body = m.group(1), m.group(2).strip()
        constraints = {}
        if body:
            for part in body.split(","):
                k, _, v = part.partition("=")
                constraints[k.strip()] = v.strip()
        return recipe, constraints

    # ---- Sec.9 step 5: toposort (also detects cycles) ----
    def _toposort(self):
        order, seen, stack = [], set(), set()

        def visit(n):
            if n in seen:
                return
            if n in stack:
                raise PipelineError(f"dependency cycle through {n.ident}")
            stack.add(n)
            for p in n.parents:
                visit(p)
            stack.discard(n)
            seen.add(n)
            order.append(n)

        for n in self.nodes:
            visit(n)
        return order

    # ---- Sec.9 step 6: resolve aliases in topological order ----
    def _resolve_aliases(self):
        for n in self.order:
            pending = dict(n.alias_defs)
            while pending:
                progressed = False
                for name, tmpl in list(pending.items()):
                    try:
                        n.aliases[name] = self._subst(tmpl, n, n.aliases, allow_pending=True)
                    except NotReady:
                        continue
                    del pending[name]
                    progressed = True
                if not progressed:
                    raise PipelineError(
                        f"{n.ident}: alias cycle among {sorted(pending)}")

    # ---- Sec.9 step 7: resolve slurm flags and command ----
    def _resolve_slurm_command(self):
        for n in self.order:
            merged = {**self.default_slurm, **n.rdef.get("slurm", {}), **n.slurm_override}
            for k in merged:
                if k not in SLURM_FLAGS:
                    raise PipelineError(f"{n.ident}: unknown slurm key {k!r} "
                                        f"(allowed: {sorted(SLURM_FLAGS)})")
            n.slurm = {k: self._subst(str(v), n, n.aliases) for k, v in merged.items()}
            if "command" in n.rdef:
                n.command = self._subst(n.rdef["command"], n, n.aliases)

    # ---- the one interpolation routine (SPEC.md Sec.4) ----
    def _subst(self, tmpl, node, aliases, allow_pending=False):
        def repl(m):
            content = m.group(1).strip()
            if "." in content:
                ref, alias = (x.strip() for x in content.split(".", 1))
                return self._parent_alias(node, ref, alias)
            if content == "node":
                return node.ident
            if content in node.binding:
                v = node.binding[content]
                return " ".join(map(str, v)) if isinstance(v, list) else str(v)
            if content in aliases:
                return aliases[content]
            if allow_pending and content in node.alias_defs:
                raise NotReady(content)
            raise PipelineError(f"{node.ident}: undefined variable ${{{content}}}")
        return VAR.sub(repl, tmpl)

    def _parent_alias(self, node, ref, alias):
        # ref is a parent recipe name, or a binding var holding capture strings
        if ref in {p.recipe for p in node.parents}:
            targets = [p for p in node.parents if p.recipe == ref]
        elif ref in node.binding and isinstance(node.binding[ref], list):
            targets = [p for cap in node.binding[ref] for p in self._match(cap)]
        else:
            raise PipelineError(f"{node.ident}: ${{{ref}.{alias}}} refers to "
                                f"{ref!r}, which is not a dependency")
        vals = []
        for p in targets:
            if alias not in p.aliases:
                raise PipelineError(f"{node.ident}: parent {p.ident} has no alias {alias!r}")
            vals.append(p.aliases[alias])
        return " ".join(vals)

    # ---- submission units: individual jobs, or one array per array-recipe ----
    def _build_units(self):
        self.units = []
        self.node_unit = {}
        self.array_unit = {}
        for name, rnodes in self.by_recipe.items():
            if self._is_array(name):
                u = Unit("array", name, rnodes)
                self.array_unit[name] = u
                self.units.append(u)
                for n in rnodes:
                    self.node_unit[n] = u
            else:
                for n in rnodes:
                    u = Unit("individual", n.ident, [n])
                    self.units.append(u)
                    self.node_unit[n] = u
        # unit-level topo order
        uparents = {u: set() for u in self.units}
        for u in self.units:
            for n in u.nodes:
                for p in n.parents:
                    pu = self.node_unit[p]
                    if pu is not u:
                        uparents[u].add(pu)
        self.uparents = uparents
        seen, order, stack = set(), [], set()

        def visit(u):
            if u in seen:
                return
            if u in stack:
                raise PipelineError(f"unit cycle through {u.name}")
            stack.add(u)
            for pu in uparents[u]:
                visit(pu)
            stack.discard(u)
            seen.add(u)
            order.append(u)

        for u in self.units:
            visit(u)
        self.unit_order = order

    # ---- Sec.9 array eligibility ----
    def _check_arrays(self):
        for name, u in self.array_unit.items():
            res = {tuple(sorted(n.slurm.items())) for n in u.nodes}
            if len(res) > 1:
                raise PipelineError(
                    f"array recipe {name!r} ineligible: non-uniform resources across cells")
            sigs = set()
            for n in u.nodes:
                sig = {}
                for R in {p.recipe for p in n.parents}:
                    if self._is_array(R):
                        sig[R] = ("array",)
                    else:
                        sig[R] = frozenset(p.ident for p in n.parents if p.recipe == R)
                sigs.add(frozenset(sig.items()))
            if len(sigs) > 1:
                raise PipelineError(
                    f"array recipe {name!r} ineligible: non-uniform dependency "
                    f"structure (nodes have distinct individual parents)")

    # ---- dependency translation for a unit ----
    def _aligned(self, child_u, parent_u):
        a = sorted(child_u.nodes, key=self._sortkey)
        b = sorted(parent_u.nodes, key=self._sortkey)
        if len(a) != len(b):
            return False
        ck = {k for k, _ in scalar_items(a[0].binding)}
        pk = {k for k, _ in scalar_items(b[0].binding)}
        shared = sorted(ck & pk)
        for x, y in zip(a, b):
            if tuple(str(x.binding.get(k)) for k in shared) != \
               tuple(str(y.binding.get(k)) for k in shared):
                return False
        return True

    def _dep_tokens(self, u, uid):
        """Return (afterok_tokens, aftercorr_tokens). `uid` maps a unit to its id."""
        afterok, aftercorr = [], []
        if u.kind == "individual":
            n = u.nodes[0]
            for p in n.parents:
                pu = self.node_unit[p]
                if pu.kind == "individual":
                    afterok.append(uid(pu))
                else:
                    afterok.append(f"{uid(pu)}_{p.array_index}")   # specific element
        else:
            parent_recipes = sorted({p.recipe for n in u.nodes for p in n.parents})
            for R in parent_recipes:
                if self._is_array(R):
                    pu = self.array_unit[R]
                    (aftercorr if self._aligned(u, pu) else afterok).append(uid(pu))
                else:
                    pids = sorted({uid(self.node_unit[p])
                                   for n in u.nodes for p in n.parents if p.recipe == R})
                    afterok.extend(pids)
        return afterok, aftercorr

    def _cmd(self, u, uid, script):
        afterok, aftercorr = self._dep_tokens(u, uid)
        deps = " ".join(f"-d {t}" for t in afterok)
        deps += ("" if not aftercorr else " " + " ".join(f"-C {t}" for t in aftercorr))
        flags = " ".join(f"{SLURM_FLAGS[k]} {shlex.quote(str(n.slurm[k]))}"
                         for k in ["cpus", "mem", "partition", "time"]
                         for n in [u.nodes[0]] if k in n.slurm)
        deps = deps.strip()
        if u.kind == "individual":
            return f"cc-submit sbatch {script} -j {u.name} {flags} {deps}".rstrip()
        return f"cc-submit array {script} -j {u.name} {flags} {deps}".rstrip()

    # ---- subcommands ----
    def dag(self):
        for u in self.unit_order:
            head = (f"[array {len(u.nodes)}]" if u.kind == "array" else "[job]")
            print(f"{head} {u.name}")
            if u.kind == "array":
                for n in sorted(u.nodes, key=self._sortkey):
                    print(f"    task {n.array_index}: {n.ident}")
            afterok, aftercorr = self._dep_tokens(u, lambda x: x.name)
            if afterok:
                print(f"    afterok:   {', '.join(afterok)}")
            if aftercorr:
                print(f"    aftercorr: {', '.join(aftercorr)}")

    def dry(self, workdir=".pipeline"):
        wd = pathlib.Path(workdir)
        for u in self.unit_order:
            script = self._materialize(u, wd)          # write the real script/cmds file
            print(self._cmd(u, lambda x: f"<{x.name}>", str(script)))
        print(f"# scripts written to {wd / 'scripts'}/", file=sys.stderr)

    # ---- materialize + invoke cc-submit for one unit ----
    def _materialize(self, u, wd):
        sdir = wd / "scripts"
        sdir.mkdir(parents=True, exist_ok=True)
        if u.kind == "individual":
            p = sdir / f"{u.name}.sh"
            p.write_text("#!/bin/bash\nset -euo pipefail\n" + (u.nodes[0].command or "") + "\n")
        else:
            p = sdir / f"{u.name}.cmds"
            p.write_text("".join((n.command or "") + "\n"
                                 for n in sorted(u.nodes, key=self._sortkey)))
        return p

    def _invoke_cc(self, cc, u, wd):
        cmd = self._cmd(u, lambda x: x.job_id, str(self._materialize(u, wd)))
        parts = shlex.split(cc) + cmd.split()[1:]      # replace leading 'cc-submit'
        return subprocess.run(parts, capture_output=True, text=True)

    @staticmethod
    def _read_log(log_path):
        last = {}
        if log_path.exists():
            for ln in log_path.read_text().splitlines():
                if ln.strip():
                    r = json.loads(ln)
                    last[r["unit"]] = r
        return last

    # ---- sacct reconciliation ----
    @staticmethod
    def _norm_state(s):
        return s.split()[0].rstrip("+").upper() if s and s.strip() else "UNKNOWN"

    @staticmethod
    def _parse_rss(s):
        s = (s or "").strip()
        if not s or s == "0":
            return 0
        mult = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
        return int(float(s[:-1]) * mult[s[-1]]) if s[-1] in mult else int(float(s))

    def _run_sacct(self, sacct, ids):
        cmd = shlex.split(sacct) + ["-j", ",".join(ids),
                                    "--format=JobID,State,ExitCode,Elapsed,MaxRSS",
                                    "--parsable2", "--noheader"]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise PipelineError(f"sacct failed: {proc.stderr}")
        return [ln.split("|") for ln in proc.stdout.splitlines() if ln.strip()]

    def _parse_sacct(self, rows):
        """Fold sacct rows (main + .batch/.extern, array tasks) per base job id."""
        groups = defaultdict(list)
        for r in rows:
            if len(r) < 5:
                continue
            jid, state, _exit, elapsed, maxrss = r[:5]
            stepless = jid.split(".")[0]                # strip .batch/.extern
            base = stepless.split("_")[0]              # array base id
            groups[base].append((("." in jid), self._norm_state(state),
                                 elapsed, self._parse_rss(maxrss)))
        out = {}
        for base, entries in groups.items():
            mains = [(st, el) for is_step, st, el, _ in entries if not is_step]
            states = [st for st, _ in mains]
            if not states:
                ust = "UNKNOWN"
            elif all(st == "COMPLETED" for st in states):
                ust = "COMPLETED"
            elif any(st in RUNNINGISH for st in states):
                ust = "RUNNING"
            else:
                ust = next(st for st in states if st != "COMPLETED")   # a failure
            rss = max((r[3] for r in entries), default=0)
            elapsed = max((el for _, el in mains), default="-")
            out[base] = {"state": ust, "max_rss": rss, "elapsed": elapsed}
        return out

    def reconcile(self, sacct, log_path):
        """Query sacct for every non-terminal job in the log, append the observed
        states, and return {unit_name: latest_record}."""
        last = self._read_log(log_path)
        query = {n: rec for n, rec in last.items()
                 if rec.get("state") in NON_TERMINAL and rec.get("job_id")}
        updates = {}
        if query:
            ids = sorted({str(rec["job_id"]) for rec in query.values()})
            parsed = self._parse_sacct(self._run_sacct(sacct, ids))
            new = []
            for name, rec in query.items():
                info = parsed.get(str(rec["job_id"]))
                if not info:                          # not in sacct yet: leave live
                    continue
                nr = {"unit": name, "kind": rec.get("kind"), "job_id": rec["job_id"],
                      "state": info["state"], "max_rss": info["max_rss"],
                      "elapsed": info["elapsed"], "time": time.time(), "reconcile": True}
                new.append(nr)
                updates[name] = nr
            if new:
                with open(log_path, "a") as f:
                    for nr in new:
                        f.write(json.dumps(nr) + "\n")
        merged = dict(last)
        merged.update(updates)
        return merged

    @staticmethod
    def _fmt_rss(b):
        if not b:
            return "-"
        for unit in ("B", "K", "M", "G", "T"):
            if b < 1024 or unit == "T":
                return f"{b:.0f}{unit}"
            b /= 1024

    def status(self, sacct, workdir=".pipeline"):
        state = self.reconcile(sacct, pathlib.Path(workdir) / "run.jsonl")
        print(f"{'unit':40} {'state':12} {'elapsed':10} maxrss")
        for u in self.unit_order:
            rec = state.get(u.name)
            if rec:
                print(f"{u.name:40} {rec.get('state','?'):12} "
                      f"{str(rec.get('elapsed','-')):10} {self._fmt_rss(rec.get('max_rss'))}")
            else:
                print(f"{u.name:40} {'absent':12}")

    def cancel_ids(self, workdir=".pipeline"):
        """Print the job ids of every still-live (non-terminal) unit in the log,
        one per line, for piping to scancel."""
        log_path = pathlib.Path(workdir) / "run.jsonl"
        last = {}
        if log_path.exists():
            for ln in log_path.read_text().splitlines():
                if ln.strip():
                    r = json.loads(ln)
                    last[r["unit"]] = r
        for rec in last.values():
            if rec.get("state") in NON_TERMINAL and rec.get("job_id"):
                print(rec["job_id"])

    def invalidate(self, globs, workdir=".pipeline"):
        """Append INVALIDATED records for matching nodes so the next `submit`
        reruns them (and their downstream). Persistent across sessions; cleared
        naturally once a node re-runs to COMPLETED. INVALIDATED is deliberately
        outside NON_TERMINAL, so reconcile won't query sacct for it and submit
        won't treat it as live."""
        wd = pathlib.Path(workdir)
        wd.mkdir(parents=True, exist_ok=True)
        log_path = wd / "run.jsonl"
        last = {}
        if log_path.exists():
            for ln in log_path.read_text().splitlines():
                if ln.strip():
                    r = json.loads(ln)
                    last[r["unit"]] = r
        matched = [u for u in self.units
                   if any(fnmatch.fnmatch(n.ident, g) for g in globs for n in u.nodes)]
        if not matched:
            print("invalidate: no nodes matched", file=sys.stderr)
            return
        with open(log_path, "a") as f:
            for u in matched:
                f.write(json.dumps({"unit": u.name, "kind": u.kind,
                                    "job_id": (last.get(u.name) or {}).get("job_id"),
                                    "state": "INVALIDATED",
                                    "nodes": [n.ident for n in u.nodes],
                                    "time": time.time()}) + "\n")
                print(f"invalidated {u.name}")

    # ---- submit: reconcile, then run only failed/absent (+ --rerun, downstream) ----
    def submit(self, cc, sacct="sacct", workdir=".pipeline", rerun=(), only=(), local=False):
        wd = pathlib.Path(workdir)
        wd.mkdir(parents=True, exist_ok=True)
        log_path = wd / "run.jsonl"
        # Local runs are synchronous: the runner's exit is authoritative, so read
        # state straight from the log and never consult sacct. A job that isn't
        # COMPLETED (incl. a stale SUBMITTED from an interrupted local run) reruns.
        state = self._read_log(log_path) if local else self.reconcile(sacct, log_path)

        def needs_run(st):
            return st != "COMPLETED" if local else (st not in NON_TERMINAL and st != "COMPLETED")

        scoped = bool(only) and set(only) != {"*"}
        if scoped:
            scope = {u for u in self.units
                     if any(fnmatch.fnmatch(n.ident, g) for g in only for n in u.nodes)}
            if not scope:
                raise PipelineError(f"--only matched no nodes: {list(only)}")
        else:
            scope = set(self.units)

        forced = {u for u in scope
                  if any(fnmatch.fnmatch(n.ident, g) for g in rerun for n in u.nodes)}
        torun = {u for u in scope
                 if u in forced or needs_run((state.get(u.name) or {}).get("state"))}

        if not scoped:
            children = defaultdict(set)               # downstream of a rerun is stale
            for u in self.units:
                for pu in self.uparents[u]:
                    children[pu].add(u)
            frontier = list(torun)
            while frontier:
                u = frontier.pop()
                for c in children[u]:
                    cst = (state.get(c.name) or {}).get("state")
                    # cluster: don't disturb live jobs; local: nothing is live
                    if c not in torun and (local or cst not in NON_TERMINAL):
                        torun.add(c)
                        frontier.append(c)
        else:
            unmet = set()                             # --only: upstream must be ready
            for u in torun:
                for pu in self.uparents[u]:
                    if (state.get(pu.name) or {}).get("state") != "COMPLETED" and pu not in torun:
                        unmet.add(f"{u.name} needs {pu.name} "
                                  f"({(state.get(pu.name) or {}).get('state', 'absent')})")
            if unmet:
                raise PipelineError("--only: unsatisfied dependencies (run them first): "
                                    + "; ".join(sorted(unmet)))

        for u in self.units:                          # skipped units keep their logged id
            if u not in torun:
                u.job_id = (state.get(u.name) or {}).get("job_id")

        def record(u, jid, st):
            return json.dumps({"unit": u.name, "kind": u.kind, "job_id": jid, "state": st,
                               "nodes": [n.ident for n in u.nodes], "time": time.time()}) + "\n"

        with open(log_path, "a") as log:
            for u in self.unit_order:
                if u in torun:
                    proc = self._invoke_cc(cc, u, wd)
                    jid = proc.stdout.strip().split()[-1] if proc.stdout.strip() else None
                    if proc.returncode != 0:
                        if local:                     # record the failure before aborting
                            log.write(record(u, jid, "FAILED"))
                        raise PipelineError(f"submit failed for {u.name}:\n{proc.stderr}")
                    u.job_id = jid
                    log.write(record(u, jid, "COMPLETED" if local else "SUBMITTED"))
                    print(f"{'done  ' if local else 'submit'} {jid}\t{u.name}")
                elif u in scope:
                    print(f"skip   {u.name}\t({(state.get(u.name) or {}).get('state','absent')})")


class Unit:
    def __init__(self, kind, name, nodes):
        self.kind = kind
        self.name = name
        self.nodes = nodes
        self.job_id = None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["dag", "dry", "submit", "status", "invalidate", "cancel-ids"])
    ap.add_argument("spec")
    ap.add_argument("globs", nargs="*", help="node-identity globs (for invalidate)")
    ap.add_argument("--cc-submit", default="cc-submit")
    ap.add_argument("--sacct", default="sacct")
    ap.add_argument("--rerun", action="append", default=[],
                    help="glob of node identities to force-resubmit now (repeatable)")
    ap.add_argument("--only", action="append", default=[],
                    help="restrict the run to nodes matching this glob (repeatable); "
                         "errors if a matched node's upstream isn't COMPLETED or in the run")
    ap.add_argument("--local", action="store_true",
                    help="synchronous runner: log terminal state from its exit; skip sacct")
    args = ap.parse_args()
    try:
        try:
            spec = tomllib.loads(pathlib.Path(args.spec).read_text())
        except tomllib.TOMLDecodeError as e:
            raise PipelineError(f"invalid TOML in {args.spec}: {e}")
        eng = Engine(spec)
        if args.action == "dag":
            eng.dag()
        elif args.action == "dry":
            eng.dry()
        elif args.action == "status":
            eng.status(sacct=args.sacct)
        elif args.action == "invalidate":
            eng.invalidate(args.globs)
        elif args.action == "cancel-ids":
            eng.cancel_ids()
        else:
            eng.submit(cc=args.cc_submit, sacct=args.sacct, rerun=args.rerun,
                       only=args.only, local=args.local)
    except PipelineError as e:
        sys.exit(f"pipeline: error: {e}")
    except BrokenPipeError:
        try:
            sys.stdout.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
