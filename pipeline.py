#!/usr/bin/env python3
"""Pipeline engine: resolve a TOML spec (see spec.md) into a SLURM job DAG and
emit cc-submit calls.

    pipeline.py dag        spec.toml           # resolved nodes + edges + dep types
    pipeline.py submit     spec.toml           # materialize scripts, submit, log ids
    pipeline.py submit     spec.toml --dry     # ...same decisions, print instead
    pipeline.py status     spec.toml [-v]      # state / elapsed / peak RSS per unit
    pipeline.py history    spec.toml [<glob>]  # every logged attempt, per unit
    pipeline.py invalidate spec.toml <glob>    # mark stale: next submit reruns it
    pipeline.py complete   spec.toml <glob>    # force to success (ran it by hand)
    pipeline.py cancel-ids spec.toml [<glob>]  # live job ids, for scancel
    pipeline.py log-ids    spec.toml [<glob>]  # remote log filename patterns

`submit` flags: --only <glob> (this scope alone, no downstream), --rerun <glob>
(force, propagates), --local (synchronous runner), --dry, --workdir.

A recipe's body is `command`, or `command_file` (a file next to the spec, interpolated
the same way). Each materialized unit is an executable declaring its own interpreter
in a shebang, so the runners exec it directly rather than assuming bash.

The resolution pipeline (spec.md Sec.9): expand params -> nodes; wire deps by
capture matching; toposort; resolve aliases (topo order); render command + slurm;
group into submission units (individual jobs / arrays); translate dependencies
(afterok / aftercorr) and submit.
"""
import argparse
import fnmatch
import itertools
import json
import os
import pathlib
import re
import shlex
import subprocess
import sys
import time
import tomllib
from collections import defaultdict
from typing import Any

RESERVED = {"params", "deps", "command", "command_file", "interpreter",
            "array", "array_axes", "slurm"}
SLURM_FLAGS = {"cpus": "-c", "mem": "-m", "partition": "-p", "time": "-t"}
# Valueless slurm keys: truthy in the recipe -> the flag is emitted with no value.
SLURM_BOOL_FLAGS = {"exclusive": "-x"}
# Job states. Only COMPLETED is success; everything else terminal is a failure
# (and thus resubmit-eligible). NON_TERMINAL states are still live: skip them.
RUNNINGISH = {"RUNNING", "PENDING", "REQUEUED", "SUSPENDED",
              "COMPLETING", "CONFIGURING", "RESIZING"}
NON_TERMINAL = RUNNINGISH | {"SUBMITTED", "UNKNOWN"}
# The fields a reconcile record observes; if none of them moved, there is nothing
# new to append to the log.
OBSERVED = ("state", "elapsed", "max_rss", "tasks")
# How many failing task identities `status -v` names before summarizing the rest.
FAILURES_SHOWN = 10
VAR = re.compile(r"\$\{([^}]+)\}")
CAP = re.compile(r"^([\w-]+)\s*\((.*)\)\s*$")   # recipe names are TOML bare keys (may contain '-')
RANGE = re.compile(r"^(-?\d+)\.\.(-?\d+)$")


class PipelineError(Exception):
    pass


class NotReady(Exception):
    """Raised mid-substitution when a sibling alias isn't resolved yet."""


def is_true(v, what="slurm flag"):
    """Truthiness for the valueless slurm flags. Strict on purpose: a value that is
    neither true-ish nor false-ish is an error, so a typo can't silently mean 'off'."""
    s = str(v).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no", ""):
        return False
    raise PipelineError(f"{what}: expected a boolean, got {v!r}")


def expand_ranges(v) -> Any:
    """Rewrite "a..b" strings into inclusive int lists, everywhere in the spec.

    Uniform on purpose: a range is simply a way of writing a list, so it means the
    same thing in `[defaults]`, on a recipe, and inside `params`. The rule is not
    "a range is a list only where a list is expected", which would make
    `${reps}` mean different things in different positions.
    """
    if isinstance(v, dict):
        return {k: expand_ranges(x) for k, x in v.items()}
    if isinstance(v, list):
        return [expand_ranges(x) for x in v]
    if isinstance(v, str):
        m = RANGE.fullmatch(v.strip())
        if m:
            lo, hi = int(m.group(1)), int(m.group(2))
            if hi < lo:
                raise PipelineError(f"range {v!r}: end is below start")
            return list(range(lo, hi + 1))
    return v


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
        self.interpreter = None    # None -> the script is bash (see _materialize)
        self.array_index = None
        self.job_id = None         # assigned at submit


class Engine:
    def __init__(self, spec, specdir=pathlib.Path(".")):
        spec = expand_ranges(spec)
        self.spec = spec
        self.specdir = pathlib.Path(specdir)
        self.defaults = spec.get("defaults", {})
        self.default_slurm = self.defaults.get("slurm", {})
        self.default_aliases = {k: v for k, v in self.defaults.items()
                                if k not in ("slurm", "max_array_size")}
        self.max_array_size = int(self.defaults.get("max_array_size", 1000))
        self.recipes = spec.get("recipe", {})
        self.nodes = []
        self.by_recipe = {}
        self._body_cache = {}
        self._build()

    # ---- Sec.9 step 2-3: expand params into identified nodes ----
    def _build(self):
        for name, rdef in self.recipes.items():
            raw = rdef.get("params", None)
            records = self._records(raw, name)
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

        self._wire()                 # step 4
        self.order = self._toposort()  # step 5
        self._resolve_aliases()      # step 6
        self._resolve_slurm_command()  # step 7
        self._build_units()          # grouping for submission
        self._check_arrays()         # Sec.9 eligibility

    @staticmethod
    def _sortkey(n):
        return tuple(str(v) for _, v in scalar_items(n.binding))

    def _records(self, raw, recipe):
        if raw is None:
            return [{}]
        if isinstance(raw, dict):        # product sugar
            keys = list(raw)
            axes = [self._axis(raw[k], recipe, k) for k in keys]
            return [dict(zip(keys, combo)) for combo in itertools.product(*axes)]
        if isinstance(raw, list):        # explicit record list
            return raw
        raise PipelineError("params must be a table (product) or a list of records")

    def _axis(self, v, recipe, key):
        """One param axis: a list is literal, a string names a declared list.

        Strings are never literal values here -- a one-value axis is written
        `[x]`. Without that rule a string silently iterates as characters, which
        is what `dataset = "all"` used to do."""
        if isinstance(v, list):
            return v
        if not isinstance(v, str):
            raise PipelineError(f"{recipe}.params.{key}: expected a list or the name "
                                f"of a declared list, got {v!r}")
        if "." in v:
            rname, rkey = v.split(".", 1)
            src = self.recipes.get(rname)
            if src is None or rkey not in src:
                raise PipelineError(f"{recipe}.params.{key}: {v!r} names no declared "
                                    f"list (no recipe {rname!r} with key {rkey!r})")
            found = src[rkey]
        elif v in self.defaults:
            found = self.defaults[v]
        else:
            raise PipelineError(f"{recipe}.params.{key}: {v!r} names no declared list "
                                f"in [defaults] (declared: {sorted(self.defaults)})")
        if not isinstance(found, list):
            raise PipelineError(f"{recipe}.params.{key}: {v!r} resolves to "
                                f"{found!r}, which is not a list")
        return found

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
            m = re.fullmatch(r"\$\{([\w-]+)\}", s)
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
            if re.fullmatch(r"[\w-]+", cap):
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
                        n.aliases[name] = self._subst_value(tmpl, n, n.aliases,
                                                            allow_pending=True)
                    except NotReady:
                        continue
                    del pending[name]
                    progressed = True
                if not progressed:
                    raise PipelineError(
                        f"{n.ident}: alias cycle among {sorted(pending)}")

    # ---- Sec.9 step 7: resolve slurm flags and command ----
    @staticmethod
    def _slurm_map(key, table, node):
        """One slurm value written as a per-param mapping (Sec.5): pick the entry
        matching this node's binding for `param`. The axis is named explicitly --
        a bare value key could match any binding, so inference would be ambiguous."""
        table = dict(table)
        param = table.pop("param", None)
        default = table.pop("default", None)
        if param is None:
            raise PipelineError(f"{node.ident}: slurm.{key} mapping has no 'param' key")
        if param not in node.binding:
            raise PipelineError(f"{node.ident}: slurm.{key} maps on {param!r}, which is "
                                f"not a param of this recipe (has: {sorted(node.binding)})")
        val = table.get(str(node.binding[param]), default)
        if val is None:
            raise PipelineError(f"{node.ident}: slurm.{key} has no entry for "
                                f"{param}={node.binding[param]!r} and no 'default'")
        return val

    def _resolve_slurm_command(self):
        for n in self.order:
            merged = {**self.default_slurm, **n.rdef.get("slurm", {}), **n.slurm_override}
            allowed = set(SLURM_FLAGS) | set(SLURM_BOOL_FLAGS)
            for k in merged:
                if k not in allowed:
                    raise PipelineError(f"{n.ident}: unknown slurm key {k!r} "
                                        f"(allowed: {sorted(allowed)})")
            # Resolve mappings first, so the closed flag set and the bool-flag
            # validation below see a plain value either way.
            merged = {k: self._slurm_map(k, v, n) if isinstance(v, dict) else v
                      for k, v in merged.items()}
            n.slurm = {k: self._subst(str(v), n, n.aliases) for k, v in merged.items()}
            for k in SLURM_BOOL_FLAGS:                # validate here: the node names the error
                if k in n.slurm:
                    is_true(n.slurm[k], f"{n.ident}: slurm.{k}")
            if "command" in n.rdef and "command_file" in n.rdef:
                raise PipelineError(f"{n.recipe}: declares both command and command_file")
            body = n.rdef.get("command")
            if "command_file" in n.rdef:
                body = self._command_file(n.rdef["command_file"], n.recipe)
            if body is not None:
                n.command = self._subst(body, n, n.aliases)
            if "interpreter" in n.rdef:
                n.interpreter = self._subst(n.rdef["interpreter"], n, n.aliases)

    # ---- the one interpolation routine (SPEC.md Sec.4) ----
    @staticmethod
    def _render(v):
        """A value as it appears inside a command: a list joins on spaces, so a
        driver can recover it with `"${somelist}".split()`."""
        return " ".join(map(str, v)) if isinstance(v, list) else str(v)

    def _subst_value(self, v, node, aliases, allow_pending=False):
        """Substitute into a declared value, preserving list structure. A list
        alias stays a list until something interpolates it, so its members are
        each substituted and it can still be joined later."""
        if isinstance(v, list):
            return [self._subst_value(x, node, aliases, allow_pending) for x in v]
        return self._subst(str(v), node, aliases, allow_pending)

    def _subst(self, tmpl, node, aliases, allow_pending=False):
        def repl(m):
            content = m.group(1).strip()
            if "." in content:
                ref, alias = (x.strip() for x in content.split(".", 1))
                if ref == "slurm":
                    return self._slurm_ref(node, alias)
                return self._parent_alias(node, ref, alias)
            if content == "node":
                return node.ident
            if content in node.binding:
                return self._render(node.binding[content])
            if content in aliases:
                return self._render(aliases[content])
            if allow_pending and content in node.alias_defs:
                raise NotReady(content)
            raise PipelineError(f"{node.ident}: undefined variable ${{{content}}}")
        return VAR.sub(repl, tmpl)

    def _command_file(self, rel, recipe):
        """Read a recipe body from a file, resolved against the spec's directory so
        a spec is relocatable. Cached: one read per recipe, not per node."""
        if rel not in self._body_cache:
            p = self.specdir / rel
            try:
                self._body_cache[rel] = p.read_text()
            except OSError as e:
                raise PipelineError(f"{recipe}: cannot read command_file {p}: {e}")
        return self._body_cache[rel]

    def _slurm_ref(self, node, key):
        # ${slurm.KEY} reads the node's resolved slurm flag. Slurm is resolved
        # just before the command (step 7), so this is usable in `command` only,
        # not in aliases (resolved earlier) or in slurm values (still being built).
        if key not in node.slurm:
            raise PipelineError(
                f"{node.ident}: ${{slurm.{key}}} refers to unset slurm flag {key!r} "
                f"(resolved: {sorted(node.slurm)}); ${{slurm.*}} is usable in a recipe's command only")
        return node.slurm[key]

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
            vals.append(self._render(p.aliases[alias]))
        return " ".join(vals)

    def _array_groups(self, name, rnodes):
        """Yield (unit_name, nodes) for an array recipe. With `array_axes`, split
        into one array per distinct combination of the scalar params NOT listed as
        axes (the axes are what sweeps *within* each array); without it, the whole
        recipe is a single array."""
        axes = self.recipes[name].get("array_axes")
        if not axes:
            yield name, rnodes
            return
        axes = [axes] if isinstance(axes, str) else list(axes)
        params = {k for k, _ in scalar_items(rnodes[0].binding)}
        bad = [a for a in axes if a not in params]
        if bad:
            raise PipelineError(
                f"array recipe {name!r}: array_axes {bad} not among its params {sorted(params)}")
        split_keys = [k for k in sorted(params) if k not in axes]
        if not split_keys:                     # axes == all params -> one array
            yield name, rnodes
            return
        groups = defaultdict(list)
        for n in rnodes:
            groups[tuple(str(n.binding[k]) for k in split_keys)].append(n)
        for key in sorted(groups):
            yield f"{name}:{'-'.join(key)}", groups[key]

    # ---- submission units: individual jobs, or one-or-more arrays per array-recipe ----
    def _build_units(self):
        self.units = []
        self.node_unit = {}
        self.array_units = defaultdict(list)   # recipe -> [Unit, ...]
        for name, rnodes in self.by_recipe.items():
            if self._is_array(name):
                for gname, gnodes in self._array_groups(name, rnodes):
                    u = Unit("array", gname, gnodes, recipe=name)
                    for i, n in enumerate(sorted(gnodes, key=self._sortkey)):
                        n.array_index = i      # index within this array (re-based per group)
                    self.array_units[name].append(u)
                    self.units.append(u)
                    for n in gnodes:
                        self.node_unit[n] = u
            else:
                for n in rnodes:
                    u = Unit("individual", n.ident, [n], recipe=name)
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
        for name, units in self.array_units.items():
            for u in units:
                if len(u.nodes) > self.max_array_size:
                    raise PipelineError(
                        f"array unit {u.name!r} has {len(u.nodes)} tasks > max_array_size "
                        f"{self.max_array_size}; split it into smaller arrays with "
                        f"array_axes (name fewer inner axes so more params become split keys)")
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

    def _dep_tokens(self, u, uid, keep=None):
        """Return (afterok_tokens, aftercorr_tokens). `uid` maps a unit to its id.

        `keep`, if given, is the set of parent units whose edge should actually be
        emitted this run — units being (re)submitted now (fresh id) or still live
        (id still valid). A parent that already COMPLETED and was skipped is NOT
        in `keep`: its output exists on disk and its ordering is already satisfied,
        so emitting `afterok:<its old id>` is both unnecessary and rejected by
        SLURM once that job ages out of the controller. `keep=None` emits every
        edge (structural view, for `dag`/`dry`)."""
        def use(pu):
            return keep is None or pu in keep
        afterok, aftercorr = [], []
        if u.kind == "individual":
            n = u.nodes[0]
            for p in n.parents:
                pu = self.node_unit[p]
                if not use(pu):
                    continue
                if pu.kind == "individual":
                    afterok.append(uid(pu))
                else:
                    afterok.append(f"{uid(pu)}_{p.array_index}")   # specific element
        else:
            parent_recipes = sorted({p.recipe for n in u.nodes for p in n.parents})
            for R in parent_recipes:
                if self._is_array(R):
                    parent_units = {self.node_unit[p]
                                    for n in u.nodes for p in n.parents if p.recipe == R}
                    for pu in sorted(parent_units, key=lambda x: x.name):
                        if not use(pu):
                            continue
                        (aftercorr if self._aligned(u, pu) else afterok).append(uid(pu))
                else:
                    pids = sorted({uid(pu)
                                   for n in u.nodes for p in n.parents if p.recipe == R
                                   for pu in [self.node_unit[p]] if use(pu)})
                    afterok.extend(pids)
        return afterok, aftercorr

    def _cmd(self, u, uid, script, keep=None):
        afterok, aftercorr = self._dep_tokens(u, uid, keep)
        deps = " ".join(f"-d {t}" for t in afterok)
        deps += ("" if not aftercorr else " " + " ".join(f"-C {t}" for t in aftercorr))
        slurm = u.nodes[0].slurm
        flags = " ".join(f"{flag} {shlex.quote(str(slurm[k]))}"
                         for k, flag in SLURM_FLAGS.items() if k in slurm)
        for k, flag in SLURM_BOOL_FLAGS.items():
            if is_true(slurm.get(k, False)):
                flags = f"{flags} {flag}".strip()
        deps = deps.strip()
        if u.kind == "individual":
            return f"cc-submit sbatch {script} -j {u.name} {flags} {deps}".rstrip()
        return f"cc-submit array {script} -j {u.name} {flags} {deps}".rstrip()

    # ---- subcommands ----
    def dag(self, globs=("*",), verbose=False):
        # Dependency tokens still name parents outside the glob -- that is the edge
        # you most want to see when inspecting a subset. Array tasks are listed only
        # under `verbose`: a real spec has thousands of them, which buries the unit
        # headers and edges that are the point of the view.
        for u in self._matching_units(globs, self.unit_order):
            head = (f"[array {len(u.nodes)}]" if u.kind == "array" else "[job]")
            print(f"{head} {u.name}")
            if u.kind == "array" and verbose:
                for n in sorted(u.nodes, key=self._sortkey):
                    print(f"    task {n.array_index}: {n.ident}")
            afterok, aftercorr = self._dep_tokens(u, lambda x: x.name)
            if afterok:
                print(f"    afterok:   {', '.join(afterok)}")
            if aftercorr:
                print(f"    aftercorr: {', '.join(aftercorr)}")

    # ---- materialize + invoke cc-submit for one unit ----
    @staticmethod
    def _write_script(path, node):
        """A materialized unit is a self-contained executable that declares its own
        interpreter, so the runners can exec it directly rather than knowing which
        language it is. `set -euo pipefail` is bash-specific and is therefore only
        injected for the default bash case -- with an `interpreter`, the body is
        copied verbatim and nothing is added but the shebang."""
        if node.interpreter:
            head = f"#!{node.interpreter}\n"
        else:
            head = "#!/bin/bash\nset -euo pipefail\n"
        path.write_text(head + (node.command or "") + "\n")
        path.chmod(0o755)
        return path

    def _materialize(self, u, wd):
        sdir = wd / "scripts"
        sdir.mkdir(parents=True, exist_ok=True)
        if u.kind == "individual":
            return self._write_script(sdir / u.name, u.nodes[0])
        # Array: one script per task, mirroring the individual path, so a task's
        # command runs intact no matter how many lines it spans. The filename is
        # keyed on array_index (the authoritative task<->node map from _build),
        # so SLURM task i always runs node i's full command.
        tdir = sdir / f"{u.name}.tasks"
        tdir.mkdir(parents=True, exist_ok=True)
        for old in tdir.glob("task-*"):        # clear stale scripts from a prior run
            old.unlink()
        for n in u.nodes:
            self._write_script(tdir / f"task-{n.array_index}", n)
        return tdir

    def _runner_argv(self, cc, u, wd, uid, keep=None):
        """The exact argv the runner receives; `cc` replaces the leading 'cc-submit'."""
        cmd = self._cmd(u, uid, str(self._materialize(u, wd)), keep)
        return shlex.split(cc) + cmd.split()[1:]

    def _invoke_cc(self, cc, u, wd, keep=None):
        return subprocess.run(self._runner_argv(cc, u, wd, lambda x: x.job_id, keep),
                              capture_output=True, text=True)

    @staticmethod
    def _read_log_all(log_path):
        """Every record in the log, in order, grouped by unit. `run.jsonl` is
        append-only, so this is the full attempt history."""
        out = defaultdict(list)
        if log_path.exists():
            for ln in log_path.read_text().splitlines():
                if ln.strip():
                    r = json.loads(ln)
                    out[r["unit"]].append(r)
        return out

    @classmethod
    def _read_log(cls, log_path):
        return {unit: recs[-1] for unit, recs in cls._read_log_all(log_path).items()}

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

    @staticmethod
    def _fold_states(states):
        """One verdict for a set of task states: success only if all succeeded,
        otherwise live beats terminal and the first failure names the outcome."""
        if not states:
            return "UNKNOWN"
        if all(st == "COMPLETED" for st in states):
            return "COMPLETED"
        if any(st in RUNNINGISH for st in states):
            return "RUNNING"
        return next(st for st in states if st != "COMPLETED")

    def _parse_sacct(self, rows):
        """Fold sacct rows into one record per base job id, keeping per-task detail.

        Two levels, because a row identifies a job *step* of an array *task*:
        `<base>_<idx>.batch`. Level one groups by stepless id, so each task's state
        and elapsed come from its own main row while its peak RSS is the max over its
        own rows -- slurm reports MaxRSS on `.batch`, not on the main row, which is
        why a single flat fold both mixes the two and smears the worst task's RSS
        across the whole array. Level two folds an array's tasks into the per-unit
        verdict, which is what `submit` reads and is unchanged.

        A pending array appears as a range (`<base>_[5-239]`), so only a numeric
        index becomes a task; a base id with no numeric index at all (an individual
        job) gets no `tasks` key.
        """
        tasks = {}
        for r in rows:
            if len(r) < 5:
                continue
            jid, state, _exit, elapsed, maxrss = r[:5]
            stepless, _, step = jid.partition(".")
            t = tasks.setdefault(stepless, {"state": None, "elapsed": "-", "max_rss": 0})
            t["max_rss"] = max(t["max_rss"], self._parse_rss(maxrss))
            if not step:
                t["state"] = self._norm_state(state)
                t["elapsed"] = elapsed
        groups = defaultdict(dict)
        for stepless, t in tasks.items():
            base, _, idx = stepless.partition("_")
            groups[base][idx] = t
        out = {}
        for base, per_task in groups.items():
            ts = list(per_task.values())
            rec = {"state": self._fold_states([t["state"] for t in ts if t["state"]]),
                   "max_rss": max((t["max_rss"] for t in ts), default=0),
                   "elapsed": max((t["elapsed"] for t in ts), default="-")}
            indexed = {i: t for i, t in per_task.items() if i.isdigit()}
            if indexed:
                rec["tasks"] = {i: indexed[i] for i in sorted(indexed, key=int)}
            out[base] = rec
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
                      "elapsed": info["elapsed"], "event": "reconcile",
                      "time": time.time(), "reconcile": True}
                if "tasks" in info:
                    nr["tasks"] = info["tasks"]
                # A running array is re-observed on every status; appending an
                # identical record would grow the log (and a 240-task table) without
                # recording anything new, so only a changed observation is logged.
                if all(rec.get(k) == nr.get(k) for k in OBSERVED):
                    continue
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

    def _task_states(self, u, rec):
        """{node: state} for one unit's tasks.

        Uses sacct's per-task table when the record carries one, and otherwise
        attributes the unit's own state to every node -- which is exactly what an
        individual job, a record written before per-task folding existed, and a
        never-submitted unit all legitimately look like."""
        st = (rec or {}).get("state", "absent")
        tasks = (rec or {}).get("tasks") or {}
        return {n: (tasks.get(str(n.array_index)) or {}).get("state", st)
                for n in u.nodes}

    @staticmethod
    def _hist(counts):
        return " · ".join(f"{counts[s]} {s}" for s in sorted(counts))

    def status(self, sacct, workdir=".pipeline", verbose=False, globs=("*",),
               local=False):
        """Per-unit state, rolled up by recipe. The histogram counts *tasks*, not
        units, so a 3-task failure inside a 240-task array is visible; `verbose`
        expands to one row per unit and names the tasks that did not complete.

        A unit is the display grain, so a glob matching any of an array's tasks
        keeps the whole array's row. `local` skips sacct entirely, for a pipeline
        that only ever ran through the synchronous runner."""
        log_path = pathlib.Path(workdir) / "run.jsonl"
        state = self._read_log(log_path) if local else self.reconcile(sacct, log_path)
        print(f"{'unit':40} {'state':12} {'elapsed':10} maxrss")

        def row(name, rec, suffix=""):
            if rec:
                print(f"{name:40} {rec.get('state','?'):12} "
                      f"{str(rec.get('elapsed','-')):10} "
                      f"{self._fmt_rss(rec.get('max_rss'))}{suffix}")
            else:
                print(f"{name:40} {'absent':12}")

        def failures(u, rec):
            """Name the tasks whose own state is not COMPLETED. Silent when every
            task shares the unit's state -- the row already said that, and listing
            240 identical identities is noise."""
            per_task = self._task_states(u, rec)
            bad = [(n, st) for n, st in sorted(per_task.items(),
                                               key=lambda kv: kv[0].array_index or 0)
                   if st != "COMPLETED"]
            if len(bad) == len(u.nodes):
                return
            for n, st in bad[:FAILURES_SHOWN]:
                print(f"      {st.lower():9} {n.ident}")
            if len(bad) > FAILURES_SHOWN:
                print(f"      ... (+ {len(bad) - FAILURES_SHOWN} more)")

        def counts(us):
            c = defaultdict(int)
            for u in us:
                for st in self._task_states(u, state.get(u.name)).values():
                    c[st] += 1
            return c

        # cluster units by originating recipe, preserving unit_order (position of
        # each recipe's first unit); any recipe with >1 unit (array groups or an
        # individual-job fan-out) rolls up to a single summary line unless --verbose.
        groups, pos = [], {}
        for u in self._matching_units(globs, self.unit_order):
            key = u.recipe or u.name
            if key not in pos:
                pos[key] = len(groups)
                groups.append((key, []))
            groups[pos[key]][1].append(u)

        for key, us in groups:
            if verbose:
                for u in us:
                    rec = state.get(u.name)
                    row(u.name, rec)
                    if rec:
                        failures(u, rec)
            elif len(us) == 1:
                u, rec = us[0], state.get(us[0].name)
                c = counts(us)
                # A single unit already shows its own state; the histogram only adds
                # something when its tasks disagree with each other.
                row(u.name, rec, f"   {self._hist(c)}" if len(c) > 1 else "")
            else:
                ntasks = sum(len(u.nodes) for u in us)
                noun = "arrays" if us[0].kind == "array" else "jobs"
                # An individual-job fan-out has one node per unit, so a task count
                # would just repeat the unit count.
                scale = f"{len(us)} {noun}" + (f", {ntasks} tasks"
                                               if ntasks != len(us) else "")
                print(f"{key:40} [{scale}]  {self._hist(counts(us))}")

    def _matching_units(self, globs, units=None):
        return [u for u in (units if units is not None else self.units)
                if any(fnmatch.fnmatch(n.ident, g) for g in globs for n in u.nodes)]

    def cancel_ids(self, globs=("*",), workdir=".pipeline"):
        """Print the job ids of every still-live (non-terminal) unit matching a glob,
        one per line, for piping to scancel.

        Matched against the *log*, not the spec, so a live job whose recipe was since
        renamed or deleted is still cancellable -- which is the point of `cancel`."""
        last = self._read_log(pathlib.Path(workdir) / "run.jsonl")
        for name, rec in last.items():
            if rec.get("state") not in NON_TERMINAL or not rec.get("job_id"):
                continue
            # A reconcile record carries no node list, so fall back to the unit name.
            idents = [name] + (rec.get("nodes") or [])
            if any(fnmatch.fnmatch(i, g) for g in globs for i in idents):
                print(rec["job_id"])

    def log_ids(self, globs, workdir=".pipeline"):
        """Print, one per line, the remote SLURM log filename pattern for each
        submitted unit whose node identity matches a glob: `slurm-<id>.out` for an
        individual job, `slurm-<id>_*.out` for an array (whose tasks land in
        slurm-<arrayid>_<idx>.out). Used by `just logs` to tail remote logs."""
        last = self._read_log(pathlib.Path(workdir) / "run.jsonl")
        for u in self._matching_units(globs, self.unit_order):
            jid = (last.get(u.name) or {}).get("job_id")
            if not jid:
                continue
            print(f"slurm-{jid}_*.out" if u.kind == "array" else f"slurm-{jid}.out")

    @staticmethod
    def _event(rec):
        """The event label for a record. Written explicitly since R5; inferred for
        older records, which distinguished themselves only by a `reconcile` flag or
        by carrying a forced state."""
        if rec.get("event"):
            return rec["event"]
        if rec.get("reconcile"):
            return "reconcile"
        return "force" if rec.get("state") in ("INVALIDATED", "COMPLETED") else "submit"

    def history(self, globs=("*",), workdir=".pipeline"):
        """Every logged attempt per matching unit, in order: when, what happened, the
        job id, the observed state, and the per-task histogram once one was recorded.

        `run.jsonl` is append-only, so this is the one view that survives a retry --
        `status` only ever shows the latest record, which makes a job that timed out
        and was then rerun indistinguishable from one that always succeeded."""
        hist = self._read_log_all(pathlib.Path(workdir) / "run.jsonl")
        for u in self._matching_units(globs, self.unit_order):
            print(u.name)
            recs = hist.get(u.name, [])
            if not recs:
                print("  (no history)")
                continue
            for rec in recs:
                when = time.strftime("%m-%d %H:%M", time.localtime(rec.get("time", 0)))
                cols = [f"  {when}  {self._event(rec):9} {str(rec.get('job_id') or '-'):8} "
                        f"{rec.get('state', '?'):13}"]
                if rec.get("elapsed") or rec.get("max_rss"):
                    cols.append(f" {str(rec.get('elapsed') or '-'):11} "
                                f"{self._fmt_rss(rec.get('max_rss')) or '-':6}")
                if rec.get("tasks"):
                    c = defaultdict(int)
                    for t in rec["tasks"].values():
                        c[t.get("state", "UNKNOWN")] += 1
                    cols.append(" " + self._hist(c))
                print("".join(cols).rstrip())

    def _force_state(self, globs, state, verb, workdir):
        """Append a `state` record for every unit matching a glob, carrying the prior
        job_id so `logs`/`cancel` still resolve."""
        wd = pathlib.Path(workdir)
        wd.mkdir(parents=True, exist_ok=True)
        log_path = wd / "run.jsonl"
        last = self._read_log(log_path)
        matched = self._matching_units(globs)
        if not matched:
            print(f"{verb}: no nodes matched", file=sys.stderr)
            return
        with open(log_path, "a") as f:
            for u in matched:
                f.write(json.dumps({"unit": u.name, "kind": u.kind,
                                    "job_id": (last.get(u.name) or {}).get("job_id"),
                                    "state": state, "event": "force",
                                    "nodes": [n.ident for n in u.nodes],
                                    "time": time.time()}) + "\n")
                print(f"{verb} {u.name}")

    def invalidate(self, globs, workdir=".pipeline"):
        """Mark matching nodes stale so the next `submit` reruns them (and their
        downstream). Persistent across sessions; cleared naturally once a node
        re-runs to COMPLETED. INVALIDATED is deliberately outside NON_TERMINAL, so
        reconcile won't query sacct for it and submit won't treat it as live."""
        self._force_state(globs, "INVALIDATED", "invalidated", workdir)

    def complete(self, globs, workdir=".pipeline"):
        """Force matching nodes to success (e.g. after manually re-running a failed
        job). COMPLETED is terminal, so reconcile won't re-query sacct and submit
        will skip the node and won't re-propagate downstream. A later
        `invalidate`/`--rerun` overrides this normally."""
        self._force_state(globs, "COMPLETED", "completed", workdir)

    def _unready_ancestors(self, units, state, needs_run):
        """Units upstream of `units` that would have to run first, in topo order so
        the list reads as a run order. The walk stops at a parent that is COMPLETED
        or still live -- either is already satisfiable as a dependency, so nothing
        above it matters."""
        out, frontier = set(), list(units)
        while frontier:
            for pu in self.uparents[frontier.pop()]:
                if pu in out or pu in units:
                    continue
                if needs_run((state.get(pu.name) or {}).get("state")):
                    out.add(pu)
                    frontier.append(pu)
        return [u for u in self.unit_order if u in out]

    # ---- submit: reconcile, then run only failed/absent (+ --rerun, downstream) ----
    def submit(self, cc, sacct="sacct", workdir=".pipeline", rerun=(), only=(),
               local=False, dry=False, deps=False):
        """Reconcile, decide what to run, and submit it. With `dry`, take the same
        path but print the runner argv instead of invoking it, and log no submission
        (reconcile still records what sacct reported -- that is observed truth, and
        `status` records it the same way)."""
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
            scope = set(self._matching_units(only))
            if not scope:
                raise PipelineError(f"--only matched no nodes: {list(only)}")
        else:
            scope = set(self.units)

        # `deps` is the upstream counterpart of downstream propagation: pull in what
        # the scope still needs. Unscoped it is a no-op -- the scope is everything.
        if scoped and deps:
            scope |= set(self._unready_ancestors(scope, state, needs_run))

        forced = set(self._matching_units(rerun, scope))
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
            for u in torun:                           # a live parent is depended on via afterok
                for pu in self.uparents[u]:
                    pst = (state.get(pu.name) or {}).get("state")
                    live_ok = (not local) and pst in NON_TERMINAL
                    if pst != "COMPLETED" and pu not in torun and not live_ok:
                        unmet.add(f"{u.name} needs {pu.name} ({pst or 'absent'})")
            if unmet and dry:
                # An inspection verb should answer the question, not refuse it. The
                # wave itself is deliberately not printed: those units' edges would be
                # dropped by `keep` below, so the argv would not be what a real run
                # issues once the prerequisites exist.
                print("# would need first (topo order, add deps to run them):")
                for u in self._unready_ancestors(scope, state, needs_run):
                    st = (state.get(u.name) or {}).get("state", "absent")
                    print(f"#   {u.name}\t({st})")
                return
            if unmet:
                raise PipelineError("--only: unsatisfied dependencies (run them first, "
                                    "or pass --deps): " + "; ".join(sorted(unmet)))

        for u in self.units:                          # skipped units keep their logged id
            if u not in torun:
                u.job_id = (state.get(u.name) or {}).get("job_id")

        # A dependency edge is only valid/needed for a parent that is part of this
        # wave (fresh id) or still live (id still known to slurmctld). A parent that
        # already COMPLETED and is skipped has its output on disk and its old job id
        # may have aged out of the controller, so targeting it errors ("Job
        # dependency problem"); drop those edges.
        live = {u for u in self.units
                if (state.get(u.name) or {}).get("state") in NON_TERMINAL}
        keep = torun | live

        def record(u, jid, st):
            return json.dumps({"unit": u.name, "kind": u.kind, "job_id": jid, "state": st,
                               "event": "submit", "nodes": [n.ident for n in u.nodes],
                               "time": time.time()}) + "\n"

        # An in-wave parent has no id yet, while a live or skipped one keeps its logged
        # id, so a dry run's dependency tokens are legitimately a mix of real and
        # <placeholder>.
        dry_uid = lambda x: x.job_id or f"<{x.name}>"          # noqa: E731
        # Nothing is submitted in a dry run, so submission records go nowhere.
        log = open(os.devnull if dry else log_path, "a")
        try:
            for u in self.unit_order:
                if u not in torun:
                    if u in scope:
                        st = (state.get(u.name) or {}).get("state", "absent")
                        print(f"skip   {u.name}\t({st})")
                    continue
                if dry:
                    print(" ".join(self._runner_argv(cc, u, wd, dry_uid, keep)))
                    continue
                proc = self._invoke_cc(cc, u, wd, keep)
                jid = proc.stdout.strip().split()[-1] if proc.stdout.strip() else None
                if proc.returncode != 0:
                    if local:                         # record the failure before aborting
                        log.write(record(u, jid, "FAILED"))
                    raise PipelineError(f"submit failed for {u.name}:\n{proc.stderr}")
                u.job_id = jid
                log.write(record(u, jid, "COMPLETED" if local else "SUBMITTED"))
                print(f"{'done  ' if local else 'submit'} {jid}\t{u.name}")
        finally:
            log.close()
        if dry:
            print(f"# scripts written to {wd / 'scripts'}/", file=sys.stderr)


class Unit:
    def __init__(self, kind, name, nodes, recipe=None):
        self.kind = kind
        self.name = name
        self.nodes = nodes
        self.recipe = recipe
        self.job_id = None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["dag", "submit", "status", "history",
                                       "invalidate", "complete", "cancel-ids",
                                       "log-ids"])
    ap.add_argument("spec")
    ap.add_argument("globs", nargs="*",
                    help="node-identity globs (dag / status / history / invalidate / "
                         "complete / cancel-ids / log-ids)")
    ap.add_argument("--cc-submit", default="cc-submit")
    ap.add_argument("--sacct", default="sacct")
    ap.add_argument("--rerun", action="append", default=[],
                    help="glob of node identities to force-resubmit now (repeatable)")
    ap.add_argument("--only", action="append", default=[],
                    help="restrict the run to nodes matching this glob (repeatable); "
                         "errors if a matched node's upstream isn't COMPLETED, live, or in the run")
    ap.add_argument("--local", action="store_true",
                    help="submit: synchronous runner, log terminal state from its exit; "
                         "status: read the log. Either way, skip sacct")
    ap.add_argument("--deps", action="store_true",
                    help="with --only: also run the scope's upstream, wherever it isn't "
                         "already COMPLETED or live (no-op without --only)")
    ap.add_argument("--dry", action="store_true",
                    help="submit: decide identically, but print the runner argv and log nothing")
    ap.add_argument("--workdir", default=".pipeline",
                    help="state directory: run.jsonl + materialized scripts")
    ap.add_argument("-v", "--verbose", action="store_true",
                    help="status: expand grouped array recipes to per-group rows; "
                         "dag: list each array's tasks")
    args = ap.parse_args()
    try:
        try:
            spec = tomllib.loads(pathlib.Path(args.spec).read_text())
        except tomllib.TOMLDecodeError as e:
            raise PipelineError(f"invalid TOML in {args.spec}: {e}")
        eng = Engine(spec, specdir=pathlib.Path(args.spec).parent)
        wd = args.workdir
        if args.action == "dag":
            eng.dag(args.globs or ["*"], verbose=args.verbose)
        elif args.action == "status":
            eng.status(sacct=args.sacct, workdir=wd, verbose=args.verbose,
                       globs=args.globs or ["*"], local=args.local)
        elif args.action == "history":
            eng.history(args.globs or ["*"], workdir=wd)
        elif args.action == "invalidate":
            eng.invalidate(args.globs, workdir=wd)
        elif args.action == "complete":
            eng.complete(args.globs, workdir=wd)
        elif args.action == "cancel-ids":
            eng.cancel_ids(args.globs or ["*"], workdir=wd)
        elif args.action == "log-ids":
            eng.log_ids(args.globs or ["*"], workdir=wd)
        else:
            eng.submit(cc=args.cc_submit, sacct=args.sacct, workdir=wd, rerun=args.rerun,
                       only=args.only, local=args.local, dry=args.dry, deps=args.deps)
    except PipelineError as e:
        sys.exit(f"pipeline: error: {e}")
    except BrokenPipeError:
        # Downstream (`| head`) went away. Retarget stdout so the interpreter's
        # shutdown flush can't re-raise on the dead pipe.
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        sys.exit(0)


if __name__ == "__main__":
    main()
