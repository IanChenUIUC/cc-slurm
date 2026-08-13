# Pipeline Spec

A TOML format for describing arbitrary SLURM job DAGs as parameterized
**recipes**. Regular pipelines carry no redundancy; fully irregular ones remain
expressible — with a single substitution mechanism explaining the whole format.
Submission goes through the `cc-submit` helper (§8), which is the only interface
to the cluster.

---

## 1. Model

- A **recipe** is a template for a job.
- A **node** is a recipe instantiated against one **binding** — one cell of the
  recipe's `params`.
- The DAG is formed by **captures**: a node's `deps` names parent nodes by
  constraining their binding.
- Everything (`command`, `slurm` values, aliases, `deps`) is produced by one
  operation: `${...}` **interpolation** over the node's binding, its aliases,
  and — across an edge — a parent's aliases.

Every key inside `[recipe.X]` falls into exactly one category:

| Category       | Keys                              | Meaning                                             |
|----------------|-----------------------------------|-----------------------------------------------------|
| **Structural** | `params`, `deps`, `command`, `command_file`, `interpreter`, `array`, `array_axes` | interpreted by the engine to build/run the graph |
| **SLURM**      | the `[recipe.X.slurm]` block      | become `cc-submit` flags                            |
| **Alias**      | any other bare key (`output`)     | user-defined derived strings, readable across edges |

**Reserved words:** `params`, `deps`, `command`, `command_file`, `interpreter`,
`array`, `array_axes`, `slurm`. These may not be used as alias names.

---

## 2. File structure

```toml
[defaults]              # aliases shared by all recipes
[defaults.slurm]        # baseline sbatch flags

[recipe.NAME]           # params / deps / command / array / aliases
[recipe.NAME.slurm]     # per-recipe sbatch flags (override defaults)
```

---

## 3. `params` — the node set

`params` defines one binding per node. Two forms, both reducing to a **list of
binding records**.

**Product form (sugar, regular grids)** — cartesian product of axis lists:

```toml
params = { dataset = ["cora", "pubmed"], method = ["metis", "pulp"] }   # 4 nodes
```

**Record form (irregular / jagged)** — an explicit list of records, one per node:

```toml
params = [
  { dataset = "web", method = "metis" },
  { dataset = "web", method = "pulp", resolution = "0.5" },   # extra key, ok
]
```

TOML inline tables must fit on **one line**. When a record needs to span lines
(e.g. a long `sources` list), write the record form as an **array of tables**
instead — arrays may span lines:

```toml
[[recipe.ensemble.params]]
dataset = "web"
name    = "cross"
slurm   = { mem = "128GB" }
sources = [
  "cluster(dataset=web, method=metis)",
  "cluster(dataset=web, method=pulp, resolution=0.5)",
]
```

**Referencing a declared list.** An axis value may be the *name* of a list declared
elsewhere, so a sweep is written once and read by every recipe that shares it:

```toml
[defaults]
testing = ["bitcoin", "friendster", "livejournal"]

[recipe.genquery]
reps   = "0..19"                 # a range: inclusive, ints
sizes  = [1, 10]
params = { dataset = "testing" }                        # bare -> [defaults]

[recipe.search]
params = { dataset = "testing", rep = "genquery.reps", size = "genquery.sizes" }
```

- **Bare** (`"testing"`) resolves in `[defaults]`; **qualified** (`"genquery.reps"`)
  is a *static lookup of `recipe.key` in the TOML*. It is **not** a parent-alias read
  (§4) — `params` fix node identity before `deps` are wired, so nothing here may
  depend on the DAG or on run state.
- Inside `params`, **a string always names a list; it is never a literal value.** A
  one-value axis is written `[x]`. (Without this rule a bare string silently iterates
  as *characters*.)
- **Ranges** — `"a..b"`, inclusive, yielding ints. A range is just a way of writing a
  list, so it means the same thing in `[defaults]`, on a recipe, and in `params`.
  Identities render values with `str()`, so int-vs-string does not affect `${node}`.
- Naming something undeclared, or something that is not a list, is a **hard error**.

Rules:

- **Literal.** No `${...}` interpolation inside `params`; it is the source of
  bindings. (A bare string names a declared list, as above — that is resolved
  statically, not interpolated.)
- **Binding variables.** Every key in a record (except the reserved `slurm`
  sub-key) is a binding variable: interpolable as `${key}`, matchable in
  captures. Values may be scalars or lists.
- **Per-cell SLURM override.** A record may carry `slurm = { ... }`, overriding
  `[recipe.X.slurm]` for that node only (§7).
- **Jaggedness allowed.** Records may define different keys; an absent key does
  not exist for that node (referencing it is an error — §4).
- **No `params`** ⇒ a single node with empty binding.

### Node identity (`${node}`)

```
${node} = <recipe> + "-" + join("-", scalar binding values in key order)
```

List-valued bindings are excluded. Identities must be unique per run (§10).

---

## 4. Interpolation (`${...}`)

One rule set, applied to `command`, alias values, `slurm` values, and `deps`.

**Simple variable — `${name}`** — resolved against, in order: node binding vars,
recipe aliases, `[defaults]` aliases. First match wins. Unresolved ⇒ **hard
error** (no silent empty, no default-if-absent). This is what forces a jagged
recipe referencing a sometimes-absent key to be split into two recipes.

**Parent alias — `${ref.alias}`** — reads an alias off the parent node(s) this
node depends on. `ref` is either a **recipe name appearing in `deps`**
(`${partition.output}`), or a **binding var holding capture strings**
(`${sources.output}`). It resolves to the matched parent set; `.alias` is read
from each and **space-joined**. Referencing an alias of a non-dependency is an
error.

**Slurm flag — `${slurm.KEY}`** — reads this node's own resolved slurm flag
(`${slurm.cpus}`, `${slurm.mem}`, …) after three-level merge (§7). `slurm` is a
reserved `ref` (it shadows a parent recipe of that name). Usable in **`command` /
`command_file` only** — slurm flags are resolved just before the command, so they are *not*
available in aliases (resolved earlier) or in other slurm values (still being
built). Referencing an unset flag is a hard error. Lets a command reuse its
allocation, e.g. `OMP_NUM_THREADS=${slurm.cpus}`.

**Lists.** In a string context (`command`, alias), a list resolves **space-joined**.
In a list context (`deps`), a list **splices** (flattens) in place. This holds for a
list-valued *alias* as well as a list-valued binding, so a declared list survives
into a body and can be recovered there — e.g. `"${sizes}".split()` in Python.

**No escape syntax.** Every `${...}` in an interpolated value is substituted, and one
that resolves to nothing is a hard error. There is deliberately no way to write a
literal `${...}`, which matters most for a `command_file` (§8): a placeholder-looking
string *anywhere* in the file — including in a comment or docstring — is interpolated
too. Build such a string at runtime instead (`os.environ["HOME"]`, string
concatenation).

`${node}` is always available.

---

## 5. Aliases

Any bare key other than the reserved words. A per-node derived value (§4) —
a string, or a **list** whose members are each interpolated —
**readable by dependents** as `${thisrecipe.alias}`.

```toml
output = "results/${dataset}.${method}.csv"
```

- May reference bindings, `[defaults]` aliases, sibling aliases, and parent
  aliases (`${parent.alias}`).
- Resolved across the DAG in **topological order** (parents before children).
- Alias cycles are an error.

Canonical use is `output`: each recipe declares where it writes **once**; every
dependent reads `${producer.output}` — paths never drift.

---

## 6. `deps` — edges

A list of **captures**; the dependent is the enclosing recipe (LHS implicit).

```toml
deps = ["partition(dataset=${dataset}, method=${method})"]
```

**Capture syntax:** `RECIPE(key=value, ...)`, each `value` an interpolated string
or literal `*`.

**Matching.** A capture selects every node of `RECIPE` such that (1) each
`key=value` holds (or `value` is `*`), **and** (2) **every** binding key of that
node is mentioned (as value or `*`). Rule (2) makes fan-in explicit: omitting a
parent's key is an error, not a silent fan-in. Because binding keys are per-node,
this matches jagged parents correctly.

- Zero matches ⇒ error.
- `deps` may reference only the node's own binding vars (never aliases), so the
  DAG builds before aliases resolve.
- A `${listvar}` splices, so `deps = ["${sources}"]` ⇒ one edge per capture.
- All dependencies are `afterok` (or `aftercorr` between aligned arrays — §9).

---

## 7. `slurm` — resources → `cc-submit` flags

The SLURM surface is **closed**: it is exactly what `cc-submit` accepts. Flags
resolve with three-level precedence, per flag, highest wins:

```
[defaults.slurm]  <  [recipe.X.slurm]  <  record's  slurm = { ... }
```

| `slurm` key | cc-submit flag | notes                          |
|-------------|----------------|--------------------------------|
| `cpus`      | `-c`           |                                |
| `mem`       | `-m`           |                                |
| `partition` | `-p`           |                                |
| `time`      | `-t`           |                                |

- Values are interpolated (§4): `cpus = "${threads}"` is valid.
- A value may instead be a **per-param mapping**, for a resource that varies with
  one axis. Interpolation can echo a param but not *map* it, and record-form
  `params` would mean re-inlining the whole list on the recipe:

  ```toml
  params = { dataset = "all_networks" }
  slurm  = { cpus = 16, mem = { param = "dataset", default = "128GB", twitter_social = "256GB" } }
  ```

  The axis is **named explicitly** (`param = "dataset"`): a bare value key could
  match any binding, so inference would be ambiguous. The chosen entry is then
  interpolated like any other value. It is an error (§10) for the mapping to omit
  `param`, to name a param the recipe lacks, or to have neither an entry for a
  node's value nor a `default`.
- **A mapping keyed on a param an array sweeps makes that recipe ineligible**
  (§9: resources must be uniform within an array). Either split on another param
  with `array_axes`, or drop `array = true` and let the recipe fan out into
  individual jobs — which keeps element-wise dependency edges to an aligned array
  parent (`-d <id>_<idx>`), so the fan-out costs scheduling precision only in the
  other direction, where an array *child* depends on the fan-out.
- **All flags are optional**; a flag absent after defaults is simply not passed,
  and the node inherits the `#SBATCH` floor baked into the cluster wrapper
  scripts (`run.sbatch.sh` / `array.sbatch.sh`).
- `-j` (job name = `${node}`) and `-d` (dependencies) are **engine-owned** — do
  not put `job-name` or `depends-on` in a `slurm` block.
- Any **unknown** `slurm.*` key is an **error** (§10) — the flag set is fixed to
  `cc-submit`'s interface; to add one, extend `cc-submit` first.

---

## 8. `command` and submission

`command` is the body run per node, interpolated per node (§4). The engine
materializes it: individual nodes → an uploaded script run by `run.sbatch.sh`;
array recipes → one script per task (`task-<idx>`) in an uploaded tasks
directory, task *i* run by `array.sbatch.sh` off `$SLURM_ARRAY_TASK_ID`. Because
each task is its own script, a `command` may span multiple lines and runs intact
— identical to the individual path. The engine passes the body through verbatim
— it is never word-split; quoting *within* it is the author's responsibility.

**A materialized unit is a self-contained executable**, mode `0755`, whose first
line is a shebang naming its own interpreter; the wrapper scripts exec it directly
rather than assuming a language. Scripts therefore carry **no file extension** — the
shebang is the single source of truth, so no suffix can contradict the contents.
(`scp` must preserve the mode, hence `cc-submit`'s `-p`.)

**`command_file` + `interpreter`.** For a body too long or too structured to sit in
TOML, `command_file` names a file **relative to the spec's directory**, interpolated
on exactly the same rules as `command`:

```toml
[recipe.analyze]
interpreter  = "/usr/bin/env python3"
command_file = "cmds/analyze.py"
```

- With no `interpreter`, the script is bash and keeps the `set -euo pipefail`
  preamble; `command` stays free-form shell (pipes, redirects, `&&`) needing no
  `bash -c` wrapping.
- With an `interpreter`, the body is copied **verbatim** under `#!<interpreter>` and
  nothing is injected — `set -euo pipefail` is bash-specific and would be meaningless.
- `interpreter` does **not** require `command_file`; a short inline `command` may
  declare one. Declaring both `command` and `command_file` is a hard error.
- The body is read once per recipe and substituted per node, so an interpolated file
  is a *template*, not a script to run by hand. Since substitution is textual and
  unquoted, assign each `${...}` to a constant at the top of the file rather than
  inlining it into an expression — and mind the no-escape rule (§4).

Submission is via `cc-submit`, whose interface is fixed:

```
# individual node:
cc-submit sbatch <script> -j ${node} <flags> -d <id> -d <id> ...

# array recipe (N tasks):
cc-submit array <commands-file> -j <recipe> <flags> -d <id> ... [-C <id> ...]
```

`<flags>` renders the closed slurm set (§7) in deterministic order (`-c -m -p -t`),
followed by any valueless flags (`-x`); `-d` (`afterok`) and `-C` (`aftercorr`)
render in deterministic node order. `cc-submit` prints the job id on stdout, which
the engine captures.

---

## 9. Arrays

A recipe may opt into submission as a single SLURM **array** (`array = true`) —
one submission, one job id, N tasks indexed by `$SLURM_ARRAY_TASK_ID`. Arrays are
lighter on the controller and are the right choice for large, homogeneous
fan-out layers. Default is individual jobs.

### Eligibility (hard errors when violated)

A recipe with `array = true` must satisfy both:

- **Uniform resources.** Every node resolves to **identical** slurm flags. A
  per-cell `slurm` override that differs across nodes ⇒ error.
- **Uniform dependency structure.** Every node's dependency set is expressible at
  array granularity — i.e. all tasks depend on the same upstream job(s)/array(s),
  either as whole-array fan-in or as an element-wise correspondence (below). A
  recipe whose nodes have **distinct individual parents** (e.g. a per-cell
  `sources` list) is **not** array-eligible ⇒ error.

If `array = true` is set on an ineligible recipe, the engine errors and names the
violation; it never silently falls back to individual jobs (their dependency
semantics differ, and that choice is yours).

### Splitting one recipe into multiple arrays (`array_axes`)

A cluster caps a single array at `MaxArraySize` (often ~1001 tasks). A recipe whose
param fan-out exceeds that must become **several** arrays. Set `array_axes` to the
list of params that sweep *within* each array; every **other** scalar param becomes
a **split key**, and the engine emits one array per distinct combination of the
split keys:

```toml
[recipe.bench]
array      = true
array_axes = ["rep", "size", "batch"]   # these vary inside each array
params     = { dataset = [...7...], rep = [...20...], size = [1,5,10,20], batch = [1,10,100] }
# -> 7 arrays (one per dataset) of 20×4×3 = 240 tasks each
```

Each group is an independent SLURM array with its **own** job id and **re-based**
`0..N-1` task indices; its unit name is `recipe:<splitvals>` (e.g. `bench:dataset_a`).
A single `array_axes` string is allowed. Omitting `array_axes` (or listing *all*
params) keeps the recipe a single array (the default). Eligibility (uniform
resources + dependency structure) is enforced **per group**.

**`max_array_size`.** `[defaults].max_array_size` (default **1000**) is the per-array
task cap the engine enforces at build time: any array unit — grouped or not — with
more tasks is a hard error naming the recipe/group and suggesting `array_axes`, so an
oversized recipe fails fast instead of being rejected by `sbatch`.

### Dependency translation

Each edge is rendered according to the kinds of its endpoints. The names below are
the SLURM dependency semantics; on the `cc-submit` command line they render as `-d`
(`afterok`) and `-C` (`aftercorr`) — see §8.

| child ← parent            | rendered dependency                    |
|---------------------------|----------------------------------------|
| individual ← individual   | `afterok:<id>`                          |
| individual ← one array task | `afterok:<arrayid>_<idx>`             |
| individual ← array (fan-in `*`) | `afterok:<arrayid>` (whole array) |
| array ← individual(s)     | `afterok:<id>[:<id>...]` (whole array waits) |
| array ← array, **grids match** | `aftercorr:<arrayid>`             |
| array ← array, grids differ | `afterok:<arrayid>` (whole array)    |

**`aftercorr` rule.** Used **iff** the child array and the captured parent array
have the **same node grid on the captured axes, as a set up to permutation**
(a bijection exists). The engine assigns each array's task indices in a
deterministic order of the shared param tuple so task *i* ↔ task *i*, then emits
`aftercorr`. If the grids differ (e.g. the child adds an axis, so it has more
tasks), no 1:1 alignment exists and the edge degrades to a whole-array
`afterok` — correct but over-synchronizing (every child task waits for the whole
parent array). Use individual jobs if you need finer cross-array ordering.

Per-task correctness never depends on the dependency granularity: the engine
bakes each task's fully-resolved `command` (including `${parent.output}` paths)
into its commands-file line, so `afterok` vs `aftercorr` only affects *ordering*,
not which inputs a task reads.

The engine records the node→array-index map in the run log (§11) so individual
dependents can target specific elements (`<arrayid>_<idx>`).

---

## 10. Errors (all hard failures)

- Reference to an undefined `${variable}`.
- `${ref.alias}` where `ref` is not a dependency of the node.
- A capture naming an unknown recipe.
- A capture omitting one of a matched node's binding keys.
- A capture matching zero nodes.
- Duplicate node identity.
- A dependency cycle, or an alias cycle.
- A reserved word (`params`/`deps`/`command`/`command_file`/`interpreter`/`array`/
  `array_axes`/`slurm`) used as an alias.
- An unknown `slurm.*` key.
- A per-param `slurm` mapping with no `param` key, naming a param the recipe lacks,
  or with neither a matching entry nor a `default`.
- A `params` string naming no declared list, or naming something that is not a list.
- A range whose end is below its start.
- A recipe declaring both `command` and `command_file`, or a `command_file` that
  cannot be read.
- `array = true` on an ineligible recipe (non-uniform resources or non-uniform
  dependency structure).

---

## 11. Execution: logging, reconciliation, re-runs

The append-only JSONL log is the project's memory; `sacct` is SLURM's. Both
`status` and `submit` reconcile the two by job id.

- **reconcile** queries `sacct` once for every job whose last logged state is
  non-terminal and appends what it observed. The fold is two-level, because a row
  identifies a job *step* of an array *task* (`<base>_<idx>.batch`): each task's
  state and `Elapsed` come from its own main row while its peak `MaxRSS` is the max
  over its own steps — slurm reports `MaxRSS` on `.batch`, not on the main row — and
  the tasks then fold into one state per unit. Tasks slurm has not started yet come
  back as a single range row (`<base>_[0-239]`, `[3,5,7-9]`, `%n` when throttled),
  which expands back into one task apiece. Only `COMPLETED` is success; every
  other terminal state is resubmit-eligible. A unit with anything live reports the
  **most advanced live state present** (`COMPLETING` > `RUNNING` > `RESIZING` >
  `SUSPENDED` > `CONFIGURING` > `REQUEUED` > `PENDING`), so an array still waiting in
  the queue reads `PENDING`; a failure names the unit only once nothing is live.
  The appended record carries the per-unit `state`/`elapsed`/`max_rss` **plus** a
  `tasks` table (`index -> {state, elapsed, max_rss}`) for arrays; an individual job
  has no task index and gets no table. A record whose observed fields are identical
  to the previous one is **not** appended — a running array is re-observed on every
  `status`, and re-logging an unchanged 240-task table records nothing new.
- **submit** reconciles first, then runs only nodes whose latest state is not
  `COMPLETED` — failed, invalidated, absent, or force-listed — plus every node
  **downstream** of a rerun (its inputs are now stale). Live nodes
  (`RUNNING`/`PENDING`/just-`SUBMITTED`) are left untouched. A dependency edge is emitted only for a parent that is
(re)submitted in the same run (fresh job id) or still live (id still known to the
controller); a skipped `COMPLETED` parent is **not** targeted — its output
already exists and its old job id may have aged out of `slurmctld`, which would
otherwise make SLURM reject the submission ("Job dependency problem"). Every emitted
dependency also carries `--kill-on-invalid-dep=yes`, so a job whose dependency becomes
unsatisfiable is killed rather than sitting `PENDING` forever as
`DependencyNeverSatisfied`.
- **`--only <glob>`** restricts the run to matching nodes only — no downstream,
  no unrelated branches. It does **not** run their upstream; instead it requires
  each matched node's parents to be already `COMPLETED`, still live (a running
  parent is depended on via `afterok`), or themselves in the run, and errors
  (running nothing) otherwise. `--rerun`/skip-completed still apply
  within the scope, so `--only` composes with them.
- **`--deps`** is the upstream counterpart of that downstream propagation: with
  `--only`, the scope grows to include every ancestor that still needs to run,
  transitively. The walk stops at an ancestor that is `COMPLETED` or live — either
  is already satisfiable as a dependency, so nothing above it is pulled in, and it
  is not re-run merely for being upstream. Downstream propagation stays suppressed,
  and `--rerun` still forces only what its own glob matches. Without `--only` it is
  a no-op, the scope being everything already.
- **`--no-retry`** runs only work that has **never been attempted**: any node the log
  already has a record for — failed, timed out, cancelled, `INVALIDATED` — is skipped,
  and so is everything downstream of it, transitively. Skipping a failure without
  skipping its subtree would submit children whose input was never produced, since the
  edge to a skipped parent is dropped (below). Each skip names its reason
  (`skip  <unit>  (blocked by <root> FAILED)`). `--rerun` still forces what its glob
  matches, so `--rerun` + `--no-retry` is "retry exactly this and nothing else".
- **A doomed `afterok` edge is not submitted.** `afterok` is satisfied only when
  *every* task of the parent succeeds, so a parent that is still live but already has
  failed tasks can never satisfy it — SLURM would kill the child, which then reads as
  the child's own failure. Those children are skipped (with their subtree) and told
  why. `aftercorr` is per-task and therefore exempt: task *i* waits on parent task
  *i*, and the healthy tasks proceed.
- **`--rerun <glob>`** (transient) force-resubmits nodes whose identity matches,
  in this invocation only. It is `invalidate` fused with the run that follows —
  `just invalidate 'g'; just run` decides identically — with one difference that is
  the reason it stays a flag: `--rerun` is unioned into the wave *ahead of* the
  eligibility check, so it beats `--no-retry`, whereas an `INVALIDATED` record counts
  as attempted and would be skipped. That makes `--rerun` + `--no-retry` the repair
  move for a single flaked job in a large frontier, and the only form previewable
  under `--dry` without first writing the mark.
- **`cancel <glob>`** is the manual "this work is failed" mark, and the one verb that
  also talks to the cluster: it appends a `CANCELLED` record for every matching unit
  and `scancel`s whatever is still live, so the log and SLURM agree. `CANCELLED` is an
  ordinary terminal failure afterwards — resubmit-eligible on the next plain run,
  skipped under `--no-retry` — so this is a mark, not a tombstone.
  Talking to the cluster is what keeps it distinct from `invalidate`: it is the verb
  for work that is **over**, whether or not SLURM still thinks so. It therefore takes
  no liveness flag in either direction — a live unit is killed, and a glob matching
  nothing live (a subtree whose dependency died and which never got submitted at all)
  is simply marked. `--dry` prints what it would mark. Nothing is protected,
  `COMPLETED` included: the log is append-only, so the prior record survives in
  `history` and one `complete` restores it.
  The two halves match different things — `scancel` matches the **log**, so a live job
  whose recipe was since renamed is still killable, while the mark matches the **spec**,
  so a unit with no records at all can still be written off.
  Both halves come out of **one** `cancelled --print-ids` invocation, which reads
  liveness before it appends and prints the ids afterwards, stdout carrying the ids
  and the per-unit lines diverted to stderr. They cannot be two processes: the mark
  makes every one of those units read terminal, so a `cancel-ids` run after it finds
  nothing and the job survives.
- **`invalidate <glob>`** (persistent) appends an `INVALIDATED` record for
  matching nodes, so the next `submit` — in any session — reruns them and their
  downstream. Cleared naturally once a node re-runs to `COMPLETED`. `--dry` prints
  what it would mark.
  It is the one state verb that **refuses a live unit**: `INVALIDATED` is terminal, so
  reconcile stops collecting that job's result, *and* resubmit-eligible, so the next
  `submit` puts a second job on the same output files. `cancel` stops the job first;
  `--force` waives the refusal for a job you know is already gone, warning instead.

- **`--local`** (used by `cc-local`) marks the runner **synchronous**: the job
  runs to completion during submission, so the engine logs its terminal state
  (`COMPLETED`/`FAILED`) directly from the runner's exit and **skips `sacct`
  entirely**. Anything not `COMPLETED` (including a stale `SUBMITTED` from an
  interrupted local run) is rerun. This is what makes `status` on a
  locally-run pipeline need no cluster access.

Array units reconcile atomically: an array is `COMPLETED` only if all its tasks
are, else the whole array is resubmit-eligible. Per-task state is *recorded and
reported* (see the `tasks` table above and `status -v`), but not yet acted on —
resubmission is still whole-array. (Per-task resubmission via sparse `--array=`
indices is the natural next step now that the state exists.)

- **`complete <glob>`** (persistent) appends a `COMPLETED` record for matching
  nodes, forcing them to success — for work re-run by hand outside the pipeline.
  Since `COMPLETED` is terminal, `submit` then skips the node *and* does not
  re-propagate downstream. Overridden by a later `invalidate`/`--rerun`. `--dry`
  prints what it would mark. On a live unit it **warns rather than refusing**: it
  loses that job's result, but `submit` skips `COMPLETED`, so nothing is resubmitted.

All three state verbs share one mechanism — append a terminal state — and therefore
one hazard: reconcile stops watching a job the moment its unit reads terminal. What
separates them is the state written, whether the cluster is touched, and how each
answers for a live unit (refuse / warn / stop it).
- **`--dry`** takes the identical path — reconcile, scope, what-needs-running,
  downstream propagation, the `keep`-filtered dependency edges — but prints the
  runner argv instead of invoking it, and writes **no submission record**. It is
  the same code as a real `submit`, so what it prints is what would run. Two
  consequences worth knowing: reconcile still appends the states `sacct` reported
  (that is observed truth, and `status` records it identically), and a parent that
  is *in the same wave* has no job id yet, so its dependency renders as
  `<unit-name>` while live and skipped parents show their real ids.
  Where a real `submit` **errors** on an `--only` scope with unmet upstream, `--dry`
  instead prints what would have to run first — transitively, in topo order, so the
  list reads as a run order — and exits successfully. An inspection verb should
  answer the question rather than refuse it. It deliberately stops there instead of
  also printing the wave: those units' dependency edges are dropped by the same
  rule as a skipped parent's, so the argv would not be what a working run issues.

- **`history [<glob>]`** reads the log without touching `sacct`: every record for
  each matching unit, in order, tagged `submit` / `reconcile` / `force`. `status`
  only ever shows the *latest* record, so a unit that timed out, was repaired, and
  now reads `COMPLETED` looks identical to one that always succeeded; `history` is
  where the earlier attempts, their job ids, and their measurements survive. A unit
  with no records prints `(no history)`, which distinguishes *never submitted* from
  *submitted and failed* — `status` reports both as `absent`.

**Subcommands:** `dag [<glob>]` (`-v`, `-vv`), `submit` (`--only`/`--rerun <glob>`, `--local`,
`--deps`, `--dry`, `--no-retry`), `status [<glob>]` (`-v`, `--local`), `history [<glob>]`, `invalidate <glob>` (`--force`, `--dry`),
`complete <glob>` (`--dry`), `cancelled <glob>` (`--dry`, `--print-ids`), `cancel-ids [<glob>]`, `log-ids [<glob>]`. All accept `--workdir` (default
`.pipeline`). Every glob matches **node identities**; `cancel-ids` (and `cancelled --print-ids`) additionally
matches unit names in the log, so a live job whose recipe was since renamed or
deleted is still cancellable.

### Which one do I reach for?

The `justfile` shipped with the template maps these to seven verbs, each taking an
optional glob, with the behaviour flags as `just` variable overrides (which must
precede the recipe name):

| you want to… | run |
|---|---|
| see what the spec expanded into | `just dag` |
| …with each recipe's units, then their tasks | `just verbose=1 dag`, `just dag -vv` |
| see what a run would do, before doing it — or, if the subset isn't ready, what it is still waiting on | `just dry=1 run 'testing-*'` |
| run whatever still needs running | `just run` |
| run one subset, upstream already done | `just run 'testing-*'` |
| run one subset **and whatever it needs** | `just up=1 run 'testing-*'` |
| run it here, synchronously, no SLURM | `just local=1 run 'testing-*'` |
| redo it: the inputs or the code changed | `just force=1 run 'testing-*'` |
| redo it: the job flaked, downstream is fine | `just force=1 down=0 run 'testing-*'` |
| move the frontier forward, leave every past failure alone | `just retry=0 run` |
| mark it stale, but don't run it now | `just invalidate 'testing-*'` |
| tell the DAG about work you did by hand | `just complete 'testing-*'` |
| stop live jobs | `just cancel 'testing-*'` |
| write off work that will never run | `just cancel 'testing-gullo-*-online*'` (`dry=1` to preview) |
| read the output | `just status`, `just logs 'testing-*'` |
| read it back after a local run, with no cluster | `just local=1 status` |
| find out which tasks of an array failed | `just verbose=1 status 'testing-*'` |
| remind yourself what the flags are | `just` (or `just help`) |
| see earlier attempts, not just the latest | `just history 'testing-*'` |

**`status` roll-up.** One row per **recipe**, however many units it expands to:

```
unit                            elapsed   maxrss  scale                   progress              tasks
build                           -         -                                                     COMPLETED
csr-format                      00:28:04  89G     [1 array, 9 tasks]      ████████████████████  9 COMPLETED
testing-csk                     01:04:02  9G      [7 arrays, 420 tasks]   ██████▒░░░░░░░░░░░░░  131 COMPLETED · 259 PENDING · 30 RUNNING
strongscaling-steiner           00:12:41  64G     [8 arrays, 72 tasks]    ███████████████████▓  71 COMPLETED · 1 TIMEOUT
```

There is deliberately **no folded state column**. No single label answers "does this
need me", "is it still going" and "is it finished" at once: live-beats-terminal hides
a `2 FAILED · 12 RUNNING` array, and worst-wins renders a mostly-running array as
`PENDING`. The histogram answers all three, so it is always printed — including when
every task agrees, which is what distinguishes a 9-task array from a single job.

The histogram counts **tasks**, not units, so three failed tasks inside a 240-task
array are visible rather than reading as one failed array; the task count is omitted
for an individual-job fan-out, where it would just repeat the unit count. Elapsed and
peak RSS on a rolled-up row are the **max** across the group — for a parallel array,
the numbers that size the budget. A node with no record counts as `ABSENT`, and
`INVALIDATED` reports as `ABSENT` too: both mean "no valid result here", and which it
was is what `history` is for.

The bar is 20 cells over four classes — completed `█`, running `▒`, waiting `░`,
failed `▓` — so it survives a pipe, `NO_COLOR`, and a monochrome terminal. **When
color is on, every class is a solid `█` and the hue alone carries the distinction**:
a shade glyph blends with the background, which renders the same color code visibly
dimmer than the identically-coded histogram text beside it. **Any nonempty class keeps at least one cell**, taken off the
largest: one bad task in seventy-two is exactly what the bar exists to show. Color is
emitted only when stdout is a TTY and `NO_COLOR` is unset.

`status -v` expands to one row per unit and, beneath it, names the tasks whose own
state is not `COMPLETED`:

```
testing-csk:bitcoin                      FAILED       01:12:33   38G
      failed    testing-csk-bitcoin-13-1-100
      timeout   testing-csk-bitcoin-14-1-1
```

It stays silent when *every* task shares the unit's state — the row already said
that, and listing 240 identical identities is noise. A record with no `tasks` table
(any record written before per-task folding existed, or an individual job) degrades
to the unit-level line rather than failing.

---

## 12. Field reference

| Key               | Where                         | Interpolated | Purpose                                          |
|-------------------|-------------------------------|--------------|--------------------------------------------------|
| `params`          | recipe                        | no           | node set: product table or record list           |
| `deps`            | recipe                        | binding only | list of parent captures `R(k=v, k=*)`            |
| `command`         | recipe                        | yes          | body to run per node                              |
| `command_file`    | recipe                        | yes          | body read from a file, path relative to the spec dir (§8) |
| `interpreter`     | recipe                        | yes          | shebang for the materialized script; default bash (§8) |
| `array`           | recipe                        | no (bool)    | opt into single-array submission (§9)            |
| `array_axes`      | recipe                        | no (list)    | params that sweep within each array; others split it into multiple arrays (§9) |
| `max_array_size`  | defaults                      | no (int)     | per-array task cap, default 1000; build-time guard (§9) |
| `slurm.*`         | defaults / recipe / record    | yes          | `cc-submit` flags (`cpus`,`mem`,`partition`,`time`); a value may be a per-param mapping (§7) |
| *(other key)*     | defaults / recipe             | yes          | alias — readable as `${recipe.key}`              |

---

## 13. Examples

### 13.1 Regular — no redundancy

```toml
[defaults.slurm]
cpus = 16
mem  = "64GB"

[defaults]
resultdir = "results"

[recipe.convert]
params  = { dataset = ["cora", "citeseer", "pubmed"] }
command = "./convert data/${dataset}.raw ${output}"
output  = "out/${dataset}.bin"

[recipe.partition]
params  = { dataset = ["cora", "citeseer", "pubmed"], method = ["metis", "pulp"] }
deps    = ["convert(dataset=${dataset})"]
command = "./partition --method ${method} ${convert.output} ${output}"
output  = "out/${dataset}.${method}.parts"

[recipe.cluster]
params  = { dataset = ["cora", "citeseer", "pubmed"], method = ["metis", "pulp"] }
deps    = ["partition(dataset=${dataset}, method=${method})"]
command = "./cluster ${partition.output} ${output}"
output  = "${resultdir}/${dataset}.${method}.csv"
```

15 nodes (3 + 6 + 6). Resources declared once; each path lives in one `output`
alias and flows downstream. Every recipe is four lines.

### 13.2 Regular, as arrays — showing `aftercorr` vs `afterok`

Same shape, opting the fan-out layers into arrays:

```toml
[recipe.convert]
array   = true
params  = { dataset = ["cora", "citeseer", "pubmed"] }        # 3 tasks
command = "./convert data/${dataset}.raw ${output}"
output  = "out/${dataset}.bin"

[recipe.partition]
array   = true
params  = { dataset = ["cora", "citeseer", "pubmed"], method = ["metis", "pulp"] }  # 6 tasks
deps    = ["convert(dataset=${dataset})"]
command = "./partition --method ${method} ${convert.output} ${output}"
output  = "out/${dataset}.${method}.parts"

[recipe.cluster]
array   = true
params  = { dataset = ["cora", "citeseer", "pubmed"], method = ["metis", "pulp"] }  # 6 tasks
deps    = ["partition(dataset=${dataset}, method=${method})"]
command = "./cluster ${partition.output} ${output}"
output  = "results/${dataset}.${method}.csv"
```

- `partition ← convert`: grids differ (6 vs 3; `partition` adds `method`), so no
  bijection ⇒ **`afterok:<convert-array>`** (each partition task waits for the
  whole convert array).
- `cluster ← partition`: identical grid `{dataset × method}` up to permutation ⇒
  **`aftercorr:<partition-array>`** (task *i* of cluster waits only on task *i*
  of partition).

### 13.3 Irregular ensemble — arrays not allowed

```toml
[defaults.slurm]
cpus = 8
mem  = "16GB"

[recipe.ensemble]
deps    = ["${sources}"]
command = "./ensemble --inputs ${sources.output} --out ${output}"
output  = "results/${dataset}.${name}.ens.csv"

[[recipe.ensemble.params]]
dataset = "web"
name    = "pulp-lohi"
sources = [
  "cluster(dataset=web, method=pulp, resolution=0.1)",
  "cluster(dataset=web, method=pulp, resolution=1.0)",
]

[[recipe.ensemble.params]]
dataset = "web"
name    = "cross"
slurm   = { mem = "128GB" }
sources = [
  "cluster(dataset=web, method=metis)",
  "cluster(dataset=web, method=pulp, resolution=0.5)",
]

[[recipe.ensemble.params]]
dataset = "web-huge"
name    = "triple"
slurm   = { cpus = 32, mem = "512GB" }
sources = [
  "cluster(dataset=web-huge, method=louvain)",
  "cluster(dataset=web-huge, method=leiden)",
  "cluster(dataset=web-huge, method=pulp, resolution=0.5)",
]
```

Individual jobs only. Setting `array = true` here is a hard error on **both**
counts: per-cell `slurm` overrides differ across nodes (non-uniform resources),
and each node has a distinct hand-picked `sources` parent set (non-uniform
dependency structure). Each irregularity costs one line in the cell that owns it.

### 13.4 Declared lists, ranges, and a body in another language

`examples/interpreter.toml` + `examples/cmds/analyze.py`. A bash recipe and a Python
one in the same DAG, with a range declared once and referenced by a consumer:

```toml
[defaults]
datasets = ["cora", "citeseer"]

[recipe.analyze]
array        = true
reps         = "0..2"
deps         = ["ingest(dataset=${dataset})"]
params       = { dataset = "datasets", rep = "analyze.reps" }
stats        = "${output}/${dataset}/stats-rep${rep}.txt"
interpreter  = "/usr/bin/env python3"
command_file = "cmds/analyze.py"
```

Runnable with no cluster: `just spec=examples/interpreter.toml local=1 run`. Note
`${parent.alias}` joins across *every* matched parent, so a `rep=*` fan-in reading an
alias its parents share repeats it once per parent — read per-parent outputs instead.
