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
| **Structural** | `params`, `deps`, `command`, `array` | interpreted by the engine to build/run the graph |
| **SLURM**      | the `[recipe.X.slurm]` block      | become `cc-submit` flags                            |
| **Alias**      | any other bare key (`output`)     | user-defined derived strings, readable across edges |

**Reserved words:** `params`, `deps`, `command`, `array`, `slurm`. These may not
be used as alias names.

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

Rules:

- **Literal.** No `${...}` interpolation inside `params`; it is the source of
  bindings.
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

**Lists.** In a string context (`command`, alias), a list resolves **space-joined**.
In a list context (`deps`), a list **splices** (flattens) in place.

`${node}` is always available.

---

## 5. Aliases

Any bare key other than the reserved words. A per-node derived string (§4),
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
- **All flags are optional**; a flag absent after defaults is simply not passed,
  and the node inherits the `#SBATCH` floor baked into the cluster wrapper
  scripts (`run.sbatch.sh` / `array.sbatch.sh`).
- `-j` (job name = `${node}`) and `-d` (dependencies) are **engine-owned** — do
  not put `job-name` or `depends-on` in a `slurm` block.
- Any **unknown** `slurm.*` key is an **error** (§10) — the flag set is fixed to
  `cc-submit`'s interface; to add one, extend `cc-submit` first.

---

## 8. `command` and submission

`command` is the shell run per node, interpolated per node (§4). The engine
materializes it: individual nodes → an uploaded script run by `run.sbatch.sh`;
array recipes → one script per task (`task-<idx>.sh`) in an uploaded tasks
directory, task *i* run by `array.sbatch.sh` off `$SLURM_ARRAY_TASK_ID`. Because
each task is its own script, a `command` may span multiple lines and runs intact
— identical to the individual path. Both wrapper scripts invoke the command through `bash` inside
the container, so `command` stays free-form shell (pipes, redirects, `&&`) and
needs **no `bash -c` wrapping by the author**. The engine passes `command`
through verbatim — it is never word-split; quoting *within* it is the author's
responsibility.

Submission is via `cc-submit`, whose interface is fixed:

```
# individual node:
cc-submit sbatch <script> -j ${node} <flags> -d <id> -d <id> ...

# array recipe (N tasks):
cc-submit array <commands-file> -j <recipe> <flags> -d <id> ... [--aftercorr <id> ...]
```

`<flags>` renders the closed slurm set (§7) in deterministic order (`-c -m -p -t`);
`-d`/`--aftercorr` render in deterministic node order. `cc-submit` prints the job
id on stdout, which the engine captures.

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

### Dependency translation

Each edge is rendered according to the kinds of its endpoints:

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
- A reserved word (`params`/`deps`/`command`/`array`/`slurm`) used as an alias.
- An unknown `slurm.*` key.
- `array = true` on an ineligible recipe (non-uniform resources or non-uniform
  dependency structure).

---

## 11. Execution: logging, reconciliation, re-runs

The append-only JSONL log is the project's memory; `sacct` is SLURM's. Both
`status` and `submit` reconcile the two by job id.

- **reconcile** queries `sacct` once for every job whose last logged state is
  non-terminal, folds the `.batch`/`.extern` sub-rows and array-task rows per job
  id, and appends the observed terminal state plus `Elapsed` and peak `MaxRSS`.
  Only `COMPLETED` is success; every other terminal state is resubmit-eligible.
- **submit** reconciles first, then runs only nodes whose latest state is not
  `COMPLETED` — failed, invalidated, absent, or force-listed — plus every node
  **downstream** of a rerun (its inputs are now stale). Live nodes
  (`RUNNING`/`PENDING`/just-`SUBMITTED`) are left untouched. Skipped `COMPLETED`
  nodes keep their logged job id so downstream `afterok`/`aftercorr` can still
  target them.
- **`--only <glob>`** restricts the run to matching nodes only — no downstream,
  no unrelated branches. It does **not** run their upstream; instead it requires
  each matched node's parents to be already `COMPLETED` or themselves in the run,
  and errors (running nothing) otherwise. `--rerun`/skip-completed still apply
  within the scope, so `--only` composes with them.
- **`--rerun <glob>`** (transient) force-resubmits nodes whose identity matches,
  in this invocation only.
- **`invalidate <glob>`** (persistent) appends an `INVALIDATED` record for
  matching nodes, so the next `submit` — in any session — reruns them and their
  downstream. Cleared naturally once a node re-runs to `COMPLETED`.

- **`--local`** (used by `cc-local`) marks the runner **synchronous**: the job
  runs to completion during submission, so the engine logs its terminal state
  (`COMPLETED`/`FAILED`) directly from the runner's exit and **skips `sacct`
  entirely**. Anything not `COMPLETED` (including a stale `SUBMITTED` from an
  interrupted local run) is rerun. This is what makes `status` on a
  locally-run pipeline need no cluster access.

Array units reconcile atomically: an array is `COMPLETED` only if all its tasks
are, else the whole array is resubmit-eligible. (Per-task array resubmission via
sparse `--array=` indices is a possible future refinement.)

**Subcommands:** `dag`, `dry`, `submit` (`--rerun <glob>`), `status`,
`invalidate <glob>`.

---

## 12. Field reference

| Key               | Where                         | Interpolated | Purpose                                          |
|-------------------|-------------------------------|--------------|--------------------------------------------------|
| `params`          | recipe                        | no           | node set: product table or record list           |
| `deps`            | recipe                        | binding only | list of parent captures `R(k=v, k=*)`            |
| `command`         | recipe                        | yes          | shell to run per node                             |
| `array`           | recipe                        | no (bool)    | opt into single-array submission (§9)            |
| `slurm.*`         | defaults / recipe / record    | yes          | `cc-submit` flags (`cpus`,`mem`,`partition`,`time`) |
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
