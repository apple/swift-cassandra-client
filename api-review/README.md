# API review

The public API of the `CassandraClient` module, one file per area, so a reviewer's comments
cluster where a decision lives rather than scattering through a single listing:

| File | Area | Symbols |
|---|---|---|
| `0001-client-and-session.md` | Client, session, configuration, authentication | 189 |
| `0002-statements-and-execution.md` | Statements, prepared statements, batches, consistency | 152 |
| `0003-results-and-data-model.md` | `Rows`, `Row`, `Column`, `PaginatedRows` | 371 |
| `0004-errors.md` | `Error`, `ConfigurationError` | 83 |
| `0005-encryption.md` | Client-side encryption | 56 |
| `0006-observability.md` | `CassandraMetrics` | 24 |

Each renders its area as nested Swift. Every public symbol lands in exactly one file, and
the renderer warns if one lands in none.

**Not meant to merge.** This lives on a review branch; anything the review settles lands as
its own change. The files are derived, and the package does not otherwise commit generated
output.

## Regenerating

The files go stale as soon as the public surface changes — this happened once already
mid-review, when `setPagingSize` became public. Regenerate rather than edit, from a tree
with `Sources/` committed:

```
swift build --target CassandraClient \
  -Xswiftc -emit-symbol-graph -Xswiftc -emit-symbol-graph-dir -Xswiftc /tmp/symgraph

api-review/render-public-api.py /tmp/symgraph/CassandraClient.symbols.json api-review/
```

Each header records the hash of `Sources/CassandraClient` rather than a commit, so two
rounds can be compared without mistaking unrelated drift for API change, and so the renderer
and its output can land in one commit — a recorded HEAD would name the parent and so predate
the renderer. Uncommitted changes there produce a warning, since the hash would describe a
state that isn't in history. Rendering is toolchain-sensitive.

`render-public-api.py` documents the judgement calls it makes: how inherited members are
attributed, how a protocol's requirements are separated from what its extensions supply, why
availability and deprecations are rendered, why noncopyability is read from the source, and
why `==` and synthesized `encode(to:)` are reconstructed — the symbol graph emits neither.
