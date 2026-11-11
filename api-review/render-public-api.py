#!/usr/bin/env python3
"""Render the CassandraClient public interface from a Swift symbol graph, as nested Swift.

Usage, from the package root, with Sources/ committed:
    swift build --target CassandraClient \
        -Xswiftc -emit-symbol-graph -Xswiftc -emit-symbol-graph-dir -Xswiftc /tmp/symgraph
    api-review/render-public-api.py /tmp/symgraph/CassandraClient.symbols.json api-review/

Writes one Markdown file per area, so a reviewer's comments cluster where a decision lives
rather than scattering through a single alphabetical listing. The Swift sits in a four-tick
fence, because doc comments contain three-tick examples that would otherwise close it.

Choices worth knowing:
  - Output is nested Swift: a type's members and nested types sit inside its braces, ordered
    by kind (initialisers, properties, subscripts, methods, operators, cases, then nested
    types) rather than alphabetically. A flat listing makes every type read as top-level.
  - Inherited protocol members are listed without their standard-library documentation. The
    declarations matter — a `Rows: Sequence` conformance hands adopters every buffering
    algorithm — but Swift's prose about `allSatisfy` does not. Discriminator: an inherited
    symbol carries no `location`.
  - Inherited members are attributed to the protocol that declared them, read off the
    symbol's USR: a synthesized member's identifier is `<origin>::SYNTHESIZED::<type>`, and
    the origin prefix matches the USR of a `conformsTo` target. The `requirementOf` and
    `defaultImplementationOf` edges do not help — all 30 cover this module's own protocols.
  - Members with neither a location nor a SYNTHESIZED marker are compiler-synthesized for the
    type itself (`init(rawValue:)`, `init(from:)`), and are marked as such rather than called
    inherited.
  - A protocol's body holds only its requirements; default implementations and extension
    members follow in an `extension`, read off the `requirementOf` edges. Folding them in
    together would misstate what a third-party conformer must implement, and would not
    compile — Swift forbids default arguments on a requirement, which most of them carry.
    Extensions are emitted at file scope, since a nested one is invalid.
  - Availability is rendered, on inherited members too, because the package declares no
    `platforms:` and these annotations are the only statement of minimum versions. That also
    surfaces members Swift itself has deprecated.
  - Noncopyability is read from the source, not the graph: 38 of 42 public types lack a
    `Copyable` conformance edge while only one is actually `~Copyable`, so absence of that
    edge is not a usable signal.
  - The graph emits no `==`, and no synthesized `encode(to:)`, though it emits `!=` and
    `init(from:)`. Both are protocol requirements whose signatures come from the protocol
    rather than the conforming type, so they are reconstructed and marked, leaving no member
    without a line to comment on.
"""

import collections
import json
import pathlib
import re
import subprocess
import sys

TYPE_KINDS = {"swift.struct", "swift.class", "swift.enum", "swift.protocol"}

# Universal, or implied by another conformance the type lists. Never shown.
IMPLIED = {"SendableMetatype", "Copyable", "Escapable", "BitwiseCopyable"}

# Guaranteed by a conformance but absent from the graph when synthesized. The declaration is
# a protocol requirement, so its signature comes from the protocol, not the conforming type.
UNEMITTED = {
    "Equatable": ("==(_:_:)", "static func == (lhs: Self, rhs: Self) -> Bool"),
    "Hashable": ("==(_:_:)", "static func == (lhs: Self, rhs: Self) -> Bool"),
    "Encodable": ("encode(to:)", "func encode(to encoder: Encoder) throws"),
}

# Within a type, the order a Swift interface reads. Nested types last, since they are usually
# longer than the members that use them.
KIND_RANK = {
    "swift.init": 0,
    "swift.property": 1,
    "swift.type.property": 1,
    "swift.subscript": 2,
    "swift.method": 3,
    "swift.type.method": 3,
    "swift.func": 3,
    "swift.func.op": 4,
    "swift.enum.case": 5,
}
NESTED_TYPE_RANK = 6

# One file per area, keyed on the top-level type name.
AREAS = [
    (
        "0001-client-and-session",
        "Client and session",
        [
            "CassandraClient",
            "CassandraSession",
            "Configuration",
            "Authenticator",
            "EventLoopGroupProvider",
        ],
    ),
    (
        "0002-statements-and-execution",
        "Statements and execution",
        [
            "Statement",
            "PreparedStatement",
            "Batch",
            "BatchType",
            "Consistency",
            "SerialConsistency",
        ],
    ),
    (
        "0003-results-and-data-model",
        "Results and data model",
        ["Rows", "Row", "Column", "PaginatedRows", "OpaquePagingStateToken", "TimeBasedUUID"],
    ),
    ("0004-errors", "Errors", ["Error", "ConfigurationError"]),
    (
        "0005-encryption",
        "Client-side encryption",
        [
            "Encryptor",
            "Encrypted",
            "EncryptionContext",
            "EncryptionSchema",
            "PrimaryKey",
            "KeyComponent",
            "KeyColumnType",
        ],
    ),
    ("0006-observability", "Observability", ["CassandraMetrics"]),
]


def noncopyable_types(sources: pathlib.Path) -> set[str]:
    """Public types declared `~Copyable`, read from the source.

    Matched on the unqualified type name, so two same-named nested types would both be
    annotated. There is one such type today; the warning below flags a change.
    """
    pattern = re.compile(
        r"public\s+(?:final\s+)?(?:struct|class|enum)\s+(\w+)\s*(?:<[^>]*>)?\s*:[^{]*~Copyable"
    )
    found: collections.Counter = collections.Counter()
    for path in sorted(sources.glob("*.swift")):
        found.update(pattern.findall(path.read_text()))
    for name, count in found.items():
        if count > 1:
            print(f"warning: '{name}' declared ~Copyable {count} times", file=sys.stderr)
    return set(found)


def availability(symbol) -> list[str]:
    """`@available` lines: version gates first, then deprecations.

    Deprecations are kept because whether they survive the 1.0 boundary is an open question.
    """
    gates, notices = [], []
    for entry in symbol.get("availability", []):
        domain = entry.get("domain")
        if introduced := entry.get("introduced"):
            if not domain:
                continue
            version = f"{introduced['major']}"
            if introduced.get("minor") is not None:
                version += f".{introduced['minor']}"
            gates.append(f"{domain} {version}")
        elif entry.get("isUnconditionallyDeprecated"):
            text = "@available(*, deprecated"
            if message := entry.get("message"):
                text += f', message: "{message}"'
            notices.append(text + ")")
        elif deprecated := entry.get("deprecated"):
            version = f"{deprecated['major']}"
            if deprecated.get("minor") is not None:
                version += f".{deprecated['minor']}"
            text = f"@available({domain}, deprecated: {version}"
            if renamed := entry.get("renamed"):
                text += f', renamed: "{renamed}"'
            notices.append(text + ")")
    return (["@available(" + ", ".join(gates) + ", *)"] if gates else []) + notices


def source_state(sources: pathlib.Path) -> tuple[str, str]:
    """The hash of the source tree described, and the toolchain that rendered it.

    A tree hash rather than a commit, so renderer and output can land in one commit and stay
    verifiable: `Sources/` does not move when only `api-review/` changes, where HEAD would
    name the parent commit and so predate the renderer that produced the files.
    """

    def run(*args):
        return subprocess.run(args, capture_output=True, text=True).stdout

    tree = run("git", "rev-parse", "--short", f"HEAD:{sources}").strip() or "unknown"
    if run("git", "status", "--porcelain", str(sources)).strip():
        tree += " (plus uncommitted changes)"
        print(
            f"warning: {sources} has uncommitted changes, so the recorded tree hash describes "
            "a state that is not in history. Commit first if this output is to be shared.",
            file=sys.stderr,
        )
    version = run("swift", "--version").splitlines()
    return tree, version[0].strip() if version else "unknown"


class Renderer:
    def __init__(self, graph, sources: pathlib.Path):
        self.symbols = [s for s in graph["symbols"] if s.get("accessLevel") == "public"]
        self.names = {
            s["identifier"]["precise"]: s["pathComponents"][-1] for s in graph["symbols"]
        }
        self.noncopyable = noncopyable_types(sources)

        self.conformances = collections.defaultdict(set)
        self.protocol_by_usr: dict[str, str] = {}
        for rel in graph["relationships"]:
            if rel["kind"] != "conformsTo":
                continue
            # An in-module protocol carries no targetFallback, only a precise identifier.
            target = rel.get("targetFallback", "").split(".")[-1] or self.names.get(rel["target"])
            if not target:
                continue
            self.protocol_by_usr[rel["target"]] = target
            if target not in IMPLIED:
                self.conformances[rel["source"]].add(target)

        # A protocol's requirements, as opposed to members its extension supplies. Swift
        # forbids default arguments on a requirement, so rendering an extension member inside
        # the protocol body produces something that cannot compile — and hides which members a
        # third-party conformer must actually implement.
        self.requirements = {
            rel["source"] for rel in graph["relationships"] if rel["kind"] == "requirementOf"
        }

        self.children = collections.defaultdict(list)
        self.type_paths = set()
        self.protocol_paths: set[str] = set()
        for symbol in self.symbols:
            self.children[".".join(symbol["pathComponents"][:-1])].append(symbol)
            if symbol["kind"]["identifier"] in TYPE_KINDS:
                self.type_paths.add(".".join(symbol["pathComponents"]))
            if symbol["kind"]["identifier"] == "swift.protocol":
                self.protocol_paths.add(".".join(symbol["pathComponents"]))

    def access(self, symbol) -> str:
        """`public ` where Swift takes an access modifier, otherwise empty.

        The graph's declaration fragments omit it. Enum cases never carry one, and a
        requirement inside a protocol body inherits the protocol's access.
        """
        if symbol["kind"]["identifier"] == "swift.enum.case":
            return ""
        parent = ".".join(symbol["pathComponents"][:-1])
        if parent in self.protocol_paths and symbol["identifier"]["precise"] in self.requirements:
            return ""
        return "public "

    def declared(self, symbol) -> str:
        """The declaration with its access modifier, attributes kept in front of it.

        Swift requires attributes before the modifier — `@preconcurrency public func`, not
        `public @preconcurrency func` — and the graph's fragments already lead with them.
        """
        text = self.declaration(symbol)
        access = self.access(symbol)
        if not access:
            return text
        attributes = []
        while text.startswith("@"):
            # An attribute may carry a parenthesised argument list before the next token.
            end = len(text)
            depth = 0
            for index, char in enumerate(text):
                if char == "(":
                    depth += 1
                elif char == ")":
                    depth -= 1
                elif char == " " and depth == 0 and index:
                    end = index
                    break
            attributes.append(text[:end])
            text = text[end:].lstrip()
        return " ".join(attributes + [access + text]) if attributes else access + text

    def declaration(self, symbol) -> str:
        text = "".join(f["spelling"] for f in symbol.get("declarationFragments", []))
        if symbol["kind"]["identifier"] not in TYPE_KINDS:
            return text
        entry = set(self.conformances.get(symbol["identifier"]["precise"], ()))
        # Hashable implies Equatable, so showing both on a declaration is noise. Attribution
        # below needs both: `!=` is Equatable's default, not Hashable's.
        if "Hashable" in entry:
            entry.discard("Equatable")
        suffix = (["~Copyable"] if symbol["pathComponents"][-1] in self.noncopyable else []) + sorted(
            entry
        )
        if suffix and " : " not in text:
            text += " : " + ", ".join(suffix)
        return text

    def declaring_protocol(self, symbol) -> str | None:
        """The protocol a synthesized member came from, or None if the compiler made it."""
        precise = symbol["identifier"]["precise"]
        if "::SYNTHESIZED::" not in precise:
            return None
        origin = precise.split("::SYNTHESIZED::")[0]
        matches = [usr for usr in self.protocol_by_usr if origin.startswith(usr)]
        return self.protocol_by_usr[max(matches, key=len)] if matches else None

    @staticmethod
    def rank(symbol) -> int:
        kind = symbol["kind"]["identifier"]
        return NESTED_TYPE_RANK if kind in TYPE_KINDS else KIND_RANK.get(kind, 3)

    def emit(self, symbol, indent: int, out: list[str], area: set[str] | None = None) -> None:
        pad = "    " * indent
        if doc := symbol.get("docComment"):
            for line in doc["lines"]:
                out.append(f'{pad}///{" " + line["text"] if line["text"] else ""}'.rstrip())
        for gate in availability(symbol):
            out.append(f"{pad}{gate}")

        text = self.declared(symbol)
        if symbol["kind"]["identifier"] not in TYPE_KINDS:
            out.append(f"{pad}{text}")
            return

        members = self.children.get(".".join(symbol["pathComponents"]), [])
        if area is not None:
            members = [m for m in members if self.area_key(m) in area]

        supplied = []
        if symbol["kind"]["identifier"] == "swift.protocol":
            supplied = [m for m in members if m["identifier"]["precise"] not in self.requirements]
            members = [m for m in members if m["identifier"]["precise"] in self.requirements]

        extras = self.unemitted(symbol, members)
        if not members and not extras:
            out.append(f"{pad}{text} {{}}")
        else:
            out.append(f"{pad}{text} {{")
            self.emit_members(members, extras, indent + 1, out, area)
            out.append(f"{pad}}}")

        if supplied:
            self.deferred_extensions.append((".".join(symbol["pathComponents"]), supplied, area))

    def unemitted(self, holder, members) -> list[str]:
        present = {m["pathComponents"][-1] for m in members}
        return sorted(
            {
                UNEMITTED[name]
                for name in self.conformances.get(holder["identifier"]["precise"], ())
                if name in UNEMITTED and UNEMITTED[name][0] not in present
            }
        )

    def emit_members(self, members, extras, indent: int, out: list[str], area=None) -> None:
        pad = "    " * indent
        own = [m for m in members if m.get("location")]
        unlocated = [m for m in members if not m.get("location")]

        previous = None
        for member in sorted(
            own, key=lambda m: (self.rank(m), m["pathComponents"][-1], self.declaration(m))
        ):
            current = self.rank(member)
            if previous is not None and (current != previous or current == NESTED_TYPE_RANK):
                out.append("")
            previous = current
            self.emit(member, indent, out, area)

        def block(label, group):
            out.append("")
            noun = "member" if len(group) == 1 else "members"
            out.append(f"{pad}// {label} {len(group)} {noun}")
            for member in group:
                for gate in availability(member):
                    out.append(f"{pad}{gate}")
                out.append(f"{pad}{self.declaration(member)}")

        by_protocol = collections.defaultdict(list)
        for member in unlocated:
            by_protocol[self.declaring_protocol(member)].append(member)
        for protocol in sorted(k for k in by_protocol if k):
            block(f"[inherited from {protocol}]", by_protocol[protocol])
        if synthesized := by_protocol.get(None):
            block("[compiler-synthesized]", synthesized)

        if extras:
            out.append("")
            noun = "member" if len(extras) == 1 else "members"
            out.append(
                f"{pad}// [reconstructed] {len(extras)} {noun} guaranteed by a conformance "
                "above that the symbol graph does not emit"
            )
            for _, text in extras:
                out.append(f"{pad}public {text}")

    def area_key(self, symbol) -> str:
        """The outermost type a symbol belongs to, used to place it in an area.

        `CassandraClient` is the namespace for nearly everything, so a nested type is keyed on
        its own name. Its direct members — `query(_:)`, `shutdown()` — are keyed on
        `CassandraClient`, which is why `pathComponents[1]` cannot be assumed to be a type.
        """
        path = symbol["pathComponents"]
        if path[0] == "CassandraClient" and len(path) > 1:
            if ".".join(path[:2]) in self.type_paths:
                return path[1]
            return "CassandraClient"
        return path[0]

    def render_area(self, title: str, keys: list[str], tree: str, toolchain: str) -> str:
        area = set(keys)
        in_area = [s for s in self.symbols if self.area_key(s) in area]
        # A type is a root of its area only if its parent is not also emitted here. Otherwise
        # a type that is both an area key and a child of another root — Configuration under
        # CassandraClient — would be emitted twice, once nested and once standalone.
        emitted_paths = {
            ".".join(s["pathComponents"])
            for s in in_area
            if s["kind"]["identifier"] in TYPE_KINDS
        }
        roots = [
            s
            for s in in_area
            if s["kind"]["identifier"] in TYPE_KINDS
            and len(s["pathComponents"]) <= 2
            and ".".join(s["pathComponents"][:-1]) not in emitted_paths
        ]
        # Module-scope types have one path component; the rest nest under CassandraClient.
        standalone = sorted(
            (s for s in roots if len(s["pathComponents"]) == 1),
            key=lambda s: s["pathComponents"][-1],
        )
        nested = sorted(
            (s for s in roots if len(s["pathComponents"]) == 2),
            key=lambda s: s["pathComponents"][-1],
        )

        out: list[str] = []
        self.deferred_extensions: list[tuple[str, list, set[str]]] = []
        for symbol in standalone:
            self.emit(symbol, 0, out, area)
            out.append("")
        if nested:
            out.append("extension CassandraClient {")
            for index, symbol in enumerate(nested):
                if index:
                    out.append("")
                self.emit(symbol, 1, out, area)
            out.append("}")

        # Extensions are only valid at file scope, so protocols nested under CassandraClient
        # have their supplied members emitted here rather than inside the enclosing block.
        for name, supplied, member_area in self.deferred_extensions:
            out.append("")
            out.append(
                "// Supplied by the module, not required of a conformer: default"
                " implementations and extension members."
            )
            out.append(f"extension {name} {{")
            self.emit_members(supplied, [], 1, out, member_area)
            out.append("}")

        while out and not out[-1]:
            out.pop()

        header = [
            f"# {title}",
            "",
            "## Overview",
            "",
            f"- **Area:** {title} — {len(in_area)} of the module's public symbols.",
            "- **Note:** This PR exists to collect API review feedback ahead of 1.0. It will be"
            " closed rather than merged; anything it settles lands as its own change.",
            f"- **Generated from:** `Sources/CassandraClient` at `{tree}`, with {toolchain}."
            " Derived, not maintained by hand — see `api-review/README.md` to regenerate.",
            "",
            "Inherited protocol members are listed without their standard-library"
            " documentation, grouped under the protocol that declared each. Members the symbol"
            " graph does not emit are marked `[reconstructed]`.",
            "",
            "## Public API",
            "",
            "````swift",
        ]
        return "\n".join(header + out + ["````", ""])


if __name__ == "__main__":
    graph_path = pathlib.Path(sys.argv[1])
    out_dir = pathlib.Path(sys.argv[2] if len(sys.argv) > 2 else "api-review")
    sources = pathlib.Path(sys.argv[3] if len(sys.argv) > 3 else "Sources/CassandraClient")

    renderer = Renderer(json.loads(graph_path.read_text()), sources)
    tree, toolchain = source_state(sources)

    covered = set()
    for slug, title, keys in AREAS:
        text = renderer.render_area(title, keys, tree, toolchain)
        (out_dir / f"{slug}.md").write_text(text)
        covered.update(keys)
        print(f"{slug}.md  {len(text.splitlines()):5} lines", file=sys.stderr)

    # Every public symbol must land in exactly one area, or the split has a hole.
    orphans = sorted(
        {
            renderer.area_key(s)
            for s in renderer.symbols
            if renderer.area_key(s) not in covered
        }
    )
    if orphans:
        print(f"warning: not in any area: {', '.join(orphans)}", file=sys.stderr)
