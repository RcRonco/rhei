#!/usr/bin/env python3
"""Guard the documentation-accuracy invariants described in DOCS-AUDIT.md.

Three checks, all cheap and hermetic (no compilation happens here — that is
`cargo test --doc --workspace`):

1. Every Markdown doc listed in VERIFIED_DOCS is wired into `rhei/src/lib.rs`
   via `include_str!`, so its Rust blocks are compiled as doctests. This stops
   someone from silently unhooking a doc from the doctest harness.

2. Every ```rust block in those docs is either compiled, or opted out with
   `ignore` *and* accompanied by a `not-compiled:` justification. Silent
   `ignore` is how documentation rots.

3. The README quick start stays byte-identical to the compiled example it
   claims to quote (`rhei/examples/quickstart.rs`, between ANCHOR markers).

Exit status is 0 when all checks pass, 1 otherwise.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# Docs whose Rust blocks must be compiled as doctests.
VERIFIED_DOCS = ["README.md", "API.md", "docs/getting-started.md"]

LIB_RS = Path("rhei/src/lib.rs")
QUICKSTART_EXAMPLE = Path("rhei/examples/quickstart.rs")

FENCE = re.compile(r"^```(?P<info>[^\n]*)$")
# A fence is a Rust block if it is bare ```rust or ```rust,<attrs>.
RUST_INFO = re.compile(r"^rust\b")

JUSTIFICATION = "not-compiled:"
# How far above a fence we look for the justification comment.
JUSTIFICATION_LOOKBACK = 6

failures: list[str] = []


def fail(msg: str) -> None:
    failures.append(msg)


def check_docs_are_wired_in() -> None:
    """Check 1: each verified doc is included as a doctest by lib.rs."""
    lib = (REPO / LIB_RS).read_text(encoding="utf-8")
    for doc in VERIFIED_DOCS:
        # lib.rs includes are relative to rhei/src/, e.g. ../../README.md
        needle = f'include_str!("../../{doc}")'
        if needle not in lib:
            fail(
                f"{LIB_RS}: missing `{needle}`. {doc} is listed in VERIFIED_DOCS "
                f"but its Rust blocks are not compiled as doctests."
            )


def iter_rust_blocks(text: str):
    """Yield (line_no, info_string, body) for every fenced Rust block."""
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        m = FENCE.match(lines[i])
        if not m:
            i += 1
            continue
        info = m.group("info").strip()
        start = i
        i += 1
        body: list[str] = []
        while i < len(lines) and not FENCE.match(lines[i]):
            body.append(lines[i])
            i += 1
        i += 1  # step over the closing fence
        if RUST_INFO.match(info):
            yield start + 1, info, body


def check_ignored_blocks_are_justified() -> None:
    """Check 2: no silent `ignore`."""
    for doc in VERIFIED_DOCS:
        path = REPO / doc
        text = path.read_text(encoding="utf-8")
        lines = text.splitlines()
        for line_no, info, body in iter_rust_blocks(text):
            attrs = {a.strip() for a in info.split(",")[1:]}
            if "ignore" not in attrs:
                continue
            # Justification may sit just above the fence (HTML comment) or on
            # the first lines of the block itself (// comment).
            lookback = lines[max(0, line_no - 1 - JUSTIFICATION_LOOKBACK) : line_no - 1]
            context = "\n".join(lookback + body[:3])
            if JUSTIFICATION not in context:
                fail(
                    f"{doc}:{line_no}: ```{info} block is excluded from doctests "
                    f"without a justification. Add a `{JUSTIFICATION} <reason>` "
                    f"comment immediately above the fence or on its first line, "
                    f"or drop `ignore` so the block is compiled."
                )


def extract_anchor(text: str, name: str) -> list[str] | None:
    start = f"// ANCHOR: {name}"
    end = f"// ANCHOR_END: {name}"
    lines = text.splitlines()
    try:
        i = next(n for n, line in enumerate(lines) if line.strip() == start)
        j = next(n for n, line in enumerate(lines) if line.strip() == end)
    except StopIteration:
        return None
    return lines[i + 1 : j]


def check_readme_quickstart_matches_example() -> None:
    """Check 3: the README quick start quotes the compiled example verbatim."""
    example_path = REPO / QUICKSTART_EXAMPLE
    if not example_path.exists():
        fail(f"{QUICKSTART_EXAMPLE}: missing; README quick start has nothing to track.")
        return

    anchored = extract_anchor(example_path.read_text(encoding="utf-8"), "quickstart")
    if anchored is None:
        fail(
            f"{QUICKSTART_EXAMPLE}: missing `// ANCHOR: quickstart` / "
            f"`// ANCHOR_END: quickstart` markers."
        )
        return

    readme = (REPO / "README.md").read_text(encoding="utf-8")
    blocks = [body for _, _, body in iter_rust_blocks(readme)]
    if not blocks:
        fail("README.md: no Rust blocks found; expected the quick start.")
        return

    expected = [line.rstrip() for line in anchored]
    if not any([line.rstrip() for line in b] == expected for b in blocks):
        fail(
            f"README.md: the quick start no longer matches the anchored region of "
            f"{QUICKSTART_EXAMPLE}. Update whichever one is stale so the README "
            f"quotes compiled, runnable code."
        )


def main() -> int:
    check_docs_are_wired_in()
    check_ignored_blocks_are_justified()
    check_readme_quickstart_matches_example()

    if failures:
        print("Documentation checks failed:\n", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        print(
            "\nSee DOCS-AUDIT.md for what these checks protect and why.",
            file=sys.stderr,
        )
        return 1

    print(
        f"Documentation checks passed "
        f"({len(VERIFIED_DOCS)} docs wired into doctests, quick start in sync)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
