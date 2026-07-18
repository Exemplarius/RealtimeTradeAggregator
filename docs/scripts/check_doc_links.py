#!/usr/bin/env python3
"""Documentation integrity checker for the docs/ routing system.

Supports the routing-maintenance workflow (see docs/ROUTES.md → Maintenance and
CLAUDE.md → Documentation routing). Run it after adding, moving, renaming, or
deleting any doc:

    python3 docs/scripts/check_doc_links.py

It verifies two invariants:

  1. Link integrity   — every relative markdown link in docs/ resolves to a file
                        that exists on disk (external http(s)/file:/mailto and pure
                        #anchors are skipped).
  2. ROUTES coverage  — every hand-written doc under docs/ is referenced by path
                        from docs/ROUTES.md, so nothing becomes undiscoverable.

Exits non-zero if either invariant is violated, so it is CI-friendly.
"""
import os
import re
import sys

# Resolve the repo root as the parent of docs/ (this file lives in docs/scripts/).
DOCS = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
REPO = os.path.normpath(os.path.join(DOCS, ".."))
LINK_RE = re.compile(r"\]\(([^)]+)\)")

# Files that are the router / entry points themselves — not part of the
# hand-written, ROUTES-indexed doc set.
COVERAGE_EXEMPT = {"docs/ROUTES.md", "docs/README.md"}
SCRIPT_DIRS = {"docs/scripts"}


def rel(path):
    return os.path.relpath(path, REPO)


def all_docs_md():
    out = []
    for root, _, files in os.walk(DOCS):
        for f in files:
            if f.endswith(".md"):
                out.append(os.path.join(root, f))
    return sorted(out)


def is_external(href):
    low = href.lower()
    return low.startswith(("http://", "https://", "mailto:", "file:")) or href.startswith(("#", "/"))


def check_link_integrity():
    broken = []
    for f in all_docs_md():
        d = os.path.dirname(f)
        with open(f) as fh:
            content = fh.read()
        for m in LINK_RE.finditer(content):
            href = m.group(1)
            if is_external(href):
                continue
            path = href.split("#", 1)[0]
            if not path:
                continue
            if not os.path.exists(os.path.normpath(os.path.join(d, path))):
                broken.append((rel(f), href))
    return broken


def check_routes_coverage():
    routes_path = os.path.join(DOCS, "ROUTES.md")
    if not os.path.exists(routes_path):
        return ["docs/ROUTES.md is missing"]
    with open(routes_path) as fh:
        routes = fh.read()
    # The exact set of doc paths (relative to docs/) that ROUTES.md links to — anchors and
    # optional link titles stripped. Exact membership, not substring, so a path that is merely a
    # suffix of a routed path is not falsely counted as covered.
    routed = set()
    for m in LINK_RE.finditer(routes):
        href = m.group(1)
        if is_external(href):
            continue
        path = href.split("#", 1)[0].split(" ", 1)[0].strip()
        if path:
            routed.add(os.path.normpath(path))
    missing = []
    for f in all_docs_md():
        r = rel(f)
        if r in COVERAGE_EXEMPT:
            continue
        if any(r.startswith(d + "/") for d in SCRIPT_DIRS):
            continue
        rel_to_docs = os.path.normpath(os.path.relpath(f, DOCS))
        if rel_to_docs not in routed:
            missing.append(r)
    return missing


def main():
    broken = check_link_integrity()
    missing = check_routes_coverage()

    print(f"Link integrity : {len(broken)} broken relative link(s)")
    for f, href in broken:
        print(f"  BROKEN  {f}  ->  {href}")
    print(f"ROUTES coverage: {len(missing)} doc(s) not referenced in ROUTES.md")
    for f in missing:
        print(f"  UNROUTED  {f}")

    if broken or missing:
        print("\nFAIL — fix the above (see docs/ROUTES.md → Maintenance).")
        return 1
    print("\nOK — all links resolve and every doc is routed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
