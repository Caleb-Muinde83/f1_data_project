"""Truncation guard (rule 4 of the session-scoped build design).

A `table` model rebuilt from a SCOPED view silently loses every other session. This reads target/manifest.json and fails when:
  1. a model whose SQL uses the scope (in_scope, scope_session_keys, telemetry_load_filter) is materialized as a table, or
  2. a `table` model reads a scoped VIEW or ephemeral model (directly, or through views in between). Reading a scoped INCREMENTAL
     model is fine: it is persisted and holds every session, or
  3. the telemetry_group tag is out of step with the graph: a model that reads a telemetry_group model, or a scoped model, is not tagged.

  python ci/check_scoped_views.py [path/to/manifest.json]      (default: target/manifest.json; run `dbt parse` first)
Exit code 1 on any violation.
"""
import json
import sys

SCOPE_MARKERS = ("in_scope(", "scope_session_keys(", "telemetry_load_filter(")
GROUP_TAG = "telemetry_group"


def _models(manifest):
    return {uid: n for uid, n in manifest["nodes"].items() if n.get("resource_type") == "model"}


def _materialized(node):
    return (node.get("config") or {}).get("materialized")


def _is_scoped(node):
    code = node.get("raw_code") or node.get("raw_sql") or ""
    return any(marker in code for marker in SCOPE_MARKERS)


def check(manifest) -> list:
    models = _models(manifest)
    scoped = {uid for uid, n in models.items() if _is_scoped(n)}
    # models that expose only the scoped sessions to whoever reads them (incremental models persist ALL sessions, so they do not)
    exposing = {uid for uid in scoped if _materialized(models[uid]) != "incremental"}
    violations = []

    for uid in sorted(scoped):
        if _materialized(models[uid]) == "table":
            violations.append(f"{models[uid]['name']}: reads the session scope but is materialized as a TABLE")

    def reaches_scoped(uid, seen):
        """Does `uid` read a scoped model, directly or through non-persisting (view / ephemeral) models?"""
        for parent in (models[uid].get("depends_on") or {}).get("nodes", []):
            if parent in exposing:
                return parent
            if parent in models and parent not in seen and _materialized(models[parent]) in ("view", "ephemeral"):
                seen.add(parent)
                hit = reaches_scoped(parent, seen)
                if hit:
                    return hit
        return None

    for uid, node in sorted(models.items()):
        if _materialized(node) == "table":
            hit = reaches_scoped(uid, {uid})
            if hit:
                violations.append(f"{node['name']}: TABLE reads scoped model {models[hit]['name']} "
                                  f"(it would silently lose every session outside the scope)")

    tagged = {uid for uid, n in models.items() if GROUP_TAG in (n.get("tags") or [])}
    for uid, node in sorted(models.items()):
        parents = set((node.get("depends_on") or {}).get("nodes", []))
        if parents & (scoped | tagged) and uid not in tagged:
            violations.append(f"{node['name']}: reads a scoped or {GROUP_TAG} model but is not tagged {GROUP_TAG} "
                              f"(add it to dbt_project.yml so Airflow builds it with the group)")
    for uid in sorted(scoped - tagged):
        violations.append(f"{models[uid]['name']}: is scoped but not tagged {GROUP_TAG}")
    return violations


def main(argv):
    path = argv[1] if len(argv) > 1 else "target/manifest.json"
    with open(path) as fh:
        manifest = json.load(fh)
    problems = check(manifest)
    models = _models(manifest)
    print(f"checked {len(models)} models; scoped: {sorted(n['name'] for n in models.values() if _is_scoped(n))}")
    for p in problems:
        print("VIOLATION:", p)
    print("OK: no table model reads scoped data" if not problems else f"{len(problems)} violation(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
