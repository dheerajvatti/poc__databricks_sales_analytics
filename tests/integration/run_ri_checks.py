import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.request


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _api_request(host: str, token: str, method: str, path: str, payload: dict | None = None) -> dict:
    url = f"{host.rstrip('/')}{path}"
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(url=url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Databricks API error {exc.code}: {body}") from exc


def _execute_sql(host: str, token: str, warehouse_id: str, sql_text: str) -> dict:
    submit = _api_request(
        host,
        token,
        "POST",
        "/api/2.0/sql/statements",
        {
            "warehouse_id": warehouse_id,
            "statement": sql_text,
            "wait_timeout": "50s",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        },
    )

    statement_id = submit.get("statement_id")
    if not statement_id:
        raise RuntimeError(f"Missing statement_id in response: {submit}")

    state = (submit.get("status") or {}).get("state")
    while state in {"PENDING", "RUNNING"}:
        time.sleep(2)
        submit = _api_request(host, token, "GET", f"/api/2.0/sql/statements/{statement_id}")
        state = (submit.get("status") or {}).get("state")

    if state != "SUCCEEDED":
        status = submit.get("status") or {}
        err = status.get("error") or {}
        msg = err.get("message") or submit
        raise RuntimeError(f"RI check SQL failed with state={state}: {msg}")

    return submit


def _rows_from_result(result_payload: dict) -> list[dict]:
    manifest = result_payload.get("manifest") or {}
    schema = (manifest.get("schema") or {}).get("columns") or []
    col_names = [c.get("name") for c in schema]

    result = result_payload.get("result") or {}
    data_array = result.get("data_array") or []

    rows = []
    for arr in data_array:
        row = {}
        for idx, name in enumerate(col_names):
            row[name] = arr[idx] if idx < len(arr) else None
        rows.append(row)
    return rows


def main() -> int:
    host = _require_env("DATABRICKS_HOST")
    token = _require_env("DATABRICKS_TOKEN")
    warehouse_id = _require_env("DATABRICKS_WAREHOUSE_ID")

    sql_file = pathlib.Path(__file__).with_name("ri_checks.sql")
    sql_text = sql_file.read_text(encoding="utf-8")

    payload = _execute_sql(host, token, warehouse_id, sql_text)
    rows = _rows_from_result(payload)

    if not rows:
        print("No RI check rows returned; treating as failure.")
        return 1

    failures = []
    print("Referential Integrity Check Results")
    for row in rows:
        check_name = row.get("check_name")
        fact_rows = int(row.get("fact_rows") or 0)
        unmatched = int(row.get("unmatched_rows") or 0)
        pct = row.get("unmatched_pct")
        print(f"- {check_name}: fact_rows={fact_rows}, unmatched_rows={unmatched}, unmatched_pct={pct}")
        if unmatched > 0:
            failures.append((check_name, unmatched))

    if failures:
        print("RI check failed. Unmatched keys detected:")
        for check_name, unmatched in failures:
            print(f"  * {check_name}: unmatched_rows={unmatched}")
        return 1

    print("All RI checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
