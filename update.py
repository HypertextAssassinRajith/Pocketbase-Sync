# filepath: e:\Code\Pocketbase Sync\update.py
"""Update existing PocketBase Form_Items records using a local update.json export.

Reads update.json (PocketBase list response shape) and appends a Form relation id
(e.g. y9180izn3z395a9) to each item record if it isn't already present.

Usage:
  python update.py --json update.json --base-url https://bkcus.samanaladanuma.lk --dry-run
  python update.py --json update.json --base-url https://bkcus.samanaladanuma.lk auth admin --email YOUR_ADMIN_EMAIL --password YOUR_ADMIN_PASSWORD

Auth:
  Default is public (no auth). If your rules require it:
    python update.py --auth admin --email ... --password ...
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, List

import requests


class PocketBaseClient:
    def __init__(self, base_url: str, timeout: int = 60):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def login_admin(self, email: str, password: str) -> None:
        url = f"{self.base_url}/api/admins/auth-with-password"
        resp = self.session.post(url, json={"identity": email, "password": password}, timeout=self.timeout)
        resp.raise_for_status()
        token = resp.json().get("token")
        if not token:
            raise RuntimeError("Admin login succeeded but token missing.")
        self.session.headers["Authorization"] = f"Bearer {token}"

    def clear_auth(self) -> None:
        self.session.headers.pop("Authorization", None)

    def update(self, collection: str, record_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/api/collections/{collection}/records/{record_id}"
        resp = self.session.patch(url, json=data, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()


def _as_list(val: Any) -> List[str]:
    if val is None:
        return []
    if isinstance(val, list):
        return [str(v) for v in val if str(v).strip()]
    s = str(val).strip()
    return [s] if s else []


def main() -> int:
    parser = argparse.ArgumentParser(description="Append a Form relation id to records listed in update.json")
    parser.add_argument("--json", dest="json_path", default="update.json", help="Path to update.json")
    parser.add_argument("--base-url", default=os.environ.get("PB_BASE_URL", "https://bkcus.samanaladanuma.lk"))
    parser.add_argument("--collection", default="Form_Items", help="Target collection (default Form_Items)")
    parser.add_argument("--add-form-id", default="y9180izn3z395a9", help="Form relation record id to append")
    parser.add_argument(
        "--auth",
        choices=["public", "admin"],
        default=os.environ.get("PB_AUTH", "public"),
        help="Auth mode: public (default) or admin",
    )
    parser.add_argument("--email", default=os.environ.get("PB_ADMIN_EMAIL"), help="PocketBase admin email")
    parser.add_argument("--password", default=os.environ.get("PB_ADMIN_PASSWORD"), help="PocketBase admin password")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    with open(args.json_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    items = payload.get("items")
    if not isinstance(items, list):
        raise SystemExit("update.json must contain a top-level 'items' array.")

    pb = PocketBaseClient(args.base_url)
    if not args.dry_run:
        if str(args.auth).lower() == "admin":
            if not args.email or not args.password:
                raise SystemExit("Admin credentials required for --auth admin.")
            pb.login_admin(args.email, args.password)
        else:
            pb.clear_auth()

    add_id = str(args.add_form_id).strip()
    if not add_id:
        raise SystemExit("--add-form-id cannot be empty")

    updated = 0
    skipped = 0
    failed = 0

    for i, it in enumerate(items, start=1):
        if not isinstance(it, dict):
            skipped += 1
            continue

        rec_id = str(it.get("id") or "").strip()
        if not rec_id:
            skipped += 1
            continue

        current_form = _as_list(it.get("Form"))
        if add_id in current_form:
            print(f"{i}: id={rec_id} already has {add_id}; skip")
            skipped += 1
            continue

        new_form = current_form + [add_id]
        patch = {"Form": new_form}

        if args.dry_run:
            print(f"{i}: [DRY-RUN] update id={rec_id} Form={new_form}")
            continue

        try:
            pb.update(args.collection, rec_id, patch)
            updated += 1
            print(f"{i}: updated id={rec_id}")
        except requests.HTTPError as e:
            failed += 1
            body = ""
            try:
                body = e.response.text if e.response is not None else ""
            except Exception:
                pass
            print(f"{i}: FAILED id={rec_id}: {e} {body}")

    print(f"Done. Updated={updated} Skipped={skipped} Failed={failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
