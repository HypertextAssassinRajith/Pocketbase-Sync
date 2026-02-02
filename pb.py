# filepath: e:\Code\Pocketbase Sync\pb.py
"""Import items from an Excel file into PocketBase Form_Items.

Usage examples:
  python pb.py --excel "Wastage Report Data.xlsx" --sheet Sheet1 --dry-run
  python pb.py --excel "Wastage Report Data.xlsx" --sheet Sheet1

Default behavior uses public access (no auth). Use --auth admin with --email/--password if needed.
"""
from __future__ import annotations

import argparse
import os
import re
from typing import Any, Dict, List, Optional

import pandas as pd
import requests


def _norm_col(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name).strip().lower())


def _pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    norm = {_norm_col(c): c for c in df.columns}
    for cand in candidates:
        key = _norm_col(cand)
        if key in norm:
            return norm[key]
    return None


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

    def create(self, collection: str, data: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/api/collections/{collection}/records"
        resp = self.session.post(url, json=data, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def update(self, collection: str, record_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/api/collections/{collection}/records/{record_id}"
        resp = self.session.patch(url, json=data, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def find_by_field(self, collection: str, field: str, value: str, per_page: int = 1) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/api/collections/{collection}/records"
        filt = f'{field}="{value}"'
        params = {"filter": filt, "perPage": per_page}
        resp = self.session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        return data.get("items") if isinstance(data, dict) else []


def main() -> int:
    parser = argparse.ArgumentParser(description="Import items into PocketBase Form_Items from Excel.")
    parser.add_argument("--excel", default="Wastage Report Data.xlsx", help="Path to Excel file (default: items.xlsx)")
    parser.add_argument("--sheet", default=None, help="Excel sheet name (default: first sheet)")
    parser.add_argument("--base-url", default=os.environ.get("PB_BASE_URL", "https://bkcus.samanaladanuma.lk"))
    parser.add_argument("--collection", default="Form_Items", help="Target PocketBase collection")
    parser.add_argument("--email", default=os.environ.get("PB_ADMIN_EMAIL"), help="PocketBase admin email")
    parser.add_argument("--password", default=os.environ.get("PB_ADMIN_PASSWORD"), help="PocketBase admin password")
    parser.add_argument(
        "--auth",
        choices=["public", "admin"],
        default=os.environ.get("PB_AUTH", "public"),
        help="Auth mode: public (default) or admin",
    )
    parser.add_argument("--relation-id", default="tv03ya5d5h53iq3", help="Relation record id to add to Form field")
    parser.add_argument("--dry-run", action="store_true", help="Print payloads without creating records")
    args = parser.parse_args()

    excel_path = args.excel
    if not os.path.isabs(excel_path):
        excel_path = os.path.join(os.getcwd(), excel_path)

    read_kwargs: Dict[str, Any] = {"dtype": str}
    if args.sheet is not None:
        read_kwargs["sheet_name"] = args.sheet

    x = pd.read_excel(excel_path, **read_kwargs)
    if isinstance(x, dict):
        if not x:
            raise SystemExit("No sheets found in the Excel file.")
        first_sheet = next(iter(x.keys()))
        df = x[first_sheet]
    else:
        df = x

    if not hasattr(df, "columns"):
        raise SystemExit("Failed to load Excel as a table. Check --sheet name and the file format.")

    # Detect likely columns from the Wastage Report screenshot: 'Code' and 'Item'
    col_code = _pick_col(df, ["Item_Code", "Item Code", "Code", "ID", "Id"])
    col_name = _pick_col(df, ["Item_Name", "Item Name", "Item", "Name"])
    col_unit = _pick_col(df, ["Unit", "UOM", "UoM"])

    if not col_name and not col_code:
        raise SystemExit("Excel must contain at least a Code or Item/Name column.")

    pb = PocketBaseClient(args.base_url)
    if not args.dry_run:
        if str(args.auth).lower() == "admin":
            if not args.email or not args.password:
                raise SystemExit("Admin credentials required for --auth admin (use --email/--password or PB_ADMIN_EMAIL/PB_ADMIN_PASSWORD).")
            pb.login_admin(args.email, args.password)
        else:
            pb.clear_auth()

    relation_id = str(args.relation_id)
    created = 0
    updated = 0

    for idx, row in df.iterrows():
        item_name = str(row[col_name]).strip() if col_name and row.get(col_name) is not None else ""
        item_code = str(row[col_code]).strip() if col_code and row.get(col_code) is not None else ""
        unit = str(row[col_unit]).strip() if col_unit and row.get(col_unit) is not None else ""

        # Skip rows without useful data
        if not item_name and not item_code:
            continue

        # Look up existing by Item_Code if available, otherwise try by name
        existing = []
        try:
            if item_code:
                existing = pb.find_by_field(args.collection, "Item_Code", item_code)
            elif item_name:
                # fallback lookup by Item_Name
                existing = pb.find_by_field(args.collection, "Item_Name", item_name)
        except requests.HTTPError as e:
            print(f"Lookup failed for row {idx+1} code={item_code} name={item_name}: {e}")

        if existing:
            rec = existing[0]
            rec_id = rec.get("id")
            current_form = rec.get("Form") or []
            if not isinstance(current_form, list):
                current_form = [current_form]
            if relation_id in current_form:
                print(f"Row={idx+1} Item_Code={item_code} already has relation {relation_id}; skipping.")
            else:
                new_form = current_form + [relation_id]
                payload = {"Form": new_form}
                if args.dry_run:
                    print(f"[DRY-RUN] Would update id={rec_id} with Form={new_form}")
                else:
                    try:
                        pb.update(args.collection, rec_id, payload)
                        updated += 1
                        print(f"Updated: id={rec_id} Item_Code={item_code} added relation {relation_id}")
                    except requests.HTTPError as e:
                        print(f"Failed to update id={rec_id} Item_Code={item_code}: {e}")
        else:
            data: Dict[str, Any] = {
                "Item_Name": item_name,
                "Item_Code": item_code,
            }
            if unit:
                data["Unit"] = unit
            # always include relation id if provided
            if relation_id:
                data["Form"] = [relation_id]

            if args.dry_run:
                print(f"[DRY-RUN] row={idx+1}: create {data}")
            else:
                try:
                    rec = pb.create(args.collection, data)
                    created += 1
                    print(f"Created: id={rec.get('id')} Item_Code={item_code}")
                except requests.HTTPError as e:
                    body = ""
                    try:
                        body = e.response.text if e.response is not None else ""
                    except Exception:
                        pass
                    print(f"Failed create row={idx+1} Item_Code={item_code}: {e} {body}")

    print(f"Done. Created {created} record(s). Updated {updated} record(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
