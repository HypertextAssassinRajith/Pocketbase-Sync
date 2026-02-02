# filepath: e:\Code\Pocketbase Sync\item.py

"""Import items from an Excel file into PocketBase.

Expected Excel columns (case-insensitive; spaces/underscores ignored):
- Item_Name (or Name)
- Item_Code (or Code)
- Unit
Optional:
- Form (comma-separated relation record ids) or Form_Id / Form_Ids

Usage (PowerShell):
  python item.py --excel items.xlsx --base-url https://bkcus.samanaladanuma.lk --email <your_email>@gmail.com --password ""

Notes:
- Requires: pip install pandas openpyxl requests
- PocketBase admin/auth login is required to create records unless your collection rules allow anonymous create.
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


def _split_relations(val: Any) -> List[str]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    s = str(val).strip()
    if not s:
        return []
    parts = [p.strip() for p in re.split(r"[,;\n\t]+", s) if p.strip()]
    return parts


def main() -> int:
    parser = argparse.ArgumentParser(description="Read items.xlsx and import items into PocketBase Form_Items.")
    parser.add_argument("--excel", default="items.xlsx", help="Path to Excel file (default: items.xlsx)")
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
    parser.add_argument("--dry-run", action="store_true", help="Print payloads without creating records")
    args = parser.parse_args()

    excel_path = args.excel
    if not os.path.isabs(excel_path):
        excel_path = os.path.join(os.getcwd(), excel_path)

    # pandas can return a dict of DataFrames when multiple sheets are read;
    # normalize to a single DataFrame.
    read_kwargs: Dict[str, Any] = {"dtype": str}
    if args.sheet is not None:
        read_kwargs["sheet_name"] = args.sheet

    x = pd.read_excel(excel_path, **read_kwargs)
    if isinstance(x, dict):
        if not x:
            raise SystemExit("No sheets found in the Excel file.")
        # If --sheet wasn't specified, default to the first sheet.
        first_sheet_name = next(iter(x.keys()))
        df = x[first_sheet_name]
    else:
        df = x

    if not hasattr(df, "columns"):
        raise SystemExit("Failed to load Excel as a table. Check --sheet name and the file format.")

    col_name = _pick_col(df, ["Item_Name", "Item Name", "Name"])
    col_code = _pick_col(df, ["Item_Code", "Item Code", "Code"])
    col_unit = _pick_col(df, ["Unit", "UOM", "UoM"])
    col_form = _pick_col(df, ["Form", "Form_Id", "Form_ID", "Form_Ids", "Form_IDs"])

    missing = [
        label
        for label, col in [("Item_Name", col_name), ("Item_Code", col_code), ("Unit", col_unit)]
        if col is None
    ]
    if missing:
        raise SystemExit(f"Missing required column(s) in Excel: {', '.join(missing)}")

    pb = PocketBaseClient(args.base_url)
    if not args.dry_run:
        if str(args.auth).lower() == "admin":
            if not args.email or not args.password:
                raise SystemExit(
                    "Admin credentials required for --auth admin (use --email/--password or PB_ADMIN_EMAIL/PB_ADMIN_PASSWORD)."
                )
            pb.login_admin(args.email, args.password)
        else:
            pb.clear_auth()

    created = 0
    for idx, row in df.iterrows():
        item_name = str(row[col_name]).strip() if row.get(col_name) is not None else ""
        item_code = str(row[col_code]).strip() if row.get(col_code) is not None else ""
        unit = str(row[col_unit]).strip() if row.get(col_unit) is not None else ""

        if not item_name and not item_code:
            continue

        data: Dict[str, Any] = {
            "Item_Name": item_name,
            "Item_Code": item_code,
            "Unit": unit,
        }

        if col_form is not None:
            rels = _split_relations(row.get(col_form))
            if rels:
                data["Form"] = rels

        if args.dry_run:
            print(f"[DRY-RUN] row={idx+1}: {data}")
            continue

        try:
            rec = pb.create(args.collection, data)
            created += 1
            print(f"Created: id={rec.get('id')} Item_Code={item_code}")
        except requests.HTTPError as e:
            # Keep going on row failures
            body = ""
            try:
                body = e.response.text if e.response is not None else ""
            except Exception:
                pass
            print(f"Failed row={idx+1} Item_Code={item_code}: {e} {body}")

    print(f"Done. Created {created} record(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
