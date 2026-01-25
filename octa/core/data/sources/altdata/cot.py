from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Mapping


@dataclass
class CotSource:
    cfg: Mapping[str, Any]
    name: str = "cot"

    def __post_init__(self) -> None:
        self.enabled = bool(self.cfg.get("enabled", False))

    def cache_key(self, *, asof: date) -> str:
        return f"{self.name}_{asof.isoformat()}"

    def fetch_raw(self, *, asof: date, allow_net: bool) -> Mapping[str, Any] | None:
        if not allow_net:
            return None
        targets = _normalize_targets(self.cfg.get("targets"))
        if not targets:
            return None
        window_days = int(self.cfg.get("window_days", 730))
        report_start = asof - timedelta(days=window_days)
        release_offset_days = int(self.cfg.get("release_offset_days", 3))
        release_hour_utc = int(self.cfg.get("release_hour_utc", 20))

        raw_rows: list[dict[str, Any]] = []
        for year in {asof.year, asof.year - 1}:
            data = _fetch_bytes(_year_url(year))
            if not data:
                continue
            raw_rows.extend(_parse_cot_zip(data))

        if not raw_rows:
            return None

        rows_out: list[dict[str, Any]] = []
        for row in raw_rows:
            market_name = row.get("market_name")
            report_date = row.get("report_date")
            if not market_name or not report_date:
                continue
            report_dt = _parse_date(report_date)
            if report_dt is None:
                continue
            if report_dt.date() < report_start or report_dt.date() > asof:
                continue
            target_id = _match_target(market_name, targets)
            if not target_id:
                continue
            release_ts = report_dt + timedelta(days=release_offset_days)
            release_ts = release_ts.replace(hour=release_hour_utc, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
            rows_out.append(
                {
                    "market_id": target_id,
                    "market_name": market_name,
                    "report_date": report_dt.date().isoformat(),
                    "release_ts": release_ts.isoformat(),
                    "noncommercial_long": row.get("noncommercial_long"),
                    "noncommercial_short": row.get("noncommercial_short"),
                    "open_interest": row.get("open_interest"),
                }
            )

        if not rows_out:
            return None
        rows_out.sort(key=lambda r: (r.get("market_id"), r.get("report_date")))
        return {"rows": rows_out, "targets": [t["id"] for t in targets]}

    def normalize(self, raw: Mapping[str, Any]) -> Mapping[str, Any]:
        return raw


def _year_url(year: int) -> str:
    return f"https://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip"


def _fetch_bytes(url: str) -> bytes | None:
    headers = {"User-Agent": "OCTA/altdata", "Accept": "application/zip"}
    try:
        import httpx  # type: ignore

        with httpx.Client(timeout=15.0) as client:
            resp = client.get(url, headers=headers)
            if resp.status_code == 200:
                return resp.content
            return None
    except Exception:
        pass
    try:
        from urllib.request import Request, urlopen

        req = Request(url, headers=headers)
        with urlopen(req, timeout=15.0) as resp:
            if getattr(resp, "status", 200) != 200:
                return None
            return resp.read()
    except Exception:
        return None


def _parse_cot_zip(data: bytes) -> list[dict[str, Any]]:
    import csv
    import io
    import zipfile

    rows: list[dict[str, Any]] = []
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            names = zf.namelist()
            if not names:
                return rows
            with zf.open(names[0]) as fp:
                text = fp.read().decode("utf-8", errors="ignore")
    except Exception:
        return rows

    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        market = row.get("Market_and_Exchange_Names") or row.get("Market and Exchange Names")
        report_date = row.get("Report_Date_as_YYYY-MM-DD") or row.get("Report Date as YYYY-MM-DD")
        if not market or not report_date:
            continue
        rows.append(
            {
                "market_name": market.strip(),
                "report_date": str(report_date).strip(),
                "noncommercial_long": _as_float(row.get("Noncommercial_Long_All")),
                "noncommercial_short": _as_float(row.get("Noncommercial_Short_All")),
                "open_interest": _as_float(row.get("Open_Interest_All")),
            }
        )
    return rows


def _normalize_targets(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        target_id = str(item.get("id", "")).strip().lower()
        if not target_id:
            continue
        candidates = item.get("candidates") or []
        cand_list = []
        for c in candidates:
            s = str(c).strip().lower()
            if s:
                cand_list.append(s)
        if not cand_list:
            continue
        out.append({"id": target_id, "candidates": cand_list})
    return out


def _match_target(market_name: str, targets: list[dict[str, Any]]) -> str | None:
    name = market_name.lower()
    for target in targets:
        for cand in target["candidates"]:
            if cand in name:
                return target["id"]
    return None


def _parse_date(value: str) -> datetime | None:
    try:
        dt = datetime.fromisoformat(str(value))
    except Exception:
        try:
            dt = datetime.strptime(str(value), "%Y-%m-%d")
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None
