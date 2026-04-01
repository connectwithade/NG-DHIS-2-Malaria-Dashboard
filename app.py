"""
DHIS2 Nigeria Malaria Dashboard — Flask Backend Proxy
Handles authentication server-side and exposes a curated malaria catalog.
"""
import json
import os
import time
import csv
from datetime import datetime, timezone
from typing import Optional

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from urllib3.util.retry import Retry
from werkzeug.exceptions import HTTPException

app = Flask(__name__, static_folder="static")
CORS(app)

DHIS2_BASE = os.getenv("DHIS2_BASE", "https://dhis2nigeria.org.ng/dhis").rstrip("/")
<<<<<<< HEAD
DHIS2_USER = os.getenv("DHIS2_USER", "").strip()
DHIS2_PASS = os.getenv("DHIS2_PASS", "").strip()
=======
DHIS2_USER = os.getenv("DHIS2_USER", "Adedamola_Akinola")
DHIS2_PASS = os.getenv("DHIS2_PASS", "#Crownstar2")
>>>>>>> eb49461 (updated files)
REQUEST_TIMEOUT = int(os.getenv("DHIS2_TIMEOUT", "30"))

session = requests.Session()
retry = Retry(
    total=3,
    connect=3,
    read=3,
    backoff_factor=0.5,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET",),
)
adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)
session.mount("http://", adapter)
session.headers.update({"Accept": "application/json", "User-Agent": "dhis2-malaria-dashboard/1.0"})

# Simple in-memory cache (key → {data, ts})
_cache: dict = {}
CACHE_TTL = 3600  # 1 hour
CACHE_MAX_ENTRIES = int(os.getenv("CACHE_MAX_ENTRIES", "32"))
CACHE_MAX_PAYLOAD_BYTES = int(os.getenv("CACHE_MAX_PAYLOAD_BYTES", "500000"))
LINKAGE_FILE = os.path.join(os.path.dirname(__file__), "malaria_indicator_linkage_live.csv")
SYNC_STATE_FILE = os.path.join(os.path.dirname(__file__), "dhis2_sync_state.json")
VERIFIED_ORG_UNIT_LEVELS = [
    {"id": "DPo33reFh7g", "name": "Federal", "level": 1},
    {"id": "sJ6qnMn6IZ8", "name": "State", "level": 2},
    {"id": "KKv44WNkKMG", "name": "LGA", "level": 3},
    {"id": "pW3jcRQ1XX7", "name": "Ward", "level": 4},
    {"id": "GQDiw63mGn3", "name": "Facility", "level": 5},
]
VERIFIED_ROOT_ORG_UNITS = [
    {"id": "s5DPBsdoE8b", "name": "ng Federal Government", "level": 1, "parent": None},
]
VERIFIED_ORG_UNIT_GROUP_SETS = []

CURATED_INDICATOR_ROWS = [
    ("ANC 1st Visit GA < 20wks", "p6aVCk9aN6S"),
    ("ANC 1st Visit GA >= 20wks", "p6aVCk9aN6S"),
    ("ANC 4th visit", "ZS5cAlmGjIE"),
    ("ANC 8th Visit", "bGXqql9dyk4"),
    ("ANC Attendance 10 - 14yrs", "SCDCoP6l9vz"),
    ("ANC Attendance 15 - 19yrs", "SCDCoP6l9vz"),
    ("ANC Attendance 20 - 34yrs", "SCDCoP6l9vz"),
    ("ANC Attendance 35 - 49yrs", "SCDCoP6l9vz"),
    ("ANC Attendance >= 50yrs", "SCDCoP6l9vz"),
    ("Children <5 yrs who received LLIN", "ZFgiCO6nyAP"),
    ("General Attendance Female 0-28d", "Jc1WjNKrObY"),
    ("General Attendance Female 29d - 11m", "Jc1WjNKrObY"),
    ("General Attendance Female 12-59m", "Jc1WjNKrObY"),
    ("General Attendance Female 5-9yrs", "Jc1WjNKrObY"),
    ("General Attendance Female 10-19 yrs", "Jc1WjNKrObY"),
    ("General Attendance Female 20yrs+", "Jc1WjNKrObY"),
    ("General Attendance Male 0-28d", "Jc1WjNKrObY"),
    ("General Attendance Male 29d - 11m", "Jc1WjNKrObY"),
    ("General Attendance Male 12-59m", "Jc1WjNKrObY"),
    ("General Attendance Male 5-9yrs", "Jc1WjNKrObY"),
    ("General Attendance Male 10-19 yrs", "Jc1WjNKrObY"),
    ("General Attendance Male 20yrs+", "Jc1WjNKrObY"),
    ("IPT1p", "pUZ0BKgsAXp"),
    ("IPT2p", "fP1E4qRJ57a"),
    ("IPT3p", "HGIz6q1NXIS"),
    ("IPT>=4", "oNUnkTiGK75"),
    ("Malaria confirmed pregnant women", "DyMyl4NeDxz"),
    ("Out-patient Attendance Female 0-28d", "KWJ3cSuyzs4"),
    ("Out-patient Attendance Female 29d - 11m", "KWJ3cSuyzs4"),
    ("Out-patient Attendance Female 12-59m", "KWJ3cSuyzs4"),
    ("Out-patient Attendance Female 5-9yrs", "KWJ3cSuyzs4"),
    ("Out-patient Attendance Female 10-19 yrs", "KWJ3cSuyzs4"),
    ("Out-patient Attendance Female 20yrs+", "KWJ3cSuyzs4"),
    ("Out-patient Attendance Male 0-28d", "KWJ3cSuyzs4"),
    ("Out-patient Attendance Male 29d - 11m", "KWJ3cSuyzs4"),
    ("Out-patient Attendance Male 12-59m", "KWJ3cSuyzs4"),
    ("Out-patient Attendance Male 5-9yrs", "KWJ3cSuyzs4"),
    ("Out-patient Attendance Male 10-19 yrs", "KWJ3cSuyzs4"),
    ("Out-patient Attendance Male 20yrs+", "KWJ3cSuyzs4"),
    ("PW who received LLIN", "w6nOgEFHWMG"),
    ("Patients Admitted Female 0-28d", "rXoqTMxWvfR"),
    ("Patients Admitted Female 29d - 11m", "rXoqTMxWvfR"),
    ("Patients Admitted Female 12-59m", "rXoqTMxWvfR"),
    ("Patients Admitted Female 5-9yrs", "rXoqTMxWvfR"),
    ("Patients Admitted Female 10-19 yrs", "rXoqTMxWvfR"),
    ("Patients Admitted Female 20yrs+", "rXoqTMxWvfR"),
    ("Patients Admitted Male 0-28d", "rXoqTMxWvfR"),
    ("Patients Admitted Male 29d - 11m", "rXoqTMxWvfR"),
    ("Patients Admitted Male 12-59m", "rXoqTMxWvfR"),
    ("Patients Admitted Male 5-9yrs", "rXoqTMxWvfR"),
    ("Patients Admitted Male 10-19 yrs", "rXoqTMxWvfR"),
    ("Patients Admitted Male 20yrs+", "rXoqTMxWvfR"),
    ("Persons Clinically diagnosed with Malaria treated with ACT <5yrs", "nDKVffS3ijX"),
    ("Persons Clinically diagnosed with Malaria treated with ACT >=5yrs (excl PW)", "nDKVffS3ijX"),
    ("Persons Clinically diagnosed with Malaria treated with ACT PW", "nDKVffS3ijX"),
    ("Persons fever tested by RDT <5yrs", "r6WOvUlcQm6"),
    ("Persons fever tested by RDT >=5yrs (excl PW)", "r6WOvUlcQm6"),
    ("Persons fever tested by RDT PW", "r6WOvUlcQm6"),
    ("Persons fever tested by Microscopy <5yrs", "k2Cpuz9BQtD"),
    ("Persons fever tested by Microscopy >=5yrs (excl PW)", "k2Cpuz9BQtD"),
    ("Persons fever tested by Microscopy PW", "k2Cpuz9BQtD"),
    ("Positive by Microscopy <5yrs", "G7iWnz9RMy9"),
    ("Positive by Microscopy >=5yrs (excl PW)", "G7iWnz9RMy9"),
    ("Positive by Microscopy PW", "G7iWnz9RMy9"),
    ("Positive by RDT <5yrs", "GEd2F6skCpT"),
    ("Positive by RDT >=5yrs (excl PW)", "GEd2F6skCpT"),
    ("Positive by RDT PW", "GEd2F6skCpT"),
    ("Confirmed Uncomplicated treated with ACT <5yrs", "ouzURM9c1FI"),
    ("Confirmed Uncomplicated treated with ACT >=5yrs (excl PW)", "ouzURM9c1FI"),
    ("Confirmed Uncomplicated treated with ACT PW", "ouzURM9c1FI"),
    ("Treated with other antimalarials <5yrs", "M1kGBHgqGvv"),
    ("Treated with other antimalarials >=5yrs (excl PW)", "M1kGBHgqGvv"),
    ("Treated with other antimalarials PW", "M1kGBHgqGvv"),
    ("Severe Malaria pre-referral treatment <5yrs", "fFe9CPuM7Xz"),
    ("Severe Malaria pre-referral treatment >=5yrs (excl PW)", "fFe9CPuM7Xz"),
    ("Severe Malaria pre-referral treatment PW", "fFe9CPuM7Xz"),
    ("Severe Malaria treated with Artesunate inj <5yrs", "Nz9eV0G9qTW"),
    ("Severe Malaria treated with Artesunate inj >=5yrs (excl PW)", "Nz9eV0G9qTW"),
    ("Severe Malaria treated with Artesunate inj PW", "Nz9eV0G9qTW"),
    ("Severe Malaria other pre-referral treatment <5yrs", "ilSvQ0EwZVJ"),
    ("Severe Malaria other pre-referral treatment >=5yrs (excl PW)", "ilSvQ0EwZVJ"),
    ("Severe Malaria other pre-referral treatment PW", "ilSvQ0EwZVJ"),
    ("Clinically diagnosed Malaria <5yrs", "K9K1G0XX4ax"),
    ("Clinically diagnosed Malaria >=5yrs (excl PW)", "K9K1G0XX4ax"),
    ("Clinically diagnosed Malaria PW", "K9K1G0XX4ax"),
    ("Confirmed uncomplicated Malaria <5yrs", "HdtaLx63988"),
    ("Confirmed uncomplicated Malaria >=5yrs (excl PW)", "HdtaLx63988"),
    ("Confirmed uncomplicated Malaria PW", "HdtaLx63988"),
    ("Persons with fever <5yrs", "m5gTF1jKQjl"),
    ("Persons with fever >=5yrs (excl PW)", "m5gTF1jKQjl"),
    ("Persons with fever PW", "m5gTF1jKQjl"),
    ("Pregnant women clinical malaria", "f0YCEriaGQC"),
    ("Severe Malaria cases seen <5yrs", "nrql7kywMLM"),
    ("Severe Malaria cases seen >=5yrs (excl PW)", "nrql7kywMLM"),
    ("Severe Malaria cases seen PW", "nrql7kywMLM"),
    ("% Confirmed Malaria (RDT or Microscopy)", "QeQ7Axn5JZT"),
    ("% Confirmed uncomplicated Malaria", "IDvOsgRb9vK"),
    ("% Under 5 receiving LLIN", "JwWA5uLA3NV"),
    ("% of Antenatal 1st Visits receiving IPT1", "pPTpnHWnpTu"),
    ("% of Antenatal 1st Visits receiving LLIN", "PU87QXkUyyf"),
    ("% of Antimalaria with mobile authentication service", "LbRUm5ZRqHY"),
    ("% of Fever cases Tested with Microscopy", "C2uGy964KNe"),
    ("% of Fever cases Tested with RDT", "quCqGMTBTHi"),
    ("% of Fever cases tested positive with Microscopy", "WAywN48snnd"),
    ("% of Fever cases tested positive with RDT", "l2eIpdluuTI"),
    ("% of Malaria cases clinically diagnosed", "vYRF9XQAt6t"),
    ("% of Under 5 deaths attributable to Malaria", "ktsIGwM8eKk"),
    ("% of all Antenatal care clients receiving malaria IPT", "hIPXrhZupqY"),
    ("% of clinically diagnosed malaria given ACT", "PFHeLBDLciz"),
    ("% of confirmed uncomplicated malaria given ACT", "acmdhxLWZb2"),
    ("% of confirmed uncomplicated malaria given other antimalaria", "i0gr75yDEx0"),
    ("% of suspected malaria receiving parasitological test", "mWT59jwbd9c"),
    ("ACT Given - Total", "DHYTgg54tAE"),
    ("Fever Testing Rate", "I0i6aUfBxii"),
    ("IPTp1 Coverage (institutional)", "hohHPLpnfnU"),
    ("IPTp2 Coverage (institutional)", "LiwGUzxMbRV"),
    ("IPTp3 Coverage (institutional)", "kxWKowbQ1si"),
    ("IPTp>=4 Coverage (institutional)", "VaPXm80AYwo"),
    ("LLIN given - Total", "IZgNib2WHSD"),
    ("Persons with Severe Malaria treated with Artesunate injection", "im9L3qLI4g9"),
    ("Persons with Severe Malaria treated with other pre referral treatment", "oZ892qhp8kg"),
    ("Test Positivity Rate(TPR) (Microscopy)", "vtKan5xpKdh"),
    ("Test Positivity Rate(TPR) (RDT)", "uiA07P72s2Z"),
]


def get_category(label: str) -> str:
    lower = label.lower()
    if any(token in lower for token in ("llin", "ipt", "anc")):
        return "prevention"
    if any(token in lower for token in ("treated", "act", "artesunate", "antimalarial", "pre-referral")):
        return "treatment"
    if any(token in lower for token in ("attendance", "admitted", "admission", "death", "mortality", "incidence")):
        return "impact"
    if any(token in lower for token in (
        "rdt",
        "microscopy",
        "positivity",
        "tested",
        "testing rate",
        "parasitological",
        "confirmed malaria",
        "clinically diagnosed",
        "confirmed uncomplicated malaria",
        "persons with fever",
        "severe malaria cases seen",
    )):
        return "diagnosis"
    return "impact"


def build_curated_catalog():
    catalog = {"prevention": [], "diagnosis": [], "treatment": [], "impact": []}
    flat = []
    by_query_uid = {}
    linkage_rows = None

    if os.path.exists(LINKAGE_FILE):
        with open(LINKAGE_FILE, newline="", encoding="utf-8") as handle:
            linkage_rows = list(csv.DictReader(handle))

    source_rows = linkage_rows or [
        {
            "clean_label": label,
            "uid_in_source": data_element_id,
            "matched_uid": data_element_id,
            "dhis2_object_type": "dataElement",
            "matched_query_type": "data_element_only",
        }
        for label, data_element_id in CURATED_INDICATOR_ROWS
    ]

    for index, row in enumerate(source_rows, start=1):
        label = row["clean_label"]
        source_uid = row["uid_in_source"]
        query_uid = row.get("matched_uid") or source_uid
        category = get_category(label)
        item = {
            "id": f"malaria-{index:03d}",
            "dx": query_uid,
            "source_uid": source_uid,
            "name": label,
            "short": label,
            "category": category,
            "dhis2_object_type": row.get("dhis2_object_type", "dataElement"),
            "matched_query_type": row.get("matched_query_type", "data_element_only"),
        }
        catalog[category].append(item)
        flat.append(item)
        by_query_uid.setdefault(query_uid, []).append(item)

    return catalog, flat, by_query_uid


DASHBOARD_INDICATORS, CURATED_INDICATORS, INDICATORS_BY_QUERY_UID = build_curated_catalog()


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def parse_server_datetime(server_date: Optional[str]):
    if not server_date:
        return datetime.now(timezone.utc)
    normalized = server_date.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.now(timezone.utc)


def build_period_options(server_date: Optional[str] = None, start_year: int = 2018):
    current = parse_server_datetime(server_date)
    months = []
    for month in range(1, 13):
        month_date = datetime(current.year, month, 1, tzinfo=current.tzinfo)
        months.append({
            "id": f"{month:02d}",
            "month": month,
            "name": month_date.strftime("%B"),
        })

    years = [
        {"id": str(year), "year": year, "name": str(year)}
        for year in range(start_year, current.year + 1)
    ]

    return {
        "server_date": current.date().isoformat(),
        "start_year": start_year,
        "current_year": current.year,
        "current_month": current.month,
        "years": years,
        "months": months,
    }


def default_sync_state():
    return {
        "status": "idle",
        "dhis2_base": DHIS2_BASE,
        "catalog_count": len(CURATED_INDICATORS),
        "unique_query_uids": len(INDICATORS_BY_QUERY_UID),
        "linkage_file": os.path.basename(LINKAGE_FILE),
        "linkage_loaded": os.path.exists(LINKAGE_FILE),
        "last_attempt_at": None,
        "last_success_at": None,
        "last_error": None,
        "dhis2_info": None,
        "validation_mode": None,
        "validated_uids": [],
    }


def load_sync_state():
    state = default_sync_state()
    if os.path.exists(SYNC_STATE_FILE):
        try:
            with open(SYNC_STATE_FILE, encoding="utf-8") as handle:
                persisted = json.load(handle)
            state.update(persisted)
        except Exception:
            state["last_error"] = "Sync state file could not be read"
    state["dhis2_base"] = DHIS2_BASE
    state["catalog_count"] = len(CURATED_INDICATORS)
    state["unique_query_uids"] = len(INDICATORS_BY_QUERY_UID)
    state["linkage_loaded"] = os.path.exists(LINKAGE_FILE)
    return state


def save_sync_state(state: dict):
    with open(SYNC_STATE_FILE, "w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)


def sample_sync_uids(limit: int = 6):
    preferred = [
        "p6aVCk9aN6S.zbr2vnRNwAW",
        "Jc1WjNKrObY.EeOmMSob3uj",
        "r6WOvUlcQm6.V86mzY788hR",
        "fFe9CPuM7Xz.V86mzY788hR",
        "IZgNib2WHSD",
        "QeQ7Axn5JZT",
    ]
    available = []
    for uid in preferred:
        if uid in INDICATORS_BY_QUERY_UID and uid not in available:
            available.append(uid)
    if len(available) < limit:
        for uid in INDICATORS_BY_QUERY_UID:
            if uid not in available:
                available.append(uid)
            if len(available) >= limit:
                break
    return available[:limit]


<<<<<<< HEAD
=======
def probe_dhis2_connection():
    """
    Confirm DHIS2 connectivity using lightweight endpoints first.
    `/api/system/info` can be slow on some installations, so we only use it
    as enrichment after a successful basic probe.
    """
    try:
        me = dhis2_get("/api/me", {
            "fields": "id,username",
        })
        probe = {
            "probe": "me",
            "user": {
                "id": me.get("id"),
                "username": me.get("username"),
            },
        }
    except RuntimeError:
        system = dhis2_get("/api/system/info")
        probe = {
            "probe": "system-info",
            "version": system.get("version"),
            "systemName": system.get("systemName"),
        }
        return system, probe

    try:
        system = dhis2_get("/api/system/info")
        system["probe"] = probe
        return system, probe
    except RuntimeError:
        return {
            "partial": True,
            "probe": probe,
        }, probe


>>>>>>> eb49461 (updated files)
def run_sync(validation_mode: str = "sample"):
    state = load_sync_state()
    state["status"] = "syncing"
    state["last_attempt_at"] = utc_now_iso()
    state["validation_mode"] = validation_mode
    state["last_error"] = None
    state["validated_uids"] = []
    save_sync_state(state)

    try:
<<<<<<< HEAD
        info = dhis2_get("/api/system/info")
=======
        info, probe = probe_dhis2_connection()
>>>>>>> eb49461 (updated files)
        state["dhis2_info"] = info
        state["status"] = "connected"
    except RuntimeError as exc:
        state["status"] = "offline"
        state["last_error"] = str(exc)
        save_sync_state(state)
        return state

    validation_uids = sample_sync_uids() if validation_mode == "sample" else list(INDICATORS_BY_QUERY_UID.keys())
    validated = []
    for uid in validation_uids:
        try:
            dhis2_get("/api/analytics.json", {
                "dimension": [f"dx:{uid}", "pe:LAST_12_MONTHS", "ou:LEVEL-1"],
                "displayProperty": "NAME",
                "outputIdScheme": "UID",
                "skipMeta": "true",
                "skipData": "false",
            })
            validated.append({"uid": uid, "ok": True})
        except RuntimeError as exc:
            validated.append({"uid": uid, "ok": False, "error": str(exc)})
            state["status"] = "degraded"

    state["validated_uids"] = validated
    state["last_success_at"] = utc_now_iso()
<<<<<<< HEAD
=======
    if state.get("dhis2_info", {}).get("partial"):
        state["status"] = "degraded"
        state["last_error"] = "Connected to DHIS2, but /api/system/info timed out"
>>>>>>> eb49461 (updated files)
    if state["status"] == "connected" and any(not item["ok"] for item in validated):
        state["status"] = "degraded"
        state["last_error"] = "One or more validation queries failed"
    save_sync_state(state)
    return state


def sync_response_payload():
    state = load_sync_state()
    if state["status"] == "idle":
        state = run_sync("sample")
    return state


def prune_cache(now: float):
    expired_keys = [
        key for key, value in _cache.items()
        if (now - value["ts"]) >= CACHE_TTL
    ]
    for key in expired_keys:
        _cache.pop(key, None)

    while len(_cache) >= CACHE_MAX_ENTRIES:
        oldest_key = min(_cache.items(), key=lambda item: item[1]["ts"])[0]
        _cache.pop(oldest_key, None)


def dhis2_get(path: str, params: dict = None):
    """Make an authenticated GET request to DHIS2 with caching."""
<<<<<<< HEAD
    if not DHIS2_USER or not DHIS2_PASS:
        raise RuntimeError("DHIS2 credentials are not configured. Set DHIS2_USER and DHIS2_PASS in Render environment variables.")
=======
>>>>>>> eb49461 (updated files)
    cache_key = path + json.dumps(params or {}, sort_keys=True)
    now = time.time()
    prune_cache(now)
    if cache_key in _cache and (now - _cache[cache_key]["ts"]) < CACHE_TTL:
        return _cache[cache_key]["data"]

    url = f"{DHIS2_BASE}{path}"
    try:
        resp = session.get(
            url,
            params=params,
            auth=HTTPBasicAuth(DHIS2_USER, DHIS2_PASS),
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"DHIS2 request failed for {path}: {exc}") from exc

    data = resp.json()
    try:
        payload_bytes = len(json.dumps(data))
    except (TypeError, ValueError):
        payload_bytes = CACHE_MAX_PAYLOAD_BYTES + 1

    if payload_bytes <= CACHE_MAX_PAYLOAD_BYTES:
        _cache[cache_key] = {"data": data, "ts": now}
    return data


def normalize_org_units(items):
    normalized = []
    for item in items or []:
        parent = item.get("parent")
        normalized.append({
            "id": item.get("id"),
            "name": item.get("displayName") or item.get("name"),
            "level": item.get("level"),
            "path": item.get("path"),
            "parent": {
                "id": parent.get("id"),
                "name": parent.get("displayName") or parent.get("name"),
            } if isinstance(parent, dict) else None,
        })
    return normalized


def json_error(message: str, status: int = 502, **extra):
    payload = {"error": message}
    payload.update(extra)
    return jsonify(payload), status


@app.errorhandler(Exception)
def handle_unexpected_error(exc):
    if isinstance(exc, HTTPException):
        return json_error(exc.description, exc.code or 500, route=request.path, error_type=exc.__class__.__name__)
    return json_error(
        str(exc) or "Unexpected server error",
        500,
        route=request.path,
        error_type=exc.__class__.__name__,
    )


# ── Meta endpoints ─────────────────────────────────────────────────────────────

@app.route("/api/meta/org-unit-levels")
def org_unit_levels():
    try:
        data = dhis2_get("/api/organisationUnitLevels.json", {
            "fields": "id,name,level",
            "order": "level:asc",
            "paging": "false",
        })
        return jsonify({
            "items": data.get("organisationUnitLevels", []),
            "source": "dhis2",
        })
    except Exception as exc:
        return jsonify({
            "items": VERIFIED_ORG_UNIT_LEVELS,
            "source": "verified-fallback",
            "warning": str(exc),
        })


@app.route("/api/meta/org-unit-groups")
def org_unit_groups():
    try:
        data = dhis2_get("/api/organisationUnitGroupSets.json", {
            "fields": "id,displayName,name,organisationUnitGroups[id,displayName,name]",
            "paging": "false",
        })
        group_sets = []
        for item in data.get("organisationUnitGroupSets", []):
            groups = [
                {
                    "id": group.get("id"),
                    "name": group.get("displayName") or group.get("name"),
                }
                for group in item.get("organisationUnitGroups", [])
            ]
            if groups:
                group_sets.append({
                    "id": item.get("id"),
                    "name": item.get("displayName") or item.get("name"),
                    "groups": groups,
                })
        return jsonify({
            "items": group_sets,
            "source": "dhis2",
        })
    except Exception as exc:
        return jsonify({
            "items": VERIFIED_ORG_UNIT_GROUP_SETS,
            "source": "verified-fallback",
            "warning": str(exc),
        })


@app.route("/api/meta/org-units")
def org_units():
    level = request.args.get("level", "1")
    parent = request.args.get("parent")
    parents = [item for item in request.args.get("parents", "").split(",") if item]
    descendants = request.args.get("descendants", "").lower() in {"1", "true", "yes"}
    group = request.args.get("group")
    try:
        params = {
            "fields": "id,displayName,level,path,parent[id,displayName]",
            "paging": "false",
        }
        filters = [f"level:eq:{level}"]
        if group:
            filters.append(f"organisationUnitGroups.id:eq:{group}")
        if parent and not parents:
            parents = [parent]

        if parents and not descendants:
            if len(parents) == 1:
                filters.append(f"parent.id:eq:{parents[0]}")
            params["filter"] = filters if len(filters) > 1 else filters[0]
            data = dhis2_get("/api/organisationUnits.json", params)
            items = normalize_org_units(data.get("organisationUnits", []))
            if len(parents) > 1:
                parent_set = set(parents)
                items = [item for item in items if item.get("parent", {}).get("id") in parent_set]
        else:
            params["filter"] = filters if len(filters) > 1 else filters[0]
            data = dhis2_get("/api/organisationUnits.json", params)
            items = normalize_org_units(data.get("organisationUnits", []))
            if parents and descendants:
                parent_set = set(parents)

                def _matches_path(path_value):
                    path = path_value or ""
                    return any(f"/{parent_id}/" in f"{path}/" for parent_id in parent_set)

                items = [item for item in items if _matches_path(item.get("path"))]
        return jsonify({
            "items": items,
            "source": "dhis2",
        })
    except Exception as exc:
        fallback_items = VERIFIED_ROOT_ORG_UNITS if str(level) == "1" else []
        return jsonify({
            "items": fallback_items,
            "source": "verified-fallback",
            "warning": str(exc),
        })


@app.route("/api/meta/indicators")
def indicators():
    """Return the curated malaria list only."""
    return jsonify({
        "source": "curated",
        "count": len(CURATED_INDICATORS),
        "items": CURATED_INDICATORS,
    })


@app.route("/api/meta/data-elements")
def data_elements():
    return jsonify({
        "source": "curated",
        "count": len(CURATED_INDICATORS),
        "items": CURATED_INDICATORS,
    })


@app.route("/api/meta/periods")
def periods():
    """Return period options aligned to the DHIS2 server date."""
    state = load_sync_state()
    server_date = (state.get("dhis2_info") or {}).get("serverDate")
    return jsonify(build_period_options(server_date))


# ── Analytics endpoints ────────────────────────────────────────────────────────

@app.route("/api/analytics")
def analytics():
    """
    Proxy to DHIS2 analytics.
    Query params: dx (semicolon-separated dx ids), ou, pe
    """
    dx = request.args.get("dx", "")
    ou = request.args.get("ou", "LEVEL-1")
    pe = request.args.get("pe", "LAST_12_MONTHS")

    if not dx:
        return json_error("dx parameter required", 400)

    unique_dx = ";".join(dict.fromkeys([item for item in dx.split(";") if item]))
    try:
        data = dhis2_get("/api/analytics.json", {
            "dimension": [
                f"dx:{unique_dx}",
                f"pe:{pe}",
                f"ou:{ou}",
            ],
            "displayProperty": "NAME",
            "outputIdScheme": "UID",
            "skipMeta": "false",
            "skipData": "false",
        })
    except RuntimeError as exc:
        return json_error(str(exc), requested_dx=unique_dx, ou=ou, pe=pe)
    return jsonify(data)


@app.route("/api/analytics/aggregate")
def analytics_aggregate():
    """Aggregate totals across org units for KPI cards."""
    dx = request.args.get("dx", "")
    ou = request.args.get("ou", "LEVEL-1")
    pe = request.args.get("pe", "LAST_12_MONTHS")

    if not dx:
        return json_error("dx required", 400)

    unique_dx = ";".join(dict.fromkeys([item for item in dx.split(";") if item]))
    try:
        data = dhis2_get("/api/analytics.json", {
            "dimension": [f"dx:{unique_dx}", f"pe:{pe}"],
            "filter": f"ou:{ou}",
            "displayProperty": "NAME",
            "outputIdScheme": "UID",
        })
    except RuntimeError as exc:
        return json_error(str(exc), requested_dx=unique_dx, ou=ou, pe=pe)
    return jsonify(data)


# ── Dashboard config — curated malaria catalog ────────────────────────────────

@app.route("/api/meta/dashboard-config")
def dashboard_config():
    return jsonify({
        "source": "curated",
        "count": len(CURATED_INDICATORS),
        "items": CURATED_INDICATORS,
        "groups": DASHBOARD_INDICATORS,
    })


# ── Sync & health ──────────────────────────────────────────────────────────────

@app.route("/api/sync/status")
def sync_status():
    return jsonify(sync_response_payload())


@app.route("/api/sync/run", methods=["POST"])
def sync_run():
    payload = request.get_json(silent=True) or {}
    validation_mode = payload.get("validation_mode", "sample")
    if validation_mode not in {"sample", "full"}:
        return json_error("validation_mode must be 'sample' or 'full'", 400)
    return jsonify(run_sync(validation_mode))


@app.route("/api/health")
def health():
<<<<<<< HEAD
    state = sync_response_payload()
    payload = {
        "status": "ok" if state["status"] in {"connected", "degraded"} else state["status"],
=======
    state = load_sync_state()
    payload = {
        "status": "ok",
>>>>>>> eb49461 (updated files)
        "sync_status": state["status"],
        "dhis2_base": state["dhis2_base"],
        "catalog_count": state["catalog_count"],
        "unique_query_uids": state["unique_query_uids"],
        "last_attempt_at": state["last_attempt_at"],
        "last_success_at": state["last_success_at"],
        "validation_mode": state["validation_mode"],
        "validated_uids": state["validated_uids"],
        "dhis2_info": state["dhis2_info"],
    }
    if state.get("last_error"):
        payload["error"] = state["last_error"]
<<<<<<< HEAD
    status_code = 200 if state["status"] in {"connected", "degraded"} else 503
    return jsonify(payload), status_code
=======
    return jsonify(payload), 200
>>>>>>> eb49461 (updated files)


@app.route("/api/cache/clear", methods=["POST"])
def clear_cache():
    _cache.clear()
    return jsonify({"cleared": True})


# ── Serve frontend ─────────────────────────────────────────────────────────────

@app.route("/")
<<<<<<< HEAD
=======
@app.route("/dhis2mal")
<<<<<<< HEAD
>>>>>>> eb49461 (updated files)
=======
@app.route("/dhis2mal/")
>>>>>>> 5ff4c9b (Update app.py)
def index():
    return send_from_directory("static", "index.html")


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5050"))
    print("\n  DHIS2 Malaria Dashboard")
    print("  ─────────────────────────────────────")
    print(f"  → Open: http://localhost:{port}")
    print(f"  → DHIS2: {DHIS2_BASE}")
    print(f"  → Curated list: {len(CURATED_INDICATORS)} labels / {len(INDICATORS_BY_QUERY_UID)} query UIDs")
    print("  → Press Ctrl+C to stop\n")
    app.run(debug=False, port=port, host="0.0.0.0")
