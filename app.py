import json
import os
import re
import secrets
import threading
import uuid
from html import escape
from datetime import datetime, timedelta
from pathlib import Path
from urllib import error as urlerror, parse, request
from zoneinfo import ZoneInfo

import streamlit as st


COUNT_KEYS = ["k", "lk", "c", "pk", "gd", "dr", "st"]
TOKEN_RE = re.compile(r"^\s*(\d+)\s*([a-zA-Z]+)\s*$", re.IGNORECASE)
TOKEN_ALIAS = {
    "k": "k",
    "lk": "lk",
    "c": "c",
    "cc": "c",
    "pk": "pk",
    "gd": "gd",
    "dr": "dr",
    "dry": "dr",
    "st": "st",
    "steam": "st",
}

ACTIVITY_GROUP_TEMPLATES = {
    "Packing IS": ["Sortir", "Packing", "Sealing", "Labeling"],
    "Packing PP": ["Packing PPMH", "Packing PPBAR"],
    "Support": ["Supply bahan", "Pindah produk"],
    "Control": ["Kerjakan laporan"],
}

SOURCE_DEPARTMENTS = [
    ("kupas", "Inbound"),
    ("gudang", "Gudang"),
    ("dry", "Dry"),
    ("steam", "Steam"),
    ("cuci", "Cuci"),
    ("packing", "Packing"),
    ("lain", "Lain-lain"),
]

PERSIST_PATH = Path(".laporan_situasi_state.json")
STATE_PREFIX_KEYS = ("mix_", "extra_", "src_", "order_", "tasks_table_", "task_name_", "pic_name_", "pic_role_")
LOCK_TTL_SECONDS = 600
STATE_IO_LOCK = threading.RLock()
TEAM_LABELS = {
    "PACKING-1": "Packing Team 1",
    "PACKING-2": "Packing Team 2",
    "PACKING-3": "Packing Team 3",
}
TELEGRAM_SOFT_LIMIT = 3200


def now_local() -> datetime:
    return datetime.now(ZoneInfo("Asia/Jakarta"))


def now_iso() -> str:
    return now_local().isoformat()


def format_dt_brief(raw_iso: str) -> str:
    try:
        return datetime.fromisoformat(str(raw_iso)).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(raw_iso)


def format_time_only(raw_iso: str) -> str:
    try:
        return datetime.fromisoformat(str(raw_iso)).strftime("%H:%M")
    except Exception:
        s = str(raw_iso)
        return s[-5:] if len(s) >= 5 else s


def split_note_lines(text: str) -> list[str]:
    raw = str(text or "").strip()
    if not raw:
        return []
    parts: list[str] = []
    for block in raw.splitlines():
        for part in re.split(r"[;]+", block):
            val = part.strip()
            if val:
                parts.append(val)
    return parts


def pretty_label(text: str) -> str:
    s = str(text or "").strip()
    if not s:
        return s
    acronyms = {"is", "tl", "qc", "bb", "pp", "mh", "pk", "gd", "dr", "st", "cc"}
    tokens = re.split(r"(\s+|/|-)", s)
    out: list[str] = []
    for tok in tokens:
        if not tok or tok.isspace() or tok in {"/", "-"}:
            out.append(tok)
            continue
        low = tok.lower()
        if low in acronyms:
            out.append(low.upper())
        elif tok.isupper():
            out.append(tok)
        else:
            out.append(tok[:1].upper() + tok[1:].lower())
    return "".join(out)


def load_team_passwords() -> dict[str, str]:
    # Priority: Streamlit secrets -> TEAM_PASSWORDS_JSON -> per-team env vars.
    out: dict[str, str] = {}
    try:
        secret_map = st.secrets.get("TEAM_PASSWORDS", {})
        if isinstance(secret_map, dict):
            out = {str(k).strip(): str(v).strip() for k, v in secret_map.items() if str(k).strip() and str(v).strip()}
    except Exception:
        out = {}

    if not out:
        raw_json = os.getenv("TEAM_PASSWORDS_JSON", "").strip()
        if raw_json:
            try:
                loaded = json.loads(raw_json)
                if isinstance(loaded, dict):
                    out = {str(k).strip(): str(v).strip() for k, v in loaded.items() if str(k).strip() and str(v).strip()}
            except Exception:
                out = {}

    if not out:
        for team_id in TEAM_LABELS.keys():
            env_key = f"TEAM_PIN_{team_id.replace('-', '_')}"
            pin = os.getenv(env_key, "").strip()
            if pin:
                out[team_id] = pin
    return out


def load_persisted_state() -> dict:
    with STATE_IO_LOCK:
        if not PERSIST_PATH.exists():
            return {}
        try:
            return json.loads(PERSIST_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}


def _write_state_atomically(raw: dict) -> None:
    tmp_path = PERSIST_PATH.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp_path, PERSIST_PATH)


def load_scoped_state(work_date: str, team_id: str) -> dict:
    return get_scope_record(work_date, team_id).get("data", {})


def _scope_key(work_date: str, team_id: str) -> str:
    return f"{work_date}::{team_id}"


def get_scope_record(work_date: str, team_id: str) -> dict:
    raw = load_persisted_state()
    key = _scope_key(work_date, team_id)
    scoped = raw.get("scopes", {}).get(key, {})
    if not isinstance(scoped, dict):
        return {"data": {}, "version": 0, "lock": None, "lock_history": []}
    if "data" in scoped:
        return {
            "data": scoped.get("data", {}) if isinstance(scoped.get("data"), dict) else {},
            "version": int(scoped.get("version", 0)),
            "lock": scoped.get("lock"),
            "lock_history": scoped.get("lock_history", []),
        }
    # Backward compatibility for old flat format.
    return {"data": scoped, "version": 0, "lock": None, "lock_history": []}


def save_scoped_state(
    work_date: str,
    team_id: str,
    payload: dict,
    expected_version: int | None = None,
) -> tuple[bool, int]:
    with STATE_IO_LOCK:
        raw = load_persisted_state()
        key = _scope_key(work_date, team_id)
        scopes = raw.get("scopes")
        if not isinstance(scopes, dict):
            scopes = {}

        existing = scopes.get(key, {})
        if isinstance(existing, dict) and "data" in existing:
            current_version = int(existing.get("version", 0))
            current_lock = existing.get("lock")
            lock_history = existing.get("lock_history", [])
        else:
            current_version = 0
            current_lock = None
            lock_history = []

        if expected_version is not None and current_version != expected_version:
            return False, current_version

        scopes[key] = {
            "data": payload,
            "version": current_version + 1,
            "lock": current_lock,
            "lock_history": lock_history,
        }
        raw["scopes"] = scopes
        _write_state_atomically(raw)
        return True, current_version + 1


def build_persist_payload() -> dict:
    static_keys = [
        "previous_total",
        "submission_id",
        "telegram_root_message_id",
        "slot_history",
        "qc_name",
        "tl_name",
        "operator_name",
        "shift",
        "reporter",
        "rolling_officer",
        "checker_kupas",
        "checker_packing",
        "nampan_ubi_officer",
        "current_total_people",
        "selected_groups",
        "manual_groups",
        "selected_departments",
        "change_reason",
        "tl_confirm",
        "move_in_raw",
        "move_out_raw",
        "event_slot",
    ]
    out = {k: st.session_state.get(k) for k in static_keys if k in st.session_state}
    for key, val in st.session_state.items():
        if key.startswith(STATE_PREFIX_KEYS):
            out[key] = val
    return out


def persist_state_to_disk() -> None:
    team_id = str(st.session_state.get("team_id", "")).strip()
    work_date = str(st.session_state.get("work_date", "")).strip()
    if not team_id or not work_date:
        return
    try:
        expected = st.session_state.get("scope_version")
        ok, new_version = save_scoped_state(work_date, team_id, build_persist_payload(), expected_version=expected)
        if not ok:
            st.warning("Penyimpanan lokal gagal karena konflik versi. Muat ulang lalu coba lagi.")
            st.session_state["scope_version"] = new_version
            return
        st.session_state["scope_version"] = new_version
    except Exception as e:
        st.warning("Penyimpanan lokal gagal: " + str(e))


def _lock_is_active(lock: dict | None) -> bool:
    if not isinstance(lock, dict):
        return False
    hb = lock.get("heartbeat_iso")
    if not hb:
        return False
    try:
        heartbeat = datetime.fromisoformat(hb)
    except Exception:
        return False
    return (now_local() - heartbeat).total_seconds() <= LOCK_TTL_SECONDS


def get_scope_lock(work_date: str, team_id: str) -> dict | None:
    return get_scope_record(work_date, team_id).get("lock")


def acquire_scope_lock(work_date: str, team_id: str, operator: str, force: bool = False) -> tuple[bool, str]:
    with STATE_IO_LOCK:
        raw = load_persisted_state()
        key = _scope_key(work_date, team_id)
        scopes = raw.get("scopes")
        if not isinstance(scopes, dict):
            scopes = {}
        existing = scopes.get(key, {})
        if isinstance(existing, dict) and "data" in existing:
            rec = existing
        else:
            rec = {"data": {}, "version": 0, "lock": None, "lock_history": []}
        lock = rec.get("lock")
        active = _lock_is_active(lock)
        token = st.session_state.get("lock_token")
        if not token:
            token = str(uuid.uuid4())
            st.session_state["lock_token"] = token

        if active and not force:
            same_owner = lock.get("token") == token
            if not same_owner:
                return False, f"Saat ini dipakai oleh {lock.get('owner', 'tidak diketahui')}"

        now = now_iso()
        new_lock = {
            "owner": operator,
            "token": token,
            "acquired_iso": lock.get("acquired_iso", now) if isinstance(lock, dict) else now,
            "heartbeat_iso": now,
        }
        lock_history = rec.get("lock_history", [])
        if force:
            lock_history.append({"action": "takeover", "at": now, "by": operator, "from": (lock or {}).get("owner", "-")})
        elif not active:
            lock_history.append({"action": "acquire", "at": now, "by": operator})

        scopes[key] = {
            "data": rec.get("data", {}),
            "version": int(rec.get("version", 0)),
            "lock": new_lock,
            "lock_history": lock_history,
        }
        raw["scopes"] = scopes
        _write_state_atomically(raw)
        return True, "Kunci berhasil diambil"

def refresh_scope_lock(work_date: str, team_id: str) -> None:
    operator = st.session_state.get("operator_name", "").strip()
    if not operator:
        return
    lock = get_scope_lock(work_date, team_id)
    token = st.session_state.get("lock_token")
    if not isinstance(lock, dict):
        return
    if lock.get("token") != token:
        return
    acquire_scope_lock(work_date, team_id, operator, force=False)


def init_state() -> None:
    if "team_id" not in st.session_state:
        st.session_state["team_id"] = "PACKING-1"
    if "work_date" not in st.session_state:
        st.session_state["work_date"] = now_local().date().isoformat()
    if "_loaded_scope" not in st.session_state:
        st.session_state["_loaded_scope"] = ""
    if "authenticated_scope" not in st.session_state:
        st.session_state["authenticated_scope"] = ""
    if "scope_version" not in st.session_state:
        st.session_state["scope_version"] = None
    if "operator_name" not in st.session_state:
        st.session_state["operator_name"] = ""

    if "previous_total" not in st.session_state:
        st.session_state.previous_total = None
    if "submission_id" not in st.session_state:
        st.session_state.submission_id = None
    if "telegram_root_message_id" not in st.session_state:
        st.session_state.telegram_root_message_id = None
    if "slot_history" not in st.session_state:
        st.session_state.slot_history = []
    if "manual_groups" not in st.session_state:
        st.session_state["manual_groups"] = []
    if "confirm_remove_group" not in st.session_state:
        st.session_state["confirm_remove_group"] = ""
    if "confirm_remove_task" not in st.session_state:
        st.session_state["confirm_remove_task"] = ""
    if "confirm_new_cycle" not in st.session_state:
        st.session_state["confirm_new_cycle"] = False


def _hhmm_to_minutes(hhmm: str) -> int:
    try:
        h, m = hhmm.split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return 0


def build_slots(start_hhmm: str = "00:00", end_hhmm: str = "23:30") -> list[str]:
    start = _hhmm_to_minutes(start_hhmm)
    end = _hhmm_to_minutes(end_hhmm)
    values: list[str] = []
    if start <= end:
        minute = start
        while minute <= end:
            values.append(f"{minute // 60:02d}:{minute % 60:02d}")
            minute += 30
    else:
        minute = start
        while minute < 24 * 60:
            values.append(f"{minute // 60:02d}:{minute % 60:02d}")
            minute += 30
        minute = 0
        while minute <= end:
            values.append(f"{minute // 60:02d}:{minute % 60:02d}")
            minute += 30
    return values


def current_slot() -> str:
    now = now_local()
    minute = 30 if now.minute >= 30 else 0
    return now.replace(minute=minute, second=0, microsecond=0).strftime("%H:%M")


def slug(text: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", text.strip().lower())
    return s.strip("_") or "item"


def parse_compact_mix(raw: str) -> tuple[dict[str, int], str | None]:
    counts = {k: 0 for k in COUNT_KEYS}
    value = (raw or "").strip()
    if not value:
        return counts, None
    # Accept flexible separators: '+', ',', ';', '/', and whitespace.
    matches = list(re.finditer(r"(\d+)\s*([a-zA-Z]+)", value))
    if matches:
        # Validate remaining text only contains separators.
        rest = re.sub(r"(\d+)\s*([a-zA-Z]+)", "", value)
        if re.sub(r"[\s,+;/|]+", "", rest):
            return counts, f"format '{value}' tidak valid"
        for m in matches:
            n = int(m.group(1))
            key_raw = m.group(2).lower()
            key = TOKEN_ALIAS.get(key_raw)
            if not key:
                return counts, f"kode '{key_raw}' tidak dikenal"
            counts[key] += n
        return counts, None
    # Backward-compatible fallback: plain number means key "k".
    if value.isdigit():
        counts["k"] += int(value)
        return counts, None
    return counts, f"format '{value}' tidak valid"


def _new_task_item(name: str) -> dict:
    return {"id": uuid.uuid4().hex[:8], "tugas": name, "deleted": False}


def get_activity_rows(group_items_map: dict[str, list[dict]]) -> tuple[list[dict], list[str]]:
    rows = []
    parse_errors: list[str] = []
    for group, items in group_items_map.items():
        for item in items:
            task = str(item.get("tugas", "")).strip()
            if not task:
                continue
            item_id = str(item.get("id", "")).strip()
            if not item_id:
                continue
            prefix = f"{slug(group)}_{item_id}"
            row = {"group": group, "task": task}
            compact_raw = str(st.session_state.get(f"mix_{prefix}", ""))
            counts, parse_error = parse_compact_mix(compact_raw)
            if parse_error:
                parse_errors.append(f"{group} / {task}: {parse_error}")
            for key in COUNT_KEYS:
                row[key] = counts[key]
            if sum(int(row[k]) for k in COUNT_KEYS) > 0:
                rows.append(row)
    return rows, parse_errors


def activity_total(rows: list[dict]) -> int:
    total = 0
    for row in rows:
        total += sum(int(row.get(k, 0)) for k in COUNT_KEYS)
    return total


def format_activity_line(row: dict) -> str:
    parts = []
    for key in COUNT_KEYS:
        val = int(row.get(key, 0))
        if val > 0:
            parts.append(f"{val}{key}")
    return "+".join(parts) if parts else "0"


def format_grouped_activities(rows: list[dict], group_pic_map: dict[str, dict] | None = None) -> list[str]:
    grouped: dict[str, list[dict]] = {}
    order = list(ACTIVITY_GROUP_TEMPLATES.keys())
    for row in rows:
        grouped.setdefault(row.get("group", "Lainnya"), []).append(row)

    lines: list[str] = []
    ordered_groups = [g for g in order if g in grouped] + [g for g in grouped.keys() if g not in order]
    for group in ordered_groups:
        pic_line = ""
        if isinstance(group_pic_map, dict):
            pic = group_pic_map.get(group, {})
            if isinstance(pic, dict):
                pic_name = str(pic.get("name", "")).strip()
                pic_role = str(pic.get("role", "")).strip()
                if pic_name or pic_role:
                    info = " / ".join(pretty_label(x) for x in [pic_name, pic_role] if x)
                    pic_line = f" (PIC: {info})"
        lines.append(f"- {pretty_label(group)}{pic_line}")
        group_total = 0
        for row in grouped[group]:
            lines.append(f"  - {pretty_label(row['task'])}: {format_activity_line(row)}")
            group_total += sum(int(row.get(k, 0)) for k in COUNT_KEYS)
        lines.append(f"  = Total {pretty_label(group)}: {group_total} pax")
        lines.append("")
    if lines and lines[-1] == "":
        lines.pop()
    return lines


def slot_sort_key(slot_value: str) -> int:
    try:
        h, m = slot_value.split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return 9999


def build_slot_section(slot_item: dict, slot_idx: int) -> list[str]:
    report_time = format_time_only(slot_item.get("report_time", slot_item.get("slot", "-")))
    sumber_line = ", ".join(
        f"{label}:{int(slot_item.get('source_composition', {}).get(key, 0))}"
        for key, label in SOURCE_DEPARTMENTS
        if int(slot_item.get("source_composition", {}).get(key, 0)) > 0
    ) or "-"

    lines: list[str] = [
        f"3-{slot_idx}) {report_time}",
        f"   Total slot: {slot_item['slot_total']} pax",
        f"   Sumber: {sumber_line}",
    ]

    delta_text = str(slot_item.get("delta_text", "")).strip()
    in_raw = str(slot_item.get("move_in_raw", "")).strip()
    out_raw = str(slot_item.get("move_out_raw", "")).strip()
    reason = str(slot_item.get("change_reason", "")).strip()
    tl_confirm = str(slot_item.get("tl_confirm", "")).strip()

    reason_lines = split_note_lines(reason if reason not in {"", "-"} else "")
    no_change = (delta_text in {"+0", "0", "N/A", ""}) and (not in_raw) and (not out_raw) and (not reason_lines)

    if not no_change:
        if delta_text in {"+0", "0"}:
            perubahan_text = "tidak berubah"
        elif delta_text in {"N/A", ""}:
            perubahan_text = "awal laporan"
        else:
            perubahan_text = f"{delta_text} pax"
        lines.append(f"   Perubahan total: {perubahan_text}")
        if in_raw or out_raw:
            lines.append(f"   Mutasi masuk: {in_raw or '-'}")
            lines.append(f"   Mutasi keluar: {out_raw or '-'}")
        if reason_lines:
            lines.append(f"   Alasan: {reason_lines[0]}")
            for extra in reason_lines[1:]:
                lines.append(f"           - {extra}")
        if tl_confirm and tl_confirm != "-":
            lines.append(f"   Konfirmasi TL: {tl_confirm}")

    lines.append("")
    lines.extend(format_grouped_activities(slot_item["activities"], slot_item.get("group_pic", {})))

    event_lines = split_note_lines(slot_item.get("event_slot", ""))
    if event_lines and (event_lines != reason_lines):
        lines.append(f"   Event slot ini: {event_lines[0]}")
        for extra in event_lines[1:]:
            lines.append(f"                 - {extra}")

    lines.append(f"   Total semua aktivitas slot ini: {slot_item['slot_total']} pax")
    return lines


def render_activity_summary_table(rows: list[dict]) -> None:
    if not rows:
        return
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("group", "-")), []).append(row)

    html_rows: list[str] = []
    group_names = list(grouped.keys())
    for gi, group in enumerate(group_names):
        items = grouped[group]
        for ri, row in enumerate(items):
            cells: list[str] = []
            if ri == 0:
                cells.append(f"<td rowspan='{len(items)}'>{escape(group)}</td>")
            cells.append(f"<td>{escape(str(row.get('task', '-')))}</td>")
            cells.append(f"<td>{escape(format_activity_line(row))}</td>")
            subtotal = sum(int(row.get(k, 0)) for k in COUNT_KEYS)
            cells.append(f"<td style='text-align:right'>{subtotal}</td>")
            html_rows.append("<tr>" + "".join(cells) + "</tr>")
        if gi < len(group_names) - 1:
            html_rows.append("<tr class='grp-gap'><td colspan='4'></td></tr>")

    table_html = """
    <style>
      table.activity-summary { width: 100%; border-collapse: collapse; font-size: 0.95rem; }
      table.activity-summary th, table.activity-summary td { border: 1px solid #d8dbe2; padding: 8px 10px; vertical-align: top; }
      table.activity-summary thead th { background: #f7f8fb; text-align: left; }
      table.activity-summary tr.grp-gap td { border: none; height: 10px; padding: 0; }
    </style>
    <table class="activity-summary">
      <thead>
        <tr>
          <th>Group</th><th>Aktivitas</th><th>Rincian</th><th>Subtotal</th>
        </tr>
      </thead>
      <tbody>
    """ + "".join(html_rows) + """
      </tbody>
    </table>
    """
    st.markdown(table_html, unsafe_allow_html=True)


def upsert_slot_history(history: list[dict], slot_item: dict) -> list[dict]:
    # Keep full chronology for tracking; append new snapshot instead of replacing by slot.
    # Suppress append when the latest business payload is identical
    # (ignore report_time noise to avoid duplicate history spam).
    if not history:
        return [slot_item]
    last = dict(history[-1])
    now = dict(slot_item)
    last.pop("report_time", None)
    now.pop("report_time", None)
    last_sig = json.dumps(last, ensure_ascii=False, sort_keys=True)
    new_sig = json.dumps(now, ensure_ascii=False, sort_keys=True)
    if last_sig == new_sig:
        return history
    return [*history, slot_item]


def validate(payload: dict) -> list[str]:
    errors: list[str] = []
    required = [
        ("qc_name", "Nama QC wajib diisi."),
        ("tl_name", "Nama TL wajib diisi."),
        ("current_total_people", "Total orang per saat ini wajib diisi."),
        ("reporter", "Pelapor wajib diisi."),
        ("checker_kupas", "Petugas cross cek inbound wajib diisi."),
        ("checker_packing", "Petugas cross cek packing wajib diisi."),
        ("rolling_officer", "Petugas rolling wajib diisi."),
        ("nampan_ubi_officer", "Petugas nampan/ubi wajib diisi."),
        ("report_slot", "Waktu laporan (slot) wajib dipilih."),
    ]
    for key, msg in required:
        if not str(payload.get(key, "")).strip():
            errors.append(msg)

    if not payload.get("shift"):
        errors.append("Shift wajib dipilih.")

    rows = payload.get("activities", [])
    parse_errors = payload.get("activity_parse_errors", [])
    if parse_errors:
        errors.append(parse_errors[0])
    if not rows:
        errors.append("Pilih minimal 1 aktivitas.")
    else:
        if all(sum(int(r.get(k, 0)) for k in COUNT_KEYS) == 0 for r in rows):
            errors.append("Isi jumlah orang untuk minimal 1 aktivitas.")

    current_total = activity_total(rows)
    if int(payload.get("current_total_people", 0)) != current_total:
        errors.append("Total orang per saat ini harus sama dengan subtotal aktivitas.")
    source_comp = payload.get("source_composition", {})
    source_sum = sum(int(source_comp.get(key, 0)) for key, _ in SOURCE_DEPARTMENTS)
    if source_sum != int(payload.get("current_total_people", 0)):
        errors.append("Komposisi sumber personel harus sama dengan Total orang per saat ini.")
    prev = st.session_state.previous_total
    move_in_total = int(payload.get("move_in_total", 0))
    move_out_total = int(payload.get("move_out_total", 0))
    if isinstance(prev, int) and current_total != prev:
        if not str(payload.get("change_reason", "")).strip():
            errors.append("Total berubah. Isi penyebab perubahan personel.")
        if str(payload.get("tl_confirm", "")).strip().lower() != "sudah cek":
            errors.append("Total berubah. Konfirmasi TL wajib ketik tepat: 'sudah cek'.")

    return errors


def _telegram_head_lines(payload: dict) -> list[str]:
    team_label = TEAM_LABELS.get(payload["team_id"], payload["team_id"])
    return [
        f"*{team_label.upper()} - SHIFT {str(payload['shift']).upper()}*",
        f"*PELAPOR: {pretty_label(payload['reporter']).upper()}*",
        "",
        "B-1-2 LAPORAN SITUASI PACKING (30 MENIT)",
        "",
        "*1) Header*",
        f"- Tim laporan: {team_label} (Shift {payload['shift']})",
        f"- Tanggal kerja: {payload['work_date']}",
        f"- QC: {pretty_label(payload['qc_name'])}",
        f"- TL: {pretty_label(payload['tl_name'])}",
        "",
        "*2) Petugas*",
        f"- Pelapor: {pretty_label(payload['reporter'])}",
        f"- Cross cek inbound: {pretty_label(payload['checker_kupas'])}",
        f"- Cross cek packing: {pretty_label(payload['checker_packing'])}",
        f"- Rolling: {pretty_label(payload['rolling_officer'])}",
        f"- Nampan/Ubi: {pretty_label(payload['nampan_ubi_officer'])}",
        "",
        "*3) Detail kerja*",
        "",
    ]


def _render_telegram_part(payload: dict, slot_items: list[dict], part_no: int) -> str:
    lines = _telegram_head_lines(payload)
    if part_no > 1 and slot_items:
        cutover = format_time_only(slot_items[0].get("report_time", slot_items[0].get("slot", "-")))
        lines.append(f"Lanjutan laporan mulai {cutover} (part {part_no})")
        lines.append("")
    if not slot_items:
        lines.append("-")
        return "\n".join(lines)
    for idx, slot_item in enumerate(slot_items, start=1):
        lines.extend(build_slot_section(slot_item, idx))
        if idx < len(slot_items):
            lines.append("")
    return "\n".join(lines)


def build_telegram_parts(payload: dict, slot_history: list[dict], max_chars: int = TELEGRAM_SOFT_LIMIT) -> list[dict]:
    if not slot_history:
        return [{"text": _render_telegram_part(payload, [], 1), "history": [], "part_no": 1}]

    parts_history: list[list[dict]] = []
    current: list[dict] = []
    for slot_item in slot_history:
        candidate = [*current, slot_item]
        probe = _render_telegram_part(payload, candidate, 1)
        if current and len(probe) > max_chars:
            parts_history.append(current)
            current = [slot_item]
        else:
            # If single slot itself is too large, keep as one part and let API decide.
            current = candidate
    if current:
        parts_history.append(current)

    out: list[dict] = []
    for idx, hist in enumerate(parts_history, start=1):
        out.append({"text": _render_telegram_part(payload, hist, idx), "history": hist, "part_no": idx})
    return out


def build_telegram(payload: dict, slot_history: list[dict], current_total: int) -> str:
    parts = build_telegram_parts(payload, slot_history, TELEGRAM_SOFT_LIMIT)
    return parts[-1]["text"]


def build_sheet_row(payload: dict, current_total: int) -> list[str]:
    prev = st.session_state.previous_total
    delta = "" if prev is None else str(current_total - prev)
    return [
        payload["work_date"],
        payload["team_id"],
        payload["system_time"],
        payload["report_slot"],
        payload["qc_name"],
        payload["tl_name"],
        payload["shift"],
        str(payload["current_total_people"]),
        payload["reporter"],
        payload["checker_kupas"],
        payload["checker_packing"],
        payload["rolling_officer"],
        payload["nampan_ubi_officer"],
        json.dumps(payload["source_composition"], ensure_ascii=False),
        json.dumps(payload["activities"], ensure_ascii=False),
        str(current_total),
        delta,
        payload["change_reason"],
        payload.get("tl_confirm", ""),
        payload.get("move_in_raw", ""),
        payload.get("move_out_raw", ""),
        payload["idempotency_key"],
    ]


def append_sheet_backup(payload: dict, row: list[str]) -> tuple[str, str]:
    webhook = os.getenv("GOOGLE_SHEETS_WEBHOOK_URL", "").strip()
    if not webhook:
        return "skip", "Sheets backup nonaktif (variabel lingkungan belum diatur)"
    body = json.dumps(
        {
            "idempotency_key": payload.get("idempotency_key"),
            "team_id": payload.get("team_id"),
            "work_date": payload.get("work_date"),
            "report_slot": payload.get("report_slot"),
            "system_time": payload.get("system_time"),
            "row": row,
        },
        ensure_ascii=False,
    ).encode("utf-8")
    req = request.Request(
        webhook,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    try:
        with request.urlopen(req, timeout=20) as resp:
            if 200 <= int(getattr(resp, "status", 200)) < 300:
                return "ok", "Sheets backup OK"
            return "error", f"Sheets backup HTTP {getattr(resp, 'status', 'unknown')}"
    except Exception as err:
        return "error", f"Sheets backup gagal: {err}"


def _telegram_api(method: str, payload: dict) -> tuple[bool, str, dict]:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        return False, "Variabel lingkungan TELEGRAM_BOT_TOKEN belum diatur.", {}
    url = f"https://api.telegram.org/bot{token}/{method}"
    data = parse.urlencode(payload).encode("utf-8")
    req = request.Request(url, data=data, method="POST")
    try:
        with request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            parsed = json.loads(body)
            if parsed.get("ok"):
                return True, "OK", parsed.get("result", {})
            return False, f"Galat respons Telegram: {parsed}", {}
    except urlerror.HTTPError as err:
        try:
            body = err.read().decode("utf-8", errors="ignore")
            parsed = json.loads(body)
            desc = parsed.get("description") or body
            return False, f"Telegram API HTTP {err.code}: {desc}", {}
        except Exception:
            return False, f"Telegram API HTTP {err.code}", {}
    except Exception as err:
        return False, f"Galat API Telegram: {err}", {}


def _escape_mdv2(text: str) -> str:
    return re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', text)


def send_new_message(message: str) -> tuple[bool, str, int | None]:
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not chat_id:
        return False, "Variabel lingkungan TELEGRAM_CHAT_ID belum diatur.", None
    message = _escape_mdv2(message)
    ok, msg, data = _telegram_api(
        "sendMessage",
        {"chat_id": chat_id, "text": message, "parse_mode": "MarkdownV2"},
    )
    if not ok:
        return False, msg, None
    return True, "Pesan baru Telegram berhasil dikirim", data.get("message_id")


def edit_existing_message(message_id: int, message: str) -> tuple[bool, str]:
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not chat_id:
        return False, "Variabel lingkungan TELEGRAM_CHAT_ID belum diatur."
    message = _escape_mdv2(message)
    ok, msg, _ = _telegram_api(
        "editMessageText",
        {"chat_id": chat_id, "message_id": message_id, "text": message, "parse_mode": "MarkdownV2"},
    )
    if not ok:
        return False, msg
    return True, "Pesan Telegram berhasil diperbarui"


def send_update_reply(root_message_id: int) -> tuple[bool, str]:
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not chat_id:
        return False, "Variabel lingkungan TELEGRAM_CHAT_ID belum diatur."
    ok, msg, _ = _telegram_api(
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": "Laporan sudah update.",
            "reply_to_message_id": root_message_id,
        },
    )
    if not ok:
        return False, msg
    return True, "Balasan pembaruan berhasil dikirim"


def sync_scope_if_needed(work_date: str, team_id: str) -> None:
    scope = f"{work_date}::{team_id}"
    if st.session_state.get("_loaded_scope") == scope:
        return

    for key in list(st.session_state.keys()):
        if key.startswith(STATE_PREFIX_KEYS):
            del st.session_state[key]

    scoped = load_scoped_state(work_date, team_id)
    rec = get_scope_record(work_date, team_id)
    for key, val in scoped.items():
        st.session_state[key] = val

    if "manual_groups" not in st.session_state or not isinstance(st.session_state.get("manual_groups"), list):
        st.session_state["manual_groups"] = []
    st.session_state["selected_groups"] = list(st.session_state["manual_groups"])

    if not scoped:
        st.session_state["previous_total"] = None
        st.session_state["submission_id"] = None
        st.session_state["telegram_root_message_id"] = None
        st.session_state["slot_history"] = []
        st.session_state["current_total_people"] = 0
        st.session_state["selected_departments"] = []
        st.session_state["change_reason"] = ""
        st.session_state["tl_confirm"] = ""
        st.session_state["move_in_raw"] = ""
        st.session_state["move_out_raw"] = ""
        st.session_state["event_slot"] = ""

    st.session_state["work_date"] = work_date
    st.session_state["team_id"] = team_id
    st.session_state["scope_version"] = int(rec.get("version", 0))
    st.session_state["_loaded_scope"] = scope
    st.rerun()


def main() -> None:
    init_state()
    st.set_page_config(page_title="Laporan Situasi Packing", layout="centered")
    st.markdown(
        """
        <style>
        .stButton > button {
          padding: 0.15rem 0.5rem;
          font-size: 0.8rem;
          line-height: 1.1;
          white-space: nowrap;
          min-height: 1.9rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.title("B-1-2 Laporan Situasi (Packing)")
    st.caption("Durasi lapor: setiap 30 menit")
    team_passwords = load_team_passwords()
    if not team_passwords:
        st.error(
            "PIN tim belum dikonfigurasi. Atur `TEAM_PASSWORDS` di Streamlit secrets "
            "atau env `TEAM_PASSWORDS_JSON` / `TEAM_PIN_PACKING_1` dst."
        )
        st.stop()

    top1, top2, top3, top4 = st.columns(4)
    with top1:
        team_choices = [x for x in TEAM_LABELS.keys() if x in team_passwords]
        if not team_choices:
            team_choices = list(team_passwords.keys())
        saved_team = st.session_state.get("team_id", "PACKING-1")
        team_index = team_choices.index(saved_team) if saved_team in team_choices else 0
        team_id = st.selectbox(
            "Tim laporan",
            team_choices,
            index=team_index,
            format_func=lambda x: TEAM_LABELS.get(x, x),
        )
    with top2:
        work_date = st.date_input(
            "Tanggal kerja",
            value=now_local().date(),
        ).isoformat()
    with top3:
        operator_name = st.text_input(
            "Pelapor",
            value=st.session_state.get("operator_name", ""),
            key="operator_name_input",
            placeholder="Nama pelapor",
        )
    with top4:
        team_pin = st.text_input("PIN Tim", type="password", key="team_pin_input")

    scope = f"{work_date}::{team_id}"
    lock_info = get_scope_lock(work_date, team_id)
    lock_active = _lock_is_active(lock_info)
    if lock_active:
        st.caption(f"Lock aktif: {lock_info.get('owner','-')} ({lock_info.get('heartbeat_iso','')})")
    st.caption("Buka Tim: hari ini pertama kali mulai laporan tim ini (PIN + lock).")
    st.caption("Take Over Tim: ambil alih saat tim sedang dilock operator lain.")
    lock_c1, lock_c2 = st.columns(2)
    open_clicked = lock_c1.button("Buka Tim")
    takeover_clicked = lock_c2.button("Take Over Tim", disabled=not lock_active)
    if open_clicked:
        if not operator_name.strip():
            st.error("Nama operator wajib diisi.")
        elif secrets.compare_digest(team_pin, team_passwords.get(team_id, "")):
            ok_lock, msg_lock = acquire_scope_lock(work_date, team_id, operator_name, force=False)
            if ok_lock:
                st.session_state["authenticated_scope"] = scope
                st.session_state["team_id"] = team_id
                st.session_state["work_date"] = work_date
                st.session_state["operator_name"] = operator_name
                st.success(f"{team_id} berhasil dibuka")
            else:
                st.error(f"{msg_lock}. Jika perlu gunakan Take Over Tim")
        else:
            st.error("PIN Tim tidak valid.")

    if takeover_clicked:
        if not operator_name.strip():
            st.error("Nama operator wajib diisi.")
        elif secrets.compare_digest(team_pin, team_passwords.get(team_id, "")):
            ok_take, msg_take = acquire_scope_lock(work_date, team_id, operator_name, force=True)
            if ok_take:
                st.session_state["authenticated_scope"] = scope
                st.session_state["team_id"] = team_id
                st.session_state["work_date"] = work_date
                st.session_state["operator_name"] = operator_name
                st.success(f"Take Over berhasil: {team_id}")
            else:
                st.error(msg_take)
        else:
            st.error("PIN Tim tidak valid untuk Take Over.")

    if st.session_state.get("authenticated_scope") != scope:
        st.warning(f"Untuk membuka data {team_id}, masukkan PIN lalu tekan 'Buka Tim'.")
        st.stop()

    sync_scope_if_needed(work_date, team_id)
    refresh_scope_lock(work_date, team_id)

    system_time = now_iso()
    shift_defaults = {
        "1": ("06:30", "14:30"),
        "2": ("20:00", "06:30"),
        "3": ("22:00", "06:30"),
        "tengah": ("13:00", "21:00"),
        "": ("00:00", "23:30"),
    }
    shift_saved = st.session_state.get("shift", "")
    default_start, default_end = shift_defaults.get(shift_saved, ("00:00", "23:30"))
    col_t1, col_t2, col_t3 = st.columns(3)
    with col_t1:
        st.text_input("Waktu sistem", value=system_time, disabled=True)
    with col_t2:
        work_start = st.time_input(
            "Jam kerja mulai",
            value=datetime.strptime(default_start, "%H:%M").time(),
            key="work_start_time",
        )
    with col_t3:
        work_end = st.time_input(
            "Jam kerja selesai",
            value=datetime.strptime(default_end, "%H:%M").time(),
            key="work_end_time",
        )
    slot_start = f"{work_start.hour:02d}:{work_start.minute:02d}"
    slot_end = f"{work_end.hour:02d}:{work_end.minute:02d}"
    slots = build_slots(slot_start, slot_end)
    suggested_slot = current_slot()
    if suggested_slot not in slots:
        suggested_slot = slots[0]
    report_slot = suggested_slot
    with col_t3:
        st.text_input("Slot otomatis (30m)", value=report_slot, disabled=True)

    st.subheader("1) Data Header")
    col_a, col_b = st.columns(2)
    with col_a:
        qc_name = st.text_input("Nama QC", placeholder="Nama QC", key="qc_name")
    with col_b:
        tl_name = st.text_input("Nama TL", placeholder="Nama TL", key="tl_name")

    col_s1, col_s2 = st.columns(2)
    with col_s1:
        shift_choices = ["", "1", "2", "3", "tengah"]
        shift_index = shift_choices.index(shift_saved) if shift_saved in shift_choices else 0
        shift = st.selectbox("Shift", shift_choices, index=shift_index, key="shift")
    with col_s2:
        reporter = st.text_input("Pelapor", placeholder="Nama pelapor", key="reporter")
    col_r1, _col_r2 = st.columns(2)
    with col_r1:
        rolling_officer = st.text_input("Petugas rolling", placeholder="Contoh: Mila", key="rolling_officer")

    with st.expander("Petugas cross check / support", expanded=False):
        col_e, col_f = st.columns(2)
        with col_e:
            checker_kupas = st.text_input("Cross cek inbound", placeholder="Contoh: Rifka", key="checker_kupas")
            nampan_ubi_officer = st.text_input("Petugas nampan / ubi", placeholder="Contoh: Hendra/Hanif", key="nampan_ubi_officer")
        with col_f:
            checker_packing = st.text_input("Cross cek packing", placeholder="Nama petugas", key="checker_packing")

    st.subheader("2) Detail Aktivitas + Keterangan")
    st.caption(f"Slot laporan aktif: {report_slot}")
    # Reset only when slot actually changes during an active session.
    # On first load / take over, keep restored values from persisted state.
    if "_last_slot_form_reset" not in st.session_state:
        st.session_state["_last_slot_form_reset"] = report_slot
    elif st.session_state.get("_last_slot_form_reset") != report_slot:
        st.session_state["current_total_people"] = 0
        st.session_state["selected_departments"] = []
        for dep_key, _dep_label in SOURCE_DEPARTMENTS:
            st.session_state[f"src_{dep_key}"] = 0
        st.session_state["move_in_raw"] = ""
        st.session_state["move_out_raw"] = ""
        st.session_state["change_reason"] = ""
        st.session_state["tl_confirm"] = ""
        st.session_state["event_slot"] = ""
        st.session_state["_last_slot_form_reset"] = report_slot

    current_total_people = st.number_input("Input TL: Total orang per saat ini", min_value=0, step=1, key="current_total_people")
    st.caption("Komposisi sumber personel (asal tim yang sedang kerja di slot ini)")
    selected_departments = st.multiselect(
        "Pilih departemen yang aktif di slot ini",
        [label for _, label in SOURCE_DEPARTMENTS],
        default=st.session_state.get("selected_departments", []),
        key="selected_departments",
    )
    active_department_keys = {k for k, label in SOURCE_DEPARTMENTS if label in selected_departments}
    source_composition = {k: 0 for k, _ in SOURCE_DEPARTMENTS}
    sc1, sc2, sc3, sc4 = st.columns(4)
    source_cols = [sc1, sc2, sc3, sc4]
    visible_departments = [(k, label) for k, label in SOURCE_DEPARTMENTS if k in active_department_keys]
    for idx, (key, label) in enumerate(visible_departments):
        with source_cols[idx % 4]:
            source_composition[key] = st.number_input(
                label,
                min_value=0,
                step=1,
                key=f"src_{key}",
            )
    source_sum_now = sum(int(source_composition[k]) for k, _ in SOURCE_DEPARTMENTS)
    st.caption(f"Total komposisi sumber: {source_sum_now} pax")
    st.caption("Singkatan: k=Inbound, lk=Line packing, c=Cuci, pk=Packing, gd=Gudang, dr=Dry, st=Steam. Contoh input: 3k+2pk")
    if "manual_groups" not in st.session_state:
        st.session_state["manual_groups"] = []

    st.markdown("**Pilih blok aktivitas**")
    g1, g2 = st.columns([8, 2])
    with g1:
        new_group_name = st.text_input(
            "Tambah blok",
            key="new_group_name",
            placeholder="contoh: Stik IS",
            label_visibility="collapsed",
        )
    with g2:
        if st.button("Tambah blok", type="secondary"):
            name = (new_group_name or "").strip()
            if name and name not in st.session_state["manual_groups"]:
                st.session_state["manual_groups"].append(name)
                st.rerun()

    selected_groups = st.session_state.get("manual_groups", [])
    st.session_state["selected_groups"] = selected_groups
    if not selected_groups:
        st.caption("Belum ada blok aktivitas. Tambah blok dulu.")

    group_items_map: dict[str, list[dict]] = {}
    group_pic_map: dict[str, dict] = {}
    for group in selected_groups:
        group_slug = slug(group)
        table_key = f"tasks_table_{group_slug}"
        if table_key not in st.session_state or not isinstance(st.session_state.get(table_key), list):
            st.session_state[table_key] = []

        with st.expander(f"- {group}", expanded=True):
            st.caption("Flow manual: tambah seperlunya, tanpa default.")
            pic_c1, pic_c2 = st.columns(2)
            with pic_c1:
                pic_name = st.text_input(
                    "PIC",
                    key=f"pic_name_{group_slug}",
                    placeholder="Nama PIC",
                )
            with pic_c2:
                pic_role = st.text_input(
                    "JABATAN",
                    key=f"pic_role_{group_slug}",
                    placeholder="Jabatan PIC",
                )
            group_pic_map[group] = {"name": pic_name, "role": pic_role}
            items = st.session_state[table_key]
            cleaned = []
            for item in items:
                if isinstance(item, dict) and item.get("id"):
                    name = str(item.get("tugas", "")).strip()
                    if name and name.lower() not in {"none", "-"}:
                        cleaned.append({"id": str(item["id"]), "tugas": name})
            st.session_state[table_key] = cleaned
            items = cleaned

            add_c1, add_c2 = st.columns([8, 2])
            with add_c1:
                new_name = st.text_input(
                    "Tambah tugas",
                    key=f"new_task_name_{group_slug}",
                    placeholder="contoh: Packing IS / Sealing",
                    label_visibility="collapsed",
                )
            with add_c2:
                if st.button("Tambah", key=f"add_first_{group_slug}", type="secondary"):
                    name = (new_name or "").strip()
                    if name:
                        items.append(_new_task_item(name))
                        st.session_state[table_key] = items
                        st.rerun()
            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

            if not items:
                st.caption("Belum ada tugas. Tambah baris tugas dulu.")

            for idx, item in enumerate(items):
                task_id = item["id"]
                row_no, row_task, row_mix, row_del = st.columns([0.8, 1.8, 6.0, 1.4])
                with row_no:
                    ord_key = f"ord_{group_slug}_{task_id}"
                    if ord_key not in st.session_state:
                        st.session_state[ord_key] = str(idx + 1)
                    st.text_input(
                        "No",
                        key=ord_key,
                        label_visibility="collapsed",
                    )
                with row_task:
                    task_name_key = f"task_name_{group_slug}_{task_id}"
                    if task_name_key not in st.session_state:
                        st.session_state[task_name_key] = item["tugas"]
                    edited_name = st.text_input(
                        f"Nama tugas {idx + 1}",
                        key=task_name_key,
                        label_visibility="collapsed",
                    ).strip()
                    item["tugas"] = edited_name
                with row_mix:
                    st.text_input(
                        f"Rincian {idx + 1}",
                        key=f"mix_{group_slug}_{task_id}",
                        placeholder="contoh: 3k+2gd",
                        label_visibility="collapsed",
                    )
                with row_del:
                    if st.button("Hapus", key=f"del_{group_slug}_{task_id}", type="secondary"):
                        st.session_state["confirm_remove_task"] = f"{group_slug}:{task_id}"
                if st.session_state.get("confirm_remove_task") == f"{group_slug}:{task_id}":
                    st.warning("Hapus baris tugas ini?")
                    t_yes, t_no = st.columns([2, 2])
                    with t_yes:
                        if st.button("Ya, hapus", key=f"del_yes_{group_slug}_{task_id}", type="secondary"):
                            if len(items) > 1:
                                del items[idx]
                            else:
                                items.clear()
                            st.session_state[table_key] = items
                            st.session_state["confirm_remove_task"] = ""
                            st.rerun()
                    with t_no:
                        if st.button("Batal", key=f"del_no_{group_slug}_{task_id}", type="secondary"):
                            st.session_state["confirm_remove_task"] = ""
                            st.rerun()
                st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

            u1, _u2 = st.columns([2, 10])
            with u1:
                if st.button("Urut", key=f"sort_all_{group_slug}", type="secondary"):
                    ranked = []
                    total_len = len(items)
                    for i, it in enumerate(items):
                        raw = str(st.session_state.get(f"ord_{group_slug}_{it['id']}", i + 1)).strip()
                        wanted = int(raw) if raw.isdigit() else (i + 1)
                        wanted = max(1, min(wanted, max(1, total_len)))
                        ranked.append((wanted, i, it))
                    ranked.sort(key=lambda x: (x[0], x[1]))
                    st.session_state[table_key] = [x[2] for x in ranked]
                    st.rerun()
            block_total = 0
            for it in items:
                mix_raw = str(st.session_state.get(f"mix_{group_slug}_{it['id']}", "")).strip()
                counts, _err = parse_compact_mix(mix_raw)
                block_total += sum(counts.values())
            st.caption(f"Subtotal blok '{group}': {block_total} pax")
            hb_sp, hb_btn = st.columns([8, 2])
            with hb_btn:
                if st.button("Hapus blok", key=f"remove_group_{group_slug}", type="secondary"):
                    st.session_state["confirm_remove_group"] = group_slug
            if st.session_state.get("confirm_remove_group") == group_slug:
                st.warning(f"Hapus blok '{group}'?")
                c_yes, c_no = st.columns([2, 2])
                with c_yes:
                    if st.button("Ya, hapus blok", key=f"remove_group_yes_{group_slug}", type="secondary"):
                        st.session_state["manual_groups"] = [x for x in st.session_state.get("manual_groups", []) if x != group]
                        st.session_state["confirm_remove_group"] = ""
                        st.rerun()
                with c_no:
                    if st.button("Batal", key=f"remove_group_no_{group_slug}", type="secondary"):
                        st.session_state["confirm_remove_group"] = ""
                        st.rerun()

            group_items_map[group] = items

    activity_rows, parse_errors = get_activity_rows(group_items_map)
    current_total = activity_total(activity_rows)
    prev = st.session_state.previous_total
    delta_text = "N/A" if prev is None else f"{current_total - prev:+d}"
    st.info(f"Total sekarang: {current_total} pax | Delta vs sebelumnya: {delta_text}")
    if isinstance(prev, int):
        st.caption(f"Pembanding otomatis: total slot sebelumnya di draft tim ini = {prev} pax")
    if st.session_state.slot_history:
        same_slot_prev = next((x for x in st.session_state.slot_history if x.get("slot") == report_slot), None)
        if same_slot_prev:
            prev_sig = json.dumps(same_slot_prev.get("activities", []), ensure_ascii=False, sort_keys=True)
            now_sig = json.dumps(activity_rows, ensure_ascii=False, sort_keys=True)
            if prev_sig != now_sig:
                st.warning("Perubahan terdeteksi: data slot ini berbeda dari versi sebelumnya.")
    if isinstance(prev, int) and current_total != prev:
        st.error("ALARM: total slot berbeda dengan laporan sebelumnya. Wajib cek penyebab.")

    if activity_rows:
        render_activity_summary_table(activity_rows)
    if parse_errors:
        st.warning(parse_errors[0])

    col_sum1, col_sum2 = st.columns(2)
    with col_sum1:
        st.markdown(f"**Subtotal aktivitas: {current_total} pax**")
    with col_sum2:
        st.markdown(f"**Total orang per saat ini: {int(current_total_people)} pax**")
        if int(current_total_people) == current_total:
            st.caption("Cocok dengan subtotal")
        else:
            st.caption(f"Tidak cocok (selisih {int(current_total_people) - current_total:+d})")
        if source_sum_now == int(current_total_people):
            st.caption("Komposisi sumber cocok")
        else:
            st.caption(f"Komposisi sumber tidak cocok (selisih {source_sum_now - int(current_total_people):+d})")

    if int(current_total_people) != current_total:
        st.error("Total orang per saat ini tidak sama dengan subtotal aktivitas.")
    if source_sum_now != int(current_total_people):
        st.error("Total komposisi sumber tidak sama dengan Total orang per saat ini.")

    st.markdown("**Analisa Perubahan Total (untuk TL)**")
    prev_text = "N/A" if prev is None else f"{prev} pax"
    st.caption(f"Total slot sebelumnya: {prev_text} | Delta sekarang: {delta_text}")
    if isinstance(prev, int) and current_total != prev:
        st.error(f"ALARM: total berubah dari {prev} ke {current_total} (delta {current_total - prev:+d}).")
    move_in_raw = st.text_input("Mutasi masuk (opsional, contoh: 2gd+1k)", placeholder="contoh: 2gd", key="move_in_raw")
    move_out_raw = st.text_input("Mutasi keluar (opsional, contoh: 2gd)", placeholder="contoh: 2gd", key="move_out_raw")
    move_in_counts, move_in_err = parse_compact_mix(move_in_raw)
    move_out_counts, move_out_err = parse_compact_mix(move_out_raw)
    move_in_total = sum(move_in_counts.values())
    move_out_total = sum(move_out_counts.values())
    if move_in_err:
        st.warning(f"Mutasi masuk: {move_in_err}")
    if move_out_err:
        st.warning(f"Mutasi keluar: {move_out_err}")

    if isinstance(prev, int):
        expected = prev + move_in_total - move_out_total
        st.caption(f"Rekonsiliasi: {prev} + {move_in_total} - {move_out_total} = {expected} (subtotal sekarang {current_total})")
        if expected != current_total and (move_in_raw.strip() or move_out_raw.strip()):
            st.warning("Mutasi masuk/keluar belum cocok dengan perubahan total.")

    col_r1, col_r2 = st.columns(2)
    with col_r1:
        change_reason = st.text_area(
            "Penyebab perubahan personel (wajib jika total berubah)",
            height=100,
            placeholder="Contoh: +2 shift tengah datang, -1 istirahat.",
            key="change_reason",
        )
    with col_r2:
        tl_confirm = st.text_input(
            "Konfirmasi TL (wajib jika total berubah): ketik 'sudah cek'",
            placeholder="sudah cek",
            key="tl_confirm",
        )

    event_slot = st.text_area(
        "Event slot ini (opsional)",
        height=80,
        placeholder="Contoh: 1 orang izin pulang sebentar: Mike / Shift tengah datang 3 pax.",
        key="event_slot",
    )

    payload = {
        "work_date": work_date,
        "team_id": team_id,
        "system_time": system_time,
        "report_slot": report_slot,
        "qc_name": qc_name,
        "tl_name": tl_name,
        "shift": shift,
        "current_total_people": current_total_people,
        "source_composition": source_composition,
        "reporter": reporter,
        "checker_kupas": checker_kupas if "checker_kupas" in locals() else "",
        "checker_packing": checker_packing if "checker_packing" in locals() else "",
        "rolling_officer": rolling_officer,
        "nampan_ubi_officer": nampan_ubi_officer if "nampan_ubi_officer" in locals() else "",
        "activities": activity_rows,
        "activity_parse_errors": parse_errors,
        "move_in_total": move_in_total,
        "move_out_total": move_out_total,
        "move_in_raw": move_in_raw,
        "move_out_raw": move_out_raw,
        "change_reason": change_reason,
        "tl_confirm": tl_confirm,
        "event_slot": event_slot,
        "idempotency_key": st.session_state.submission_id or str(uuid.uuid4()),
    }

    current_slot_item = {
        "slot": report_slot,
        "report_time": system_time,
        "activities": activity_rows,
        "group_pic": group_pic_map,
        "slot_total": current_total,
        "source_composition": source_composition,
        "delta_text": delta_text,
        "move_in_total": move_in_total,
        "move_out_total": move_out_total,
        "move_in_raw": move_in_raw,
        "move_out_raw": move_out_raw,
        "change_reason": change_reason or "-",
        "tl_confirm": tl_confirm or "-",
        "event_slot": event_slot or "",
    }
    preview_history = upsert_slot_history(st.session_state.slot_history, current_slot_item)
    preview_parts = build_telegram_parts(payload, preview_history, TELEGRAM_SOFT_LIMIT)
    active_part = preview_parts[-1]

    st.subheader("Preview (Telegram message)")
    preview_text = active_part["text"]
    st.code(preview_text, language="text")
    if len(preview_parts) > 1:
        st.warning(
            f"Panjang pesan melebihi batas. Akan lanjut otomatis ke part {active_part['part_no']} "
            f"(slot mulai {format_time_only(active_part['history'][0].get('report_time', active_part['history'][0].get('slot', '-')))})."
        )

    root_id = st.session_state.telegram_root_message_id
    if root_id:
        st.caption(f"Mode: Update pesan existing (message_id={root_id})")
    else:
        st.caption("Mode: Kirim pesan baru (pertama kali)")

    if st.button("Mulai siklus laporan baru"):
        st.session_state["confirm_new_cycle"] = True
    if st.session_state.get("confirm_new_cycle"):
        st.warning("Siklus lama akan direset. Lanjut?")
        nx1, nx2 = st.columns(2)
        with nx1:
            if st.button("Ya, reset siklus", type="secondary"):
                st.session_state.telegram_root_message_id = None
                st.session_state.previous_total = None
                st.session_state.slot_history = []
                st.session_state["confirm_new_cycle"] = False
                persist_state_to_disk()
                st.success("Siklus baru dimulai. Submit berikutnya akan kirim pesan baru.")
                st.rerun()
        with nx2:
            if st.button("Batal reset", type="secondary"):
                st.session_state["confirm_new_cycle"] = False
                st.rerun()

    submitted = st.button("Kirim Telegram")
    if submitted:
        if st.session_state.get("_submitting"):
            st.warning("Sedang diproses. Mohon tunggu sebentar.")
            return
        st.session_state["_submitting"] = True
        try:
            current_rec = get_scope_record(work_date, team_id)
            live_version = int(current_rec.get("version", 0))
            live_lock = current_rec.get("lock")
            if not isinstance(live_lock, dict) or live_lock.get("token") != st.session_state.get("lock_token"):
                st.error("Anda tidak memegang kunci. Buka Tim atau Take Over Tim lalu coba lagi.")
                return
            # If lock token is ours, accept latest live version to avoid false conflict blocks.
            session_ver = st.session_state.get("scope_version")
            if session_ver is not None and live_version != int(session_ver):
                st.session_state["scope_version"] = live_version

            errors = validate(payload)
            if errors:
                st.error(errors[0])
                return

            root_message_id = st.session_state.telegram_root_message_id
            if root_message_id and len(preview_parts) == 1:
                ok, msg = edit_existing_message(root_message_id, preview_text)
                if not ok:
                    m = msg.lower()
                    if "message is not modified" in m:
                        st.info("Konten sama dengan pesan sebelumnya. Tidak ada edit baru.")
                    elif ("message to edit not found" in m) or ("can't be edited" in m) or ("message can't be edited" in m):
                        # Fallback: if edit target is gone/uneditable, send as a new message and relink root.
                        ok_new, msg_new, message_id_new = send_new_message(preview_text)
                        if not ok_new:
                            st.error(msg)
                            return
                        st.session_state.telegram_root_message_id = message_id_new
                        st.warning("Pesan lama tidak bisa di-edit. Dikirim sebagai pesan baru.")
                        st.success(f"{msg_new} (message_id={message_id_new})")
                        st.session_state.submission_id = payload["idempotency_key"]
                        st.session_state.previous_total = current_total
                        st.session_state.slot_history = preview_history
                        persist_state_to_disk()
                        row = build_sheet_row(payload, current_total)
                        sheet_state, msg_sheet = append_sheet_backup(payload, row)
                        if sheet_state == "ok":
                            st.caption("Sheets backup: OK")
                        elif sheet_state == "error":
                            st.warning(msg_sheet)
                        else:
                            st.caption(msg_sheet)
                        st.caption(f"Idempotency Key: {payload['idempotency_key']}")
                        return
                    else:
                        st.error(msg)
                        return
                ok_reply, msg_reply = send_update_reply(root_message_id)
                if not ok_reply:
                    st.warning(f"Pesan terupdate, tapi reply gagal: {msg_reply}")
                st.success("Pesan Telegram berhasil diperbarui dan update reply sudah diproses")
            else:
                ok, msg, message_id = send_new_message(preview_text)
                if not ok:
                    st.error(msg)
                    return
                st.session_state.telegram_root_message_id = message_id
                if root_message_id and len(preview_parts) > 1:
                    st.warning(
                        "Batas panjang tercapai. Laporan dilanjutkan sebagai pesan baru "
                        f"(part {active_part['part_no']})."
                    )
                st.success(f"{msg} (message_id={message_id})")

            st.session_state.submission_id = payload["idempotency_key"]
            st.session_state.previous_total = current_total
            st.session_state.slot_history = preview_history
            persist_state_to_disk()

            row = build_sheet_row(payload, current_total)
            sheet_state, msg_sheet = append_sheet_backup(payload, row)
            if sheet_state == "ok":
                st.caption("Sheets backup: OK")
            elif sheet_state == "error":
                st.warning(msg_sheet)
            else:
                st.caption(msg_sheet)
            st.caption(f"Idempotency Key: {payload['idempotency_key']}")
        finally:
            st.session_state["_submitting"] = False

    if st.button("Simpan draft lokal"):
        persist_state_to_disk()
        st.success("Draft lokal tersimpan.")

    # Persist latest inputs on each rerun so reopen continues from last state.
    persist_state_to_disk()


if __name__ == "__main__":
    main()

