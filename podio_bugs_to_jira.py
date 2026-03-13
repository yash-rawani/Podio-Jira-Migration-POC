"""
migrate_podio_bugs_to_jira.py

Purpose:
    Migrate Podio Bugs tickets into Jira Bug issues.

What this script does:
    1. Authenticates to Podio and Jira
    2. Scans Podio Bugs tickets page by page
    3. Filters tickets by VPC value and attachment presence
    4. Creates Jira Bug issues
    5. Maps Podio fields to Jira fields:
        - Podio Created On -> Jira Created Date
        - Podio Created By -> Jira Reporter
        - Podio Developer assigned -> Jira Developer
    6. Uploads regular Podio file attachments
    7. Adds Podio comments into Jira comments
    8. Uploads Podio activity history as a .txt attachment

Important:
    - Store real credentials in .env
    - Never commit .env to GitHub
    - Commit .env.example instead
"""

import os
import re
import time
import random
from typing import Any, Dict, List, Optional, Tuple, Set

import requests
from dotenv import load_dotenv

PODIO_API_BASE = "https://api.podio.com"
HTML_TAG_RE = re.compile(r"<[^>]+>")


# -----------------------------
# Environment helpers
# -----------------------------
def env_str(name: str) -> str:
    """Read a required string environment variable."""
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing env var: {name}")
    return value


def env_int(name: str, default: int) -> int:
    """Read an integer environment variable, or return default."""
    value = os.getenv(name)
    return int(value) if value else default


def env_float(name: str, default: float) -> float:
    """Read a float environment variable, or return default."""
    value = os.getenv(name)
    return float(value) if value else default


# -----------------------------
# Common text utilities
# -----------------------------
def strip_html(text: Any) -> str:
    """Remove HTML tags and normalize whitespace."""
    if not text:
        return ""
    return " ".join(HTML_TAG_RE.sub("", str(text)).split()).strip()


def split_iso_date(date_str: Any) -> str:
    """Convert Podio datetime-like values to YYYY-MM-DD."""
    if not date_str:
        return ""
    ds = str(date_str)
    if len(ds) >= 10 and ds[4] == "-" and ds[7] == "-":
        return ds[:10]
    return ds


# -----------------------------
# Podio API helpers
# -----------------------------
def podio_request_with_retry(
    method: str,
    url: str,
    headers: Dict[str, str],
    *,
    params: Optional[dict] = None,
    json_body: Optional[dict] = None,
    data_body: Optional[dict] = None,
    timeout: int = 60,
    max_retries: int = 10,
    max_backoff_seconds: int = 120,
) -> requests.Response:
    """
    Make a Podio API request with retry/backoff for transient failures.
    Retries on 420/429/5xx responses.
    """
    for attempt in range(1, max_retries + 1):
        response = requests.request(
            method,
            url,
            headers=headers,
            params=params,
            json=json_body,
            data=data_body,
            timeout=timeout,
        )

        if response.status_code in (420, 429, 500, 502, 503, 504):
            wait = min(max_backoff_seconds, 2 ** attempt) + random.uniform(0, 1.5)
            print(f"⚠️ Podio {response.status_code}. Sleep {wait:.1f}s (attempt {attempt}/{max_retries})")
            time.sleep(wait)
            continue

        if response.status_code >= 400:
            response.raise_for_status()

        return response

    raise RuntimeError("Podio request failed after retries")


def podio_access_token_app_auth(client_id: str, client_secret: str, app_id: int, app_token: str) -> str:
    """Authenticate to Podio using app auth and return an access token."""
    url = f"{PODIO_API_BASE}/oauth/token"
    data = {
        "grant_type": "app",
        "client_id": client_id,
        "client_secret": client_secret,
        "app_id": app_id,
        "app_token": app_token,
    }
    response = requests.post(url, data=data, timeout=30)
    response.raise_for_status()
    return response.json()["access_token"]


def podio_fetch_items_page(access_token: str, app_id: int, limit: int, offset: int, max_retries: int, max_backoff: int) -> List[Dict[str, Any]]:
    """Fetch one page of Podio items from the app filter endpoint."""
    url = f"{PODIO_API_BASE}/item/app/{app_id}/filter/"
    headers = {"Authorization": f"OAuth2 {access_token}"}
    payload = {"limit": limit, "offset": offset, "sort_by": "created_on", "sort_desc": True}
    response = podio_request_with_retry(
        "POST",
        url,
        headers,
        json_body=payload,
        timeout=60,
        max_retries=max_retries,
        max_backoff_seconds=max_backoff,
    )
    items = response.json().get("items") or []
    return items if isinstance(items, list) else []


def podio_fetch_item_detail(access_token: str, item_id: int, max_retries: int, max_backoff: int) -> Dict[str, Any]:
    """Fetch full Podio item detail for a single ticket."""
    url = f"{PODIO_API_BASE}/item/{item_id}"
    headers = {"Authorization": f"OAuth2 {access_token}"}
    response = podio_request_with_retry(
        "GET",
        url,
        headers,
        timeout=60,
        max_retries=max_retries,
        max_backoff_seconds=max_backoff,
    )
    return response.json() if isinstance(response.json(), dict) else {}


def podio_fetch_comments(access_token: str, item_id: int, max_retries: int, max_backoff: int) -> List[Dict[str, Any]]:
    """Fetch Podio comments for a ticket."""
    url = f"{PODIO_API_BASE}/comment/item/{item_id}"
    headers = {"Authorization": f"OAuth2 {access_token}"}
    response = podio_request_with_retry(
        "GET",
        url,
        headers,
        timeout=60,
        max_retries=max_retries,
        max_backoff_seconds=max_backoff,
    )
    data = response.json()
    return data if isinstance(data, list) else []


def podio_fetch_item_revisions(access_token: str, item_id: int, max_retries: int, max_backoff: int) -> List[Dict[str, Any]]:
    """Fetch Podio revision history for a ticket."""
    url = f"{PODIO_API_BASE}/item/{item_id}/revision/"
    headers = {"Authorization": f"OAuth2 {access_token}"}
    response = podio_request_with_retry(
        "GET",
        url,
        headers,
        timeout=60,
        max_retries=max_retries,
        max_backoff_seconds=max_backoff,
    )
    data = response.json()
    return data if isinstance(data, list) else []


def podio_fetch_revision_diff(access_token: str, item_id: int, rev_from: int, rev_to: int, max_retries: int, max_backoff: int) -> Any:
    """Fetch the diff between two Podio revisions."""
    url = f"{PODIO_API_BASE}/item/{item_id}/revision/{rev_from}/{rev_to}"
    headers = {"Authorization": f"OAuth2 {access_token}"}
    response = podio_request_with_retry(
        "GET",
        url,
        headers,
        timeout=60,
        max_retries=max_retries,
        max_backoff_seconds=max_backoff,
    )
    return response.json()


def podio_file_meta(access_token: str, file_id: int, max_retries: int, max_backoff: int) -> Dict[str, Any]:
    """Fetch Podio file metadata."""
    url = f"{PODIO_API_BASE}/file/{file_id}"
    headers = {"Authorization": f"OAuth2 {access_token}"}
    response = podio_request_with_retry(
        "GET",
        url,
        headers,
        timeout=60,
        max_retries=max_retries,
        max_backoff_seconds=max_backoff,
    )
    return response.json() if isinstance(response.json(), dict) else {}


def podio_download_file_bytes(access_token: str, file_id: int, max_retries: int, max_backoff: int) -> bytes:
    """Download raw file bytes from Podio."""
    url = f"{PODIO_API_BASE}/file/{file_id}/raw"
    headers = {"Authorization": f"OAuth2 {access_token}"}
    response = podio_request_with_retry(
        "GET",
        url,
        headers,
        timeout=180,
        max_retries=max_retries,
        max_backoff_seconds=max_backoff,
    )
    return response.content


# -----------------------------
# Podio data extraction helpers
# -----------------------------
def podio_stub_vpc(item: Dict[str, Any]) -> str:
    """Read the VPC field from the lightweight item stub if present."""
    fields = item.get("fields") or []
    if not isinstance(fields, list):
        return ""

    for field in fields:
        if not isinstance(field, dict):
            continue
        if field.get("external_id") != "vpc":
            continue

        values = field.get("values") or []
        if not values:
            return ""

        value0 = values[0]
        if not isinstance(value0, dict):
            return ""

        val = value0.get("value")
        if isinstance(val, dict):
            return strip_html(val.get("text") or val.get("value") or "")
        return strip_html(val)

    return ""


def podio_stub_has_attachments(item: Dict[str, Any]) -> bool:
    """Check if the lightweight item stub suggests attachments exist."""
    if isinstance(item.get("file_count"), int) and item["file_count"] > 0:
        return True
    if isinstance(item.get("files_count"), int) and item["files_count"] > 0:
        return True
    files = item.get("files")
    return isinstance(files, list) and len(files) > 0


def podio_collect_file_ids(item_detail: Dict[str, Any]) -> List[int]:
    """Collect all Podio file IDs attached to an item."""
    file_ids: Set[int] = set()

    files = item_detail.get("files") or []
    if isinstance(files, list):
        for file_obj in files:
            if isinstance(file_obj, dict) and isinstance(file_obj.get("file_id"), int):
                file_ids.add(file_obj["file_id"])

    fields = item_detail.get("fields") or []
    if isinstance(fields, list):
        for field in fields:
            if not isinstance(field, dict):
                continue
            if field.get("type") not in {"image", "file"}:
                continue

            for wrapped_value in field.get("values") or []:
                if not isinstance(wrapped_value, dict):
                    continue
                val = wrapped_value.get("value")
                if isinstance(val, dict) and isinstance(val.get("file_id"), int):
                    file_ids.add(val["file_id"])
                elif isinstance(val, int):
                    file_ids.add(val)

    return sorted(file_ids)


def podio_get_created_on(item_detail: Dict[str, Any]) -> str:
    """Extract the Podio created_on date as YYYY-MM-DD."""
    return split_iso_date(item_detail.get("created_on") or "")


def podio_get_created_by_name_email(item_detail: Dict[str, Any]) -> Tuple[str, str]:
    """Extract Podio created_by user name and email if available."""
    created_by = item_detail.get("created_by") or {}
    if not isinstance(created_by, dict):
        return "", ""

    name = str(created_by.get("name") or "").strip()
    email = ""

    mail = created_by.get("mail")
    if isinstance(mail, list) and mail:
        email = str(mail[0]).strip()
    elif isinstance(mail, str):
        email = mail.strip()

    return name, email


def podio_get_multi_user_field_names_emails(item_detail: Dict[str, Any], external_id: str) -> List[Tuple[str, str]]:
    """Extract a multi-user Podio contact field as [(name, email), ...]."""
    out: List[Tuple[str, str]] = []
    fields = item_detail.get("fields") or []
    if not isinstance(fields, list):
        return out

    for field in fields:
        if not isinstance(field, dict):
            continue
        if field.get("external_id") != external_id or field.get("type") != "contact":
            continue

        for wrapped_value in field.get("values") or []:
            if not isinstance(wrapped_value, dict):
                continue
            val = wrapped_value.get("value")
            if not isinstance(val, dict):
                continue

            name = str(val.get("name") or "").strip()
            email = ""
            mail = val.get("mail")
            if isinstance(mail, list) and mail:
                email = str(mail[0]).strip()
            elif isinstance(mail, str):
                email = mail.strip()

            if name or email:
                out.append((name, email))
        break

    return out


def podio_extract_all_fields(item_detail: Dict[str, Any]) -> Dict[str, str]:
    """
    Extract Podio fields for use in Jira description.
    Skip fields that are mapped to Jira fields separately.
    """
    out: Dict[str, str] = {}
    fields = item_detail.get("fields") or []
    if not isinstance(fields, list):
        return out

    skip_labels = {"Created On", "Created By", "Developer assigned", "Developer Assigned"}

    for field in fields:
        if not isinstance(field, dict):
            continue

        label = strip_html(field.get("label") or field.get("external_id") or "Field")
        if label in skip_labels:
            continue

        values = field.get("values") or []
        if not isinstance(values, list) or not values:
            continue

        parts: List[str] = []
        for wrapped_value in values:
            if not isinstance(wrapped_value, dict):
                continue

            val = wrapped_value.get("value")
            field_type = field.get("type")

            if field_type in {"text", "calculation"}:
                parts.append(strip_html(val))
            elif field_type == "category":
                if isinstance(val, dict):
                    parts.append(strip_html(val.get("text") or val.get("value") or str(val)))
                else:
                    parts.append(strip_html(val))
            elif field_type == "date":
                if isinstance(val, dict):
                    parts.append(split_iso_date(val.get("start") or val.get("end") or ""))
                else:
                    parts.append(split_iso_date(val))
            elif field_type == "contact":
                if isinstance(val, dict):
                    parts.append(strip_html(val.get("name") or ""))
            elif field_type == "embed":
                if isinstance(val, dict):
                    url = val.get("url") or val.get("link") or ""
                    title = val.get("title") or ""
                    parts.append(f"{strip_html(title)} ({url})" if url and title else url or strip_html(str(val)))
                else:
                    parts.append(strip_html(val))
            elif field_type == "image":
                if isinstance(val, dict) and "file_id" in val:
                    parts.append(f"file_id={val.get('file_id')}")
                elif isinstance(val, int):
                    parts.append(f"file_id={val}")
            else:
                parts.append(strip_html(val))

        parts = [p for p in parts if p]
        if parts:
            out[label] = " | ".join(parts)

    return out


# -----------------------------
# Podio revision/activity helpers
# -----------------------------
def _extract_value_display(field_type: str, wrapped: dict) -> str:
    """Render one revision diff value into human-readable text."""
    if not isinstance(wrapped, dict):
        return ""
    val = wrapped.get("value")

    if field_type == "contact":
        return str(val.get("name") or "").strip() if isinstance(val, dict) else ""

    if field_type == "category":
        if isinstance(val, dict):
            return str(val.get("text") or val.get("value") or "").strip()
        return str(val or "").strip()

    if field_type == "date":
        if isinstance(val, dict):
            return str(val.get("start") or val.get("end") or "").strip()
        return str(val or "").strip()

    if isinstance(val, dict):
        return str(val.get("value") or val.get("text") or val.get("title") or val).strip()

    return str(val or "").strip()


def _list_values(field_type: str, arr: list) -> list:
    """Convert a revision diff list into readable values."""
    if not isinstance(arr, list):
        return []
    out = []
    for item in arr:
        text = _extract_value_display(field_type, item)
        if text:
            out.append(text)
    return out


def format_revision_diff(diff_json: Any) -> List[str]:
    """Format a Podio revision diff into readable lines."""
    if not isinstance(diff_json, list):
        return []

    lines: List[str] = []
    for change in diff_json:
        if not isinstance(change, dict):
            continue

        label = str(change.get("label") or change.get("external_id") or "Field").strip()
        field_type = str(change.get("type") or "").strip()

        from_vals = _list_values(field_type, change.get("from", []))
        to_vals = _list_values(field_type, change.get("to", []))

        from_set = set(from_vals)
        to_set = set(to_vals)

        added = sorted(list(to_set - from_set))
        removed = sorted(list(from_set - to_set))

        if added or removed:
            if added:
                lines.append(f"Added to {label}: {', '.join(added)}")
            if removed:
                lines.append(f"Removed from {label}: {', '.join(removed)}")
            continue

        if from_vals != to_vals:
            left = ", ".join(from_vals) if from_vals else "(empty)"
            right = ", ".join(to_vals) if to_vals else "(empty)"
            lines.append(f"{label}: {left} → {right}")

    return lines


def podio_build_activity_log(access_token: str, item_id: int, max_retries: int, max_backoff: int, activity_pace_seconds: float) -> str:
    """Build a plain-text activity log from Podio revisions."""
    revisions = podio_fetch_item_revisions(access_token, item_id, max_retries, max_backoff)
    revisions = [r for r in revisions if isinstance(r, dict) and isinstance(r.get("revision"), int)]
    if len(revisions) < 2:
        return ""

    revisions_sorted = sorted(revisions, key=lambda r: r.get("revision", 0))
    lines: List[str] = []

    for i in range(1, len(revisions_sorted)):
        prev_rev = revisions_sorted[i - 1]["revision"]
        curr_rev = revisions_sorted[i]["revision"]

        actor = revisions_sorted[i].get("created_by") or {}
        actor_name = actor.get("name") if isinstance(actor, dict) else ""
        actor_name = str(actor_name or "").strip()
        created_on = str(revisions_sorted[i].get("created_on") or "").strip()

        diff = podio_fetch_revision_diff(access_token, item_id, prev_rev, curr_rev, max_retries, max_backoff)

        if activity_pace_seconds > 0:
            time.sleep(activity_pace_seconds)

        change_lines = format_revision_diff(diff)
        if not change_lines:
            change_lines = ["Item updated"]

        lines.append(f"{created_on} — {actor_name}:")
        for change_line in change_lines:
            lines.append(f"  - {change_line}")
        lines.append("")

    return "\n".join(lines).strip()


# -----------------------------
# Jira API helpers
# -----------------------------
def jira_session(email: str, api_token: str) -> requests.Session:
    """Create an authenticated Jira session."""
    session = requests.Session()
    session.auth = (email, api_token)
    return session


def jira_get_createmeta_fields(s: requests.Session, base_url: str, project_key: str, issue_type_name: str) -> Dict[str, Dict[str, Any]]:
    """Fetch Jira create metadata fields for one project + issue type."""
    url = f"{base_url}/rest/api/3/issue/createmeta"
    params = {"projectKeys": project_key, "expand": "projects.issuetypes.fields"}
    response = s.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    for project in data.get("projects") or []:
        if project.get("key") != project_key:
            continue
        for issue_type in project.get("issuetypes") or []:
            if issue_type.get("name") == issue_type_name:
                return issue_type.get("fields") or {}
    return {}


def jira_find_field_id_by_name(createmeta_fields: Dict[str, Dict[str, Any]], field_name: str) -> Optional[str]:
    """Find a Jira field ID by its display name."""
    for field_id, meta in createmeta_fields.items():
        if isinstance(meta, dict) and meta.get("name") == field_name:
            return field_id
    return None


def jira_search_user_account_id(s: requests.Session, base_url: str, query: str) -> Optional[str]:
    """Find a Jira user accountId by email or name."""
    if not query:
        return None
    url = f"{base_url}/rest/api/3/user/search"
    params = {"query": query, "maxResults": 10}
    response = s.get(url, params=params, timeout=60)
    if response.status_code >= 400:
        return None
    users = response.json() if isinstance(response.json(), list) else []
    if not users:
        return None
    return users[0].get("accountId")


def adf_doc_from_text(text: str, max_chunk: int = 3000) -> Dict[str, Any]:
    """Convert plain text into Jira Atlassian Document Format."""
    text = text or ""
    chunks = [text[i:i + max_chunk] for i in range(0, len(text), max_chunk)] or [""]
    content = []
    for chunk in chunks:
        content.append({
            "type": "paragraph",
            "content": [{"type": "text", "text": chunk}],
        })
    return {"type": "doc", "version": 1, "content": content}


def adf_doc_with_headings(sections: List[Tuple[str, str]]) -> Dict[str, Any]:
    """Build Jira description ADF using heading + paragraph sections."""
    content: List[Dict[str, Any]] = []
    for title, body in sections:
        title = (title or "").strip()
        body = (body or "").strip()

        if title:
            content.append({
                "type": "heading",
                "attrs": {"level": 3},
                "content": [{"type": "text", "text": title}],
            })

        if body:
            for line in body.split("\n"):
                line = line.strip()
                if not line:
                    continue
                content.append({
                    "type": "paragraph",
                    "content": [{"type": "text", "text": line}],
                })

    if not content:
        content = [{"type": "paragraph", "content": [{"type": "text", "text": ""}]}]

    return {"type": "doc", "version": 1, "content": content}


def jira_add_attachment_bytes(s: requests.Session, base_url: str, issue_key: str, filename: str, content: bytes):
    """Upload a binary attachment to a Jira issue."""
    url = f"{base_url}/rest/api/3/issue/{issue_key}/attachments"
    headers = {"X-Atlassian-Token": "no-check", "Accept": "application/json"}
    files = {"file": (filename, content)}
    response = s.post(url, headers=headers, files=files, timeout=180)
    if response.status_code >= 400:
        print("Jira attachment error:", response.status_code, response.text)
    response.raise_for_status()


def jira_add_attachment_text(s: requests.Session, base_url: str, issue_key: str, filename: str, text: str):
    """Upload a plain-text attachment to a Jira issue."""
    jira_add_attachment_bytes(s, base_url, issue_key, filename, text.encode("utf-8"))


def jira_add_comment(s: requests.Session, base_url: str, issue_key: str, original_author: str, original_timestamp: str, comment_text: str):
    """
    Add a Jira comment.
    Jira Cloud cannot impersonate original authors, so author/time are preserved in the body.
    """
    prefix = ""
    if original_author or original_timestamp:
        prefix = f"Original author: {original_author}\nOriginal time: {original_timestamp}\n\n"
    body = prefix + (comment_text or "")

    url = f"{base_url}/rest/api/3/issue/{issue_key}/comment"
    payload = {"body": adf_doc_from_text(body)}

    response = s.post(
        url,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        json=payload,
        timeout=60,
    )
    if response.status_code >= 400:
        print("Jira add comment error:", response.status_code, response.text)
    response.raise_for_status()


def jira_create_issue(
    s: requests.Session,
    base_url: str,
    project_key: str,
    issue_type: str,
    summary: str,
    description_adf: Dict[str, Any],
    labels: List[str],
    created_date_value: str,
    reporter_account_id: Optional[str],
    developer_account_ids: List[str],
    createmeta_fields: Dict[str, Dict[str, Any]],
) -> str:
    """
    Create a Jira issue and populate mapped custom/system fields.
    Mappings:
        - Created Date
        - Reporter
        - Developer
    """
    created_date_field_id = jira_find_field_id_by_name(createmeta_fields, "Created Date")
    developer_field_id = jira_find_field_id_by_name(createmeta_fields, "Developer")

    fields: Dict[str, Any] = {
        "project": {"key": project_key},
        "summary": summary,
        "issuetype": {"name": issue_type},
        "description": description_adf,
        "labels": labels,
    }

    if created_date_field_id and created_date_value:
        fields[created_date_field_id] = created_date_value

    if developer_field_id and developer_account_ids:
        fields[developer_field_id] = [{"accountId": account_id} for account_id in developer_account_ids if account_id]

    if reporter_account_id:
        fields["reporter"] = {"accountId": reporter_account_id}

    url = f"{base_url}/rest/api/3/issue"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    response = s.post(url, headers=headers, json={"fields": fields}, timeout=60)

    # Retry without reporter if Jira rejects it.
    if response.status_code == 400:
        try:
            err = response.json().get("errors", {})
        except Exception:
            err = {}

        if "reporter" in err:
            print(f"⚠️ Jira rejected reporter. Retrying without reporter. Error={err.get('reporter')}")
            fields.pop("reporter", None)
            response_retry = s.post(url, headers=headers, json={"fields": fields}, timeout=60)
            if response_retry.status_code >= 400:
                print("Jira create issue error (retry):", response_retry.status_code, response_retry.text)
            response_retry.raise_for_status()
            return response_retry.json()["key"]

    if response.status_code >= 400:
        print("Jira create issue error:", response.status_code, response.text)
    response.raise_for_status()
    return response.json()["key"]


# -----------------------------
# Main migration flow
# -----------------------------
def main():
    """Main entry point for Podio -> Jira migration."""
    load_dotenv()

    # Podio configuration
    podio_client_id = env_str("PODIO_CLIENT_ID")
    podio_client_secret = env_str("PODIO_CLIENT_SECRET")
    podio_app_id = int(env_str("PODIO_APP_ID"))
    podio_app_token = env_str("PODIO_APP_TOKEN")
    vpc_target = strip_html(env_str("PODIO_VPC_TARGET")).lower()

    # Jira configuration
    jira_base = env_str("JIRA_BASE_URL").rstrip("/")
    jira_email = env_str("JIRA_EMAIL")
    jira_api_token = env_str("JIRA_API_TOKEN")
    jira_project_key = env_str("JIRA_PROJECT_KEY")
    jira_issue_type = env_str("JIRA_ISSUE_TYPE")

    # Migration controls
    take_count = env_int("TAKE_COUNT", 10)
    max_attachments = env_int("MAX_ATTACHMENTS", 10)
    max_attachment_size_mb = env_int("MAX_ATTACHMENT_SIZE_MB", 50)
    max_attachment_bytes = max_attachment_size_mb * 1024 * 1024

    max_retries = env_int("PODIO_MAX_RETRIES", 10)
    max_backoff = env_int("PODIO_MAX_BACKOFF_SECONDS", 120)
    pace_seconds = env_float("PODIO_PACE_SECONDS", 0.5)
    activity_pace_seconds = env_float("PODIO_ACTIVITY_PACE_SECONDS", 0.8)
    comments_pace_seconds = env_float("PODIO_COMMENTS_PACE_SECONDS", 0.2)

    print("🚀 Starting Podio -> Jira migration...")

    # Authenticate
    podio_token = podio_access_token_app_auth(
        podio_client_id, podio_client_secret, podio_app_id, podio_app_token
    )
    print("✅ Podio auth OK")

    jira = jira_session(jira_email, jira_api_token)
    print("✅ Jira auth OK")

    # Load Jira field metadata
    createmeta_fields = jira_get_createmeta_fields(jira, jira_base, jira_project_key, jira_issue_type)
    if not createmeta_fields:
        raise ValueError(
            f"No create metadata found for project={jira_project_key}, issue_type={jira_issue_type}"
        )

    print("\nJira fields found on create screen:")
    for name in ["Created Date", "Developer", "Reporter"]:
        field_id = jira_find_field_id_by_name(createmeta_fields, name)
        print(f"  - {name}: {field_id}")

    # Cache Jira user lookups
    user_cache: Dict[str, Optional[str]] = {}

    def account_id_for(name: str, email: str) -> Optional[str]:
        """Resolve a Jira accountId from email first, then name."""
        query = (email or "").strip() or (name or "").strip()
        if not query:
            return None
        if query in user_cache:
            return user_cache[query]
        account_id = jira_search_user_account_id(jira, jira_base, query)
        user_cache[query] = account_id
        return account_id

    # Scan Podio tickets until we find enough matches
    matched: List[Dict[str, Any]] = []
    offset = 0
    page_size = 50

    while len(matched) < take_count:
        print(f"🔎 Scanning... offset={offset} matched={len(matched)}/{take_count}")
        page = podio_fetch_items_page(podio_token, podio_app_id, page_size, offset, max_retries, max_backoff)
        if not page:
            break

        for item_stub in page:
            item_id = item_stub.get("item_id")
            if not isinstance(item_id, int):
                continue

            stub_vpc = strip_html(podio_stub_vpc(item_stub)).lower()
            if stub_vpc and stub_vpc != vpc_target:
                continue

            if not podio_stub_has_attachments(item_stub):
                continue

            item_detail = podio_fetch_item_detail(podio_token, item_id, max_retries, max_backoff)

            if pace_seconds > 0:
                time.sleep(pace_seconds)

            file_ids = podio_collect_file_ids(item_detail)
            if not file_ids:
                continue

            item_stub["_detail"] = item_detail
            item_stub["_file_ids"] = file_ids
            matched.append(item_stub)

            if len(matched) >= take_count:
                break

        offset += page_size

    if not matched:
        print("❌ No matching tickets found.")
        return

    print(f"\n✅ Found {len(matched)} tickets. Starting Jira migration...\n")

    for index, item_stub in enumerate(matched, start=1):
        item_detail = item_stub["_detail"]
        file_ids: List[int] = item_stub["_file_ids"]
        item_id = item_detail["item_id"]

        podio_link = str(item_detail.get("link") or item_stub.get("link") or "").strip()
        title = strip_html(item_detail.get("title") or item_stub.get("title") or f"Podio Item {item_id}")

        # Extract mapped fields
        created_on = podio_get_created_on(item_detail)
        created_by_name, created_by_email = podio_get_created_by_name_email(item_detail)
        dev_people = podio_get_multi_user_field_names_emails(item_detail, "developer-assigned")

        reporter_account_id = account_id_for(created_by_name, created_by_email)

        developer_account_ids: List[str] = []
        for name, email in dev_people:
            account_id = account_id_for(name, email)
            if account_id:
                developer_account_ids.append(account_id)
        developer_account_ids = list(dict.fromkeys(developer_account_ids))

        # Build description from remaining fields
        all_fields = podio_extract_all_fields(item_detail)
        sections: List[Tuple[str, str]] = []

        primary_desc = all_fields.get("Description", "") or all_fields.get("Description and steps to reproduce", "")
        if primary_desc:
            sections.append(("", primary_desc))

        sections.append(("Podio Ticket Link", podio_link))

        for label in sorted(all_fields.keys()):
            if label in {"Description", "Description and steps to reproduce"} and primary_desc:
                continue
            sections.append((label, all_fields[label]))

        description_adf = adf_doc_with_headings(sections)
        labels = ["podio-migration", "podio-bugs", f"vpc-{vpc_target}"]

        # Create Jira issue
        issue_key = jira_create_issue(
            s=jira,
            base_url=jira_base,
            project_key=jira_project_key,
            issue_type=jira_issue_type,
            summary=title,
            description_adf=description_adf,
            labels=labels,
            created_date_value=created_on,
            reporter_account_id=reporter_account_id,
            developer_account_ids=developer_account_ids,
            createmeta_fields=createmeta_fields,
        )

        jira_link = f"{jira_base}/browse/{issue_key}"

        # Upload regular Podio attachments
        uploaded = 0
        skipped_size = 0
        failed = 0

        for file_id in file_ids[:max_attachments]:
            try:
                meta = podio_file_meta(podio_token, file_id, max_retries, max_backoff)
                filename = meta.get("name") or f"podio_file_{file_id}"
                size = meta.get("size")

                if isinstance(size, int) and size > max_attachment_bytes:
                    skipped_size += 1
                    continue

                blob = podio_download_file_bytes(podio_token, file_id, max_retries, max_backoff)
                if len(blob) > max_attachment_bytes:
                    skipped_size += 1
                    continue

                jira_add_attachment_bytes(jira, jira_base, issue_key, filename, blob)
                uploaded += 1
            except Exception as exc:
                print(f"   ⚠️ Attachment failed file_id={file_id}: {exc}")
                failed += 1

        # Add Podio comments into Jira comments
        added_comments = 0
        try:
            comments = podio_fetch_comments(podio_token, item_id, max_retries, max_backoff)
            comments = [comment for comment in comments if isinstance(comment, dict)]
            comments_sorted = sorted(comments, key=lambda c: c.get("created_on", ""))

            for comment in comments_sorted:
                raw_text = comment.get("value") or ""
                text = strip_html(raw_text)
                if not text:
                    continue

                created_on_comment = str(comment.get("created_on") or "")
                created_by = comment.get("created_by") or {}
                author_name = created_by.get("name") if isinstance(created_by, dict) else ""
                author_name = str(author_name or "").strip()

                jira_add_comment(
                    jira,
                    jira_base,
                    issue_key,
                    author_name or "Unknown",
                    created_on_comment or "Unknown",
                    text,
                )
                added_comments += 1

                if comments_pace_seconds > 0:
                    time.sleep(comments_pace_seconds)

        except Exception as exc:
            print(f"   ⚠️ Comments migration failed for item {item_id}: {exc}")

        # Upload Podio activity log as text attachment
        try:
            activity_text = podio_build_activity_log(
                podio_token, item_id, max_retries, max_backoff, activity_pace_seconds
            )
            if activity_text:
                jira_add_attachment_text(jira, jira_base, issue_key, f"podio_activity_{item_id}.txt", activity_text)
        except Exception as exc:
            print(f"   ⚠️ Activity attachment failed for item {item_id}: {exc}")

        # Print summary
        print(f"{index}. ✅ Created Jira issue: {issue_key}")
        print(f"   Podio: {podio_link}")
        print(f"   Jira : {jira_link}")
        print(f"   Created Date -> {created_on}")
        print(f"   Reporter     -> {created_by_name} / {created_by_email} / {reporter_account_id}")
        print(f"   Developer    -> {developer_account_ids}")
        print(f"   Comments     -> added={added_comments}")
        print(f"   Attachments  -> uploaded={uploaded}, skipped_size={skipped_size}, failed={failed}\n")

    print("✅ Done. Migrated requested tickets.")


if __name__ == "__main__":
    main()
