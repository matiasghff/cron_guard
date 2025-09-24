#!/usr/bin/env python3
import json, urllib.parse, requests
from datetime import datetime, timezone

# ==== DANGER: hard-coded secrets (your call) =========================
PROJECT_ID   = "fif%2Fdata-factory%2Fcross%2Fargo%2Fbigqueryorchestrator%2Fbusinessunits%2Fcross%2Fcustom-processing%2Fscheduled-workflows-gitops"
REF          = "custom-processing-prod"
PRIVATE_TOKEN = "47z3ye3HYNjGgarRaDyp"  # <-- hard-coded
WEBHOOK_URL   = "https://defaultc4a8886bf140478bac47249555c30a.fd.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/52812b9197b3442ba86526996379a716/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=At9pNK6c8tKVXeP4jgH-hb823vYJJSH1hYBvhAsCUR0"  # <-- hard-coded
# ====================================================================

HEADERS = {"PRIVATE-TOKEN": PRIVATE_TOKEN}

FILE_PATHS = [
    "jira_ptus/jira_issues/cookiecutter.json",
    "jira_ptus/jira_dashboards/cookiecutter.json",
    "jira_ptus/google_sheets/cookiecutter.json",
    "jira_ptus/jira_indicators/cookiecutter.json",
    "jira_ptus/jira_maintenance/cookiecutter.json",
    "jira_ptus/jira_mstr/cookiecutter.json",
]

def is_true(val):
    if isinstance(val, bool): return val
    if isinstance(val, str): return val.strip().lower() == "true"
    return False

def fetch_cookie_json(project_id: str, ref: str, path: str) -> dict:
    enc = urllib.parse.quote(path, safe="")
    url = f"https://gitlab.falabella.tech/api/v4/projects/{project_id}/repository/files/{enc}/raw?ref={ref}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return json.loads(r.text)

def send_adaptive_card(webhook_url: str, title: str, lines: list[str]) -> None:
    blocks = [{"type": "TextBlock", "text": title, "weight": "Bolder", "size": "Medium"}]
    for line in lines:
        blocks.append({"type": "TextBlock", "text": line, "wrap": True})
    payload = {
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": blocks,
            },
        }]
    }
    resp = requests.post(webhook_url, json=payload, timeout=20)
    # Don’t print the full body; keep logs clean
    print("Teams webhook POST →", resp.status_code)
    resp.raise_for_status()

def main():
    any_false, problems = False, []
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")

    for file_path in FILE_PATHS:
        try:
            data = fetch_cookie_json(PROJECT_ID, REF, file_path)
            raw_val = data.get("execute_as_cron")
            ok = is_true(raw_val)
            print(f"{file_path}: execute_as_cron = {raw_val}")
            if not ok:
                any_false = True
                problems.append(f"- {file_path}: execute_as_cron={raw_val!r}")
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else "?"
            print(f"{file_path}: HTTP {code}")
            any_false = True
            problems.append(f"- {file_path}: fetch error (HTTP {code})")
        except Exception as e:
            print(f"{file_path}: error {e}")
            any_false = True
            problems.append(f"- {file_path}: {e}")

    if any_false:
        title = f"🚨 Cron guard: some cookiecutters are NOT scheduled (checked {now})"
        send_adaptive_card(WEBHOOK_URL, title, problems)
    else:
        print("✅ All cookiecutters have execute_as_cron=true")

if __name__ == "__main__":
    main()
