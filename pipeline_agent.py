"""
Pipeline Health Agent
- Pulls all active deals from HubSpot
- Analyzes risks and acceleration opportunities via Claude
- Creates HubSpot tasks for deal owners
- Slacks deal owners with their action items
- Sends manager (Jake) a full pipeline health summary
"""

import os
import json
import requests
from datetime import datetime, timezone
from anthropic import Anthropic

# ── Config ─────────────────────────────────────────────────────────────────
HUBSPOT_TOKEN = os.environ["HUBSPOT_TOKEN"]
SLACK_TOKEN   = os.environ["SLACK_TOKEN"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
MANAGER_SLACK_ID = os.environ.get("MANAGER_SLACK_ID", "U08357HEYJF")  # Jake Stone

client = Anthropic(api_key=ANTHROPIC_KEY)

# Owner ID → { name, slack_id }
OWNER_MAP = {
    "85012029":  {"name": "Octavio Pala",   "slack_id": "U09RZCGQQJJ"},
    "84032188":  {"name": "Kylene Warne",   "slack_id": "U09KUM1CP5K"},
    "88178787":  {"name": "Brandon Perez",  "slack_id": "U0AE5DA12N9"},
    "300195503": {"name": "Jacob Bolton",   "slack_id": "U07R34DT45S"},
    "299068163": {"name": "Jacob Simon",    "slack_id": "U02F8F5B8RM"},
    "170827178": {"name": "Jake Stone",     "slack_id": "U08357HEYJF"},
}

PIPELINE_NAMES = {
    "default":    "Sales Pipeline",
    "716607755":  "Growth Pipeline",
    "718920103":  "No Show Pipeline",
    "668490044":  "Enterprise Pipeline",
    "691837998":  "Partner Pipeline",
}

STAGE_NAMES = {
    "appointmentscheduled": "Demo Scheduled",
    "1083966814": "Post Demo - Pending Internal Alignment",
    "1083966816": "Follow Up Meeting Scheduled",
    "1083966815": "Pricing Estimate Delivered",
    "1083966817": "Pending IT/Legal Review",
    "1083966818": "Pending Customer Reference",
    "contractsent": "Onboarding Scheduled",
    "1009943555": "On Hold - Long Term",
    "980617890":  "Initial Discovery",
    "980617891":  "Multi-stakeholder Demo",
    "980617892":  "Initial Proposal",
    "980617893":  "Infosec / Legal Review",
    "980617894":  "POC",
    "980617895":  "Expansion Verbal Commit",
    "1072305424": "OB Held",
    "1045587374": "Further OB Work Needed",
    "1104816808": "Pilot Period",
    "1243051168": "Expansion Opportunity [ACTIONABLE]",
    "1045587373": "At Risk [ACTIONABLE]",
    "1243051170": "Retention Convo",
    "1048510752": "No Show - Post One Month",
    "1048488772": "Re-engaged - Move Back to SP",
}

EXCLUDED_STAGES = {
    "closedwon", "closedlost", "941713498", "982154351",
    "1012659618", "998944549", "1104889877", "1045587377",
    "1045587376", "980617896", "1243051169",
}

# ── HubSpot helpers ────────────────────────────────────────────────────────
def hs_get(path, params=None):
    r = requests.get(
        f"https://api.hubapi.com{path}",
        headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}"},
        params=params,
    )
    r.raise_for_status()
    return r.json()

def hs_post(path, payload):
    r = requests.post(
        f"https://api.hubapi.com{path}",
        headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"},
        json=payload,
    )
    r.raise_for_status()
    return r.json()

def fetch_active_deals():
    """Fetch all active deals (paginated)."""
    deals, after = [], None
    while True:
        body = {
            "filterGroups": [{"filters": [
                {"propertyName": "dealstage", "operator": "NOT_IN",
                 "values": list(EXCLUDED_STAGES)}
            ]}],
            "properties": ["dealname","dealstage","pipeline","amount","closedate",
                           "hubspot_owner_id","notes_last_updated","hs_deal_stage_probability"],
            "limit": 100,
        }
        if after:
            body["after"] = after
        data = hs_post("/crm/v3/objects/deals/search", body)
        deals.extend(data.get("results", []))
        paging = data.get("paging", {}).get("next", {})
        after = paging.get("after")
        if not after:
            break
    return deals

def create_hs_task(deal_id, owner_id, task_body):
    """Create a HubSpot task and associate it with the deal."""
    due_ts = int((datetime.now(timezone.utc).timestamp() + 86400) * 1000)  # due tomorrow
    task = hs_post("/crm/v3/objects/tasks", {
        "properties": {
            "hs_task_subject": f"🤖 AI Action: {task_body[:80]}",
            "hs_task_body": task_body,
            "hs_task_status": "NOT_STARTED",
            "hs_task_type": "TODO",
            "hubspot_owner_id": owner_id,
            "hs_timestamp": str(due_ts),
        },
        "associations": [{"to": {"id": deal_id}, "types": [
            {"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 216}
        ]}],
    })
    return task["id"]

# ── Slack helper ───────────────────────────────────────────────────────────
def slack_dm(user_id, message):
    r = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_TOKEN}", "Content-Type": "application/json"},
        json={"channel": user_id, "text": message, "mrkdwn": True},
    )
    r.raise_for_status()
    return r.json()

# ── AI Analysis ────────────────────────────────────────────────────────────
def analyze_deal(deal):
    props = deal["properties"]
    name       = props.get("dealname") or "Unnamed Deal"
    stage      = STAGE_NAMES.get(props.get("dealstage",""), props.get("dealstage","Unknown"))
    pipeline   = PIPELINE_NAMES.get(props.get("pipeline",""), "Unknown Pipeline")
    amount     = props.get("amount") or "0"
    closedate  = props.get("closedate","No close date")
    last_act   = props.get("notes_last_updated","Unknown")
    prob       = float(props.get("hs_deal_stage_probability") or 0)
    owner_id   = props.get("hubspot_owner_id","")
    owner_name = OWNER_MAP.get(owner_id, {}).get("name","Unknown Owner")

    now = datetime.now(timezone.utc)
    days_since_activity = "unknown"
    if last_act and last_act != "Unknown":
        try:
            last_dt = datetime.fromisoformat(last_act.replace("Z","+00:00"))
            days_since_activity = (now - last_dt).days
        except:
            pass

    close_days_away = "unknown"
    if closedate and closedate != "No close date":
        try:
            close_dt = datetime.fromisoformat(closedate.replace("Z","+00:00"))
            close_days_away = (close_dt - now).days
        except:
            pass

    prompt = f"""You are an expert sales coach analyzing a B2B insurance software deal.

Deal: {name}
Pipeline: {pipeline}
Stage: {stage}
Amount: ${amount}
Close Date: {closedate} ({close_days_away} days away)
Win Probability: {round(prob*100)}%
Days Since Last Activity: {days_since_activity}
Owner: {owner_name}

Analyze this deal and respond ONLY with a valid JSON object (no markdown, no explanation) with exactly these keys:
{{
  "risk_score": <1-10, where 10 is highest risk>,
  "risks": ["<risk 1>", "<risk 2>"],
  "accelerators": ["<action 1>", "<action 2>"],
  "top_action": "<single most important action for the rep to take TODAY, specific and actionable>",
  "manager_flag": "<null or a specific thing the manager should do to help this deal>"
}}"""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=600,
        messages=[{"role": "user", "content": prompt}]
    )
    text = response.content[0].text.strip()
    # strip markdown fences if present
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    return json.loads(text.strip())

def generate_manager_summary(deal_analyses):
    """Generate a high-level pipeline health summary for Jake."""
    summary_data = json.dumps([{
        "deal": d["name"],
        "owner": d["owner"],
        "pipeline": d["pipeline"],
        "amount": d["amount"],
        "stage": d["stage"],
        "risk_score": d["analysis"]["risk_score"],
        "top_action": d["analysis"]["top_action"],
        "manager_flag": d["analysis"]["manager_flag"],
    } for d in deal_analyses], indent=2)

    prompt = f"""You are a VP of Sales reviewing your team's pipeline health report.

Here is the analysis of all active deals:
{summary_data}

Write a concise, executive Slack message (use Slack mrkdwn formatting) for the sales manager that covers:
1. *Overall Pipeline Health* - a quick summary (deal count, total value, avg risk)
2. *Highest Priority Deals* - top 3-5 deals needing immediate attention
3. *Actions for You as Manager* - specific things the manager should do to unblock deals
4. *Team Performance Notes* - any reps who need coaching or recognition

Be direct, specific, and actionable. Use bullet points and bold text. Max 400 words."""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.content[0].text.strip()

# ── Main orchestration ─────────────────────────────────────────────────────
def run():
    print("🔍 Fetching active deals from HubSpot...")
    deals = fetch_active_deals()
    print(f"📊 Found {len(deals)} active deals")

    deal_analyses = []
    owner_actions = {}  # owner_id → list of { deal_name, action, task_id }

    for deal in deals:
        props = deal["properties"]
        name = props.get("dealname") or "Unnamed Deal"
        owner_id = props.get("hubspot_owner_id","")
        amount = props.get("amount") or "0"
        stage = STAGE_NAMES.get(props.get("dealstage",""), props.get("dealstage",""))
        pipeline = PIPELINE_NAMES.get(props.get("pipeline",""), "Unknown Pipeline")
        owner_name = OWNER_MAP.get(owner_id, {}).get("name","Unknown")

        print(f"  🤖 Analyzing: {name}")
        try:
            analysis = analyze_deal(deal)
        except Exception as e:
            print(f"    ⚠️ Analysis failed for {name}: {e}")
            continue

        # Only act on medium/high risk deals (risk >= 4) or high value
        should_act = analysis["risk_score"] >= 4 or float(amount or 0) >= 9600

        task_id = None
        if should_act and owner_id in OWNER_MAP:
            try:
                task_id = create_hs_task(deal["id"], owner_id, analysis["top_action"])
                print(f"    ✅ HubSpot task created: {task_id}")
            except Exception as e:
                print(f"    ⚠️ Task creation failed: {e}")

            if owner_id not in owner_actions:
                owner_actions[owner_id] = []
            owner_actions[owner_id].append({
                "deal_name": name,
                "amount": amount,
                "stage": stage,
                "action": analysis["top_action"],
                "risks": analysis["risks"],
                "task_id": task_id,
            })

        deal_analyses.append({
            "name": name,
            "owner": owner_name,
            "pipeline": pipeline,
            "stage": stage,
            "amount": amount,
            "analysis": analysis,
        })

    # ── Slack deal owners ──────────────────────────────────────────────────
    print("\n📨 Sending Slack messages to deal owners...")
    for owner_id, actions in owner_actions.items():
        slack_id = OWNER_MAP[owner_id]["slack_id"]
        owner_name = OWNER_MAP[owner_id]["name"]

        lines = [f"*🤖 Pipeline Action Items for You — {datetime.now().strftime('%B %d, %Y')}*\n"]
        for a in actions:
            amt = f"${float(a['amount']):,.0f}" if a['amount'] else "No amount"
            lines.append(f"*{a['deal_name']}* ({amt} · {a['stage']})")
            lines.append(f"  ✅ *Action:* {a['action']}")
            if a['risks']:
                lines.append(f"  ⚠️ *Risks:* {', '.join(a['risks'][:2])}")
            if a['task_id']:
                lines.append(f"  📋 HubSpot task created and assigned to you")
            lines.append("")

        lines.append("_Generated by your AI Pipeline Agent_")
        message = "\n".join(lines)

        try:
            slack_dm(slack_id, message)
            print(f"  ✅ Slacked {owner_name}")
        except Exception as e:
            print(f"  ⚠️ Slack failed for {owner_name}: {e}")

    # ── Manager summary ────────────────────────────────────────────────────
    print("\n📊 Generating manager summary...")
    manager_summary = generate_manager_summary(deal_analyses)
    header = f"*🤖 Pipeline Health Report — {datetime.now().strftime('%B %d, %Y')}*\n\n"

    try:
        slack_dm(MANAGER_SLACK_ID, header + manager_summary)
        print("✅ Manager summary sent to Jake")
    except Exception as e:
        print(f"⚠️ Manager Slack failed: {e}")

    print("\n🎉 Pipeline agent run complete!")
    return {"deals_analyzed": len(deal_analyses), "actions_created": sum(len(v) for v in owner_actions.values())}

if __name__ == "__main__":
    result = run()
    print(json.dumps(result, indent=2))
