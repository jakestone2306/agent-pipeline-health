"""
Pipeline Health Agent - Optimized
Batches deal analysis via a single Claude call instead of one per deal.
"""

import os
import json
import requests
from datetime import datetime, timezone
from anthropic import Anthropic

HUBSPOT_TOKEN    = os.environ["HUBSPOT_TOKEN"]
SLACK_TOKEN      = os.environ["SLACK_TOKEN"]
ANTHROPIC_KEY    = os.environ["ANTHROPIC_API_KEY"]
MANAGER_SLACK_ID = os.environ.get("MANAGER_SLACK_ID", "U08357HEYJF")

client = Anthropic(api_key=ANTHROPIC_KEY)

OWNER_MAP = {
    "85012029":  {"name": "Octavio Pala",   "slack_id": "U09RZCGQQJJ"},
    "84032188":  {"name": "Kylene Warne",   "slack_id": "U09KUM1CP5K"},
    "88178787":  {"name": "Brandon Perez",  "slack_id": "U0AE5DA12N9"},
    "300195503": {"name": "Jacob Bolton",   "slack_id": "U07R34DT45S"},
    "299068163": {"name": "Jacob Simon",    "slack_id": "U02F8F5B8RM"},
    "170827178": {"name": "Jake Stone",     "slack_id": "U08357HEYJF"},
}

PIPELINE_NAMES = {
    "default":   "Sales Pipeline",
    "716607755": "Growth Pipeline",
    "718920103": "No Show Pipeline",
    "668490044": "Enterprise Pipeline",
    "691837998": "Partner Pipeline",
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
    "980617892":  "Initial Proposal",
    "980617893":  "Infosec / Legal Review",
    "980617894":  "POC",
    "1072305424": "OB Held",
    "1045587374": "Further OB Work Needed",
    "1104816808": "Pilot Period",
    "1243051168": "Expansion Opportunity",
    "1045587373": "At Risk",
    "1243051170": "Retention Convo",
    "1048510752": "No Show - Post One Month",
}

EXCLUDED_STAGES = {
    "closedwon", "closedlost", "941713498", "982154351",
    "1012659618", "998944549", "1104889877", "1045587377",
    "1045587376", "980617896", "1243051169",
}

def hs_post(path, payload):
    r = requests.post(
        f"https://api.hubapi.com{path}",
        headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"},
        json=payload,
    )
    r.raise_for_status()
    return r.json()

def fetch_active_deals():
    deals, after = [], None
    while True:
        body = {
            "filterGroups": [{"filters": [
                {"propertyName": "dealstage", "operator": "NOT_IN", "values": list(EXCLUDED_STAGES)}
            ]}],
            "properties": ["dealname","dealstage","pipeline","amount","closedate",
                           "hubspot_owner_id","notes_last_updated","hs_deal_stage_probability"],
            "limit": 100,
        }
        if after:
            body["after"] = after
        data = hs_post("/crm/v3/objects/deals/search", body)
        deals.extend(data.get("results", []))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
    return deals

def prioritize_deals(deals):
    """Score and return top 30 most important deals to analyze."""
    scored = []
    now = datetime.now(timezone.utc)
    for deal in deals:
        p = deal["properties"]
        amount = float(p.get("amount") or 0)
        prob   = float(p.get("hs_deal_stage_probability") or 0)
        last_act = p.get("notes_last_updated")
        days_stale = 0
        if last_act:
            try:
                last_dt = datetime.fromisoformat(last_act.replace("Z","+00:00"))
                days_stale = (now - last_dt).days
            except: pass

        # Score: high value + high prob + stale = highest priority
        score = (amount / 1000) + (prob * 50) + (days_stale * 2)
        scored.append((score, deal))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [d for _, d in scored[:30]]

def analyze_deals_batch(deals):
    """Analyze all deals in a single Claude call."""
    now = datetime.now(timezone.utc)

    deal_summaries = []
    for deal in deals:
        p = deal["properties"]
        name      = p.get("dealname") or "Unnamed"
        stage     = STAGE_NAMES.get(p.get("dealstage",""), p.get("dealstage",""))
        pipeline  = PIPELINE_NAMES.get(p.get("pipeline",""), "Unknown")
        amount    = p.get("amount") or "0"
        closedate = p.get("closedate","")
        last_act  = p.get("notes_last_updated","")
        prob      = float(p.get("hs_deal_stage_probability") or 0)
        owner_id  = p.get("hubspot_owner_id","")
        owner     = OWNER_MAP.get(owner_id, {}).get("name","Unknown")

        days_stale, close_in = "?", "?"
        if last_act:
            try:
                days_stale = (now - datetime.fromisoformat(last_act.replace("Z","+00:00"))).days
            except: pass
        if closedate:
            try:
                close_in = (datetime.fromisoformat(closedate.replace("Z","+00:00")) - now).days
            except: pass

        deal_summaries.append({
            "id": deal["id"],
            "name": name,
            "owner": owner,
            "owner_id": owner_id,
            "pipeline": pipeline,
            "stage": stage,
            "amount": amount,
            "prob_pct": round(prob * 100),
            "days_stale": days_stale,
            "close_in_days": close_in,
        })

    prompt = f"""You are an expert sales coach analyzing a B2B insurance software pipeline.

Analyze each deal below and return a JSON array. Each element must have:
- id: the deal id (string, exactly as given)
- risk_score: 1-10 (10=highest risk)
- risks: array of 1-2 short risk strings
- top_action: single most important action for the rep TODAY (specific, actionable)
- manager_flag: null OR a specific action for the sales manager

Deals to analyze:
{json.dumps(deal_summaries, indent=2)}

Return ONLY a valid JSON array, no markdown, no explanation."""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4000,
        messages=[{"role": "user", "content": prompt}]
    )
    text = response.content[0].text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    return {str(a["id"]): a for a in json.loads(text.strip())}

def generate_manager_summary(deal_data, analyses):
    """Single Claude call for manager summary."""
    combined = []
    for d in deal_data:
        a = analyses.get(str(d["id"]), {})
        combined.append({
            "name": d["name"], "owner": d["owner"], "pipeline": d["pipeline"],
            "stage": d["stage"], "amount": d["amount"],
            "risk_score": a.get("risk_score", "?"),
            "top_action": a.get("top_action",""),
            "manager_flag": a.get("manager_flag"),
        })

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1500,
        messages=[{"role": "user", "content": f"""You are a VP of Sales writing a pipeline health summary for the sales manager.

Pipeline data (top priority deals):
{json.dumps(combined, indent=2)}

Write a concise Slack message (mrkdwn) covering:
1. *Overall Pipeline Health* - deal count, total value at risk, avg risk score
2. *Top 5 Priority Deals* - most urgent deals needing attention
3. *Your Action Items as Manager* - specific things you should do today
4. *Rep Coaching Notes* - any reps who need support or recognition

Be direct and specific. Max 500 words. Use Slack mrkdwn (*bold*, bullet points).
Do NOT use ## headers."""}]
    )
    return response.content[0].text.strip()

def create_hs_note(deal_id, owner_id, note_body):
    now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    note = hs_post("/crm/v3/objects/notes", {
        "properties": {
            "hs_note_body": f"🤖 AI Action Item\n\n{note_body}",
            "hs_timestamp": str(now_ts),
            "hubspot_owner_id": owner_id,
        },
        "associations": [{"to": {"id": deal_id}, "types": [
            {"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 214}
        ]}],
    })
    return note["id"]

def slack_dm(user_id, message):
    r = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_TOKEN}", "Content-Type": "application/json"},
        json={"channel": user_id, "text": message, "mrkdwn": True},
    )
    r.raise_for_status()
    resp = r.json()
    if not resp.get("ok"):
        raise Exception(f"Slack error: {resp.get('error')}")
    return resp

def run():
    print("🔍 Fetching active deals...")
    all_deals = fetch_active_deals()
    print(f"📊 Found {len(all_deals)} active deals — selecting top 30 to analyze")

    deals = prioritize_deals(all_deals)

    print(f"🤖 Analyzing {len(deals)} deals in batch...")
    analyses = analyze_deals_batch(deals)
    print(f"✅ Analysis complete")

    # Build deal data list for manager summary
    deal_data = []
    owner_actions = {}

    for deal in deals:
        p = deal["properties"]
        deal_id  = deal["id"]
        name     = p.get("dealname") or "Unnamed"
        owner_id = p.get("hubspot_owner_id","")
        amount   = p.get("amount") or "0"
        stage    = STAGE_NAMES.get(p.get("dealstage",""), p.get("dealstage",""))
        pipeline = PIPELINE_NAMES.get(p.get("pipeline",""), "Unknown")
        owner    = OWNER_MAP.get(owner_id, {}).get("name","Unknown")

        analysis = analyses.get(str(deal_id), {})
        risk     = analysis.get("risk_score", 0)
        action   = analysis.get("top_action","")

        deal_data.append({"id": deal_id, "name": name, "owner": owner,
                          "pipeline": pipeline, "stage": stage, "amount": amount})

        # Create HubSpot note for medium/high risk or high value deals
        if (risk >= 4 or float(amount) >= 9600) and owner_id in OWNER_MAP and action:
            try:
                create_hs_note(deal_id, owner_id, action)
                print(f"  📋 Note created for: {name}")
            except Exception as e:
                print(f"  ⚠️ Note failed for {name}: {e}")

            if owner_id not in owner_actions:
                owner_actions[owner_id] = []
            owner_actions[owner_id].append({
                "deal_name": name, "amount": amount, "stage": stage,
                "action": action, "risks": analysis.get("risks",[]),
            })

    # Slack reps
    print(f"\n📨 Slacking {len(owner_actions)} deal owners...")
    for owner_id, actions in owner_actions.items():
        slack_id   = OWNER_MAP[owner_id]["slack_id"]
        owner_name = OWNER_MAP[owner_id]["name"]
        lines = [f"*🤖 Pipeline Action Items — {datetime.now().strftime('%B %d, %Y')}*\n"]
        for a in actions:
            amt = f"${float(a['amount']):,.0f}" if a['amount'] else "No amount"
            lines.append(f"*{a['deal_name']}* ({amt} · {a['stage']})")
            lines.append(f"  ✅ *Action:* {a['action']}")
            if a['risks']:
                lines.append(f"  ⚠️ *Risks:* {', '.join(a['risks'][:2])}")
            lines.append(f"  📋 HubSpot note created on this deal")
            lines.append("")
        lines.append("_Generated by your AI Pipeline Agent_")
        try:
            slack_dm(slack_id, "\n".join(lines))
            print(f"  ✅ Slacked {owner_name}")
        except Exception as e:
            print(f"  ⚠️ Slack failed for {owner_name}: {e}")

    # Manager summary
    print("\n📊 Generating manager summary...")
    summary = generate_manager_summary(deal_data, analyses)
    header  = f"*🤖 Pipeline Health Report — {datetime.now().strftime('%B %d, %Y')}*\n\n"
    try:
        slack_dm(MANAGER_SLACK_ID, header + summary)
        print("✅ Manager summary sent to Jake")
    except Exception as e:
        print(f"⚠️ Manager Slack failed: {e}")

    print("\n🎉 Done!")
    return {
        "total_deals": len(all_deals),
        "deals_analyzed": len(deals),
        "actions_created": sum(len(v) for v in owner_actions.values()),
    }

if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
