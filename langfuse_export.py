"""
Langfuse Dataset Exporter for Markly
=====================================
Pulls feedback + eval scores from Langfuse Dataset Runs.

SETUP:
  pip install requests

USAGE:
  1. Fill in the config below with your Langfuse host and API keys
  2. Fill in the Exports dict with the dataset and run names
  3. Run: python langfuse_export.py
"""
import os
import urllib.parse
import json
import requests
import time
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================
# CONFIG - Fill these in
# ============================================================
LANGFUSE_HOST = " add host here"  # No trailing slash
LANGFUSE_PUBLIC_KEY = " add public key here "
LANGFUSE_SECRET_KEY = " add secret key here "

# ============================================================
# After running STEP 1 (discover), fill these in
# with the exact dataset and run names printed by discover()
# ============================================================
EXPORTS = {
    "name": {
      # add name of dataset here
        "dataset": " ",
      # add name of evaluation run here
        "run": " ",
    },
    "name": {
        "dataset": "Copy of question_2b",
        "run": "Prompt markly_ai_service_v2_q2b-v2 on dataset Copy of question_2b - 2026-03-02T08:13:01.032Z",
    },
}


def get_auth():
    return (LANGFUSE_PUBLIC_KEY, LANGFUSE_SECRET_KEY)

def api_get(url, **kwargs):
    return requests.get(url, auth=get_auth(), verify=False, **kwargs)
# ============================================================
# STEP 1: Discover datasets and runs
# ============================================================

def list_datasets():
    datasets = []
    page = 1
    while True:
        resp = api_get( f"{LANGFUSE_HOST}/api/public/datasets",
            params={"limit": 50, "page": page}
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("data", [])
        if not items:
            break
        datasets.extend(items)
        if len(items) < 50:
            break
        page += 1
    return datasets


def get_dataset_runs(dataset_name):
    runs = []
    page = 1
    safe_name = urllib.parse.quote(dataset_name) # URL Encode
    while True:
        resp = api_get(f"{LANGFUSE_HOST}/api/public/datasets/{safe_name}/runs",
            params={"limit": 50, "page": page}
        )        
        resp.raise_for_status()
        data = resp.json()
        items = data.get("data", [])
        if not items:
            break
        runs.extend(items)
        if len(items) < 50:
            break
        page += 1
    return runs


def discover():
    print("=" * 60)
    print("STEP 1: DISCOVERING DATASETS AND RUNS")
    print("=" * 60)

    datasets = list_datasets()
    print(f"\nFound {len(datasets)} datasets:\n")

    for ds in datasets:
        name = ds.get("name", "unnamed")
        desc = ds.get("description", "")
        print(f'  Dataset: "{name}"')
        if desc:
            print(f"  Description: {desc}")
        print(f"  Created: {ds.get('createdAt', 'unknown')}")

        try:
            runs = get_dataset_runs(name)
            print(f"  Runs ({len(runs)}):")
            for r in runs:
                run_name = r.get("name", "unnamed")
                run_items = r.get("datasetRunItems", [])
                print(f'    - "{run_name}" ({len(run_items)} items)')
        except Exception as e:
            print(f"  Error fetching runs: {e}")

        print()

    print("=" * 60)
    print("\nNEXT: Copy the exact dataset and run names into the")
    print("EXPORTS dict at the top of this script, then run again.\n")


# ============================================================
# STEP 2: Export a dataset run
# ============================================================

def get_dataset_run_items(dataset_name, run_name):
    safe_ds = urllib.parse.quote(dataset_name)   # URL Encode
    safe_run = urllib.parse.quote(run_name)      # URL Encode
    resp = api_get(f"{LANGFUSE_HOST}/api/public/datasets/{safe_ds}/runs/{safe_run}")
    resp.raise_for_status()
    return resp.json()


def get_dataset_items(dataset_name, limit=50):
    items = []
    page = 1
    while True:
        resp = api_get(f"{LANGFUSE_HOST}/api/public/dataset-items",
                params={"datasetName": dataset_name, "limit": limit, "page": page}
            )
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("data", [])
        if not batch:
            break
        items.extend(batch)
        if len(batch) < limit:
            break
        page += 1
    return items

def get_observation(observation_id):
    resp = api_get(         f"{LANGFUSE_HOST}/api/public/observations/{observation_id}",
    )
    resp.raise_for_status()
    return resp.json()


def get_trace(trace_id):
    resp = api_get(         f"{LANGFUSE_HOST}/api/public/traces/{trace_id}",
    )
    resp.raise_for_status()
    return resp.json()


def get_scores_for_trace(trace_id):
    resp = api_get(         f"{LANGFUSE_HOST}/api/public/scores",
        params={"traceId": trace_id, "limit": 50}
    )
    resp.raise_for_status()
    return resp.json().get("data", [])


def export_run(label, dataset_name, run_name):
    print(f"\n{'='*60}")
    print(f"EXPORTING: {label}")
    print(f"  Dataset: {dataset_name}")
    print(f"  Run: {run_name}")
    print(f"{'='*60}\n")

    # Get run details (includes run items with trace/observation links)
    run_data = get_dataset_run_items(dataset_name, run_name)
    run_items = run_data.get("datasetRunItems", [])
    print(f"Found {len(run_items)} run items\n")

    # Sort by createdAt to preserve Langfuse order
    run_items.sort(key=lambda x: x.get("createdAt", ""))

    # Get dataset items for the input data
    ds_items = get_dataset_items(dataset_name)
    ds_item_map = {item["id"]: item for item in ds_items}

    entries = []
    for i, ri in enumerate(run_items):
        print(f"  [{i+1}/{len(run_items)}] Fetching...", end=" ")

        trace_id = ri.get("traceId")
        observation_id = ri.get("observationId")
        dataset_item_id = ri.get("datasetItemId")

        # Get the dataset item (input)
        ds_item = ds_item_map.get(dataset_item_id, {})
        item_input = ds_item.get("input", {})
        expected_output = ds_item.get("expectedOutput", {})

        # Get the observation (AI output)
        output = {}
        if observation_id:
            try:
                obs = get_observation(observation_id)
                output = obs.get("output", {})
            except Exception as e:
                print(f"(obs error: {e})", end=" ")

        # Fallback to trace output
        if not output and trace_id:
            try:
                trace = get_trace(trace_id)
                output = trace.get("output", {})
            except Exception as e:
                print(f"(trace error: {e})", end=" ")

        # Get eval scores
        scores = {}
        if trace_id:
            try:
                raw_scores = get_scores_for_trace(trace_id)
                for s in raw_scores:
                    score_name = s.get("name", "")
                    score_val = s.get("value")
                    scores[score_name] = score_val
            except Exception as e:
                print(f"(scores error: {e})", end=" ")

        entry = {
            "row": i + 1,
            "dataset_item_id": dataset_item_id,
            "trace_id": trace_id,
            "observation_id": observation_id,
            "created_at": ri.get("createdAt", ""),
            "input": item_input,
            "expected_output": expected_output,
            "output": output,
            "eval_scores": scores,
        }
        entries.append(entry)
        print("OK")

        # Rate limit protection
        time.sleep(0.2)

    # Save
    os.makedirs("data", exist_ok=True)
    filename = f"data/langfuse_{label}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(entries, f, indent=2, ensure_ascii=False)

    print(f"\nSaved {len(entries)} entries to {filename}")

    # Print sample
    if entries:
        e = entries[0]
        print(f"\nSample entry (row 1):")
        inp = e["input"]
        out = e["output"]
        print(f"  Input keys: {list(inp.keys()) if isinstance(inp, dict) else type(inp)}")
        print(f"  Output keys: {list(out.keys()) if isinstance(out, dict) else type(out)}")
        print(f"  Eval scores: {e['eval_scores']}")

    return entries


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("Langfuse Dataset Exporter for Markly\n")

    if not EXPORTS:
        discover()
    else:
        for label, config in EXPORTS.items():
            export_run(label, config["dataset"], config["run"])

        print(f"\n{'='*60}")
        print("ALL EXPORTS COMPLETE")
        print("="*60)
        print("\nFiles created:")
        for label in EXPORTS:
            print(f"  langfuse_{label}.json")
        print("\nShare these 4 JSON files with me and I will rebuild")
        print("the evaluation dashboard with correct ordering.")
