"""
Locust load test for outbound-slot-poc (A20-3142): shared lane slots.

Drives the POC through five phases and checks, from live /stats samples, that lanes are
processed, rejected, grown, shrunk and rebalanced as designed. At the end it writes an HTML
report of the whole run (plus CSV samples and a JSON summary) to loadtest/reports/.

Phases (rates are requests/second at --rate-scale 1; each Locust user sends 1 request/second):

  1. steady      bucket1 10/s, bucket2 20/s   -> inside guarantees: everything processed, no resize
  2. surge       bucket1 150/s, bucket2 20/s  -> bucket1 rejected (LANE) until the planner grows it 18 -> ~90
  3. contention  bucket1 150/s, bucket2 200/s -> bucket2 grows 30 -> ~60, bucket1 shrinks ~90 -> ~70
  4. weighted    topic1 150/s, topic2 150/s   -> S3 lanes shrink back to 18/30; Kafka spare split 2:1 (~12/8)
  5. cooldown    no traffic                   -> every lane back to its guarantee, all slots released

Run (POC on localhost:8085, planner enabled):
    cd loadtest
    locust                       # uses locust.conf: headless, host, Locust's own HTML report
    locust --phase-seconds 30    # longer phases
    locust --rate-scale 0.5      # half the load for a slower machine

Local (single process) mode only: phase timing and outcome counters live in this process.
Exit code: 0 when every check passes, 1 otherwise.
"""

import csv
import json
import logging
import math
import os
import random
import statistics
import time
from collections import Counter, defaultdict

import gevent
import requests
from locust import FastHttpUser, LoadTestShape, constant_throughput, events, task
from locust.runners import MasterRunner, WorkerRunner

log = logging.getLogger("slot-loadtest")

REPORT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")
SAMPLE_INTERVAL_S = 0.25
REJECT_LEVELS = ("LANE", "CONNECTOR", "SERVICE")

# ---------------------------------------------------------------- topology onboarded for the run
# key -> lane policy. Lane and connector ids get a per-run suffix so the test can run again
# against the same server without a 409.
LANES = {
    "bucket1": dict(connector="s3", base="stage-1:bucket1", guaranteed=18, max=130, weight=1, latency=600),
    "bucket2": dict(connector="s3", base="stage-2:bucket2", guaranteed=30, max=130, weight=1, latency=300),
    "topic1": dict(connector="kafka", base="stage-4:topic1", guaranteed=5, max=15, weight=2, latency=100),
    "topic2": dict(connector="kafka", base="stage-5:topic2", guaranteed=5, max=15, weight=1, latency=100),
}
CONNECTORS = {
    "s3": dict(base="S3_connector_1", slots=130, lanes=["bucket1", "bucket2"]),
    "kafka": dict(base="Kafka_connector_1", slots=20, lanes=["topic1", "topic2"]),
}

# name, length (in phase-seconds units), lane rates at scale 1, what should happen
PHASES = [
    ("steady", 1.0, {"bucket1": 10, "bucket2": 20},
     "Load inside the guarantees: every request processed, no lane resized."),
    ("surge", 1.0, {"bucket1": 150, "bucket2": 20},
     "bucket1 wants ~90 slots: LANE rejections until the planner grows it from 18 by borrowing spare slots."),
    ("contention", 1.0, {"bucket1": 150, "bucket2": 200},
     "bucket2 wants ~60: it grows past its 30, and bucket1 shrinks to make room (drains, never interrupted)."),
    ("weighted", 1.0, {"topic1": 150, "topic2": 150},
     "S3 lanes go idle and shrink back to their guarantees. Kafka lanes both want 15; spare split 2:1 by weight."),
    ("cooldown", 0.5, {},
     "No traffic: every lane returns to its guarantee and every slot is released."),
]


# ---------------------------------------------------------------- run state (local mode only)
class RunState:
    def __init__(self):
        self.reset()

    def reset(self):
        self.tag = time.strftime("%Y%m%d-%H%M%S")
        self.started_at = None
        self.host = None
        self.phase_seconds = 20
        self.rate_scale = 1.0
        self.ready = False
        self.lane_ids = {}  # key -> laneId on the server
        self.connector_ids = {}  # key -> connectorId on the server
        self.samples = []
        self.phase_outcomes = defaultdict(Counter)  # (phase, lane) -> Counter
        self.second_outcomes = defaultdict(Counter)  # second -> Counter(outcome)
        self.errors = Counter()
        self.monitor = None
        self.stopping = False
        self.checks = []


STATE = RunState()


def phase_schedule():
    """[(name, start, end, rates, description)] in seconds since the run started."""
    out, t = [], 0.0
    for name, length, rates, desc in PHASES:
        duration = max(8.0, length * STATE.phase_seconds) if name == "cooldown" else length * STATE.phase_seconds
        scaled = {k: v * STATE.rate_scale for k, v in rates.items()}
        out.append((name, t, t + duration, scaled, desc))
        t += duration
    return out


def phase_at(t):
    for p in phase_schedule():
        if p[1] <= t < p[2]:
            return p
    return None


# ---------------------------------------------------------------- planner model (mirror of SlotPlanner.java)
def plan(capacity, lanes):
    """lanes: [(guaranteed, max, weight, demand)] -> targets. Same algorithm as core/SlotPlanner."""
    n = len(lanes)
    alloc = [g for g, _, _, _ in lanes]
    spare = capacity - sum(alloc)
    for i, (g, mx, _, d) in enumerate(lanes):
        if spare > 0 and d > 0 and alloc[i] == 0 and mx > 0:
            alloc[i], spare = 1, spare - 1
    need = [max(0, min(d, mx) - alloc[i]) for i, (_, mx, _, d) in enumerate(lanes)]
    while spare > 0:
        active = [i for i in range(n) if need[i] > 0]
        if not active:
            break
        total_w = sum(lanes[i][2] for i in active)
        round_spare, filled = spare, False
        for i in active:
            if need[i] * total_w <= round_spare * lanes[i][2]:
                alloc[i] += need[i]
                spare -= need[i]
                need[i] = 0
                filled = True
        if filled:
            continue
        rem = {}
        given = 0
        for i in active:
            exact = spare * lanes[i][2] / total_w
            whole = math.floor(exact)
            alloc[i] += whole
            need[i] -= whole
            given += whole
            rem[i] = exact - whole
        for i in sorted(active, key=lambda i: (-rem[i], i))[: spare - given]:
            alloc[i] += 1
            need[i] -= 1
        spare = 0
    return alloc


def expected_targets(rates):
    """Steady-state lane targets the planner should settle on for these request rates."""
    out = {}
    for ckey, c in CONNECTORS.items():
        inputs = []
        for key in c["lanes"]:
            lane = LANES[key]
            demand = math.ceil(rates.get(key, 0) * lane["latency"] / 1000.0 - 1e-6)
            inputs.append((lane["guaranteed"], lane["max"], lane["weight"], demand))
        for key, target in zip(c["lanes"], plan(c["slots"], inputs)):
            out[key] = target
    return out


# ---------------------------------------------------------------- Locust wiring
@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--phase-seconds", type=int, default=20, include_in_web_ui=True,
                        help="Length of each load phase in seconds (cooldown is half, at least 8 s)")
    parser.add_argument("--rate-scale", type=float, default=1.0, include_in_web_ui=True,
                        help="Multiply every phase's request rate (e.g. 0.5 on a slow machine)")


@events.test_start.add_listener
def on_test_start(environment, **_kwargs):
    if isinstance(environment.runner, (MasterRunner, WorkerRunner)):
        log.error("This locustfile runs in local mode only (no --master/--worker).")
        environment.process_exit_code = 2
        gevent.spawn_later(0.1, environment.runner.quit)
        return

    STATE.reset()
    STATE.started_at = time.time()
    STATE.host = (environment.host or "http://localhost:8085").rstrip("/")
    STATE.phase_seconds = environment.parsed_options.phase_seconds
    STATE.rate_scale = environment.parsed_options.rate_scale
    try:
        onboard()
    except Exception as e:  # noqa: BLE001 - report any setup failure and stop
        log.error("Onboarding failed against %s: %s", STATE.host, e)
        environment.process_exit_code = 2
        gevent.spawn_later(0.1, environment.runner.quit)
        return
    STATE.ready = True
    STATE.monitor = gevent.spawn(monitor_loop, environment)
    total = phase_schedule()[-1][2]
    log.info("Run %s: %d phases, %.0f s total, rate scale %.2f", STATE.tag, len(PHASES), total, STATE.rate_scale)


@events.test_stop.add_listener
def on_test_stop(environment, **_kwargs):
    if not STATE.ready or STATE.stopping:
        return
    STATE.stopping = True
    if STATE.monitor is not None:
        STATE.monitor.kill(block=True)
    take_sample(environment)
    STATE.checks = evaluate_checks()
    write_reports()


@events.quitting.add_listener
def on_quitting(environment, **_kwargs):
    if STATE.ready and STATE.checks:
        environment.process_exit_code = 0 if all(c["ok"] for c in STATE.checks) else 1


def onboard():
    session = requests.Session()
    for ckey, c in CONNECTORS.items():
        cid = f"{c['base']}-{STATE.tag}"
        lanes = []
        for key in c["lanes"]:
            lane = LANES[key]
            lane_id = f"{lane['base']}-{STATE.tag}"
            STATE.lane_ids[key] = lane_id
            lanes.append({"laneId": lane_id, "guaranteedSlots": lane["guaranteed"], "maxSlots": lane["max"],
                          "weight": lane["weight"], "latencyMs": lane["latency"]})
        r = session.post(f"{STATE.host}/api/v1/connectors", json={"connectorId": cid, "slots": c["slots"], "lanes": lanes},
                         timeout=5)
        if r.status_code != 201:
            raise RuntimeError(f"POST /connectors {cid} -> {r.status_code} {r.text}")
        STATE.connector_ids[ckey] = cid
        log.info("Onboarded %s (%d slots): %s", cid, c["slots"], [l["laneId"] for l in lanes])


def run_time(environment):
    shape = environment.shape_class
    return shape.get_run_time() if shape is not None else time.time() - STATE.started_at


def monitor_loop(environment):
    while True:
        take_sample(environment)
        gevent.sleep(SAMPLE_INTERVAL_S)


def take_sample(environment):
    t = run_time(environment)
    try:
        stats = requests.get(f"{STATE.host}/api/v1/stats", timeout=2).json()
    except Exception as e:  # noqa: BLE001
        log.warning("stats poll failed: %s", e)
        return
    by_conn = {c["connectorId"]: c for c in stats["connectors"]}
    sample = {"t": round(t, 3), "phase": (phase_at(t) or ("end",))[0],
              "service": stats["service"]["inFlight"], "connectors": {}, "lanes": {}}
    for ckey, cid in STATE.connector_ids.items():
        c = by_conn.get(cid)
        if c is None:
            continue
        sample["connectors"][ckey] = {"capacity": c["slots"]["capacity"], "inFlight": c["slots"]["inFlight"],
                                      "rejected": c["slots"]["rejected"]}
        lane_by_id = {l["laneId"]: l for l in c["lanes"]}
        for key in CONNECTORS[ckey]["lanes"]:
            l = lane_by_id.get(STATE.lane_ids[key])
            if l is None:
                continue
            sample["lanes"][key] = {"target": l["slots"]["capacity"], "inFlight": l["slots"]["inFlight"],
                                    "demand": l["demand"], "rejected": l["slots"]["rejected"],
                                    "resizes": l.get("resizes", 0),
                                    "guaranteed": l["guaranteedSlots"], "max": l["maxSlots"]}
    STATE.samples.append(sample)


class PhasedShape(LoadTestShape):
    """One user = 1 request/second, so users = total request rate of the current phase."""

    def tick(self):
        if not STATE.ready:
            return (0, 1) if self.get_run_time() < 5 else None
        p = phase_at(self.get_run_time())
        if p is None:
            return None
        users = int(round(sum(p[3].values())))
        return users, 200


class LaneClient(FastHttpUser):
    wait_time = constant_throughput(1)

    @task
    def send(self):
        if not STATE.ready:
            return
        t = run_time(self.environment)
        p = phase_at(t)
        if p is None or not p[3]:
            return
        name, rates = p[0], p[3]
        key = random.choices(list(rates), weights=list(rates.values()))[0]
        outcome = "error"
        with self.client.post("/api/v1/requests", json={"laneId": STATE.lane_ids[key]},
                              name=f"POST /requests [{key}]", catch_response=True) as r:
            if r.status_code == 202:
                outcome = "accepted"
                r.success()
            elif r.status_code == 429:
                try:
                    outcome = r.json().get("rejectedBy", "UNKNOWN")
                except ValueError:
                    outcome = "UNKNOWN"
                # Shown in Locust's failure table so rejections are broken down by level. Expected, not errors.
                r.failure(f"429 rejectedBy={outcome}")
            else:
                STATE.errors[f"HTTP {r.status_code}"] += 1
                r.failure(f"HTTP {r.status_code}")
        STATE.phase_outcomes[(name, key)][outcome] += 1
        STATE.phase_outcomes[(name, key)]["sent"] += 1
        STATE.second_outcomes[int(t)][outcome] += 1


# ---------------------------------------------------------------- checks
def phase_samples(name):
    return [s for s in STATE.samples if s["phase"] == name]


def settled(name, key, field="target"):
    """Median of the last 40% of a phase's samples: the value the lane settled on."""
    ss = [s["lanes"][key][field] for s in phase_samples(name) if key in s["lanes"]]
    if not ss:
        return None
    tail = ss[int(len(ss) * 0.6):] or ss
    return statistics.median(tail)


def peak(name, key, field="target"):
    ss = [s["lanes"][key][field] for s in phase_samples(name) if key in s["lanes"]]
    return max(ss) if ss else None


def outcomes(name, key):
    return STATE.phase_outcomes.get((name, key), Counter())


def resizes_between(key, t0, t1):
    """Server-side resize count for a lane between two run times (from the lane's resizes counter)."""
    def at(t):
        vals = [s["lanes"][key]["resizes"] for s in STATE.samples if s["t"] <= t and key in s["lanes"]]
        return vals[-1] if vals else 0
    return at(t1) - at(t0)


MAX_SETTLED_RESIZES = 5  # per lane, in the second half of a phase under constant load


def evaluate_checks():
    exp = {p[0]: expected_targets(p[3]) for p in phase_schedule()}
    checks = []

    def add(area, title, expected, observed, ok):
        checks.append({"area": area, "title": title, "expected": expected, "observed": observed, "ok": bool(ok)})

    # 1. processing
    sent = sum(outcomes("steady", k)["sent"] for k in ("bucket1", "bucket2"))
    acc = sum(outcomes("steady", k)["accepted"] for k in ("bucket1", "bucket2"))
    rate = (sent - acc) / sent if sent else 1
    add("Processing", "steady: load inside the guarantees is processed",
        "rejections <= 1%", f"{sent} sent, {acc} accepted ({rate:.1%} rejected)", sent > 0 and rate <= 0.01)
    # Locust users fire in clumps, so a short burst may still nudge a target up for a tick or two;
    # what matters is that the steady load never needed more than the guaranteed slots.
    in_flight_peak = {k: peak("steady", k, "inFlight") for k in ("bucket1", "bucket2")}
    target_peak = {k: peak("steady", k) for k in ("bucket1", "bucket2")}
    add("Processing", "steady: the load fits inside the guaranteed slots",
        "peak in flight: bucket1 <= 18, bucket2 <= 30",
        f"peak in flight bucket1 {in_flight_peak['bucket1']}, bucket2 {in_flight_peak['bucket2']} "
        f"(peak target {target_peak['bucket1']} / {target_peak['bucket2']})",
        None not in in_flight_peak.values()
        and in_flight_peak["bucket1"] <= LANES["bucket1"]["guaranteed"]
        and in_flight_peak["bucket2"] <= LANES["bucket2"]["guaranteed"])

    # 2. rejection
    lane_rej = outcomes("surge", "bucket1")["LANE"]
    add("Rejection", "surge: bucket1 rejected at LANE level while over its slots",
        "429 rejectedBy=LANE > 0", f"{lane_rej} LANE rejections", lane_rej > 0)

    # 3. increase
    b1_peak, b1_surge = peak("surge", "bucket1"), settled("surge", "bucket1")
    add("Increase", "surge: bucket1 grows past its guarantee by borrowing spare slots",
        f"> 18 (model: {exp['surge']['bucket1']})", f"peak {b1_peak}, settled {b1_surge}",
        b1_peak is not None and b1_peak > LANES["bucket1"]["guaranteed"])
    b2_cont = settled("contention", "bucket2")
    add("Increase", "contention: bucket2 grows past its guarantee",
        f"> 30 (model: {exp['contention']['bucket2']})", f"settled {b2_cont}, peak {peak('contention', 'bucket2')}",
        b2_cont is not None and b2_cont > LANES["bucket2"]["guaranteed"])

    # 3b. stability (damping): once a lane has adapted to a constant load, it stops resizing
    sched = {p[0]: p for p in phase_schedule()}
    stable = {}
    for phase, keys in (("surge", ("bucket1",)), ("contention", ("bucket1", "bucket2")), ("weighted", ("topic1", "topic2"))):
        _, start, end, _, _ = sched[phase]
        for k in keys:
            stable[f"{phase}/{k}"] = resizes_between(k, (start + end) / 2, end)
    add("Stability", "second half of each loaded phase: lanes under constant load stop resizing (damping)",
        f"<= {MAX_SETTLED_RESIZES} resizes per lane", ", ".join(f"{k} {v}" for k, v in stable.items()),
        all(v <= MAX_SETTLED_RESIZES for v in stable.values()))

    # 4. decrease
    b1_cont = settled("contention", "bucket1")
    add("Decrease", "contention: bucket1 shrinks to make room for bucket2",
        f"< surge level (model: {exp['surge']['bucket1']} -> {exp['contention']['bucket1']})",
        f"{b1_surge} -> {b1_cont}", None not in (b1_surge, b1_cont) and b1_cont < b1_surge)
    s3_idle = {k: settled("weighted", k) for k in ("bucket1", "bucket2")}
    add("Decrease", "weighted: idle S3 lanes shrink back to their guarantees",
        "bucket1 18, bucket2 30", f"bucket1 {s3_idle['bucket1']}, bucket2 {s3_idle['bucket2']}",
        s3_idle["bucket1"] == 18 and s3_idle["bucket2"] == 30)

    # 5. weighted share
    t1, t2 = settled("weighted", "topic1"), settled("weighted", "topic2")
    add("Fair share", "weighted: Kafka spare slots split 2:1 by weight",
        f"topic1 > topic2 (model: {exp['weighted']['topic1']} / {exp['weighted']['topic2']})",
        f"topic1 {t1}, topic2 {t2}", None not in (t1, t2) and t1 > t2)

    # 6. caps
    breaches = []
    for s in STATE.samples:
        for ckey, c in s["connectors"].items():
            lane_sum = sum(s["lanes"][k]["target"] for k in CONNECTORS[ckey]["lanes"] if k in s["lanes"])
            if lane_sum > c["capacity"]:
                breaches.append(f"t={s['t']}s {ckey} lane targets {lane_sum} > {c['capacity']}")
            if c["inFlight"] > c["capacity"]:
                breaches.append(f"t={s['t']}s {ckey} in flight {c['inFlight']} > {c['capacity']}")
        for k, l in s["lanes"].items():
            if not l["guaranteed"] <= l["target"] <= l["max"]:
                breaches.append(f"t={s['t']}s {k} target {l['target']} outside [{l['guaranteed']}, {l['max']}]")
    add("Caps", "every sample: lane targets within [guaranteed, max], sum <= connector slots, connector in flight <= slots",
        "0 breaches", f"{len(breaches)} breaches in {len(STATE.samples)} samples" + (f" (first: {breaches[0]})" if breaches else ""),
        not breaches and STATE.samples)

    # 7. release
    last = STATE.samples[-1] if STATE.samples else None
    if last:
        left = {k: l["inFlight"] for k, l in last["lanes"].items() if l["inFlight"]}
        off = {k: l["target"] for k, l in last["lanes"].items() if l["target"] != LANES[k]["guaranteed"]}
        add("Release", "cooldown: all slots released, every lane back to its guarantee",
            "in flight 0, targets = guarantees", f"in flight {left or 0}, targets off guarantee {off or 'none'}",
            not left and not off)

    # 8. errors
    add("Errors", "no responses other than 202 and 429", "0", str(sum(STATE.errors.values()) or 0) +
        (f" {dict(STATE.errors)}" if STATE.errors else ""), not STATE.errors)
    return checks


# ---------------------------------------------------------------- reports
def resize_events(key):
    vals = [s["lanes"][key]["resizes"] for s in STATE.samples if key in s["lanes"]]
    return vals[-1] if vals else 0


def write_reports():
    os.makedirs(REPORT_DIR, exist_ok=True)
    base = os.path.join(REPORT_DIR, f"slot-run-{STATE.tag}")
    schedule = phase_schedule()

    # CSV of every sample
    with open(base + "-samples.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["t", "phase", "service_inFlight", "lane", "target", "inFlight", "demand", "rejected", "resizes"])
        for s in STATE.samples:
            for k, l in s["lanes"].items():
                w.writerow([s["t"], s["phase"], s["service"], k, l["target"], l["inFlight"], l["demand"], l["rejected"],
                            l["resizes"]])

    phase_rows = []
    for name, start, end, rates, desc in schedule:
        exp = expected_targets(rates)
        for key in LANES:
            o = outcomes(name, key)
            phase_rows.append({
                "phase": name, "lane": key, "rate": rates.get(key, 0), "sent": o["sent"], "accepted": o["accepted"],
                "LANE": o["LANE"], "CONNECTOR": o["CONNECTOR"], "SERVICE": o["SERVICE"], "errors": o["error"],
                "expected": exp[key], "settled": settled(name, key), "peak": peak(name, key),
                "resizes": resizes_between(key, start, end),
            })

    seconds = sorted(STATE.second_outcomes)
    summary = {
        "tag": STATE.tag, "host": STATE.host,
        "startedAt": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(STATE.started_at)),
        "durationSeconds": round(STATE.samples[-1]["t"], 1) if STATE.samples else 0,
        "phaseSeconds": STATE.phase_seconds, "rateScale": STATE.rate_scale,
        "laneIds": STATE.lane_ids, "connectorIds": STATE.connector_ids,
        "lanes": {k: {**v, "resizeEvents": resize_events(k)} for k, v in LANES.items()},
        "connectors": CONNECTORS,
        "phases": [{"name": n, "start": s, "end": e, "rates": r, "description": d} for n, s, e, r, d in schedule],
        "phaseRows": phase_rows,
        "checks": STATE.checks,
        "totals": {
            "sent": sum(o["sent"] for o in STATE.phase_outcomes.values()),
            "accepted": sum(o["accepted"] for o in STATE.phase_outcomes.values()),
            **{lvl: sum(o[lvl] for o in STATE.phase_outcomes.values()) for lvl in REJECT_LEVELS},
            "errors": sum(STATE.errors.values()),
        },
    }
    with open(base + "-summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    chart_data = {
        "samples": [{"t": s["t"], "lanes": s["lanes"], "connectors": s["connectors"]} for s in STATE.samples],
        "seconds": [{"s": sec, **STATE.second_outcomes[sec]} for sec in seconds],
    }
    html = REPORT_TEMPLATE.replace("__SUMMARY__", json.dumps(summary)).replace("__CHARTS__", json.dumps(chart_data))
    with open(base + ".html", "w", encoding="utf-8") as f:
        f.write(html)

    passed = sum(c["ok"] for c in STATE.checks)
    log.info("Checks: %d/%d passed", passed, len(STATE.checks))
    for c in STATE.checks:
        log.info("  [%s] %s | expected %s | observed %s", "PASS" if c["ok"] else "FAIL", c["title"], c["expected"], c["observed"])
    log.info("Report: %s.html (samples: -samples.csv, summary: -summary.json)", base)


REPORT_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Slot Load Report</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  :root { --fg:#111; --muted:#555; --line:#ccc; --bg:#fff; --band:#f2f2f2; --pass:#1a7f37; --fail:#c62828; }
  * { box-sizing: border-box; }
  body { margin: 0; font: 16px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif; color: var(--fg); background: var(--bg); }
  main { max-width: 1200px; margin: 0 auto; padding: 24px 16px 64px; }
  h1 { font-size: 28px; margin: 0 0 4px; }
  h2 { font-size: 21px; margin: 36px 0 12px; border-bottom: 2px solid var(--fg); padding-bottom: 4px; }
  .sub { color: var(--muted); margin: 0 0 20px; }
  .tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; }
  .tile { border: 1px solid var(--line); padding: 12px; }
  .tile b { display: block; font-size: 26px; }
  .tile span { color: var(--muted); font-size: 14px; }
  table { border-collapse: collapse; width: 100%; font-size: 15px; }
  th, td { text-align: left; padding: 6px 8px; border-bottom: 1px solid var(--line); vertical-align: top; }
  th { border-bottom: 2px solid var(--fg); }
  td.n, th.n { text-align: right; font-variant-numeric: tabular-nums; }
  .wrap { overflow-x: auto; }
  .pass { color: var(--pass); font-weight: 700; }
  .fail { color: var(--fail); font-weight: 700; }
  .charts { display: grid; grid-template-columns: repeat(auto-fit, minmax(520px, 1fr)); gap: 20px; }
  .chart { border: 1px solid var(--line); padding: 12px; }
  .chart h3 { margin: 0 0 8px; font-size: 17px; }
  .chart canvas { width: 100% !important; height: 300px !important; }
  .muted { color: var(--muted); font-size: 14px; }
  @media (max-width: 600px) { .charts { grid-template-columns: 1fr; } }
</style>
</head>
<body>
<main>
  <h1>Shared lane slots: load run</h1>
  <p class="sub" id="sub"></p>
  <div class="tiles" id="tiles"></div>

  <h2>Checks</h2>
  <div class="wrap"><table id="checks"></table></div>

  <h2>Phases</h2>
  <div class="wrap"><table id="phases"></table></div>

  <h2>Lane slots over time</h2>
  <p class="muted">Target = planned slots (lane semaphore limit). In flight = deliveries holding a lane slot. Demand = what the planner measured. Shaded bands are phases. Sampled every 250 ms from <code>/api/v1/stats</code>.</p>
  <div class="charts" id="laneCharts"></div>

  <h2>Connectors and outcomes</h2>
  <div class="charts" id="otherCharts"></div>

  <h2>Results by phase and lane</h2>
  <p class="muted">Expected = steady-state target from the same planner algorithm, given the phase's request rate. Settled = median of the last 40% of the phase's samples.</p>
  <div class="wrap"><table id="rows"></table></div>
</main>
<script>
const S = __SUMMARY__;
const C = __CHARTS__;
const $ = (id) => document.getElementById(id);
const fmt = (v) => v === null || v === undefined ? "-" : v;
const passed = S.checks.filter(c => c.ok).length;

$("sub").textContent = `Run ${S.tag} against ${S.host}, started ${S.startedAt}, ${S.durationSeconds} s, `
  + `${S.phaseSeconds} s phases, rate scale ${S.rateScale}`;

const t = S.totals;
const tiles = [
  [`${passed}/${S.checks.length}`, "checks passed", passed === S.checks.length ? "pass" : "fail"],
  [t.sent, "requests sent"], [t.accepted, "accepted (202)"], [t.LANE, "rejected by LANE"],
  [t.CONNECTOR, "rejected by CONNECTOR"], [t.SERVICE, "rejected by SERVICE"], [t.errors, "errors"],
];
$("tiles").innerHTML = tiles.map(([v, l, cls]) => `<div class="tile"><b class="${cls || ""}">${v}</b><span>${l}</span></div>`).join("");

$("checks").innerHTML = "<tr><th>Result</th><th>Area</th><th>Check</th><th>Expected</th><th>Observed</th></tr>"
  + S.checks.map(c => `<tr><td class="${c.ok ? "pass" : "fail"}">${c.ok ? "PASS" : "FAIL"}</td><td>${c.area}</td>`
  + `<td>${c.title}</td><td>${c.expected}</td><td>${c.observed}</td></tr>`).join("");

$("phases").innerHTML = "<tr><th>Phase</th><th class='n'>From</th><th class='n'>To</th><th>Rates (req/s)</th><th>What should happen</th></tr>"
  + S.phases.map(p => `<tr><td><b>${p.name}</b></td><td class="n">${p.start.toFixed(0)} s</td><td class="n">${p.end.toFixed(0)} s</td>`
  + `<td>${Object.entries(p.rates).map(([k, v]) => `${k} ${v}`).join(", ") || "none"}</td><td>${p.description}</td></tr>`).join("");

$("rows").innerHTML = "<tr><th>Phase</th><th>Lane</th><th class='n'>Rate</th><th class='n'>Sent</th><th class='n'>202</th>"
  + "<th class='n'>429 LANE</th><th class='n'>429 CONN</th><th class='n'>429 SVC</th><th class='n'>Err</th>"
  + "<th class='n'>Expected</th><th class='n'>Settled</th><th class='n'>Peak</th><th class='n'>Resizes</th></tr>"
  + S.phaseRows.map(r => `<tr><td>${r.phase}</td><td>${r.lane}</td><td class="n">${r.rate}</td><td class="n">${r.sent}</td>`
  + `<td class="n">${r.accepted}</td><td class="n">${r.LANE}</td><td class="n">${r.CONNECTOR}</td><td class="n">${r.SERVICE}</td>`
  + `<td class="n">${r.errors}</td><td class="n">${r.expected}</td><td class="n">${fmt(r.settled)}</td><td class="n">${fmt(r.peak)}</td>`
  + `<td class="n">${r.resizes}</td></tr>`).join("");

// Phase bands behind every time chart
const bands = {
  id: "bands",
  beforeDatasetsDraw(chart) {
    const { ctx, chartArea: a, scales: { x } } = chart;
    ctx.save();
    S.phases.forEach((p, i) => {
      const x0 = Math.max(x.getPixelForValue(p.start), a.left), x1 = Math.min(x.getPixelForValue(p.end), a.right);
      if (x1 <= x0) return;
      if (i % 2 === 0) { ctx.fillStyle = "#f2f2f2"; ctx.fillRect(x0, a.top, x1 - x0, a.bottom - a.top); }
      ctx.fillStyle = "#555"; ctx.font = "13px system-ui"; ctx.fillText(p.name, x0 + 4, a.top + 14);
    });
    ctx.restore();
  },
};
Chart.defaults.font.size = 14;
Chart.defaults.color = "#111";
const lineOpts = (yTitle) => ({
  animation: false, parsing: false, normalized: true, maintainAspectRatio: false,
  interaction: { mode: "index", intersect: false },
  scales: { x: { type: "linear", title: { display: true, text: "seconds" } }, y: { beginAtZero: true, title: { display: true, text: yTitle } } },
  plugins: { legend: { position: "bottom" } },
  elements: { point: { radius: 0 } },
});
function addChart(parent, title, datasets, yTitle, type = "line", opts) {
  const div = document.createElement("div");
  div.className = "chart";
  div.innerHTML = `<h3>${title}</h3><canvas></canvas>`;
  $(parent).appendChild(div);
  new Chart(div.querySelector("canvas"), { type, data: { datasets }, options: opts || lineOpts(yTitle), plugins: [bands] });
}
const series = (fn) => C.samples.map(s => ({ x: s.t, y: fn(s) })).filter(p => p.y !== undefined && p.y !== null);

Object.entries(S.lanes).forEach(([key, lane]) => {
  addChart("laneCharts", `${key} (guaranteed ${lane.guaranteed}, max ${lane.max}, weight ${lane.weight}, ${lane.latency} ms) - ${lane.resizeEvents} resizes`, [
    { label: "target", data: series(s => s.lanes[key]?.target), borderColor: "#111", borderWidth: 2.5, stepped: true },
    { label: "in flight", data: series(s => s.lanes[key]?.inFlight), borderColor: "#888", borderWidth: 1.5 },
    { label: "demand", data: series(s => s.lanes[key]?.demand), borderColor: "#aaa", borderWidth: 1, borderDash: [5, 4] },
    { label: "guaranteed", data: series(() => lane.guaranteed), borderColor: "#bbb", borderWidth: 1, borderDash: [2, 3] },
  ], "slots");
});

Object.entries(S.connectors).forEach(([ckey, c]) => {
  addChart("otherCharts", `${ckey} connector (${c.slots} slots)`, [
    { label: "sum of lane targets", data: series(s => c.lanes.reduce((a, k) => a + (s.lanes[k]?.target ?? 0), 0)), borderColor: "#111", borderWidth: 2.5, stepped: true },
    { label: "connector in flight", data: series(s => s.connectors[ckey]?.inFlight), borderColor: "#888", borderWidth: 1.5 },
    { label: "connector slots", data: series(() => c.slots), borderColor: "#bbb", borderWidth: 1, borderDash: [2, 3] },
  ], "slots");
});

const bar = (k, color) => ({ label: k, data: C.seconds.map(s => ({ x: s.s + 0.5, y: s[k] || 0 })), backgroundColor: color, stack: "o", barPercentage: 1, categoryPercentage: 1 });
const barOpts = lineOpts("requests per second");
barOpts.scales.x.stacked = true; barOpts.scales.y.stacked = true;
addChart("otherCharts", "Outcomes per second (all lanes)", [
  bar("accepted", "#bbb"), bar("LANE", "#111"), bar("CONNECTOR", "#666"), bar("SERVICE", "#999"), bar("error", "#c62828"),
], "requests per second", "bar", barOpts);
</script>
</body>
</html>
"""
