"""
SCADA Flask Application
All paths resolved relative to __file__ — works on Windows / Linux / Mac
regardless of the working directory.
"""

import os
import sys
import json
import time
import queue
import logging
import threading
from datetime import datetime

# ── Ensure the project root is always on sys.path ────────────────────────────
_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from flask import (Flask, render_template, request, jsonify,
                   Response, redirect, url_for, stream_with_context)

from database.db import DatabaseManager
from engine import engine

# ── Flask app: explicit template/static folders relative to this file ─────────
app = Flask(
    __name__,
    template_folder=os.path.join(_ROOT, "templates"),
    static_folder=os.path.join(_ROOT, "static"),
)
app.secret_key = "scada-industrial-secret-2024"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

db = DatabaseManager()  # singleton — same instance as engine.db


# ─────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────
def ok(data=None, status: int = 200):
    return jsonify({"success": True,  "data": data}), status

def err(msg: str, status: int = 400):
    return jsonify({"success": False, "error": msg}), status


# ─────────────────────────────────────────────
# Page Routes
# ─────────────────────────────────────────────
@app.route("/")
def index():
    return redirect(url_for("dashboard"))

@app.route("/dashboard")
def dashboard():
    return render_template("dashboard.html",
                           plcs=db.get_plcs(),
                           tags=db.get_tags(),
                           stats=db.get_statistics())

@app.route("/config/plcs")
def config_plcs():
    return render_template("config_plcs.html", plcs=db.get_plcs())

@app.route("/config/tags")
def config_tags():
    return render_template("config_tags.html",
                           plcs=db.get_plcs(), tags=db.get_tags())

@app.route("/config/alerts")
def config_alerts():
    return render_template("config_alerts.html",
                           tags=db.get_tags(), alerts=db.get_alerts())

@app.route("/config/notifications")
def config_notifications():
    return render_template("config_notifications.html",
                           settings=db.get_notification_settings())

@app.route("/config/settings")
def config_settings():
    return render_template("config_settings.html",
                           settings=db.get_all_settings())

@app.route("/history")
def history():
    return render_template("history.html", tags=db.get_tags())

@app.route("/alarms")
def alarms():
    return render_template("alarms.html",
                           active=db.get_active_events(),
                           history=db.get_event_history(100))

@app.route("/control-log")
def control_log():
    return render_template("control_log.html", logs=db.get_control_log())


# ─────────────────────────────────────────────
# SSE Streams
# ─────────────────────────────────────────────
@app.route("/stream/tags")
def stream_tags():
    """Real-time tag values via Server-Sent Events."""
    def generate():
        q = engine.store.subscribe()
        # Enrich tag metadata for client use
        all_tags = {t["id"]: t for t in db.get_tags()}
        try:
            # Send full snapshot immediately on connect
            for tid, reading in engine.store.get_all().items():
                meta  = all_tags.get(tid, {})
                yield _tag_sse(reading, meta)
            # Then stream live updates
            while True:
                try:
                    item = q.get(timeout=30)
                    meta = all_tags.get(item["tag_id"], {})
                    yield _tag_sse(item, meta)
                except queue.Empty:
                    yield "data: {\"ping\":1}\n\n"
        except GeneratorExit:
            pass
        finally:
            engine.store.unsubscribe(q)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering":"no",
            "Connection":       "keep-alive",
        },
    )


def _tag_sse(reading: dict, meta: dict) -> str:
    payload = {
        **reading,
        "unit":        meta.get("unit", ""),
        "decimals":    meta.get("decimals", 2),
        "color":       meta.get("color", "#00d4ff"),
        "widget_type": meta.get("widget_type", "value"),
        "gauge_min":   meta.get("gauge_min", 0),
        "gauge_max":   meta.get("gauge_max", 100),
    }
    return f"data: {json.dumps(payload)}\n\n"


@app.route("/stream/alerts")
def stream_alerts():
    def generate():
        prev = -1
        while True:
            try:
                events = db.get_active_events(50)
                if len(events) != prev:
                    prev = len(events)
                    yield f"data: {json.dumps(events)}\n\n"
                time.sleep(2)
            except GeneratorExit:
                break
            except Exception:
                time.sleep(2)

    return Response(stream_with_context(generate()),
                    mimetype="text/event-stream",
                    headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})


@app.route("/stream/status")
def stream_status():
    def generate():
        while True:
            try:
                payload = {
                    "engine": engine.get_engine_status(),
                    "stats":  db.get_statistics(),
                    "time":   datetime.now().strftime("%H:%M:%S"),
                }
                yield f"data: {json.dumps(payload)}\n\n"
                time.sleep(3)
            except GeneratorExit:
                break
            except Exception:
                time.sleep(3)

    return Response(stream_with_context(generate()),
                    mimetype="text/event-stream",
                    headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})


# ─────────────────────────────────────────────
# API: PLCs
# ─────────────────────────────────────────────
@app.route("/api/plcs", methods=["GET"])
def api_get_plcs():
    plcs   = db.get_plcs()
    status = engine.get_engine_status()
    for p in plcs:
        s = status.get(p["id"], {})
        p["connected"]    = s.get("connected",    False)
        p["poll_count"]   = s.get("poll_count",   0)
        p["last_poll_ms"] = s.get("last_poll_ms", 0)
    return ok(plcs)

@app.route("/api/plcs", methods=["POST"])
def api_create_plc():
    d = request.json
    if not d or not d.get("name"):
        return err("name is required", 400)
    try:
        pid = db.create_plc(d)
        engine.restart_poller(pid)
        return ok({"id": pid}), 201
    except Exception as exc:
        return err(str(exc), 400)

@app.route("/api/plcs/<int:pid>", methods=["GET"])
def api_get_plc(pid):
    p = db.get_plc(pid)
    return ok(p) if p else err("Not found", 404)

@app.route("/api/plcs/<int:pid>", methods=["PUT"])
def api_update_plc(pid):
    d = request.json
    try:
        db.update_plc(pid, d)
        engine.restart_poller(pid)
        return ok({"id": pid})
    except Exception as exc:
        return err(str(exc), 400)

@app.route("/api/plcs/<int:pid>", methods=["DELETE"])
def api_delete_plc(pid):
    engine.stop_poller(pid)
    db.delete_plc(pid)
    return ok({"deleted": pid})

@app.route("/api/plcs/<int:pid>/restart", methods=["POST"])
def api_restart_plc(pid):
    engine.restart_poller(pid)
    return ok({"restarted": pid})


# ─────────────────────────────────────────────
# API: Tags
# ─────────────────────────────────────────────
@app.route("/api/tags", methods=["GET"])
def api_get_tags():
    plc_id = request.args.get("plc_id", type=int)
    tags   = db.get_tags(plc_id=plc_id)
    snap   = engine.store.get_all()
    for t in tags:
        r = snap.get(t["id"])
        t["current_value"] = r["value"]     if r else None
        t["quality"]       = r["quality"]   if r else 0
        t["last_update"]   = r["timestamp"] if r else None
    return ok(tags)

@app.route("/api/tags", methods=["POST"])
def api_create_tag():
    d = request.json
    if not d or not d.get("tag_name") or not d.get("plc_id"):
        return err("tag_name and plc_id required", 400)
    try:
        tid = db.create_tag(d)
        return ok({"id": tid}), 201
    except Exception as exc:
        return err(str(exc), 400)

@app.route("/api/tags/<int:tid>", methods=["GET"])
def api_get_tag(tid):
    t = db.get_tag(tid)
    return ok(t) if t else err("Not found", 404)

@app.route("/api/tags/<int:tid>", methods=["PUT"])
def api_update_tag(tid):
    d = request.json
    try:
        db.update_tag(tid, d)
        return ok({"id": tid})
    except Exception as exc:
        return err(str(exc), 400)

@app.route("/api/tags/<int:tid>", methods=["DELETE"])
def api_delete_tag(tid):
    db.delete_tag(tid)
    return ok({"deleted": tid})

@app.route("/api/tags/<int:tid>/readings")
def api_readings(tid):
    minutes = request.args.get("minutes", 60, type=int)
    limit   = request.args.get("limit",  500, type=int)
    return ok(db.get_readings(tid, minutes=minutes, limit=limit))

@app.route("/api/tags/values")
def api_all_values():
    snap   = engine.store.get_all()
    result = []
    for t in db.get_tags():
        r = snap.get(t["id"])
        result.append({
            "tag_id":       t["id"],
            "tag_name":     t["tag_name"],
            "display_name": t["display_name"],
            "plc_name":     t["plc_name"],
            "value":        r["value"]     if r else None,
            "quality":      r["quality"]   if r else 0,
            "unit":         t["unit"],
            "timestamp":    r["timestamp"] if r else None,
        })
    return ok(result)


# ─────────────────────────────────────────────
# API: Control (write)
# ─────────────────────────────────────────────
@app.route("/api/tags/<int:tid>/write", methods=["POST"])
def api_write(tid):
    d = request.json
    if d is None or "value" not in d:
        return err("value required", 400)
    operator = d.get("operator", "operator")
    success, msg = engine.write_tag(tid, d["value"], operator)
    return ok({"written": d["value"]}) if success else err(msg, 400)

@app.route("/api/tags/<int:tid>/toggle", methods=["POST"])
def api_toggle(tid):
    snap    = engine.store.get_all()
    r       = snap.get(tid)
    current = int(r["value"]) if r else 0
    new_val = 0 if current else 1
    success, msg = engine.write_tag(tid, new_val, "operator")
    return ok({"toggled": new_val}) if success else err(msg, 400)


# ─────────────────────────────────────────────
# API: Alerts
# ─────────────────────────────────────────────
@app.route("/api/alerts", methods=["GET"])
def api_get_alerts():
    tag_id = request.args.get("tag_id", type=int)
    return ok(db.get_alerts(tag_id=tag_id))

@app.route("/api/alerts", methods=["POST"])
def api_create_alert():
    try:
        return ok({"id": db.create_alert(request.json)}), 201
    except Exception as exc:
        return err(str(exc), 400)

@app.route("/api/alerts/<int:aid>", methods=["PUT"])
def api_update_alert(aid):
    try:
        db.update_alert(aid, request.json)
        return ok({"id": aid})
    except Exception as exc:
        return err(str(exc), 400)

@app.route("/api/alerts/<int:aid>", methods=["DELETE"])
def api_delete_alert(aid):
    db.delete_alert(aid)
    return ok({"deleted": aid})

@app.route("/api/alert-events/active")
def api_active_events():
    return ok(db.get_active_events())

@app.route("/api/alert-events/history")
def api_event_history():
    return ok(db.get_event_history(request.args.get("limit", 200, type=int)))

@app.route("/api/alert-events/<int:eid>/acknowledge", methods=["POST"])
def api_ack(eid):
    op = (request.json or {}).get("operator", "operator")
    db.acknowledge_event(eid, op)
    return ok({"acknowledged": eid})


# ─────────────────────────────────────────────
# API: Notifications
# ─────────────────────────────────────────────
@app.route("/api/notifications", methods=["GET"])
def api_get_notif():
    return ok(db.get_notification_settings())

@app.route("/api/notifications/<channel>", methods=["PUT"])
def api_update_notif(channel):
    db.update_notification(channel, request.json)
    return ok({"channel": channel})

@app.route("/api/notifications/test", methods=["POST"])
def api_test_notif():
    channel = (request.json or {}).get("channel", "telegram")
    engine.dispatcher.send(
        "🧪 SCADA Test Notification — System Online", [channel]
    )
    return ok({"sent": channel})


# ─────────────────────────────────────────────
# API: Settings & Engine
# ─────────────────────────────────────────────
@app.route("/api/settings", methods=["GET"])
def api_get_settings():
    return ok(db.get_all_settings())

@app.route("/api/settings", methods=["PUT"])
def api_update_settings():
    for k, v in (request.json or {}).items():
        db.set_setting(k, str(v))
    return ok({"updated": list((request.json or {}).keys())})

@app.route("/api/engine/status")
def api_engine_status():
    return ok({"engine": engine.get_engine_status(),
                "stats":  db.get_statistics()})

@app.route("/api/control-log")
def api_control_log():
    return ok(db.get_control_log(request.args.get("limit", 200, type=int)))


# ─────────────────────────────────────────────
# Demo data seeding
# ─────────────────────────────────────────────
def seed_demo_data():
    if db.get_plcs():
        return   # already seeded

    plc1 = db.create_plc({"name":"Line-1 Mixer PLC",   "plc_type":"simulator",
                           "host":"192.168.1.10","port":502,"poll_interval":1.0,
                           "description":"Production Line 1 — Mixing Station",
                           "location":"Zone A","enabled":1})
    plc2 = db.create_plc({"name":"Line-2 Conveyor PLC","plc_type":"simulator",
                           "host":"192.168.1.11","port":502,"poll_interval":0.5,
                           "description":"Production Line 2 — Conveyor Control",
                           "location":"Zone B","enabled":1})
    plc3 = db.create_plc({"name":"Utilities PLC",      "plc_type":"simulator",
                           "host":"192.168.1.20","port":502,"poll_interval":2.0,
                           "description":"Utilities — HVAC, Power, Water",
                           "location":"Utility Room","enabled":1})

    tags_l1 = [
        {"tag_name":"L1_TEMP_REACTOR","display_name":"Reactor Temperature",
         "unit":"°C","widget_type":"gauge","gauge_min":0,"gauge_max":300,
         "sim_type":"sine","sim_min":140,"sim_max":180,"sim_period":45,
         "tag_group":"Line 1","decimals":1,"color":"#ff6b35","icon":"thermometer",
         "data_type":"FLOAT32","address":"40001"},
        {"tag_name":"L1_PRESSURE","display_name":"Tank Pressure",
         "unit":"bar","widget_type":"gauge","gauge_min":0,"gauge_max":10,
         "sim_type":"random_walk","sim_min":3.5,"sim_max":5.5,"sim_period":20,
         "tag_group":"Line 1","decimals":2,"color":"#00d4ff","icon":"gauge",
         "data_type":"FLOAT32","address":"40002"},
        {"tag_name":"L1_FLOW_RATE","display_name":"Flow Rate",
         "unit":"L/min","widget_type":"chart","gauge_min":0,"gauge_max":500,
         "sim_type":"sine","sim_min":200,"sim_max":350,"sim_period":60,
         "tag_group":"Line 1","decimals":1,"color":"#00ff88","icon":"activity",
         "data_type":"FLOAT32","address":"40003"},
        {"tag_name":"L1_MIXER_SPEED","display_name":"Mixer Speed",
         "unit":"RPM","widget_type":"gauge","gauge_min":0,"gauge_max":3000,
         "sim_type":"random_walk","sim_min":1400,"sim_max":1600,"sim_period":30,
         "tag_group":"Line 1","decimals":0,"color":"#ffd700","icon":"zap",
         "data_type":"FLOAT32","address":"40004","writable":1},
        {"tag_name":"L1_MOTOR_ON","display_name":"Motor Start/Stop",
         "unit":"","widget_type":"toggle","gauge_min":0,"gauge_max":1,
         "sim_type":"bool_toggle","sim_min":0,"sim_max":1,"sim_period":25,
         "tag_group":"Line 1","decimals":0,"color":"#00ff88","icon":"power",
         "data_type":"BOOL","address":"00001","register_type":"coil","writable":1},
        {"tag_name":"L1_VALVE_A","display_name":"Inlet Valve A",
         "unit":"","widget_type":"toggle","gauge_min":0,"gauge_max":1,
         "sim_type":"bool_toggle","sim_min":0,"sim_max":1,"sim_period":15,
         "tag_group":"Line 1","decimals":0,"color":"#00d4ff","icon":"sliders",
         "data_type":"BOOL","address":"00002","register_type":"coil","writable":1},
        {"tag_name":"L1_LEVEL","display_name":"Tank Level",
         "unit":"%","widget_type":"gauge","gauge_min":0,"gauge_max":100,
         "sim_type":"sawtooth","sim_min":20,"sim_max":85,"sim_period":120,
         "tag_group":"Line 1","decimals":1,"color":"#7c5cbf","icon":"droplets",
         "data_type":"FLOAT32","address":"40005"},
    ]
    tags_l2 = [
        {"tag_name":"L2_BELT_SPEED","display_name":"Belt Speed",
         "unit":"m/min","widget_type":"gauge","gauge_min":0,"gauge_max":60,
         "sim_type":"random_walk","sim_min":25,"sim_max":35,"sim_period":30,
         "tag_group":"Line 2","decimals":1,"color":"#00d4ff","icon":"activity",
         "data_type":"FLOAT32","address":"40001","writable":1},
        {"tag_name":"L2_TEMP_OVEN","display_name":"Oven Temperature",
         "unit":"°C","widget_type":"gauge","gauge_min":0,"gauge_max":500,
         "sim_type":"sine","sim_min":220,"sim_max":260,"sim_period":60,
         "tag_group":"Line 2","decimals":1,"color":"#ff6b35","icon":"thermometer",
         "data_type":"FLOAT32","address":"40002"},
        {"tag_name":"L2_CONVEYOR_ON","display_name":"Conveyor Run",
         "unit":"","widget_type":"toggle","gauge_min":0,"gauge_max":1,
         "sim_type":"bool_toggle","sim_min":0,"sim_max":1,"sim_period":40,
         "tag_group":"Line 2","decimals":0,"color":"#00ff88","icon":"power",
         "data_type":"BOOL","address":"00001","register_type":"coil","writable":1},
        {"tag_name":"L2_PRODUCT_COUNT","display_name":"Product Count",
         "unit":"pcs","widget_type":"value","gauge_min":0,"gauge_max":5000,
         "sim_type":"sawtooth","sim_min":0,"sim_max":5000,"sim_period":300,
         "tag_group":"Line 2","decimals":0,"color":"#ffd700","icon":"box",
         "data_type":"UINT32","address":"40005"},
        {"tag_name":"L2_VIBRATION","display_name":"Vibration Level",
         "unit":"mm/s","widget_type":"chart","gauge_min":0,"gauge_max":20,
         "sim_type":"random_walk","sim_min":0.5,"sim_max":4.5,"sim_period":15,
         "tag_group":"Line 2","decimals":2,"color":"#ff6b35","icon":"activity",
         "data_type":"FLOAT32","address":"40006"},
    ]
    tags_ut = [
        {"tag_name":"UTIL_POWER_KW","display_name":"Total Power",
         "unit":"kW","widget_type":"chart","gauge_min":0,"gauge_max":500,
         "sim_type":"sine","sim_min":180,"sim_max":320,"sim_period":120,
         "tag_group":"Utilities","decimals":1,"color":"#ffd700","icon":"zap",
         "data_type":"FLOAT32","address":"40001"},
        {"tag_name":"UTIL_WATER_FLOW","display_name":"Water Flow",
         "unit":"m³/h","widget_type":"gauge","gauge_min":0,"gauge_max":50,
         "sim_type":"random_walk","sim_min":15,"sim_max":25,"sim_period":60,
         "tag_group":"Utilities","decimals":2,"color":"#00d4ff","icon":"droplets",
         "data_type":"FLOAT32","address":"40002"},
        {"tag_name":"UTIL_HVAC_TEMP","display_name":"HVAC Air Temp",
         "unit":"°C","widget_type":"gauge","gauge_min":15,"gauge_max":35,
         "sim_type":"sine","sim_min":20,"sim_max":26,"sim_period":600,
         "tag_group":"Utilities","decimals":1,"color":"#00ff88","icon":"wind",
         "data_type":"FLOAT32","address":"40003"},
        {"tag_name":"UTIL_COMPRESSOR","display_name":"Compressor",
         "unit":"","widget_type":"toggle","gauge_min":0,"gauge_max":1,
         "sim_type":"bool_toggle","sim_min":0,"sim_max":1,"sim_period":60,
         "tag_group":"Utilities","decimals":0,"color":"#7c5cbf","icon":"power",
         "data_type":"BOOL","address":"00001","register_type":"coil","writable":1},
    ]

    tag_ids = {}
    for td in tags_l1:
        td["plc_id"] = plc1; tag_ids[td["tag_name"]] = db.create_tag(td)
    for td in tags_l2:
        td["plc_id"] = plc2; tag_ids[td["tag_name"]] = db.create_tag(td)
    for td in tags_ut:
        td["plc_id"] = plc3; tag_ids[td["tag_name"]] = db.create_tag(td)

    alert_defs = [
        {"tag_id":tag_ids["L1_TEMP_REACTOR"],"alert_name":"High Temp CRITICAL",
         "condition":"gt","threshold":175,"severity":"critical","notify_web":1,
         "message":"Reactor temperature exceeded critical limit!"},
        {"tag_id":tag_ids["L1_TEMP_REACTOR"],"alert_name":"Low Temp WARNING",
         "condition":"lt","threshold":145,"severity":"warning","notify_web":1,
         "message":"Reactor temperature below minimum"},
        {"tag_id":tag_ids["L1_PRESSURE"],"alert_name":"High Pressure",
         "condition":"gt","threshold":5.2,"severity":"critical","notify_web":1},
        {"tag_id":tag_ids["L1_LEVEL"],"alert_name":"Low Tank Level",
         "condition":"lt","threshold":25,"severity":"warning","notify_web":1},
        {"tag_id":tag_ids["L2_VIBRATION"],"alert_name":"High Vibration",
         "condition":"gt","threshold":4.0,"severity":"warning","notify_web":1},
        {"tag_id":tag_ids["UTIL_POWER_KW"],"alert_name":"Power Overload",
         "condition":"gt","threshold":310,"severity":"critical","notify_web":1},
    ]
    for ad in alert_defs:
        db.create_alert(ad)

    logger.info("Demo data seeded: 3 PLCs, 16 tags, 6 alerts")


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────
if __name__ == "__main__":
    seed_demo_data()
    engine.start()
    logger.info("=" * 60)
    logger.info("  SCADA Dashboard — http://127.0.0.1:5000")
    logger.info("=" * 60)
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False,
        threaded=True,
        use_reloader=False,
    )
