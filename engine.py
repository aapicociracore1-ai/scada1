"""
SCADA Engine
- Multi-PLC polling with per-PLC configurable intervals
- Real-time alert evaluation with deadband & delay timers
- Telegram / LINE notification dispatch (background queue)
- Data retention cleanup (hourly)

All path references are absolute and resolved from __file__.
"""

import os
import sys
import threading
import time
import json
import logging
import queue
import urllib.request
import urllib.parse
import urllib.error
from typing import Any, Dict, List, Optional, Set, Tuple
from datetime import datetime

# ── Ensure the project root is on sys.path ────────────────────────────────────
_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from database.db import DatabaseManager
from plc_drivers.drivers import create_driver, BasePLCDriver

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Notification Dispatcher (background thread)
# ─────────────────────────────────────────────
class NotificationDispatcher:
    def __init__(self, db: DatabaseManager):
        self.db = db
        self._queue: queue.Queue = queue.Queue(maxsize=200)
        self._thread = threading.Thread(target=self._worker, daemon=True,
                                        name="notif-dispatcher")
        self._thread.start()

    def send(self, message: str, channels: List[str], severity: str = "warning"):
        try:
            self._queue.put_nowait(
                {"message": message, "channels": channels, "severity": severity}
            )
        except queue.Full:
            logger.warning("Notification queue full — dropped message")

    def _worker(self):
        while True:
            try:
                item = self._queue.get(timeout=2)
                self._dispatch(item)
            except queue.Empty:
                continue
            except Exception as exc:
                logger.error(f"Notification worker: {exc}")

    def _dispatch(self, item: dict):
        settings = self.db.get_notification_settings()
        for ch in item.get("channels", []):
            cfg = settings.get(ch, {})
            if not cfg.get("enabled"):
                continue
            try:
                if ch == "telegram":
                    self._send_telegram(cfg, item["message"])
                elif ch == "line":
                    self._send_line(cfg, item["message"])
            except Exception as exc:
                logger.error(f"Notify {ch} failed: {exc}")

    def _send_telegram(self, cfg: dict, message: str):
        token   = (cfg.get("token") or "").strip()
        chat_id = (cfg.get("chat_id") or "").strip()
        if not token or not chat_id:
            logger.warning("Telegram: token/chat_id not configured")
            return
        url  = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode(
            {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        ).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        with urllib.request.urlopen(req, timeout=10) as resp:
            logger.info(f"Telegram sent — HTTP {resp.status}")

    def _send_line(self, cfg: dict, message: str):
        token = (cfg.get("token") or "").strip()
        if not token:
            logger.warning("LINE: token not configured")
            return
        url  = "https://notify-api.line.me/api/notify"
        data = urllib.parse.urlencode({"message": message}).encode("utf-8")
        req  = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        with urllib.request.urlopen(req, timeout=10) as resp:
            logger.info(f"LINE sent — HTTP {resp.status}")


# ─────────────────────────────────────────────
# Alert Evaluator
# ─────────────────────────────────────────────
class AlertEvaluator:
    _CONDITION: Dict[str, Any] = {
        "gt":      lambda v, lo, hi: v > lo,
        "lt":      lambda v, lo, hi: v < lo,
        "gte":     lambda v, lo, hi: v >= lo,
        "lte":     lambda v, lo, hi: v <= lo,
        "eq":      lambda v, lo, hi: v == lo,
        "neq":     lambda v, lo, hi: v != lo,
        "between": lambda v, lo, hi: lo <= v <= hi,
        "outside": lambda v, lo, hi: v < lo or v > (hi if hi is not None else lo),
    }

    def __init__(self, db: DatabaseManager, dispatcher: NotificationDispatcher):
        self.db = db
        self.dispatcher = dispatcher
        self._active:  Dict[int, int]   = {}   # alert_id → event_id
        self._timers:  Dict[int, float] = {}   # alert_id → first-trigger epoch

    def evaluate(self, tag_id: int, tag_name: str, value: float):
        for alert in self.db.get_alerts(tag_id=tag_id):
            if not alert.get("enabled"):
                continue
            try:
                self._check(alert, tag_id, tag_name, value)
            except Exception as exc:
                logger.error(f"Alert eval error: {exc}")

    def _check(self, alert: dict, tag_id: int, tag_name: str, value: float):
        aid       = alert["id"]
        cond      = alert.get("condition", "gt")
        lo        = float(alert["threshold"])
        hi        = float(alert["threshold_high"]) if alert.get("threshold_high") is not None else lo
        deadband  = float(alert.get("deadband") or 0)
        delay     = int(alert.get("delay_seconds") or 0)
        severity  = alert.get("severity", "warning")

        fn        = self._CONDITION.get(cond, self._CONDITION["gt"])
        triggered = fn(value, lo, hi)

        if triggered:
            if aid not in self._timers:
                self._timers[aid] = time.monotonic()
            if time.monotonic() - self._timers[aid] < delay:
                return                          # still in delay window
            if aid not in self._active:         # fire new event
                msg = (alert.get("message") or
                       f"[{severity.upper()}] {tag_name} = {value:.4f}  "
                       f"(cond: {cond} {lo})")
                event_id = self.db.insert_event({
                    "alert_id": aid, "tag_id": tag_id, "tag_name": tag_name,
                    "value": value, "threshold": lo,
                    "severity": severity, "message": msg,
                })
                self._active[aid] = event_id
                channels = []
                if alert.get("notify_telegram"): channels.append("telegram")
                if alert.get("notify_line"):      channels.append("line")
                if channels:
                    full = (
                        f"🏭 <b>SCADA Alert</b>\n"
                        f"Tag:      {tag_name}\n"
                        f"Value:    {value:.4f}\n"
                        f"Cond:     {cond} {lo}\n"
                        f"Severity: {severity}\n"
                        f"Time:     {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    self.dispatcher.send(full, channels, severity)
                logger.warning(f"ALERT FIRED  [{severity}] {alert['alert_name']}: "
                               f"{tag_name}={value}")
        else:
            self._timers.pop(aid, None)
            if aid in self._active:
                # Apply deadband before clearing
                clear = True
                if cond in ("gt", "gte") and value > lo - deadband:
                    clear = False
                elif cond in ("lt", "lte") and value < lo + deadband:
                    clear = False
                if clear:
                    self.db.clear_event(self._active.pop(aid))


# ─────────────────────────────────────────────
# In-Memory Data Store  (SSE source)
# ─────────────────────────────────────────────
class DataStore:
    def __init__(self):
        self._data:  Dict[int, dict]   = {}
        self._lock:  threading.RLock   = threading.RLock()
        self._subs:  List[queue.Queue] = []
        self._slock: threading.Lock    = threading.Lock()

    def update(self, tag_id: int, tag_name: str, plc_name: str,
               value: float, timestamp: str):
        entry = {
            "tag_id": tag_id, "tag_name": tag_name,
            "plc_name": plc_name, "value": value,
            "quality": 1, "timestamp": timestamp,
        }
        with self._lock:
            self._data[tag_id] = entry
        self._notify(entry)

    def set_quality(self, tag_id: int, quality: int):
        with self._lock:
            if tag_id in self._data:
                self._data[tag_id]["quality"] = quality

    def get(self, tag_id: int) -> Optional[dict]:
        with self._lock:
            return self._data.get(tag_id)

    def get_all(self) -> Dict[int, dict]:
        with self._lock:
            return dict(self._data)

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=1000)
        with self._slock:
            self._subs.append(q)
        return q

    def unsubscribe(self, q: queue.Queue):
        with self._slock:
            try:
                self._subs.remove(q)
            except ValueError:
                pass

    def _notify(self, payload: dict):
        dead = []
        with self._slock:
            for q in self._subs:
                try:
                    q.put_nowait(payload)
                except queue.Full:
                    dead.append(q)
            for q in dead:
                try:
                    self._subs.remove(q)
                except ValueError:
                    pass


# ─────────────────────────────────────────────
# PLC Poller (one thread per PLC)
# ─────────────────────────────────────────────
class PLCPoller(threading.Thread):
    def __init__(self, plc: dict, db: DatabaseManager,
                 evaluator: AlertEvaluator, store: DataStore):
        super().__init__(daemon=True, name=f"plc-{plc['name']}")
        self.plc      = plc
        self.db       = db
        self.evaluator = evaluator
        self.store    = store
        self._stop    = threading.Event()
        self._driver: Optional[BasePLCDriver] = None
        # Stats
        self.poll_count  = 0
        self.error_count = 0
        self.last_poll_ms = 0.0

    def stop(self):
        self._stop.set()

    @property
    def connected(self) -> bool:
        return self._driver is not None and self._driver.is_connected()

    def run(self):
        self._connect()
        while not self._stop.is_set():
            t0 = time.perf_counter()
            try:
                self._poll()
            except Exception as exc:
                logger.error(f"[{self.plc['name']}] poll exception: {exc}")
                self.error_count += 1
            self.last_poll_ms = (time.perf_counter() - t0) * 1000
            interval = max(0.05, float(self.plc.get("poll_interval", 1.0))
                           - self.last_poll_ms / 1000)
            self._stop.wait(interval)

    def _connect(self):
        try:
            cfg = self._build_cfg()
            self._driver = create_driver(self.plc.get("plc_type", "simulator"), cfg)
            ok = self._driver.connect()
            if ok:
                logger.info(f"[{self.plc['name']}] connected "
                            f"({self.plc['plc_type']})")
            else:
                logger.error(f"[{self.plc['name']}] connect failed: "
                             f"{self._driver.last_error}")
        except Exception as exc:
            logger.error(f"[{self.plc['name']}] _connect exception: {exc}")

    def _build_cfg(self) -> dict:
        cfg = {
            "host":     self.plc.get("host", ""),
            "port":     self.plc.get("port", 502),
            "unit_id":  self.plc.get("unit_id", 1),
            "rack":     self.plc.get("rack", 0),
            "slot":     self.plc.get("slot", 1),
            "endpoint": self.plc.get("endpoint", ""),
        }
        try:
            extra = json.loads(self.plc.get("extra_config") or "{}")
            cfg.update(extra)
        except (json.JSONDecodeError, TypeError):
            pass
        return cfg

    def _poll(self):
        if not self._driver:
            return
        if not self._driver.is_connected():
            self._driver.connect()
            return

        tags = self.db.get_tags(plc_id=self.plc["id"])
        if not tags:
            return

        batch: List[Tuple[int, float, int]] = []
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for tag in tags:
            try:
                raw = self._driver.read_tag(tag)
                if raw is None:
                    self.store.set_quality(tag["id"], 0)
                    continue
                value = round(float(raw), 6)
                self.store.update(tag["id"], tag["tag_name"],
                                  self.plc["name"], value, ts)
                batch.append((tag["id"], value, 1))
                self.evaluator.evaluate(tag["id"], tag["tag_name"], value)
            except Exception as exc:
                self.error_count += 1
                self.store.set_quality(tag["id"], 0)
                logger.debug(f"[{self.plc['name']}] read '{tag['tag_name']}': {exc}")

        if batch:
            try:
                self.db.bulk_insert_readings(batch)
            except Exception as exc:
                logger.error(f"DB bulk insert failed: {exc}")

        self.poll_count += 1

    def write_value(self, tag: dict, value: Any) -> bool:
        if not self._driver or not self._driver.is_connected():
            return False
        try:
            return self._driver.write_tag(tag, value)
        except Exception as exc:
            logger.error(f"Write error: {exc}")
            return False


# ─────────────────────────────────────────────
# SCADA Engine  (top-level orchestrator)
# ─────────────────────────────────────────────
class SCADAEngine:
    def __init__(self):
        # DatabaseManager is a singleton — safe to call multiple times
        self.db         = DatabaseManager()
        self.store      = DataStore()
        self.dispatcher = NotificationDispatcher(self.db)
        self.evaluator  = AlertEvaluator(self.db, self.dispatcher)
        self._pollers:  Dict[int, PLCPoller]  = {}
        self._running   = False
        self._cleanup_thread: Optional[threading.Thread] = None

    def start(self):
        if self._running:
            return
        self._running = True
        self._start_cleanup()
        self._reload_pollers()
        logger.info("SCADA Engine started")

    def stop(self):
        self._running = False
        for p in list(self._pollers.values()):
            p.stop()
        self._pollers.clear()
        logger.info("SCADA Engine stopped")

    def _reload_pollers(self):
        for plc in self.db.get_plcs():
            if plc["enabled"] and plc["id"] not in self._pollers:
                self._start_poller(plc)

    def _start_poller(self, plc: dict):
        poller = PLCPoller(plc, self.db, self.evaluator, self.store)
        poller.start()
        self._pollers[plc["id"]] = poller

    def restart_poller(self, plc_id: int):
        if plc_id in self._pollers:
            self._pollers[plc_id].stop()
            del self._pollers[plc_id]
        plc = self.db.get_plc(plc_id)
        if plc and plc["enabled"]:
            self._start_poller(plc)

    def stop_poller(self, plc_id: int):
        if plc_id in self._pollers:
            self._pollers[plc_id].stop()
            del self._pollers[plc_id]

    def write_tag(self, tag_id: int, value: Any,
                  operator: str = "operator") -> Tuple[bool, str]:
        tag = self.db.get_tag(tag_id)
        if not tag:
            return False, "Tag not found"
        if not tag.get("writable"):
            return False, "Tag is read-only"

        poller = self._pollers.get(tag["plc_id"])
        if not poller:
            return False, "PLC poller not running"
        if not poller.connected:
            return False, "PLC not connected"

        old = self.db.get_latest_reading(tag_id)
        old_val = old["value"] if old else None

        ok = poller.write_value(tag, value)
        self.db.log_control(tag_id, tag["tag_name"], tag["plc_name"],
                            old_val, value, ok, operator)
        return (True, "OK") if ok else (False, "PLC write failed")

    def get_engine_status(self) -> Dict:
        out = {}
        for pid, poller in self._pollers.items():
            plc = self.db.get_plc(pid)
            if plc:
                out[pid] = {
                    "name":         plc["name"],
                    "connected":    poller.connected,
                    "poll_count":   poller.poll_count,
                    "error_count":  poller.error_count,
                    "last_poll_ms": round(poller.last_poll_ms, 2),
                }
        return out

    def _start_cleanup(self):
        def _loop():
            while self._running:
                try:
                    days = int(self.db.get_setting("data_retention_days", "90"))
                    self.db.purge_old_readings(days)
                except Exception as exc:
                    logger.error(f"Cleanup error: {exc}")
                time.sleep(3600)

        self._cleanup_thread = threading.Thread(
            target=_loop, daemon=True, name="db-cleanup"
        )
        self._cleanup_thread.start()


# ── Single global engine instance ────────────────────────────────────────────
engine = SCADAEngine()
