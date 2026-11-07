#!/usr/bin/env python3
"""Bounded, isolated process tests using the real mailbox protocol and CLI tools."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import copy
import json
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time
import uuid


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def eventually(probe, description, timeout=8):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = probe()
        if result:
            return result
        time.sleep(0.05)
    raise AssertionError(f"timeout: {description}")


class Daemon:
    def __init__(self, binary_dir, root, backend, mode):
        self.root = root
        self.process = None
        self.log = (root / "daemon.log").open("w")
        # CLI configuration is loaded beside each executable, so isolate those binaries too.
        for name in ("MainMQ", "yirangmq-cli-publisher", "yirangmq-cli-consumer", "sqlite_schema.sql"):
            shutil.copy2(binary_dir / name, root / name)
        config = {
            "backend": backend,
            "paths": {"dataRoot": "./data", "logRoot": "./logs"},
            "sqlite": {"dbPath": "./data/test.db", "schemaPath": "./sqlite_schema.sql"},
            "filesystem": {"root": "./data/fs"},
            "ipc": {"root": "./ipc", "pollIntervalMs": 20, "useFolderWatcher": mode == "event"},
            "lease": {"sweepIntervalMs": 100},
        }
        policy = {
            "visibilityTimeoutSec": 30,
            "ttlSec": 0,
            "retry": {"limit": 3, "backoff": "fixed", "initialDelaySec": 1, "maxDelaySec": 1},
            "dlq": {"enabled": True},
        }
        schema = {
            "name": "OperationalTest",
            "rules": [
                {"field": "deviceId", "type": "required"},
                {"field": "deviceId", "type": "type", "expectedType": "string"},
            ],
        }
        names = ["cli", "invalid", "owner", "route", "retry", "expiry", "extension",
                 "delay", "ttl", "concurrent", "batch", "restart", "background"]
        config["queues"] = []
        for name in names:
            queue_policy = copy.deepcopy(policy)
            queue_policy["ttlSec"] = 1 if name == "ttl" else 0
            config["queues"].append({"name": name, "policy": queue_policy, "messageSchema": schema})
        (root / "main_mq_configuration.json").write_text(json.dumps(config))
        (root / "ipc/requests").mkdir(parents=True)

    def start(self):
        check(self.process is None, "daemon already owned")
        self.process = subprocess.Popen(
            [str(self.root / "MainMQ"), "--write-console-log", "3", "--write-file-log", "0"],
            cwd=self.root, stdout=self.log, stderr=subprocess.STDOUT,
        )

        def ready():
            check(self.process.poll() is None, "daemon exited before readiness")
            try:
                return self.call("health", {}, timeout=0.3)["ok"]
            except TimeoutError:
                return False

        eventually(ready, "daemon health", timeout=10)

    def stop(self):
        check(self.process is not None, "no owned daemon")
        process = self.process
        check(process.poll() is None, "daemon exited unexpectedly")
        process.terminate()
        try:
            code = process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
            self.process = None
            raise AssertionError("daemon did not stop within 5 seconds") from None
        self.process = None
        check(code == 0, f"daemon shutdown exit code: {code}")

    def cleanup(self):
        if self.process is not None and self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=5)
        self.log.close()

    def submit(self, content, request_id):
        path = self.root / "ipc/requests" / f"{request_id}.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(content)
        temporary.replace(path)
        return path

    def call(self, command, payload, timeout=3):
        request_id = uuid.uuid4().hex
        request = {"requestId": request_id, "clientId": request_id, "command": command,
                   "timestampMs": int(time.time() * 1000),
                   "deadlineMs": int((time.time() + timeout) * 1000), "payload": payload}
        path = self.submit(json.dumps(request), request_id)
        response_path = self.root / "ipc/responses" / request_id / f"{request_id}.json"
        deadline = time.monotonic() + timeout
        try:
            while time.monotonic() < deadline:
                if response_path.exists():
                    response = json.loads(response_path.read_text())
                    response_path.unlink()
                    check(response["requestId"] == request_id, "response correlation mismatch")
                    check(type(response["ok"]) is bool, "invalid response status")
                    return response
                time.sleep(0.01)
            raise TimeoutError(f"no response for {command}")
        finally:
            path.unlink(missing_ok=True)

    def ok(self, command, payload):
        response = self.call(command, payload)
        check(response["ok"], f"{command}: {response}")
        return response["data"]

    def reject(self, command, payload, code):
        response = self.call(command, payload)
        check(not response["ok"], f"unexpected success: {command} {payload}")
        check(response["error"]["code"] == code, f"wrong rejection: {response}")

    def publish(self, queue, marker="sample", **options):
        return self.ok("publish", {"queue": queue, "message": json.dumps({"deviceId": marker}),
                                   **options})["messageKey"]

    def consume(self, queue, owner="worker", visibility=30):
        return self.ok("consume", {"queue": queue, "consumerId": owner,
                                   "visibilityTimeoutSec": visibility})

    def lease(self, queue, key, owner="worker", visibility=30):
        result = self.consume(queue, owner, visibility)
        check(result["message"] is not None, f"no delivery: {key}")
        check(result["message"]["messageKey"] == key, f"wrong delivery: {result}")
        check(result["lease"]["messageKey"] == key, "lease/message mismatch")
        check(result["lease"]["consumerId"] == owner, "lease owner mismatch")
        return result

    def settle(self, command, result, **options):
        return self.ok(command, {**result["lease"], **options})

    def state(self, queue, **expected):
        metrics = self.ok("status", {"queue": queue})["metrics"]
        counts = dict(ready=0, inflight=0, delayed=0, dlq=0)
        counts.update(expected)
        return metrics == counts

    def empty(self, queue):
        check(self.state(queue), f"messages remain in {queue}")
        check(self.consume(queue)["message"] is None, f"unexpected delivery in {queue}")

    def cli(self, binary, *args, success=True):
        result = subprocess.run(
            [str(self.root / binary), *args, "--ipc-root", str(self.root / "ipc"),
             "--timeout", "1000"], cwd=self.root, capture_output=True, text=True, timeout=3,
        )
        check(result.returncode == (0 if success else 1),
              f"CLI {args} exit {result.returncode}: {result.stdout} {result.stderr}")


def health_cli(mq):
    mq.cli("yirangmq-cli-publisher", "health")
    mq.cli("yirangmq-cli-publisher", "publish", "--queue", "cli",
           "--message", '{"deviceId":"cli-marker","note":"a{b}c"}')
    result = mq.consume("cli")
    check(result["message"]["payload"] == {"deviceId": "cli-marker", "note": "a{b}c"},
          "CLI payload did not round-trip intact")
    lease = result["lease"]
    mq.cli("yirangmq-cli-consumer", "ack", "--message-key", lease["messageKey"],
           "--lease-id", lease["leaseId"], "--consumer-id", "wrong", success=False)
    check(mq.state("cli", inflight=1), "rejected CLI ACK changed state")
    mq.cli("yirangmq-cli-consumer", "ack", "--message-key", lease["messageKey"],
           "--lease-id", lease["leaseId"], "--consumer-id", "worker")
    mq.cli("yirangmq-cli-consumer", "consume", "--queue", "cli")
    mq.empty("cli")
    metrics = mq.ok("metrics", {})
    check(metrics["requests"]["total"] > 0, "request metrics not recorded")


def invalid_requests(mq):
    mq.reject("publish", {"queue": "invalid", "message": "{}"}, "ERR_VALIDATION_FAILED")
    mq.reject("publish", {"queue": "invalid", "message": "{"}, "ERR_VALIDATION_FAILED")
    mq.reject("publish", {"queue": "../escape", "message": "{}"}, "ERR_INVALID_REQUEST")
    mq.reject("ack", {"messageKey": "msg_invalid:id"}, "ERR_INVALID_REQUEST")
    mq.reject("unsupported", {}, "ERR_UNKNOWN_COMMAND")
    mq.cli("yirangmq-cli-publisher", "--queue", "invalid", "--message", "{}", success=False)
    mq.empty("invalid")
    key = mq.publish("invalid")
    mq.settle("ack", mq.lease("invalid", key))
    mq.empty("invalid")


def ownership(mq):
    key = mq.publish("owner")
    result = mq.lease("owner", key)
    for command in ("ack", "nack", "extend_lease"):
        for bad in ({"consumerId": "wrong"}, {"leaseId": "wrong"}):
            mq.reject(command, {**result["lease"], "requeue": True, **bad}, "ERR_INTERNAL_ERROR")
            check(mq.state("owner", inflight=1), f"{command} rejection changed lease")
    mq.settle("ack", result)
    mq.reject("ack", result["lease"], "ERR_INTERNAL_ERROR")
    mq.empty("owner")


def direct_routing(mq):
    target = mq.publish("route", "target", targetConsumerId="target", priority=10)
    general = mq.publish("route", "general")
    mq.settle("ack", mq.lease("route", general, "other"))
    check(mq.consume("route", "other")["message"] is None, "wrong consumer received targeted message")
    check(mq.state("route", ready=1), "targeted message disappeared")
    mq.settle("ack", mq.lease("route", target, "target"))
    mq.empty("route")


def nack_dlq(mq):
    key = mq.publish("retry")
    for attempt in range(1, 4):
        result = mq.lease("retry", key)
        check(result["message"]["attempt"] == attempt, "attempt did not increase")
        mq.settle("nack", result, requeue=True, reason="retry-exhausted")
    check(mq.state("retry", dlq=1), "retry limit did not move message to DLQ")
    dead = mq.ok("list_dlq", {"queue": "retry"})
    check(dead["count"] == 1 and dead["messages"][0]["messageKey"] == key, "wrong DLQ entry")
    check(dead["messages"][0]["attempt"] == 3, "wrong DLQ attempt")
    check("retry-exhausted" in dead["messages"][0]["reason"], "NACK reason lost")
    mq.ok("reprocess", {"messageKey": key})
    result = mq.lease("retry", key)
    check(result["message"]["attempt"] == 1, "redrive did not reset attempt")
    mq.settle("ack", result)
    mq.reject("reprocess", {"messageKey": key}, "ERR_DLQ_NOT_FOUND")
    mq.empty("retry")


def expired_lease(mq):
    key = mq.publish("expiry")
    old = mq.lease("expiry", key, visibility=1)
    eventually(lambda: mq.state("expiry", ready=1), "expired lease retry")
    current = mq.lease("expiry", key)
    check(current["lease"]["leaseId"] != old["lease"]["leaseId"], "lease token reused")
    check(current["message"]["attempt"] == 2, "recovery attempt incorrect")
    for command in ("ack", "nack", "extend_lease"):
        mq.reject(command, {**old["lease"], "requeue": True}, "ERR_INTERNAL_ERROR")
    check(mq.state("expiry", inflight=1), "stale token changed current lease")
    mq.settle("ack", current)
    mq.empty("expiry")


def extended_lease(mq):
    key = mq.publish("extension")
    result = mq.lease("extension", key, visibility=1)
    mq.settle("extend_lease", result, visibilityTimeoutSec=5)
    time.sleep(1.2)
    check(mq.consume("extension", "other")["message"] is None, "extended lease redelivered early")
    mq.settle("ack", result)
    mq.empty("extension")


def delay_ttl(mq):
    delayed = mq.publish("delay", "later", delayMs=1000, priority=10)
    ready = mq.publish("delay", "now")
    mq.settle("ack", mq.lease("delay", ready))
    check(mq.consume("delay")["message"] is None, "delayed message visible early")
    eventually(lambda: mq.state("delay", ready=1), "delayed message becomes ready")
    mq.settle("ack", mq.lease("delay", delayed))
    mq.empty("delay")
    mq.publish("ttl", "expires-ready")
    mq.publish("ttl", "expires-delayed", delayMs=60000)
    eventually(lambda: mq.state("ttl"), "TTL purge of ready and delayed", timeout=8)
    mq.empty("ttl")


def concurrent_consumers(mq):
    key = mq.publish("concurrent")
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda owner: mq.consume("concurrent", owner), ["a", "b"]))
    deliveries = [result for result in results if result["message"] is not None]
    check(len(deliveries) == 1, f"two consumers leased one message: {results}")
    check(deliveries[0]["message"]["messageKey"] == key, "wrong concurrent delivery")
    mq.settle("ack", deliveries[0])
    mq.empty("concurrent")


def batch(mq):
    published = mq.ok("batch_publish", {"queue": "batch", "messages": [
        {"message": {"deviceId": "one"}}, {"message": {}}, {"message": {"deviceId": "two"}}]})
    check(published == {"published": 2, "skipped": 1, "total": 3}, f"batch validation: {published}")
    consumed = mq.ok("batch_consume", {"queue": "batch", "consumerId": "batch-owner", "maxMessages": 10})
    check(consumed["count"] == 2, f"batch count: {consumed}")
    check({item["message"]["payload"]["deviceId"] for item in consumed["messages"]} == {"one", "two"},
          "batch delivered invalid or duplicate payload")
    for result in consumed["messages"]:
        mq.settle("ack", result)
    mq.empty("batch")


def malformed_stale(mq):
    for content in ("{", json.dumps({"requestId": "expired", "clientId": "expired", "command": "health",
                                    "deadlineMs": 1, "payload": {}})):
        request_id = uuid.uuid4().hex
        mq.submit(content, request_id)
        eventually(lambda: list((mq.root / "ipc/dead").glob(f"{request_id}*")), "bad request quarantine")
    check(mq.ok("health", {})["status"] == "ok", "bad request stopped daemon")


def restart(mq):
    key = mq.publish("restart", "persisted")
    old = mq.lease("restart", key, visibility=1)
    mq.stop()
    mq.start()
    eventually(lambda: mq.state("restart", ready=1), "inflight recovery after graceful restart")
    current = mq.lease("restart", key)
    check(current["message"]["payload"]["deviceId"] == "persisted", "payload lost after restart")
    mq.reject("ack", old["lease"], "ERR_INTERNAL_ERROR")
    mq.settle("ack", current)
    mq.empty("restart")


def client_timeout(mq):
    mq.stop()
    mq.cli("yirangmq-cli-publisher", "health", success=False)
    check(not list((mq.root / "ipc/requests").glob("*.json")), "timed-out client left request behind")
    mq.start()
    mq.cli("yirangmq-cli-publisher", "health")


CASES = [health_cli, invalid_requests, ownership, direct_routing, nack_dlq, expired_lease,
         extended_lease, delay_ttl, concurrent_consumers, batch, malformed_stale, restart, client_timeout]


def seed_background(mq):
    key = mq.publish("background", "held", targetConsumerId="held")
    mq.lease("background", key, "held", 3600)
    mq.publish("background", "ready", targetConsumerId="reserved")
    mq.publish("background", "delayed", targetConsumerId="reserved", delayMs=3600000)
    key = mq.publish("background", "dead", targetConsumerId="dead")
    mq.settle("nack", mq.lease("background", key, "dead"), requeue=False, reason="background")
    check(mq.state("background", ready=1, inflight=1, delayed=1, dlq=1), "background setup failed")


def check_demo_failures(binary_dir):
    # These protocol doubles test the test tools themselves, separately from real-daemon cases.
    delivery = {"message": {"messageKey": "msg:telemetry:probe", "attempt": 1, "payload": {}},
                "lease": {"leaseId": "probe", "messageKey": "msg:telemetry:probe",
                          "consumerId": "demo-worker-01", "leaseUntil": 1}}
    probes = [
        ("sensor-timeout", "sensor", ["--count", "1", "--bad-frame", "0"],
         {"health": {"status": "ok"}}),
        ("ack-timeout", "worker", ["--stall-ms", "10", "--max-messages", "1"],
         {"consume_next": delivery}),
        ("backend-error", "worker", ["--stall-ms", "10", "--max-messages", "1"],
         {"consume_next": delivery, "ack": "database unavailable"}),
        ("consumer-timeout", "worker", ["--idle-exit-ms", "100"], {}),
        ("missing-delivery", "worker", ["--idle-exit-ms", "100", "--expect-messages", "1"],
         {"consume_next": {"message": None}}),
        ("conflicting-state-bounds", "monitor",
         ["wait", "--state", "ready", "--count", "1", "--max", "0", "--timeout-ms", "100"],
         {"status": {"metrics": {"ready": 0, "inflight": 0, "delayed": 0, "dlq": 0}}}),
        ("missing-state-counter", "monitor", ["check-empty"],
         {"status": {"metrics": {"ready": 0, "delayed": 0, "dlq": 0}, "policy": {}}}),
    ]
    for name, binary, arguments, replies in probes:
        with tempfile.TemporaryDirectory(prefix="yirangmq-probe-") as directory:
            root = Path(directory)
            requests = root / "ipc/requests"
            requests.mkdir(parents=True)
            with (root / "probe.log").open("w") as log:
                process = subprocess.Popen(
                    [str(binary_dir / f"yirangmq-demo-{binary}"), *arguments,
                     "--ipc-root", str(root / "ipc"), "--timeout", "100"],
                    cwd=root, stdout=log, stderr=subprocess.STDOUT,
                )
                try:
                    deadline = time.monotonic() + 4
                    while process.poll() is None and time.monotonic() < deadline:
                        for path in requests.glob("*.json"):
                            request = json.loads(path.read_text())
                            path.unlink()
                            reply = replies.get(request["command"])
                            if reply is None:
                                continue
                            response = {"requestId": request["requestId"], "ok": not isinstance(reply, str)}
                            if isinstance(reply, str):
                                response["error"] = {"code": "ERR_INTERNAL_ERROR", "message": reply}
                            else:
                                response["data"] = reply
                            target = root / "ipc/responses" / request["clientId"] / f"{request['requestId']}.json"
                            target.parent.mkdir(parents=True, exist_ok=True)
                            temporary = target.with_suffix(".tmp")
                            temporary.write_text(json.dumps(response))
                            temporary.replace(target)
                        time.sleep(0.005)
                    check(process.poll() == 1, f"{name}: expected exit 1, got {process.poll()}")
                    print(f"PASS harness-guard/{name}", flush=True)
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=3)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", type=Path, default=Path("/app"))
    parser.add_argument("--backend", choices=["all", "sqlite", "filesystem", "hybrid"], default="all")
    parser.add_argument("--mode", choices=["all", "poll", "event"], default="all")
    args = parser.parse_args()
    binary_dir = args.bin_dir.resolve()
    backends = ["sqlite", "filesystem", "hybrid"] if args.backend == "all" else [args.backend]
    modes = ["poll", "event"] if args.mode == "all" else [args.mode]
    check_demo_failures(binary_dir)
    passed = 0
    failed = 0
    for backend in backends:
        for mode in modes:
            with tempfile.TemporaryDirectory(prefix="yirangmq-integration-") as directory:
                mq = Daemon(binary_dir, Path(directory), backend, mode)
                label = f"{backend}/{mode}"
                try:
                    mq.start()
                    seed_background(mq)
                    for round_number in (1, 2):
                        # Reuse the same daemon/storage with mixed residual states; reverse order on repeat.
                        ordered = CASES if round_number == 1 else list(reversed(CASES))
                        for case in ordered:
                            label = f"{backend}/{mode}/round-{round_number}/IT-{CASES.index(case) + 1:02}/{case.__name__}"
                            case(mq)
                            check(mq.state("background", ready=1, inflight=1, delayed=1, dlq=1),
                                  "scenario changed unrelated residual messages")
                            passed += 1
                            print(f"PASS {label}", flush=True)
                    label = f"{backend}/{mode}/graceful-shutdown"
                    mq.stop()
                    print(f"PASS {label}", flush=True)
                except (AssertionError, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
                    failed += 1
                    print(f"FAIL {label}: {error}", flush=True)
                    mq.log.flush()
                    print((mq.root / "daemon.log").read_text()[-8000:], flush=True)
                finally:
                    mq.cleanup()
    print(f"Operational scenarios: {passed} passed, {failed} failed", flush=True)
    return 1 if failed else 0


def handle_termination(signum, _frame):
    raise SystemExit(128 + signum)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_termination)
    raise SystemExit(main())
