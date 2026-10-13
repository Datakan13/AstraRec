#!/usr/bin/env python3
"""Mail a recorder status report (heartbeat + upload status).

    python3 -I acrec_mail.py /opt/acrec/mail.env [--dry-run] [--note TEXT]

Reads <ROOT>/heartbeat.json and <ROOT>/upload_status.json and sends a plain-text
summary over SMTP. Subject starts with OK, WARN or DOWN:
  DOWN  heartbeat missing or older than STALE_MIN minutes (recorder not running)
  WARN  heartbeat warnings, a group with no live connection, writer errors,
        an upload error, or local disk below the recorder's minimum
  OK    otherwise
Counts and states only, never prices (heartbeat.json holds none).
Python standard library only. --dry-run prints the mail instead of sending it.

mail.env (KEY=value lines):
  ROOT=/data/acrec
  MAIL_TO=you@example.com            (comma-separated for several)
  MAIL_FROM=you@gmail.com
  SMTP_HOST=smtp.gmail.com
  SMTP_PORT=587
  SMTP_TLS=starttls                  (starttls | ssl | none)
  SMTP_USER=you@gmail.com
  SMTP_PASSWORD_FILE=/opt/acrec/smtp_password   (Gmail: an app password, mode 600)
  STALE_MIN=5
  HOST_LABEL=acrec-tokyo             (optional, shown in the subject)
"""
import argparse
import json
import os
import shutil
import smtplib
import socket
import ssl
import sys
import time
from datetime import datetime, timezone
from email.message import EmailMessage


def load_env(path):
    env = {}
    with open(path) as f:
        for line in f:
            line = line.split("#", 1)[0].strip()
            if "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def read_json(path):
    try:
        with open(path) as f:
            return json.load(f), os.path.getmtime(path)
    except (OSError, ValueError):
        return None, None


def gb(n):
    return f"{n / 1e9:.1f} GB" if n >= 1e9 else f"{n / 1e6:.0f} MB"


def local_days(root):
    days = []
    for name in sorted(os.listdir(root)) if os.path.isdir(root) else []:
        p = os.path.join(root, name)
        if len(name) == 10 and name[4] == "-" and os.path.isdir(p):
            size = sum(os.path.getsize(os.path.join(p, f)) for f in os.listdir(p) if os.path.isfile(os.path.join(p, f)))
            uploaded = os.path.exists(os.path.join(p, ".uploaded")) and os.path.getsize(os.path.join(p, ".uploaded")) > 0
            days.append((name, size, uploaded))
    return days


def build_report(env, note=None):
    root = env.get("ROOT", "/data/acrec")
    stale_min = float(env.get("STALE_MIN", "5"))
    now = time.time()
    hb, hb_mtime = read_json(os.path.join(root, "heartbeat.json"))
    up, _ = read_json(os.path.join(root, "upload_status.json"))

    problems = []
    lines = []
    status = "OK"
    if hb is None:
        status = "DOWN"
        problems.append("heartbeat.json missing: the recorder has not started or the data root is wrong")
    else:
        age_min = (now - hb_mtime) / 60
        if age_min > stale_min:
            status = "DOWN"
            problems.append(f"heartbeat is {age_min:.0f} min old: the recorder is not running")

    if hb is not None:
        lines.append(f"Recorder heartbeat {hb.get('utc', '?')} (uptime {int(hb.get('uptime_s', 0)) // 3600} h)")
        lines.append("")
        lines.append(f"{'group':8s} {'msgs/s':>9s} {'conns':>7s} {'quiet s':>8s} {'dropped':>8s}")
        for g, v in hb.get("groups", {}).items():
            conns = f"{v.get('conns_up', 0)}/{v.get('conns_total', 0)}"
            lines.append(f"{g:8s} {v.get('msgs_per_s', 0):9.1f} {conns:>7s} {v.get('quiet_s', -1):8d} {v.get('dropped_total', 0):8d}")
            if v.get("conns_total", 0) and v.get("conns_up", 0) < v.get("conns_total", 0):
                problems.append(f"{g}: only {v.get('conns_up')}/{v.get('conns_total')} connections up")
        lines.append("")
        lines.append(f"gaps today          {hb.get('gaps_today', 0)}")
        lines.append(f"written today       {gb(hb.get('bytes_written_today', 0))}")
        lines.append(f"disk free           {hb.get('disk_free_gb', -1):.1f} GB")
        q = hb.get("writer_queue", {})
        lines.append(f"writer queue        {q.get('fill', 0)}/{q.get('capacity', 0)}")
        lines.append(f"depth set           {hb.get('depth_set_size', 0)} symbols")
        if hb.get("last_error"):
            lines.append(f"last error          {hb['last_error']}")
            problems.append(f"last error: {hb['last_error']}")
        problems += [f"heartbeat warning: {w}" for w in hb.get("warnings", [])]

    lines.append("")
    if up is None:
        lines.append("Upload: no upload_status.json yet")
    else:
        lines.append(f"Upload {up.get('utc', '?')}: {up.get('state')} - {up.get('message')}")
        lines.append(f"  last upload {up.get('last_upload') or '-'}, Drive free {up.get('drive_free_gb')} GB")
        if up.get("state") == "error":
            problems.append(f"upload error: {up.get('message')}")

    days = local_days(root)
    if days:
        lines.append("")
        lines.append("Local days (newest last):")
        for name, size, uploaded in days[-8:]:
            lines.append(f"  {name}  {gb(size):>9s}  {'uploaded' if uploaded else 'not uploaded yet'}")
    try:
        free = shutil.disk_usage(root).free
        lines.append(f"  disk free now {gb(free)}")
    except OSError:
        pass

    if status == "OK" and problems:
        status = "WARN"
    head = [f"Status: {status}"]
    if note:
        head.append(note)
    if problems:
        head.append("")
        head.append("Problems:")
        head += [f"  - {p}" for p in problems]
    head.append("")
    return status, "\n".join(head + lines) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser(description="Mail the recorder status report.")
    ap.add_argument("env")
    ap.add_argument("--dry-run", action="store_true", help="print the mail instead of sending it")
    ap.add_argument("--note", help="extra line at the top, e.g. 'recorder (re)started'")
    args = ap.parse_args(argv)
    env = load_env(args.env)

    status, body = build_report(env, args.note)
    label = env.get("HOST_LABEL") or socket.gethostname()
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    msg = EmailMessage()
    msg["Subject"] = f"[acrec] {status} {label} {stamp}"
    msg["From"] = env.get("MAIL_FROM") or env.get("SMTP_USER", "acrec@localhost")
    msg["To"] = env["MAIL_TO"]
    msg.set_content(body)

    if args.dry_run:
        print(msg)
        return 0

    password = ""
    if env.get("SMTP_PASSWORD_FILE"):
        with open(env["SMTP_PASSWORD_FILE"]) as f:
            password = f.read().strip()
    host, port, tls = env.get("SMTP_HOST", "smtp.gmail.com"), int(env.get("SMTP_PORT", "587")), env.get("SMTP_TLS", "starttls")
    ctx = ssl.create_default_context()
    try:
        if tls == "ssl":
            server = smtplib.SMTP_SSL(host, port, context=ctx, timeout=30)
        else:
            server = smtplib.SMTP(host, port, timeout=30)
            if tls == "starttls":
                server.starttls(context=ctx)
        with server:
            if env.get("SMTP_USER"):
                server.login(env["SMTP_USER"], password)
            server.send_message(msg)
    except (OSError, smtplib.SMTPException) as e:
        print(f"acrec_mail: sending failed: {e}", file=sys.stderr)
        return 1
    print(f"acrec_mail: sent {status} report to {env['MAIL_TO']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
