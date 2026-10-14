#!/usr/bin/env bash
# run_recorder.sh: one script for everything. Starts the recorder, restarts it if
# it ever stops, and runs the Storage Box upload (acrec_upload.sh) every hour
# at :30 UTC. Ctrl-C / SIGTERM stops everything cleanly.
#
#   run_recorder.sh                          # defaults below
#   RECORDER=./build/recorder CONFIG=./build/recorder.json run_recorder.sh
#
# Run it in the background with:   nohup ./run_recorder.sh >> run_recorder.log 2>&1 &
# or inside tmux/screen, or as a single systemd service (run_recorder.service).
# Prints states only (started, exited, uploading), never data.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${RECORDER:=/opt/acrec/recorder}"           # recorder binary
: "${CONFIG:=/opt/acrec/recorder.json}"        # recorder config
: "${UPLOAD:=$HERE/acrec_upload.sh}"           # uploader
: "${UPLOAD_ENV:=/opt/acrec/upload.env}"       # uploader settings
: "${UPLOAD_MINUTE:=30}"                       # upload every hour at this minute (UTC)
: "${UPLOAD_ENABLED:=1}"                       # 0 = only run the recorder
: "${MAIL:=$HERE/acrec_mail.py}"               # status mailer
: "${MAIL_ENV:=/opt/acrec/mail.env}"           # mailer settings; mails are off if this file is missing
: "${MAIL_EVERY_H:=6}"                         # report every N hours (UTC, from 00:00)
: "${MAIL_FIRST_DELAY_S:=120}"                 # first report this long after start

log() { echo "$(date -u +%FT%TZ) run_recorder: $*"; }

[[ -f "$RECORDER" && -x "$RECORDER" ]] || { log "ERROR: recorder binary not found: $RECORDER"; exit 1; }
[[ -f "$CONFIG" ]] || { log "ERROR: config not found: $CONFIG"; exit 1; }
if [[ "$UPLOAD_ENABLED" == 1 && ! -x "$UPLOAD" ]]; then
  log "ERROR: uploader not found or not executable: $UPLOAD (set UPLOAD_ENABLED=0 to skip uploads)"
  exit 1
fi

rec_pid="" upload_pid="" stopping=0

stop_all() {
  stopping=1
  log "stopping"
  if [[ -n "$rec_pid" ]] && kill -0 "$rec_pid" 2>/dev/null; then
    kill -TERM "$rec_pid"
    # The recorder ends its frames and renames every .part; give it up to 60 s.
    for _ in $(seq 60); do kill -0 "$rec_pid" 2>/dev/null || break; sleep 1; done
    kill -0 "$rec_pid" 2>/dev/null && { log "recorder did not stop in 60 s; killing"; kill -KILL "$rec_pid"; }
  fi
  # An interrupted upload is safe: nothing is marked uploaded, the next run redoes it.
  [[ -n "$upload_pid" ]] && kill -0 "$upload_pid" 2>/dev/null && kill -TERM "$upload_pid"
  wait 2>/dev/null
  log "stopped"
  exit 0
}
trap stop_all TERM INT

start_recorder() {
  "$RECORDER" "$CONFIG" &
  rec_pid=$!
  rec_started=$(date +%s)
  log "recorder started (pid $rec_pid)"
}

next_upload_time() {   # next HH:UPLOAD_MINUTE UTC, as epoch seconds
  local now t
  now=$(date -u +%s)
  t=$(( now - now % 3600 + UPLOAD_MINUTE * 60 ))
  (( t <= now )) && t=$(( t + 3600 ))
  echo "$t"
}

MAIL_ENABLED=0
[[ -f "$MAIL_ENV" ]] && MAIL_ENABLED=1
next_mail_time() {     # next multiple of MAIL_EVERY_H hours UTC
  local now=$(date -u +%s) p=$(( MAIL_EVERY_H * 3600 ))
  echo $(( now - now % p + p ))
}
send_mail() {          # runs in the background; never blocks the loop
  ( python3 -I "$MAIL" "$MAIL_ENV" "$@" >/dev/null 2>"$MAIL_ERR" \
      && log "status mail sent" || log "status mail FAILED: $(tail -1 "$MAIL_ERR")" ) &
}
MAIL_ERR="$(mktemp)"

backoff=5 restart_at=0
start_recorder
next_upload=$(next_upload_time)
log "uploads every hour at :$(printf '%02d' "$UPLOAD_MINUTE") UTC$([[ "$UPLOAD_ENABLED" == 1 ]] || echo ' (disabled)')"
if (( MAIL_ENABLED )); then
  next_mail=$(next_mail_time)
  log "status mail every ${MAIL_EVERY_H} h (UTC); first one in ${MAIL_FIRST_DELAY_S} s"
  first_mail=$(( $(date -u +%s) + MAIL_FIRST_DELAY_S ))   # once the recorder has written a heartbeat
else
  log "status mail off ($MAIL_ENV not found)"
fi

while (( ! stopping )); do
  # Recorder supervision: restart if it exited. Quick repeated failures back off
  # (5 s doubling to 5 min) so a broken config doesn't spin. The restart is
  # scheduled rather than slept through, so uploads stay on time meanwhile.
  if [[ -n "$rec_pid" ]] && ! kill -0 "$rec_pid" 2>/dev/null; then
    wait "$rec_pid"; code=$?
    ran=$(( $(date +%s) - rec_started ))
    (( ran >= 300 )) && backoff=5
    log "recorder exited with code $code after ${ran}s; restarting in ${backoff}s"
    rec_pid=""
    restart_at=$(( $(date +%s) + backoff ))
    backoff=$(( backoff * 2 > 300 ? 300 : backoff * 2 ))
  fi
  if [[ -z "$rec_pid" ]] && (( $(date +%s) >= restart_at )); then
    start_recorder
  fi

  # Hourly upload, never two at once (acrec_upload.sh also holds a lock).
  if [[ "$UPLOAD_ENABLED" == 1 ]] && (( $(date -u +%s) >= next_upload )); then
    next_upload=$(next_upload_time)
    if [[ -n "$upload_pid" ]] && kill -0 "$upload_pid" 2>/dev/null; then
      log "previous upload still running; skipping this hour"
    else
      log "upload starting"
      ( "$UPLOAD" "$UPLOAD_ENV"; log "upload finished with code $?" ) &
      upload_pid=$!
    fi
  fi

  # Status mail: one shortly after start (so restarts show up), then every MAIL_EVERY_H hours.
  if (( MAIL_ENABLED )); then
    t=$(date -u +%s)
    if (( first_mail && t >= first_mail )); then
      first_mail=0
      send_mail --note "run_recorder (re)started at $(date -u -d @"$rec_started" +%FT%TZ)"
    elif (( t >= next_mail )); then
      next_mail=$(next_mail_time)
      send_mail
    fi
  fi

  # `sleep & wait` so a signal interrupts the pause immediately.
  sleep 5 & wait $!
done
