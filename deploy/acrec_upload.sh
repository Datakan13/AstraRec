#!/usr/bin/env bash
# acrec_upload.sh: ship recorder data to the Hetzner Storage Box (rclone, SFTP on
# port 23; any rclone remote works) and free local disk. Run hourly (run_recorder.sh or acrec-upload.timer); safe to rerun at any time.
#
#   acrec_upload.sh [/opt/acrec/upload.env]
#
# For each <root>/YYYY-MM-DD:
#   - what is uploaded: finished segment files (*.bin.zst, not .part) at any time,
#     so with recorder "segment_minutes": 60 each hour goes up during the day.
#     meta.json and the CSVs keep changing until the day closes (past midnight
#     UTC + GRACE_MIN), so they go up once it has.
#   - how: new or changed files are streamed as ONE tar straight into rclone (no
#     local temp copy), with SHA256SUMS_NNN inside. Names: <day>_000.tar,
#     <day>_001.tar, ... An existing name on the remote is never overwritten.
#   - check: the tar's MD5 as streamed must equal the MD5 the remote reports, or nothing
#     is recorded and the next run retries. Verified files go into <day>/.uploaded.
#   - local cleanup: a segment verified in an EARLIER run and older than
#     KEEP_LOCAL_HOURS is deleted and replaced by an empty "<name>.pruned" marker
#     (the recorder never reuses that segment number). A closed day with every
#     file verified and older than KEEP_DAYS is deleted entirely.
#   - gaps: <day>/gaps.csv is also copied every run, as a plain file, to
#     <REMOTE>/gaps/<day>_gaps.csv. It's replaced while the day is still being
#     written, so the box always has the latest one without unpacking a tar.
#     (The final gaps.csv still goes into the day's tar as well.) A failure here
#     doesn't stop the data upload but is mailed.
#
# The data is a sealed holdout: this script never reads or prints contents,
# only names, sizes and counts. The .bin.zst files are already zstd-compressed,
# so the tar is not compressed again.
set -euo pipefail

ENV_FILE="${1:-/opt/acrec/upload.env}"
# shellcheck source=/dev/null
[[ -f "$ENV_FILE" ]] && source "$ENV_FILE"
: "${ROOT:=/data/acrec}"            # recorder data root (recorder.json "root")
: "${REMOTE:=storagebox:acrec}"     # rclone remote:path (Storage Box: no leading / in the path)
: "${KEEP_DAYS:=7}"                 # delete a closed, fully verified day this many days after it ends
: "${KEEP_LOCAL_HOURS:=-1}"         # >=0: also delete verified segments this old (-1 = keep until the day goes)
: "${GRACE_MIN:=15}"                # wait this long after midnight UTC (recorder closes at +2 min)
: "${MIN_REMOTE_FREE_GB:=${MIN_DRIVE_FREE_GB:-5}}"   # refuse to upload when the remote would end below this
: "${RCLONE:=rclone}"
# The Storage Box allows 10 simultaneous connections per account: stay well below.
: "${RCLONE_FLAGS:=--retries 5 --low-level-retries 20 --stats 0 --checkers 2 --transfers 1}"

log() { echo "$(date -u +%FT%TZ) acrec_upload: $*"; }
die() {
  log "ERROR: $*"
  write_status "error" "$*"
  # Mail at once: failed uploads mean nothing can be deleted locally, so the disk
  # fills up within about a day.
  mail_note "UPLOAD FAILED: $*"
  exit 1
}
mail_note() {   # mail a one-line note at once (if mail is set up)
  if [[ -n "${MAIL_ENV:-}" && -f "$MAIL_ENV" ]]; then
    python3 -I "${MAIL:-$(dirname "$(readlink -f "$0")")/acrec_mail.py}" "$MAIL_ENV" \
      --note "$1" >/dev/null 2>&1 || log "could not send the mail"
  fi
}

STATUS="$ROOT/upload_status.json"
LAST_UPLOAD=""
write_status() {   # state, message: for monitoring next to heartbeat.json
  local tmp="$STATUS.tmp"
  printf '{"utc": "%s", "state": "%s", "message": "%s", "last_upload": "%s", "remote_free_gb": %s}\n' \
    "$(date -u +%FT%TZ)" "$1" "${2//\"/\'}" "$LAST_UPLOAD" "${REMOTE_FREE_GB:-null}" > "$tmp" && mv -f "$tmp" "$STATUS"
}

# Start from / : the caller's directory may be unreadable for this user (e.g. sudo
# -u acrec from /root), which makes find complain.
cd /
command -v "$RCLONE" >/dev/null || die "rclone not found"
[[ -d "$ROOT" ]] || die "data root $ROOT missing"

# One run at a time.
exec 9>"$ROOT/.upload.lock"
flock -n 9 || { log "another run is active; exiting"; exit 0; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

remote_free_gb() {   # -1 if the remote can't report it
  "$RCLONE" about "$REMOTE" --json 2>/dev/null | python3 -I -c 'import json,sys; d=json.load(sys.stdin); print(int(d.get("free", -1))//10**9 if "free" in d else -1)' 2>/dev/null || echo -1
}

now=$(date -u +%s)
uploaded=0 pruned=0 segpruned=0 waiting=0 gapsfailed=0

# Copy <day>/gaps.csv to $REMOTE/gaps/<day>_gaps.csv if it changed since the last
# verified copy (size and mtime in <day>/.gaps_uploaded). A snapshot is sent, so
# rows the recorder appends meanwhile can't break the MD5 check; they change the
# mtime and go up next run.
upload_gaps() {   # dir day
  local src="$1/gaps.csv" state="$1/.gaps_uploaded" dst="$REMOTE/gaps/$2_gaps.csv" now_state local_md5 remote_md5
  [[ -f "$src" ]] || return 0
  now_state=$(stat -c '%s %Y' "$src")
  [[ "$(cat "$state" 2>/dev/null)" == "$now_state" ]] && return 0
  cp -- "$src" "$WORK/gaps.csv"
  local_md5=$(md5sum < "$WORK/gaps.csv" | cut -d' ' -f1)
  # shellcheck disable=SC2086  # RCLONE_FLAGS is a list of flags
  if "$RCLONE" copyto $RCLONE_FLAGS "$WORK/gaps.csv" "$dst" &&
     remote_md5=$("$RCLONE" md5sum "$dst" 2>/dev/null | awk '{print $1}') &&
     [[ "$local_md5" == "$remote_md5" ]]; then
    echo "$now_state" > "$state.tmp" && mv -f "$state.tmp" "$state"
  else
    log "$2: could not copy gaps.csv to $dst; will retry"
    gapsfailed=$((gapsfailed + 1))
  fi
}

for dir in "$ROOT"/[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]; do
  [[ -d "$dir" ]] || continue
  day="$(basename "$dir")"
  day_end=$(date -u -d "$day + 1 day" +%s) || { log "skip $day: bad name"; continue; }

  closed=0
  (( now >= day_end + GRACE_MIN * 60 )) && closed=1
  if (( closed )) && compgen -G "$dir/*.part" >/dev/null; then
    log "$day: closed but a .part file is open (late segment?); uploading what is finished"
    waiting=$((waiting + 1))
  fi

  manifest="$dir/.uploaded"
  touch "$manifest"
  verified() {   # name size mtime -> true if uploaded and verified unchanged
    awk -v n="$1" -v s="$2" -v m="$3" '$1==n && $2==s && $3==m {found=1} END {exit !found}' "$manifest"
  }

  upload_gaps "$dir" "$day"

  # 1. Free disk: segments verified in an earlier run (the manifest as it is now)
  #    and older than KEEP_LOCAL_HOURS. A marker keeps the segment number taken.
  if (( KEEP_LOCAL_HOURS >= 0 )); then
    while IFS= read -r -d '' f; do
      name="${f##*/}"
      read -r size mtime < <(stat -c '%s %Y' "$f")
      if (( now - mtime >= KEEP_LOCAL_HOURS * 3600 )) && verified "$name" "$size" "$mtime"; then
        : > "$f.pruned" && rm -f -- "$f"
        segpruned=$((segpruned + 1))
      fi
    done < <(find "$dir" -maxdepth 1 -type f -name '*.bin.zst' -print0)
  fi

  # 2. What to upload: finished segments always; everything else once closed.
  : > "$WORK/todo"
  if (( closed )); then
    filter=(! -name '.*' ! -name '*.tmp' ! -name '*.part' ! -name '*.pruned')
  else
    filter=(-name '*.bin.zst')
  fi
  while IFS= read -r -d '' f; do
    name="${f##*/}"
    read -r size mtime < <(stat -c '%s %Y' "$f")
    verified "$name" "$size" "$mtime" || echo "$name $size $mtime" >> "$WORK/todo"
  done < <(find "$dir" -maxdepth 1 -type f "${filter[@]}" -print0 | sort -z)

  if [[ -s "$WORK/todo" ]]; then
    # Next part number after the highest one already used for this day; an
    # existing name on the remote is skipped, never overwritten.
    part=$(awk '{n = $5; sub(/.*_/, "", n); sub(/\.tar$/, "", n); n += 0; if (n >= max) max = n + 1} END {print max + 0}' "$manifest")
    tarname=$(printf '%s_%03d.tar' "$day" "$part")
    while [[ -n "$("$RCLONE" lsf --files-only --include "/$tarname" "$REMOTE" 2>/dev/null)" ]]; do
      part=$((part + 1)); tarname=$(printf '%s_%03d.tar' "$day" "$part")
    done
    sumsname=$(printf 'SHA256SUMS_%03d' "$part")
    nfiles=$(wc -l < "$WORK/todo")
    bytes=$(awk '{s += $2} END {print s + 0}' "$WORK/todo")

    REMOTE_FREE_GB=$(remote_free_gb)
    if (( REMOTE_FREE_GB >= 0 && REMOTE_FREE_GB * 10**9 - bytes < MIN_REMOTE_FREE_GB * 10**9 )); then
      die "$REMOTE has ${REMOTE_FREE_GB} GB free; $tarname needs $((bytes / 10**9 + 1)) GB + ${MIN_REMOTE_FREE_GB} GB reserve. Move data off it (acrec_pull.sh PULL_MODE=move) or free space."
    fi

    # Checksums of exactly the files going into this tar.
    sums="$WORK/$day"
    mkdir -p "$sums"
    (cd "$dir" && awk '{print $1}' "$WORK/todo" | xargs -d '\n' sha256sum) > "$sums/$sumsname"

    log "$day: uploading $tarname ($nfiles files, $((bytes / 1000000)) MB)"
    awk -v d="$day" '{print d "/" $1}' "$WORK/todo" > "$WORK/members"
    # MD5 of exactly the bytes sent, computed by a background reader of a FIFO
    # that tee feeds, and waited for explicitly before it is compared.
    rm -f "$WORK/md5pipe" "$WORK/md5"
    mkfifo "$WORK/md5pipe"
    md5sum < "$WORK/md5pipe" | cut -d' ' -f1 > "$WORK/md5" &
    md5_pid=$!
    set +e
    # One archive: the day's files (relative to ROOT), then SHA256SUMS (relative to WORK).
    # shellcheck disable=SC2086  # RCLONE_FLAGS is a list of flags
    tar -cf - -C "$ROOT" --files-from="$WORK/members" -C "$WORK" "$day/$sumsname" |
      tee "$WORK/md5pipe" |
      "$RCLONE" rcat $RCLONE_FLAGS "$REMOTE/$tarname"
    rc=("${PIPESTATUS[@]}")
    wait "$md5_pid"
    set -e
    if ! [[ "${rc[*]}" =~ ^0( 0)*$ ]]; then
      # rclone may have stored a partial tar: remove it so it doesn't linger on the remote.
      "$RCLONE" deletefile "$REMOTE/$tarname" >/dev/null 2>&1 || true
      die "$day: upload pipeline failed (exit codes ${rc[*]}: tar, tee, rclone); will retry"
    fi

    local_md5=$(cat "$WORK/md5")
    remote_md5=$("$RCLONE" md5sum "$REMOTE/$tarname" 2>/dev/null | awk '{print $1}')
    if ! [[ -n "$local_md5" && "$local_md5" == "$remote_md5" ]]; then
      "$RCLONE" deletefile "$REMOTE/$tarname" >/dev/null 2>&1 || true
      die "$day: MD5 mismatch for $tarname (local $local_md5, remote ${remote_md5:-none}); removed it, will retry"
    fi

    # Record each verified file (atomically).
    cp "$manifest" "$manifest.tmp"
    while read -r name size mtime; do
      sha=$(awk -v n="$name" '$2==n {print $1}' "$sums/$sumsname")
      echo "$name $size $mtime $sha $tarname" >> "$manifest.tmp"
    done < "$WORK/todo"
    mv -f "$manifest.tmp" "$manifest"
    LAST_UPLOAD="$tarname"
    uploaded=$((uploaded + 1))
    log "$day: $tarname verified on $REMOTE (md5 $local_md5)"
    continue   # never delete a day in the same run as an upload
  fi

  # 3. Closed, everything verified, old enough: delete the whole day folder.
  if (( closed )) && ! compgen -G "$dir/*.part" >/dev/null && (( now >= day_end + KEEP_DAYS * 86400 )); then
    log "$day: all files verified on $REMOTE; deleting local copy (KEEP_DAYS=$KEEP_DAYS)"
    rm -rf -- "$dir"
    pruned=$((pruned + 1))
  fi
done

REMOTE_FREE_GB=$(remote_free_gb)
log "done: $uploaded uploaded, $segpruned segments and $pruned days deleted locally, $waiting waiting, $gapsfailed gaps.csv copies failed; remote free ${REMOTE_FREE_GB} GB"
if (( gapsfailed )); then
  write_status "warn" "$uploaded uploaded, but $gapsfailed gaps.csv copies to $REMOTE/gaps failed (will retry)"
  mail_note "UPLOAD WARNING: $gapsfailed gaps.csv copies to $REMOTE/gaps failed; data tars are fine; will retry"
else
  write_status "ok" "$uploaded uploaded, $segpruned segments + $pruned days deleted locally, $waiting waiting"
fi
