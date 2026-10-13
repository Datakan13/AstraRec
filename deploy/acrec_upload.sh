#!/usr/bin/env bash
# acrec_upload.sh: ship closed recorder days to Google Drive (rclone), then
# prune old local days. Run hourly by acrec-upload.timer; safe to rerun at any time.
#
#   acrec_upload.sh [/opt/acrec/upload.env]
#
# For each <root>/YYYY-MM-DD that is closed (past midnight + GRACE_MIN, no .part):
#   1. files not yet uploaded (new, or changed size/mtime) are streamed as ONE tar
#      straight into rclone: no local temp copy. The tar holds the files plus
#      SHA256SUMS for checking after extraction. Names: <day>_00.tar, then
#      <day>_01.tar for anything that changed later (e.g. a late straggler segment).
#   2. the tar's MD5 as streamed must equal the MD5 Drive reports, or nothing is
#      recorded and the next run retries.
#   3. verified files go into <day>/.uploaded (size, mtime, sha256, tar name).
# A day whose files are all verified and that is older than KEEP_DAYS is deleted.
#
# The data is a sealed holdout: this script never reads or prints contents,
# only names, sizes and counts. The .bin.zst files are already zstd-compressed,
# so the tar is not compressed again.
set -euo pipefail

ENV_FILE="${1:-/opt/acrec/upload.env}"
# shellcheck source=/dev/null
[[ -f "$ENV_FILE" ]] && source "$ENV_FILE"
: "${ROOT:=/data/acrec}"            # recorder data root (recorder.json "root")
: "${REMOTE:=gdrive:acrec}"         # rclone remote:path
: "${KEEP_DAYS:=7}"                 # keep verified days locally this long
: "${GRACE_MIN:=15}"                # wait this long after midnight UTC (recorder closes at +2 min)
: "${MIN_DRIVE_FREE_GB:=5}"         # refuse to upload when Drive would end below this
: "${RCLONE:=rclone}"
: "${RCLONE_FLAGS:=--retries 5 --low-level-retries 20 --stats 0}"

log() { echo "$(date -u +%FT%TZ) acrec_upload: $*"; }
die() { log "ERROR: $*"; write_status "error" "$*"; exit 1; }

STATUS="$ROOT/upload_status.json"
LAST_UPLOAD=""
write_status() {   # state, message: for monitoring next to heartbeat.json
  local tmp="$STATUS.tmp"
  printf '{"utc": "%s", "state": "%s", "message": "%s", "last_upload": "%s", "drive_free_gb": %s}\n' \
    "$(date -u +%FT%TZ)" "$1" "${2//\"/\'}" "$LAST_UPLOAD" "${DRIVE_FREE_GB:-null}" > "$tmp" && mv -f "$tmp" "$STATUS"
}

command -v "$RCLONE" >/dev/null || die "rclone not found"
[[ -d "$ROOT" ]] || die "data root $ROOT missing"

# One run at a time.
exec 9>"$ROOT/.upload.lock"
flock -n 9 || { log "another run is active; exiting"; exit 0; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

drive_free_gb() {
  "$RCLONE" about "$REMOTE" --json 2>/dev/null | python3 -I -c 'import json,sys; d=json.load(sys.stdin); print(int(d.get("free", -1))//10**9 if "free" in d else -1)' 2>/dev/null || echo -1
}

now=$(date -u +%s)
uploaded=0 pruned=0 waiting=0

for dir in "$ROOT"/[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]; do
  [[ -d "$dir" ]] || continue
  day="$(basename "$dir")"
  day_end=$(date -u -d "$day + 1 day" +%s) || { log "skip $day: bad name"; continue; }

  # Not closed yet: still today (or just after midnight), or a segment is open.
  if (( now < day_end + GRACE_MIN * 60 )); then continue; fi
  if compgen -G "$dir/*.part" >/dev/null; then
    log "$day: .part file present (recorder still writing a late segment?); retry later"
    waiting=$((waiting + 1)); continue
  fi

  manifest="$dir/.uploaded"
  touch "$manifest"

  # Files that are new or changed since their last verified upload.
  : > "$WORK/todo"
  while IFS= read -r -d '' f; do
    name="${f##*/}"
    read -r size mtime < <(stat -c '%s %Y' "$f")
    if ! awk -v n="$name" -v s="$size" -v m="$mtime" '$1==n && $2==s && $3==m {found=1} END {exit !found}' "$manifest"; then
      echo "$name $size $mtime" >> "$WORK/todo"
    fi
  done < <(find "$dir" -maxdepth 1 -type f ! -name '.*' ! -name '*.tmp' -print0 | sort -z)

  if [[ -s "$WORK/todo" ]]; then
    part=$(awk '{print $5}' "$manifest" | sort -u | wc -l)
    tarname=$(printf '%s_%02d.tar' "$day" "$part")
    nfiles=$(wc -l < "$WORK/todo")
    bytes=$(awk '{s += $2} END {print s + 0}' "$WORK/todo")

    DRIVE_FREE_GB=$(drive_free_gb)
    if (( DRIVE_FREE_GB >= 0 && DRIVE_FREE_GB * 10**9 - bytes < MIN_DRIVE_FREE_GB * 10**9 )); then
      die "Drive has ${DRIVE_FREE_GB} GB free; $tarname needs $((bytes / 10**9 + 1)) GB + ${MIN_DRIVE_FREE_GB} GB reserve. Pull data to the PC (acrec_pull.sh) or free space."
    fi

    # Checksums of exactly the files going into this tar.
    sums="$WORK/$day"
    mkdir -p "$sums"
    (cd "$dir" && awk '{print $1}' "$WORK/todo" | xargs -d '\n' sha256sum) > "$sums/SHA256SUMS_$(printf '%02d' "$part")"

    log "$day: uploading $tarname ($nfiles files, $((bytes / 1000000)) MB)"
    # Stream: tar -> tee (MD5 of exactly what is sent) -> rclone. No local copy.
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
    tar -cf - -C "$ROOT" --files-from="$WORK/members" -C "$WORK" "$day/SHA256SUMS_$(printf '%02d' "$part")" |
      tee "$WORK/md5pipe" |
      "$RCLONE" rcat $RCLONE_FLAGS "$REMOTE/$tarname"
    rc=("${PIPESTATUS[@]}")
    wait "$md5_pid"
    set -e
    [[ "${rc[*]}" =~ ^0( 0)*$ ]] || die "$day: upload pipeline failed (exit codes ${rc[*]}); will retry"

    local_md5=$(cat "$WORK/md5")
    remote_md5=$("$RCLONE" md5sum "$REMOTE/$tarname" 2>/dev/null | awk '{print $1}')
    [[ -n "$local_md5" && "$local_md5" == "$remote_md5" ]] || die "$day: MD5 mismatch for $tarname (local $local_md5, Drive ${remote_md5:-none}); will retry"

    # Record each verified file (atomically).
    cp "$manifest" "$manifest.tmp"
    while read -r name size mtime; do
      sha=$(awk -v n="$name" '$2==n {print $1}' "$sums/SHA256SUMS_$(printf '%02d' "$part")")
      echo "$name $size $mtime $sha $tarname" >> "$manifest.tmp"
    done < "$WORK/todo"
    mv -f "$manifest.tmp" "$manifest"
    LAST_UPLOAD="$tarname"
    uploaded=$((uploaded + 1))
    log "$day: $tarname verified on Drive (md5 $local_md5)"
    continue   # never prune in the same run as an upload
  fi

  # Everything verified: prune once old enough.
  if (( now > day_end + KEEP_DAYS * 86400 )); then
    log "$day: all files verified on Drive and older than $KEEP_DAYS days; deleting local copy"
    rm -rf -- "$dir"
    pruned=$((pruned + 1))
  fi
done

DRIVE_FREE_GB=$(drive_free_gb)
log "done: $uploaded uploaded, $pruned pruned, $waiting waiting; Drive free ${DRIVE_FREE_GB} GB"
write_status "ok" "$uploaded uploaded, $pruned pruned, $waiting waiting"
