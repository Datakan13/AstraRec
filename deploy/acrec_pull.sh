#!/usr/bin/env bash
# acrec_pull.sh: run on your PC (Linux or WSL). Moves the recorder's day tars
# from Google Drive to the PC, unpacks them and checks every file.
#
#   acrec_pull.sh DEST [REMOTE]        e.g. acrec_pull.sh /mnt/d/acrec gdrive:acrec
#
#   DEST/incoming/   tars downloaded from Drive
#   DEST/data/       unpacked day folders (same layout as on the server)
#
# rclone move deletes a tar from Drive only after the download's MD5 matches, so
# Drive space is freed only once the PC has a verified copy. A tar is deleted from
# incoming/ only after every file in it matched SHA256SUMS.
# Prints names and counts only, never data.
set -euo pipefail

DEST="${1:?usage: acrec_pull.sh DEST [REMOTE]}"
REMOTE="${2:-gdrive:acrec}"
RCLONE="${RCLONE:-rclone}"
mkdir -p "$DEST/incoming" "$DEST/data"

log() { echo "$(date -u +%FT%TZ) acrec_pull: $*"; }

log "moving tars from $REMOTE"
"$RCLONE" move "$REMOTE" "$DEST/incoming" --include '*.tar' --retries 5 --stats-one-line --stats 30s

bad=0 ok=0
# Name order: <day>_000.tar before <day>_001.tar, so later parts overwrite changed files.
while IFS= read -r -d '' tarfile; do
  name="$(basename "$tarfile" .tar)"          # 2026-11-01_000
  day="${name%_*}" part="${name##*_}"
  tar -xf "$tarfile" -C "$DEST/data"
  if (cd "$DEST/data/$day" && sha256sum --quiet -c "SHA256SUMS_$part"); then
    rm -f -- "$tarfile"
    ok=$((ok + 1))
    log "$name: unpacked and verified"
  else
    bad=$((bad + 1))
    log "ERROR $name: checksum mismatch; data/$day may hold a damaged file. Tar kept in incoming/. Get a good copy from the server (kept KEEP_DAYS days) or Drive's trash."
  fi
done < <(find "$DEST/incoming" -maxdepth 1 -name '*.tar' -print0 | sort -z)

log "done: $ok verified, $bad failed"
(( bad == 0 ))
