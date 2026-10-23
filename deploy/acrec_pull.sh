#!/usr/bin/env bash
# acrec_pull.sh: run on your PC (Linux or WSL). Fetches the recorder's tars from
# the Hetzner Storage Box (any rclone remote works), unpacks them and checks
# every file.
#
#   acrec_pull.sh DEST [REMOTE]        e.g. acrec_pull.sh /mnt/d/acrec storagebox:acrec
#   PULL_MODE=move acrec_pull.sh ...   also delete them from the box (see below)
#
#   DEST/incoming/   tars being fetched
#   DEST/data/       unpacked day folders (same layout as on the server)
#   DEST/.pulled     tars already fetched, unpacked and verified (copy mode skips them)
#   DEST/gaps/       <day>_gaps.csv, refreshed every run (the box's gaps/ folder;
#                    never deleted from the box, it's small)
#
# PULL_MODE=copy (default): the box keeps every tar as the archive; each tar is
#   downloaded once.
# PULL_MODE=move: additionally, tars that are unpacked and verified on the PC are
#   deleted from the box, to free space there. A tar is never deleted from the box
#   before every file in it matched SHA256SUMS on the PC.
# A tar is deleted from incoming/ only after every file in it matched SHA256SUMS.
# Prints names and counts only, never data.
set -euo pipefail

DEST="${1:?usage: acrec_pull.sh DEST [REMOTE]}"
REMOTE="${2:-storagebox:acrec}"
RCLONE="${RCLONE:-rclone}"
PULL_MODE="${PULL_MODE:-copy}"
[[ "$PULL_MODE" == copy || "$PULL_MODE" == move ]] || { echo "PULL_MODE must be copy or move" >&2; exit 2; }
# The Storage Box allows 10 simultaneous connections per account; the server's
# uploader may be using a few.
FLAGS=(--retries 5 --checkers 2 --transfers 2 --stats-one-line --stats 30s)
mkdir -p "$DEST/incoming" "$DEST/data"
touch "$DEST/.pulled"

log() { echo "$(date -u +%FT%TZ) acrec_pull: $*"; }

# One ordered filter list (rclone applies the first match): skip tars already
# pulled, take every other tar at the top level, ignore everything else.
FILTER="$(mktemp)"
trap 'rm -f "$FILTER"' EXIT
sed 's#^#- /#' "$DEST/.pulled" > "$FILTER"
printf '+ /*.tar\n- *\n' >> "$FILTER"

log "fetching new tars from $REMOTE"
"$RCLONE" copy "$REMOTE" "$DEST/incoming" "${FLAGS[@]}" --filter-from "$FILTER"

if "$RCLONE" lsf --dirs-only "$REMOTE" | grep -qx 'gaps/'; then   # absent until the first upload
  log "fetching gaps/ from $REMOTE"
  "$RCLONE" copy "$REMOTE/gaps" "$DEST/gaps" "${FLAGS[@]}" --checksum
fi

bad=0 ok=0
# Name order: <day>_000.tar before <day>_001.tar, so later parts overwrite changed files.
while IFS= read -r -d '' tarfile; do
  name="$(basename "$tarfile" .tar)"          # 2026-11-01_000
  day="${name%_*}" part="${name##*_}"
  tar -xf "$tarfile" -C "$DEST/data"
  if (cd "$DEST/data/$day" && sha256sum --quiet -c "SHA256SUMS_$part"); then
    rm -f -- "$tarfile"
    echo "$name.tar" >> "$DEST/.pulled"
    ok=$((ok + 1))
    log "$name: unpacked and verified"
  else
    bad=$((bad + 1))
    log "ERROR $name: checksum mismatch; data/$day may hold a damaged file. Tar kept in incoming/; delete it to fetch it again (copy mode)."
  fi
done < <(find "$DEST/incoming" -maxdepth 1 -name '*.tar' -print0 | sort -z)

if [[ "$PULL_MODE" == move && -s "$DEST/.pulled" ]]; then
  # Only names in .pulled: unpacked and verified here. Names on the box are never
  # reused, so a listed name is exactly the file that was verified.
  log "deleting verified tars from $REMOTE"
  "$RCLONE" delete "$REMOTE" --files-from-raw "$DEST/.pulled" --retries 5 --checkers 2
fi

log "done: $ok verified, $bad failed"
(( bad == 0 ))
