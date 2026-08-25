#!/bin/bash
# Beta auf die Ingest-Architektur: der eigene Service faellt weg, StdArchive
# macht es selbst. Bleibt er in archive_services stehen, findet WeeWX ihn nicht
# mehr und startet nicht.
set -e
cd /opt/weewx/src
git fetch https -q
git merge --no-edit https/ingest-table 2>&1 | tail -3

python3 - <<'PY'
import sys
sys.path.insert(0, "/opt/weewx/src/src")
import weecfg
path = "/opt/weewx/data/weewx.conf"
_, config = weecfg.read_config(path)
services = config["Engine"]["Services"]["archive_services"]
if not isinstance(services, list):
    services = [services]
before = list(services)
services = [s for s in services if "StdLoopStore" not in s]
config["Engine"]["Services"]["archive_services"] = services
# Die Optionen gehoeren jetzt zu StdArchive.
store = config.pop("StdLoopStore", {})
config["StdArchive"]["retain_days"] = store.get("retain_days", "7")
config["StdArchive"]["archive_days"] = store.get("archive_days", "365")
config["StdArchive"]["ingest_binding"] = store.get("data_binding", "loop_binding")
weecfg.save(config, path)
print("archive_services: %s -> %s" % (before, services))
print("StdArchive: retain_days=%s archive_days=%s ingest_binding=%s" % (
    config["StdArchive"]["retain_days"], config["StdArchive"]["archive_days"],
    config["StdArchive"]["ingest_binding"]))
PY
cd /opt/weewx && docker compose restart >/dev/null 2>&1
sleep 30
echo "--- Start ---"
docker logs --tail 60 weewx 2>&1 | grep -iE "ingest|loopstore|archive interval|error|critical|traceback" | head -8
