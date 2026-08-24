#!/bin/bash
#
# Check a WeeWX installation made from a package.
#
# Run this inside a container or a VM that has just had the package installed.
# It prints one line per check and exits non-zero if any of them failed.
#
# Optional: put an extension tarball at /tmp/pmon.tgz and the extension checks
# will run too. src/weecfg/tests/pmon.tgz in this repository will do.
#
pass=0
fail=0
skip=0

ok()  { echo "  ok    $1"; pass=$((pass+1)); }
bad() { echo "  FAIL  $1"; fail=$((fail+1)); }
na()  { echo "  skip  $1 ($2)"; skip=$((skip+1)); }

CFG=/etc/weewx/weewx.conf
HTML=/var/www/html/weewx
DB=/var/lib/weewx/weewx.sdb
EXT=/tmp/pmon.tgz

# How the config file is protected, as "mode owner:group".
config_protection() { stat -c '%a %U:%G' "$CFG"; }

echo "== package =="
dpkg -l weewx >/dev/null 2>&1 && ok "package is installed" || bad "package is installed"
[ -f "$CFG" ] && ok "$CFG exists" || bad "$CFG exists"
for c in weewxd weectl; do
    command -v $c >/dev/null && ok "$c on PATH" || bad "$c on PATH"
done

echo "== user and permissions =="
id weewx >/dev/null 2>&1 && ok "user weewx exists" || bad "user weewx exists"
# The point of the weewx user is that it can edit its own configuration, and the
# point of the group-only mode is that nobody else can read the passwords in it.
su -s /bin/bash weewx -c "test -w $CFG" \
    && ok "weewx user can write $CFG" || bad "weewx user can write $CFG"
mode=$(stat -c '%a' "$CFG")
[ "${mode: -1}" = "0" ] \
    && ok "$CFG is not world readable ($mode)" \
    || bad "$CFG is world readable ($mode), and it holds passwords"
for d in /etc/weewx /var/lib/weewx "$HTML"; do
    if [ -d "$d" ]; then
        owner=$(stat -c '%U' "$d")
        [ "$owner" = "weewx" ] && ok "$d owned by weewx" \
                               || bad "$d owned by $owner, expected weewx"
    else
        bad "$d exists"
    fi
done

echo "== operating system integration =="
udev=$(ls /usr/lib/udev/rules.d/60-weewx.rules /lib/udev/rules.d/60-weewx.rules 2>/dev/null | head -1)
[ -n "$udev" ] && ok "udev rules installed" || bad "udev rules installed"
unit=$(ls /usr/lib/systemd/system/weewx.service /lib/systemd/system/weewx.service 2>/dev/null | head -1)
if [ -n "$unit" ]; then
    ok "systemd unit installed"
    if [ -d /run/systemd/system ]; then
        systemctl start weewx && sleep 5 && systemctl is-active --quiet weewx \
            && ok "weewx starts" || bad "weewx starts"
        systemctl stop weewx && sleep 2 && ! systemctl is-active --quiet weewx \
            && ok "weewx stops" || bad "weewx stops"
    else
        na "start and stop weewx" "systemd is not pid 1"
    fi
else
    if [ -d /run/systemd/system ]; then
        bad "systemd unit installed"
    else
        na "systemd unit installed" "postinst only installs it under systemd"
    fi
fi

echo "== the simulator produces data =="
# A short archive interval, so this finishes in seconds rather than minutes.
sed -i 's/^\( *archive_interval *=\).*/\1 10/' "$CFG"
rm -f "$DB"
timeout 40 weewxd --config="$CFG" >/tmp/weewxd.log 2>&1
[ -f "$DB" ] && ok "database created" || bad "database created"
records=$(python3 -c "import sqlite3
try:
    print(sqlite3.connect('$DB').execute('select count(*) from archive').fetchone()[0])
except Exception:
    print(0)")
[ "$records" -gt 0 ] && ok "archive records written ($records)" \
                     || bad "archive records written"
html=$(find "$HTML" -name '*.html' 2>/dev/null | wc -l)
[ "$html" -gt 0 ] && ok "reports generated ($html html files)" || bad "reports generated"
png=$(find "$HTML" -name '*.png' 2>/dev/null | wc -l)
[ "$png" -gt 0 ] && ok "plots generated ($png images)" || bad "plots generated"
# Note: without a syslog daemon there is no /dev/log, and python's SysLogHandler
# prints a traceback for every message. Install rsyslog in the test machine, or
# these will look like WeeWX errors.
if grep -qiE 'CRITICAL|Traceback' /tmp/weewxd.log; then
    bad "no critical errors in the log"
    grep -iE 'CRITICAL|Traceback' /tmp/weewxd.log | head -3 | sed 's/^/        /'
else
    ok "no critical errors in the log"
fi

echo "== extensions =="
if [ -f "$EXT" ]; then
    before=$(config_protection)
    if weectl extension install "$EXT" --yes >/tmp/ext.log 2>&1; then
        ok "extension installs"
        grep -q 'ProcessMonitor' "$CFG" && ok "extension reached the config" \
                                        || bad "extension reached the config"
    else
        bad "extension installs"
        tail -5 /tmp/ext.log | sed 's/^/        /'
    fi
    if weectl extension uninstall pmon --yes >>/tmp/ext.log 2>&1; then
        ok "extension uninstalls"
        grep -q 'ProcessMonitor' "$CFG" && bad "config cleaned up after uninstall" \
                                        || ok "config cleaned up after uninstall"
    else
        bad "extension uninstalls"
        tail -3 /tmp/ext.log | sed 's/^/        /'
    fi
    # Installing an extension rewrites weewx.conf. It should come back the way it
    # was: writable by the weewx user, unreadable by everyone else.
    after=$(config_protection)
    [ "$before" = "$after" ] \
        && ok "config protection survives an extension cycle ($after)" \
        || bad "config protection changed: was $before, now $after"
else
    na "extension checks" "no $EXT supplied"
fi

echo
echo "passed $pass, failed $fail, skipped $skip"
[ "$fail" -eq 0 ]
