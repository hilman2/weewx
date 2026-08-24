# System tests

A sketch for [#1050](https://github.com/weewx/weewx/issues/1050): install the
package on a clean machine and check that the result works.

`check-install.sh` is the checks themselves. It knows nothing about how the
machine was made, so it runs unchanged in a container, in a Vagrant guest, or on
a real box that has just had the package installed. It prints one line per check
and exits non-zero if any failed.

`run-docker.sh` is one way of getting a machine to run it on: it builds the
`.deb` from the current checkout, installs it in a clean `debian:12`, and runs
the checks. About two minutes, and it removes both containers afterwards.

```
./dev/system-tests/run-docker.sh            # debian:12
./dev/system-tests/run-docker.sh debian:13
```

## What is covered

From the list in the issue: the package installs, the `weewx` user exists and
can edit its own configuration, the directories have the right owner, the udev
rules are in place, the simulator runs and writes archive records, reports and
plots are generated, the log is clean, and an extension can be installed and
removed again.

## What is not

**Anything needing systemd.** `postinst` installs the unit only when pid 1 is
systemd, so in an ordinary container that step does not happen and starting and
stopping the service cannot be checked at all. Those checks report `skip` rather
than passing quietly. Covering them needs either a systemd container
(`--privileged`, cgroups mounted) or a real VM, which is where the existing
`vagrant/` guests would come in.

**Upgrades.** Part two of the issue's list, that an upgrade leaves `weewx.conf`
and modified skins alone and respects extensions already installed, needs an
older package to upgrade *from*. Nothing here does that yet.

**Anything not apt-based.** `check-install.sh` uses `dpkg -l` for its first
check; the rest is generic.

## A note on syslog

Install a syslog daemon in the test machine. Without one there is no `/dev/log`,
and python's `SysLogHandler` prints a traceback for every message logged, which
looks exactly like WeeWX failing. `run-docker.sh` installs `rsyslog` for this
reason.
