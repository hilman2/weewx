# [StdLoopStore]

The `StdLoopStore` service keeps LOOP packets in a database of its own for a few
days, so that an archive record can be worked out again from them.

WeeWX turns LOOP packets into an archive record once, and then the packets are gone.
The record is not a view of anything; it is the only copy. That is why a reading which
arrives after its archive period has ended cannot reach the record it belongs to, and
why a restart in the middle of a period loses that period.

An archive record is a function of the LOOP packets of its period. With the packets
kept, `StdLoopStore` works each period out again after `StdArchive` has had its turn
and compares the result with the database. Where the two differ, the packets win: they
are the measurements, the record is a summary of them.

* A period that has packets but no archive record gets one.
* A record that does not agree with its packets is worked out again.

That covers a second console whose readings arrive a minute late, a sensor relayed
through a service, and a restart between two archive records. Rain is why the record
has to be worked out again rather than merely filled in: rainfall is a sum, and a late
tip of the bucket cannot be added to a total that is already written.

When a record changes, the daily summaries for that day are built again the way a live
run builds them: the archive records give the sums and counts, and then the LOOP
packets of each period lay their own highs and lows over the top. Those extremes are
finer than any finished record can show, and rebuilding from the records alone would
quietly coarsen them.

#### enable

Set to `False` to keep no packets at all. Default is `True`.

## Where the packets are kept

The packets are held in two places, one after the other.

While they are recent they live in a database table, indexed by time. When a day is
older than `retain_days` it is written out as a single gzipped NDJSON file, one JSON
object per line, and dropped from the table. Both are read when a record is worked out
again, so it makes no difference to the result which of the two a packet is in.

Measured on real LOOP packets, the file is about a thirtieth of what the same packets
take up in the database: 27 bytes a packet against 911. A station reporting every eight
seconds therefore writes about 10 MB a day into the table, and keeps a year of history
in roughly 110 MB of files.

The files sit in a directory called `packets` beside the database, and are ordinary
gzipped text:

```
zcat /var/lib/weewx/packets/2026-08-25.ndjson.gz | head -1
{"dateTime":1787664177,"usUnits":1,"outTemp":62.6,"windSpeed":3.4, ...}
```

A period can be worked out again as long as the whole of it is still held, in either
place. Beyond that the periods are left alone and a warning says so: building a record
from whatever survived the trimming would replace a hundred readings with a handful,
which is worse than leaving it be. For data older than that, use
`weectl import --update`.

#### retain_days

How long the packets stay in the database, in days. Default is `7`.

Set this to what your hardware can be late by, plus a margin. A station reporting every
60 seconds writes about 1,400 packets a day, one reporting every 8 seconds about
10,800.

#### archive_days

How long the day files are kept, in days. Default is `365`. Set to `0` to keep them
for ever.

#### archive_dir

Where the day files go. Default is a directory called `packets` beside the database.
Leave unset unless the database is not SQLite, in which case there is no obvious place
and nothing is written out until this is given.

#### data_binding

The data binding used to store the packets. Default is `loop_binding`, which is
defined in [[DataBindings]](data-bindings.md). If there is no such binding, the store
goes into a database beside the archive, named after it.

#### source_field

Which field of a packet says where it came from, recorded alongside the packet.
Default is `station`. Nothing is decided by it; it is there for diagnosis when several
consoles report to one WeeWX.
