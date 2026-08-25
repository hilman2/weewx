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

#### retain_days

How long the packets are kept, in days. Default is `7`.

A station reporting every 60 seconds writes about 1,400 packets a day. One reporting
every 8 seconds writes about 10,800. Set this to what your hardware can be late by,
plus a margin, rather than to what your disk can hold.

#### catchup_days

How far back to look at startup for periods with packets but no archive record.
Default is `2`.

#### data_binding

The data binding used to store the packets. Default is `loop_binding`, which is
defined in [[DataBindings]](data-bindings.md). If there is no such binding, the store
goes into a database beside the archive, named after it.

#### source_field

Which field of a packet says where it came from, recorded alongside the packet.
Default is `station`. Nothing is decided by it; it is there for diagnosis when several
consoles report to one WeeWX.
