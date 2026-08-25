# [StdLoopStore]

The `StdLoopStore` service keeps LOOP packets in a database of its own for a few
days, so that an archive record can be worked out again from them.

WeeWX turns LOOP packets into an archive record once, and then the packets are gone.
The record is not a view of anything; it is the only copy. That is why a reading which
arrives after its archive period has ended cannot reach the record it belongs to, and
why a restart in the middle of a period loses that period.

With the packets kept, `StdLoopStore` compares them against the archive after
`StdArchive` has had its turn:

* A period that has packets but no archive record gets one.
* A record with empty fields the packets can fill has them filled.

Nothing is overwritten. A field that already holds a value keeps it, so a record can
gain data but never change it. That covers a second console whose readings arrive a
minute late, a sensor relayed through a service, and a restart between two archive
records. It does not cover a late packet that would change an average which is already
in the database.

Only the fields being filled in reach the daily summaries, so no sum is counted twice.

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
