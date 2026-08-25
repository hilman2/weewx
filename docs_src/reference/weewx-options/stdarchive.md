# [StdArchive]

The `StdArchive` service stores data into a database.

Every LOOP packet goes into an ingest table first, and every archive record is worked
out from what that table holds for its period. A record is therefore what its packets
add up to, and not a separate thing kept in memory alongside them. Two things follow
from that. A reading that arrives after its period has ended still reaches the record
it belongs to, because the record is written again from the packets. And a restart in
the middle of a period does not lose that period, because the packets outlive the
process.

Where a record has to be written again, no `NEW_ARCHIVE_RECORD` event is raised for
it: the record has been through that once already, and raising it a second time would
send the same reading to Wunderground and the rest twice. Reports pick the corrected
value up on their next run. Uploads do not pick it up at all.

A console that keeps its own archive is the authority for its own periods, and its
records are left as they are. That is decided by what the console can actually do, not
by `record_generation`: a station may have a logger and still not offer it over the
protocol it uploads with, in which case WeeWX falls back to software generation and
the packets have the last word after all.

The packets are kept for `retain_days` in the table, then for `archive_days` as one
gzipped NDJSON file per day, which is about a thirtieth of the size. A record can be
worked out again for as long as the whole of its period is still held in one place or
the other. Beyond that the period is left alone and a warning says so, because a
record built from what survived the trimming would be worse than the one already
there.

#### ==archive_interval==

If your station hardware supports data logging then the archive interval will
be downloaded from the station. Otherwise, you must specify it here in
seconds, and it must be evenly divisible by 60. Optional. Default is `300`.

#### archive_delay

How long to wait in seconds after the top of an archiving interval before
fetching new data off the station. For example, if your archive interval is
5 minutes and archive_delay is set to 15, then the data will be fetched at
00:00:15, 00:05:15, 00:10:15, etc. This delay is to give the station a few
seconds to archive the data internally, and in case your server has any other
tasks to do at the top of the minute. Default is `15`.

#### record_generation

Set to whether records should be downloaded off the hardware (recommended),
or generated in software. If set to `hardware`, then WeeWX tries to download
archive records from your station. However, not all types of stations support
this, in which case WeeWX falls back to software generation. A setting of
`hardware` will work for most users. A notable exception is [users who have
cobbled together homebrew serial interfaces](https://www.wxforum.net/index.php?topic=10315.0)
for the Vantage stations that do not include memory for a logger. These users
should set this option to `software`, forcing software record generation.
Default is `hardware`.

#### record_augmentation

When performing hardware record generation, this option will attempt to
augment the record with any additional observation types that it can extract
out of the LOOP packets. Default is `true`.

#### no_catchup

Many weather stations have internal memory that can continue to record weather
data even when WeeWX is not running. Normally, when WeeWX starts up, it will
download this data and archive it. However, if you set this option to `true`,
then WeeWX will not attempt to catch up. Default is `false`.

#### loop_hilo

Set to `true` to have LOOP data and archive data to be used for high / low
statistics. Set to `false` to have only archive data used. If your sensor
emits lots of spiky data, setting to `false` may help. Default is `true`.

#### log_success

If you set a value for `log_success` here, it will override the value set at
the [top-level](general.md#log_success)  and will apply only to archiving
operations.

#### log_failure

If you set a value for `log_failure` here, it will override the value set at
the [top-level](general.md#log_failure)  and will apply only to archiving
operations.

#### data_binding

The data binding to be used to store the data. This should match one of the
bindings in the [`[DataBindings]`](data-bindings.md) section. Optional.
Default is `wx_binding`.

#### retain_days

How long LOOP packets stay in the ingest table, in days. Optional. Default is `7`.

A station reporting every 60 seconds writes about 1,400 packets a day, one reporting
every 2 seconds about 43,000. At roughly 700 bytes each, a two-second station uses
about 30 MB a day. Lower this where space is short.

#### archive_days

How long the day files are kept after a day leaves the table, in days. Optional.
Default is `365`. Set to `0` to keep them for ever.

#### archive_dir

Where the day files go. Optional. Default is a directory called `packets` beside the
database. Only needed when the ingest table is not SQLite, where there is no obvious
place and nothing is written out until this is given.

#### ingest_binding

The data binding used for the ingest table. This should match one of the bindings in
the [`[DataBindings]`](data-bindings.md) section. Optional. Default is `loop_binding`.
If there is no such binding, the table goes into a database beside the archive, named
after it.

#### source_field

Which field of a LOOP packet says where it came from, recorded alongside the packet.
Optional. Default is `station`. Nothing is decided by it; it is there for diagnosis
when several sources report to one WeeWX.
