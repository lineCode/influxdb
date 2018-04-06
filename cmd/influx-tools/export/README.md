`influx-tools export`
=====================

Used with `influx-tools import`, the export tool transforms existing shards to a new shard duration in order to
consolidate into fewer shards. It is also possible to separate into a greater number of shards.


Field type conflicts
--------------------

A field type in a given measurement can be different per shard. This creates the potential for data type conflicts
when exporting new shard durations. If this happens, the data type will be determined by the first occurrence of data
for that field in the target shard duration. All conflicting data will be discarded when exporting the data, to ensure
it can be written by the `import` sub command. The `check-conflicts` sub command can be used to identify any conflicts.

**TODO(sgc)**: how will a user export conflicting data? `check-conflicts` seems like the correct tool using a command switch.


Usage
-----

### `--range <range>`

The range 