# FanIn multiple-failure note

When multiple FanIn predecessors fail concurrently, each will try to submit an error callback task with the same pre-assigned ULID. The first writer wins; subsequent submissions overwrite or conflict depending on the adapter. This is acceptable for the initial implementation — in practice most DAGs have at most one failing predecessor per fan-in. This can be addressed later with a Redis-set guard similar to the fan-in tracking set.
