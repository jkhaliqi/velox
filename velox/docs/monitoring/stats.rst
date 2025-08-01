=============
Runtime Stats
=============

Runtime stats are used to collect the per-query velox runtime events for
offline query analysis purpose. The collected stats can provide insights into
the operator level query execution internals, such as how much time a query
operator spent in disk spilling. The collected stats are organized in a
free-form key-value for easy extension. The key is the event name and the
value is defined as RuntimeCounter which is used to store and aggregate a
particular event occurrences during the operator execution. RuntimeCounter has
three types: kNone used to record event count, kNanos used to record event time
in nanoseconds and kBytes used to record memory or storage size in bytes. It
records the count of events, and the min/max/sum of the event values. The stats
are stored in OperatorStats structure. The query system can aggregate the
operator level stats collected from each driver by pipeline and task for
analysis.

Memory Arbitration
------------------
These stats are reported by all operators.

.. list-table::
   :widths: 50 25 50
   :header-rows: 1

   * - Stats
     - Unit
     - Description
   * - memoryReclaimCount
     -
     - The number of times that the memory arbitration to reclaim memory from
       an spillable operator.
       This stats only applies for spillable operators.
   * - memoryReclaimWallNanos
     - nanos
     - The memory reclaim execution time of an operator during the memory
       arbitration. It collects time spent on disk spilling or file write.
       This stats only applies for spillable operators.
   * - reclaimedMemoryBytes
     - bytes
     - The reclaimed memory bytes of an operator during the memory arbitration.
       This stats only applies for spillable operators.
   * - globalArbitrationCount
     -
     - The number of times a request for more memory hit the arbitrator's
       capacity limit and initiated a global arbitration attempt where
       memory is reclaimed from viable candidates chosen among all running
       queries based on a criterion.
   * - localArbitrationCount
     -
     - The number of times a request for more memory hit the query memory
       limit and initiated a local arbitration attempt where memory is
       reclaimed from the requestor itself.
   * - localArbitrationQueueWallNanos
     -
     - The time of an operator waiting in local arbitration queue.
   * - localArbitrationLockWaitWallNanos
     -
     - The time of an operator waiting to acquire the local arbitration lock.
   * - globalArbitrationLockWaitWallNanos
     -
     - The time of an operator waiting to acquire the global arbitration lock.

HashBuild, HashAggregation
--------------------------
These stats are reported only by HashBuild and HashAggregation operators.

.. list-table::
   :widths: 50 25 50
   :header-rows: 1

   * - Stats
     - Unit
     - Description
   * - hashtable.capacity
     -
     - Number of slots across all buckets in the hash table.
   * - hashtable.numRehashes
     -
     - Number of rehash() calls.
   * - hashtable.numDistinct
     -
     - Number of distinct keys in the hash table.
   * - hashtable.numTombstones
     -
     - Number of tombstone slots in the hash table.
   * - hashtable.buildWallNanos
     - nanos
     - Time spent on building the hash table from rows collected by all the
       hash build operators. This stat is only reported by the HashBuild operator.

TableScan
---------
These stats are reported only by TableScan operator

.. list-table::
   :widths: 50 25 50
   :header-rows: 1

   * - Stats
     - Unit
     - Description
   * - numRunningScanThreads
     -
     - The number of running table scan drivers.

TableWriter
-----------
These stats are reported only by TableWriter operator

.. list-table::
   :widths: 50 25 50
   :header-rows: 1

   * - Stats
     - Unit
     - Description
   * - earlyFlushedRawBytes
     - bytes
     - Number of bytes pre-maturely flushed from file writers because of memory reclaiming.
   * - rebalanceTriggers
     -
     - The number of times that we triggers the rebalance of table partitions
       for a non-bucketed partition table.
   * - scaledPartitions
     -
     - The number of times that we scale a partition processing for a
       non-bucketed partition table.
   * - scaledWriters
     -
     - The number of times that we scale writers for a non-partitioned table.
   * - runningWallNanos
     - nanos
     - The running wall time of a writer operator since its creation.
   * - numWrittenFiles
     -
     - TThe number of files written by a writer operator
   * - writeIOWallNanos
     - nanos
     - The file write IO walltime.
   * - writeRecodeWallNanos
     - nanos
     - The walltime spend on file write data recoding.
   * - writeCompressionWallNanos
     - nanos
     - The walltime spent on file write data compression.

LookupIndexJoin
---------------
These stats are reported only by IndexLookupJoin operator

.. list-table::
   :widths: 50 25 50
   :header-rows: 1

   * - Stats
     - Unit
     - Description
   * - connectorlookupWallNanos
     - nanos
     - The end-to-end walltime in nanoseconds that the index connector do the lookup.
   * - connectorlookupWaitWallNanos
     - nanos
     - The walltime in nanoseconds that the index connector wait for the lookup from
       remote storage.
   * - connectorResultPrepareCpuNanos
     - nanos
     - The cpu time in nanoseconds that the index connector process response from storages
       client for followup processing by index join operator.
   * - clientlookupWaitWallNanos
     - nanos
     - The walltime in nanoseconds that the storage client wait for the lookup from remote storage.
   * - clientNumStorageRequests
     - nanos
     - The number of split requests sent to remote storage for a client lookup request.
   * - clientRequestProcessCpuNanos
     - nanos
     - The cpu time in nanoseconds that the storage client process request for remote
       storage lookup such as encoding the lookup input data into remotr storage request.
   * - clientResultProcessCpuNanos
     - nanos
     - The cpu time in nanoseconds that the storage client process response from remote
       storage lookup such as decoding the response data into velox vectors.
   * - clientLookupResultRawSize
     - bytes
     - The byte size of the raw result received from the remote storage lookup.
   * - clientLookupResultSize
     - bytes
     - The byte size of the result data in velox vectors that are decoded from the raw data
       received from the remote storage lookup.
   * - clientNumLazyDecodedResultBatches
     -
     - The number of lazy decoded result batches returned from the storage client.

Spilling
--------
These stats are reported by operators that support spilling.

.. list-table::
   :widths: 50 25 50
   :header-rows: 1

   * - Stats
     - Unit
     - Description
   * - spillNotSupported
     - nanos
     - The number of a spillable operators that don't support spill because of
       spill limitation. For instance, a window operator do not support spill
       if there is no partitioning.
   * - spillFillWallNanos
     - nanos
     - The time spent on filling rows for spilling.
   * - spillSortWallNanos
     - nanos
     - The time spent on sorting rows for spilling.
   * - spillExtractVectorWallNanos
     - nanos
     - The time spent on extracting Vector from RowContainer for spilling.
   * - spillSerializationWallNanos
     - nanos
     - The time spent on serializing rows for spilling.
   * - spillFlushWallNanos
     - nanos
     - The time spent on copy out serialized rows for disk write. If compression
       is enabled, this includes the compression time.
   * - spillWrites
     -
     - The number of spill writer flushes, equivalent to number of write calls to
       underlying filesystem.
   * - spillWriteWallNanos
     - nanos
     - The time spent on writing spilled rows to disk.
   * - spillRuns
     -
     - The number of times that spilling runs on an operator.
   * - exceededMaxSpillLevel
     -
     - The number of times that an operator exceeds the max spill limit.
   * - spillReadBytes
     - bytes
     - The number of bytes read from spilled files.
   * - spillReads
     -
     - The number of spill reader reads, equivalent to the number of read calls to the underlying filesystem.
   * - spillReadWallNanos
     - nanos
     - The time spent on read data from spilled files.
   * - spillDeserializationWallNanos
     - nanos
     - The time spent on deserializing rows read from spilled files.

Shuffle
--------
These stats are reported by shuffle operators.

.. list-table::
   :widths: 50 25 50
   :header-rows: 1

   * - Stats
     - Unit
     - Description
   * - shuffleSerdeKind
     -
     - Indicates the vector serde kind used by an operator for shuffle with 1
       for Presto, 2 for CompactRow, 3 for UnsafeRow. It is reported by Exchange,
       MergeExchange and PartitionedOutput operators for now.
   * - shuffleCompressionKind
     -
     - Indicates the compression kind used by an operator for shuffle. The
       reported value is set to the corresponding CompressionKind enum with 0
       (CompressionKind_NONE) as no compression.

PrefixSort
----------
These stats are reported by prefix sort.

.. list-table::
   :widths: 50 25 50
   :header-rows: 1

   * - Stats
     - Unit
     - Description
   * - numPrefixSortKeys
     -
     - The number of columns sorted using prefix sort.

IterativeVectorSerializer
-------------------------
These stats are reported by IterativeVectorSerializer.

.. list-table::
   :widths: 50 25 50
   :header-rows: 1

   * - Stats
     - Unit
     - Description
   * - compressionInputBytes
     -
     - The number of bytes before compression.
   * - compressedBytes
     -
     - The number of bytes after compression.
   * - compressionSkippedBytes
     -
     - The number of bytes that skip in-efficient compression.
