/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.memstore.replication;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.RegionAdminServiceCallable;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.RetryingCallable;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ReplicationProtbufUtil;
import org.apache.hadoop.hbase.regionserver.memstore.replication.DefaultMemstoreReplicator.EntryBuffers;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL.Entry;

import com.google.common.cache.Cache;
import com.google.common.collect.Lists;

@InterfaceAudience.Private
public abstract class BaseMemstoreReplicator implements MemstoreReplicator {

  private static final Log LOG = LogFactory.getLog(BaseMemstoreReplicator.class);
  protected static final int DEFAULT_WAL_SYNC_TIMEOUT_MS = 5 * 60 * 1000; // in ms, 5min

  protected static String CLIENT_RETRIES_NUMBER =
      "hbase.region.replica.replication.client.retries.number";

  protected Configuration conf;
  protected ClusterConnection connection;
  protected TableDescriptors tableDescriptors;

  // Reuse WALSplitter constructs as a WAL pipe
  protected PipelineController controller;
  protected EntryBuffers entryBuffers;

  // Number of writer threads
  protected int numWriterThreads;

  protected int operationTimeout;

  protected ExecutorService pool;
  // Timeout that specifies the time that it can wait for the replication to be completed.
  // Probably we should split the total time into number of replicas so that we should wait
  // in each node for a specific time that totally adds up to the total replica wait time
  protected long replicationTimeoutNs;

  /**
   * A buffer of some number of edits for a given region. This accumulates edits and also provides a
   * memory optimization in order to share a single byte array instance for the table and region
   * name. Also tracks memory usage of the accumulated edits.
   */
  static class RegionEntryBuffer implements HeapSize {
    long heapInBuffer = 0;
    List<MemstoreReplicationEntry> entryBuffer;
    List<CompletedFuture> futures;
    TableName tableName;
    byte[] encodedRegionName;
    int replicaId;

    RegionEntryBuffer(TableName tableName, byte[] region, int replicaId) {
      this.tableName = tableName;
      this.encodedRegionName = region;
      this.entryBuffer = new LinkedList<>();
      this.futures = new LinkedList<>();
      this.replicaId = replicaId;
    }

    void appendFuture(CompletedFuture future) {
      futures.add(future);
    }

    long appendEntry(MemstoreReplicationEntry entry) {
      internify(entry);
      entryBuffer.add(entry);
      long incrHeap =
          entry.getMemstoreEdits().heapSize() + ClassSize.align(2 * ClassSize.REFERENCE) + 1 + // WALKey
          // pointers
              0; // TODO linkedlist entry
      heapInBuffer += incrHeap;
      return incrHeap;
    }

    private void internify(MemstoreReplicationEntry entry) {
      MemstoreReplicationKey k = entry.getMemstoreReplicationKey();
      k.internTableName(this.tableName);
      k.internEncodedRegionName(this.encodedRegionName);
    }

    @Override
    public long heapSize() {
      return heapInBuffer;
    }

    public byte[] getEncodedRegionName() {
      return encodedRegionName;
    }

    public List<MemstoreReplicationEntry> getEntryBuffer() {
      return entryBuffer;
    }

    public TableName getTableName() {
      return tableName;
    }

    public int getReplicaId() {
      return this.replicaId;
    }

    public List<CompletedFuture> getFutures() {
      return this.futures;
    }
  }

  public static abstract class OutputSink {

    protected PipelineController controller;
    protected EntryBuffers entryBuffers;

    protected Map<byte[], SinkWriter> writers =
        Collections.synchronizedMap(new TreeMap<byte[], SinkWriter>(Bytes.BYTES_COMPARATOR));;

    protected final Map<byte[], Long> regionMaximumEditLogSeqNum =
        Collections.synchronizedMap(new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR));

    protected final List<WriterThread> writerThreads = Lists.newArrayList();

    /* Set of regions which we've decided should not output edits */
    protected final Set<byte[]> blacklistedRegions =
        Collections.synchronizedSet(new TreeSet<>(Bytes.BYTES_COMPARATOR));

    protected boolean closeAndCleanCompleted = false;

    protected boolean writersClosed = false;

    protected final int numThreads;

    protected CancelableProgressable reporter = null;

    protected AtomicLong skippedEdits = new AtomicLong();

    protected List<Path> splits = null;

    public OutputSink(PipelineController controller, EntryBuffers entryBuffers, int numWriters) {
      numThreads = numWriters;
      this.controller = controller;
      this.entryBuffers = entryBuffers;
    }

    void setReporter(CancelableProgressable reporter) {
      this.reporter = reporter;
    }

    /**
     * Start the threads that will pump data from the entryBuffers to the output files.
     */
    public synchronized void startWriterThreads() {
      for (int i = 0; i < numThreads; i++) {
        WriterThread t = new WriterThread(controller, entryBuffers, this, i);
        t.start();
        writerThreads.add(t);
      }
    }

    /**
     * Update region's maximum edit log SeqNum.
     */
    void updateRegionMaximumEditLogSeqNum(Entry entry) {
      synchronized (regionMaximumEditLogSeqNum) {
        Long currentMaxSeqNum =
            regionMaximumEditLogSeqNum.get(entry.getKey().getEncodedRegionName());
        if (currentMaxSeqNum == null || entry.getKey().getSequenceId() > currentMaxSeqNum) {
          regionMaximumEditLogSeqNum.put(entry.getKey().getEncodedRegionName(),
            entry.getKey().getSequenceId());
        }
      }
    }

    Long getRegionMaximumEditLogSeqNum(byte[] region) {
      return regionMaximumEditLogSeqNum.get(region);
    }

    /**
     * @return the number of currently opened writers
     */
    int getNumOpenWriters() {
      return this.writers.size();
    }

    long getSkippedEdits() {
      return this.skippedEdits.get();
    }

    AtomicLong getSkippedEditsCounter() {
      return skippedEdits;
    }

    /**
     * Wait for writer threads to dump all info to the sink
     * @return true when there is no error
     * @throws IOException
     */
    protected boolean finishWriting(boolean interrupt) throws IOException {
      LOG.debug("Waiting for split writer threads to finish");
      boolean progress_failed = false;
      for (WriterThread t : writerThreads) {
        t.finish();
      }
      if (interrupt) {
        for (WriterThread t : writerThreads) {
          t.interrupt(); // interrupt the writer threads. We are stopping now.
        }
      }

      for (WriterThread t : writerThreads) {
        if (!progress_failed && reporter != null && !reporter.progress()) {
          progress_failed = true;
        }
        try {
          t.join();
        } catch (InterruptedException ie) {
          IOException iie = new InterruptedIOException();
          iie.initCause(ie);
          throw iie;
        }
      }
      controller.checkForErrors();
      LOG.info(this.writerThreads.size() + " split writers finished; closing...");
      return (!progress_failed);
    }

    public abstract List<Path> finishWritingAndClose() throws IOException;

    /**
     * @return a map from encoded region ID to the number of edits written out for that region.
     */
    public abstract Map<byte[], Long> getOutputCounts();

    /**
     * @return number of regions we've recovered
     */
    public abstract int getNumberOfRecoveredRegions();

    /**
     * @param buffer A WAL Edit Entry
     * @throws IOException
     */
    // Make it one impl
    public abstract void append(RegionEntryBuffer buffer) throws IOException;

    /**
     * WriterThread call this function to help flush internal remaining edits in buffer before close
     * @return true when underlying sink has something to flush
     */
    public boolean flush() throws IOException {
      return false;
    }

    /**
     * Some WALEdit's contain only KV's for account on what happened to a region. Not all sinks will
     * want to get all of those edits.
     * @return Return true if this sink wants to accept this region-level WALEdit.
     */
    public abstract boolean keepRegionEvent(Entry entry);
  }

  static void markDone(List<CompletedFuture> futures) {
    for (CompletedFuture future : futures) {
      future.markDone();
    }
  }

  static void markException(List<CompletedFuture> futures, Throwable t) {
    for (CompletedFuture future : futures) {
      future.markException(t);
    }
  }

  static class RegionReplicaSinkWriter extends SinkWriter {
    OutputSink sink;
    ClusterConnection connection;
    RpcControllerFactory rpcControllerFactory;
    RpcRetryingCallerFactory rpcRetryingCallerFactory;
    int operationTimeout;
    ExecutorService pool;
    Cache<TableName, Boolean> disabledAndDroppedTables;

    public RegionReplicaSinkWriter(OutputSink sink, ClusterConnection connection,
        ExecutorService pool, int operationTimeout) {
      this.sink = sink;
      this.connection = connection;
      this.operationTimeout = operationTimeout;
      this.rpcRetryingCallerFactory =
          RpcRetryingCallerFactory.instantiate(connection.getConfiguration());
      this.rpcControllerFactory = RpcControllerFactory.instantiate(connection.getConfiguration());
      this.pool = pool;
    }

    public void append(TableName tableName, byte[] encodedRegionName, byte[] row,
        List<CompletedFuture> futures, List<MemstoreReplicationEntry> entries, boolean replay,
        int currentReplicaId) throws IOException {

      /*
       * if (disabledAndDroppedTables.getIfPresent(tableName) != null) { if (LOG.isTraceEnabled()) {
       * LOG.trace("Skipping " + entries.size() + " entries because table " + tableName +
       * " is cached as a disabled or dropped table"); for (MemstoreReplicationEntry entry :
       * entries) { LOG.trace("Skipping : " + entry); } }
       * sink.getSkippedEditsCounter().addAndGet(entries.size()); return; }
       */
      // If the table is disabled or dropped, we should not replay the entries, and we can skip
      // replaying them. However, we might not know whether the table is disabled until we
      // invalidate the cache and check from meta
      RegionLocations locations = null;
      boolean useCache = true;
      while (true) {
        // get the replicas of the primary region
        try {
          locations = RegionReplicaReplayCallable.getRegionLocations(connection, tableName, row,
            useCache, 0);

          if (locations == null) {
            throw new HBaseIOException(
                "Cannot locate locations for " + tableName + ", row:" + Bytes.toStringBinary(row));
          }
        } catch (TableNotFoundException e) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Skipping " + entries.size() + " entries because table " + tableName
                + " is dropped. Adding table to cache.");
            for (MemstoreReplicationEntry entry : entries) {
              LOG.trace("Skipping : " + entry);
            }
          }
          disabledAndDroppedTables.put(tableName, Boolean.TRUE); // put to cache. Value ignored
          // skip this entry
          sink.getSkippedEditsCounter().addAndGet(entries.size());
          // mark the future as done as there is nothing to replicate. // Probably make this more
          // easier to read
          markDone(futures);
          return;
        }

        // check whether we should still replay this entry. If the regions are changed, or the
        // entry is not coming from the primary region, filter it out.
        HRegionLocation primaryLocation = locations.getDefaultRegionLocation();
        if (currentReplicaId == HRegionInfo.DEFAULT_REPLICA_ID) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Mutation started by primary region");
          }
          if (!Bytes.equals(primaryLocation.getRegionInfo().getEncodedNameAsBytes(),
            encodedRegionName)) {
            if (useCache) {
              useCache = false;
              continue; // this will retry location lookup
            }
            if (LOG.isTraceEnabled()) {
              LOG.trace("Skipping " + entries.size() + " entries in table " + tableName
                  + " because located region " + primaryLocation.getRegionInfo().getEncodedName()
                  + " is different than the original region "
                  + Bytes.toStringBinary(encodedRegionName) + " from WALEdit");
              for (MemstoreReplicationEntry entry : entries) {
                LOG.trace("Skipping : " + entry);
              }
            }
            sink.getSkippedEditsCounter().addAndGet(entries.size());
            markDone(futures);
            return;
          }
        }
        break;
      }

      if (locations.size() == 1) {
        markDone(futures);
        return;
      }

      ArrayList<Future<ReplicateWALEntryResponse>> tasks = new ArrayList<>(locations.size() - 1);

      // All passed entries should belong to one region because it is coming from the EntryBuffers
      // split per region. But the regions might split and merge (unlike log recovery case).
      for (int replicaId = 0; replicaId < locations.size(); replicaId++) {
        HRegionLocation location = locations.getRegionLocation(replicaId);
        if (!RegionReplicaUtil.isDefaultReplica(replicaId)) {
          HRegionInfo regionInfo = location == null
              ? RegionReplicaUtil.getRegionInfoForReplica(
                locations.getDefaultRegionLocation().getRegionInfo(), replicaId)
              : location.getRegionInfo();
          if (regionInfo.getReplicaId() == currentReplicaId + 1) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Mutation being replicated to the next highest replica " + regionInfo);
            }
            // send mutations only to the next replica. Let the other replica handle the replication
            // to its replica
            // Important TODO : Handle failure cases
            RegionReplicaReplayCallable callable =
                new RegionReplicaReplayCallable(connection, rpcControllerFactory, tableName,
                    location, regionInfo, row, entries, sink.getSkippedEditsCounter());
            Future<ReplicateWALEntryResponse> task = pool.submit(
              new RetryingRpcCallable<>(rpcRetryingCallerFactory, callable, operationTimeout));
            tasks.add(task);
          }
        }
      }

      if (tasks.isEmpty()) {
        markDone(futures);
        return;
      }
      boolean tasksCancelled = false;
      for (Future<ReplicateWALEntryResponse> task : tasks) {
        try {
          task.get();
          markDone(futures);
        } catch (InterruptedException e) {
          markException(futures, e);
          throw new InterruptedIOException(e.getMessage());
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof IOException) {
            // The table can be disabled or dropped at this time. For disabled tables, we have no
            // cheap mechanism to detect this case because meta does not contain this information.
            // ClusterConnection.isTableDisabled() is a zk call which we cannot do for every replay
            // RPC. So instead we start the replay RPC with retries and check whether the table is
            // dropped or disabled which might cause SocketTimeoutException, or
            // RetriesExhaustedException or similar if we get IOE.
            if (cause instanceof TableNotFoundException || connection.isTableDisabled(tableName)) {
              if (LOG.isTraceEnabled()) {
                LOG.trace("Skipping " + entries.size() + " entries in table " + tableName
                    + " because received exception for dropped or disabled table",
                  cause);
                for (MemstoreReplicationEntry entry : entries) {
                  LOG.trace("Skipping : " + entry);
                }
              }
              disabledAndDroppedTables.put(tableName, Boolean.TRUE); // put to cache for later.
              if (!tasksCancelled) {
                sink.getSkippedEditsCounter().addAndGet(entries.size());
                tasksCancelled = true; // so that we do not add to skipped counter again
              }
              continue;
            }
            // otherwise rethrow
            markException(futures, cause);
            throw (IOException) cause;
          }
          // unexpected exception
          markException(futures, cause);
          throw new IOException(cause);
        }
      }
    }
  }

  static class RetryingRpcCallable<V> implements Callable<V> {
    RpcRetryingCallerFactory factory;
    RetryingCallable<V> callable;
    int timeout;

    public RetryingRpcCallable(RpcRetryingCallerFactory factory, RetryingCallable<V> callable,
        int timeout) {
      this.factory = factory;
      this.callable = callable;
      this.timeout = timeout;
    }

    @Override
    public V call() throws Exception {
      return factory.<V> newCaller().callWithRetries(callable, timeout);
    }
  }

  /**
   * Calls replay on the passed edits for the given set of entries belonging to the region. It skips
   * the entry if the region boundaries have changed or the region is gone.
   */
  static class RegionReplicaReplayCallable
      extends RegionAdminServiceCallable<ReplicateWALEntryResponse> {
    private final List<MemstoreReplicationEntry> entries;
    private final byte[] initialEncodedRegionName;
    private final AtomicLong skippedEntries;

    public RegionReplicaReplayCallable(ClusterConnection connection,
        RpcControllerFactory rpcControllerFactory, TableName tableName, HRegionLocation location,
        HRegionInfo regionInfo, byte[] row, List<MemstoreReplicationEntry> entries,
        AtomicLong skippedEntries) {
      super(connection, rpcControllerFactory, location, tableName, row, regionInfo.getReplicaId());
      this.entries = entries;
      this.skippedEntries = skippedEntries;
      this.initialEncodedRegionName = regionInfo.getEncodedNameAsBytes();
    }

    public ReplicateWALEntryResponse call(HBaseRpcController controller) throws Exception {
      // Check whether we should still replay this entry. If the regions are changed, or the
      // entry is not coming form the primary region, filter it out because we do not need it.
      // Regions can change because of (1) region split (2) region merge (3) table recreated
      boolean skip = false;
      if (!Bytes.equals(location.getRegionInfo().getEncodedNameAsBytes(),
        initialEncodedRegionName)) {
        skip = true;
      }
      if (!this.entries.isEmpty() && !skip) {
        MemstoreReplicationEntry[] entriesArray = new MemstoreReplicationEntry[this.entries.size()];
        entriesArray = this.entries.toArray(entriesArray);

        // set the region name for the target region replica
        Pair<AdminProtos.ReplicateWALEntryRequest, CellScanner> p =
            ReplicationProtbufUtil.buildReplicateMemstoreEntryRequest(entriesArray,
              location.getRegionInfo().getEncodedNameAsBytes(), null, null, null);
        controller.setCellScanner(p.getSecond());
        return stub.replay(controller, p.getFirst());
      }

      if (skip) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Skipping " + entries.size() + " entries in table " + tableName
              + " because located region " + location.getRegionInfo().getEncodedName()
              + " is different than the original region "
              + Bytes.toStringBinary(initialEncodedRegionName) + " from WALEdit");
          for (MemstoreReplicationEntry entry : entries) {
            LOG.trace("Skipping : " + entry);
          }
        }
        skippedEntries.addAndGet(entries.size());
      }
      return ReplicateWALEntryResponse.newBuilder().build();

    }
  }

  /**
   * Class wraps the actual writer which writes data out and related statistics
   */
  public abstract static class SinkWriter {
    /* Count of edits written to this path */
    long editsWritten = 0;
    /* Count of edits skipped to this path */
    long editsSkipped = 0;
    /* Number of nanos spent writing to this log */
    long nanosSpent = 0;

    void incrementEdits(int edits) {
      editsWritten += edits;
    }

    void incrementSkippedEdits(int skipped) {
      editsSkipped += skipped;
    }

    void incrementNanoTime(long nanos) {
      nanosSpent += nanos;
    }
  }

  public static class PipelineController {
    // If an exception is thrown by one of the other threads, it will be
    // stored here.
    AtomicReference<Throwable> thrown = new AtomicReference<>();

    // Wait/notify for when data has been produced by the writer thread,
    // consumed by the reader thread, or an exception occurred
    public final Object dataAvailable = new Object();

    void writerThreadError(Throwable t) {
      thrown.compareAndSet(null, t);
    }

    /**
     * Check for errors in the writer threads. If any is found, rethrow it.
     */
    void checkForErrors() throws IOException {
      Throwable thrown = this.thrown.get();
      if (thrown == null) return;
      if (thrown instanceof IOException) {
        throw new IOException(thrown);
      } else {
        throw new RuntimeException(thrown);
      }
    }
  }

  public static class WriterThread extends Thread {
    private volatile boolean shouldStop = false;
    private PipelineController controller;
    private EntryBuffers entryBuffers;
    private OutputSink outputSink = null;

    WriterThread(PipelineController controller, EntryBuffers entryBuffers, OutputSink sink, int i) {
      super(Thread.currentThread().getName() + "-Writer-" + i);
      this.controller = controller;
      this.entryBuffers = entryBuffers;
      outputSink = sink;
    }

    @Override
    public void run() {
      try {
        doRun();
      } catch (Throwable t) {
        LOG.error("Exiting thread", t);
        controller.writerThreadError(t);
      }
    }

    private void doRun() throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("Writer thread starting");
      while (true) {
        RegionEntryBuffer buffer = entryBuffers.getChunkToWrite();
        if (buffer == null) {
          //LOG.info("Got a null");
          // No data currently available, wait on some more to show up
         /* synchronized (controller.dataAvailable) {
            if (shouldStop) {
              return;
            }
            try {
              controller.dataAvailable.wait(500);
            } catch (InterruptedException ie) {
              if (!shouldStop) {
                throw new RuntimeException(ie);
              }
            }
          }*/
          continue;
        }

        assert buffer != null;
        try {
          writeBuffer(buffer);
        } finally {
          entryBuffers.doneWriting(buffer);
        }
      }
    }

    private void writeBuffer(RegionEntryBuffer buffer) throws IOException {
      outputSink.append(buffer);
    }

    void finish() {
      synchronized (controller.dataAvailable) {
        shouldStop = true;
        controller.dataAvailable.notifyAll();
      }
    }
  }
}
