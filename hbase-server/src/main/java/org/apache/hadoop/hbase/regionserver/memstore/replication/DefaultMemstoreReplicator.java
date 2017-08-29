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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionAdminServiceCallable;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.RetryingCallable;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ReplicationProtbufUtil;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.memstore.replication.DefaultMemstoreReplicator.RegionReplicaOutputSink;
import org.apache.hadoop.hbase.regionserver.memstore.replication.BaseMemstoreReplicator.RegionEntryBuffer;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL.Entry;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.collect.Lists;

/**
 * This replicator per region server collects all the {@link MemstoreEdits} and
 * {@link MemstoreReplicationKey} and forms a {@link MemstoreReplicationEntry} per region and uses
 * the {@link MemstoreReplicaEndPoint} to replicate the entries to another region server
 */
@InterfaceAudience.Private
public class DefaultMemstoreReplicator extends BaseMemstoreReplicator {
  private static final Log LOG = LogFactory.getLog(DefaultMemstoreReplicator.class);

  private RegionReplicaOutputSink outputSink;

  public DefaultMemstoreReplicator(Configuration conf, RegionServerServices rsServices) {
    this.conf = HBaseConfiguration.create(conf);
    this.tableDescriptors = ((HRegionServer) rsServices).getTableDescriptors();
    // HRS multiplies client retries by 10 globally for meta operations, but we do not want this.
    // We are resetting it here because we want default number of retries (35) rather than 10 times
    // that which makes very long retries for disabled tables etc.
    int defaultNumRetries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    // TODO : what should be the retries here? This is sync flow so cannot be too high also?
    if (defaultNumRetries > 10) {
      int mult = conf.getInt("hbase.client.serverside.retries.multiplier", 10);
      defaultNumRetries = defaultNumRetries / mult; // reset if HRS has multiplied this already
    }

    conf.setInt("hbase.client.serverside.retries.multiplier", 1);
    int numRetries = conf.getInt(CLIENT_RETRIES_NUMBER, defaultNumRetries);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, numRetries);

    // TODO : Like in WAL I think we should have writer threads equal to the number of handlers
    this.numWriterThreads = this.conf.getInt("hbase.region.replica.replication.writer.threads", 5);
    controller = new PipelineController();
    entryBuffers = new EntryBuffers(controller,
        this.conf.getInt("hbase.region.replica.replication.buffersize", 128 * 1024 * 1024));

    // use the regular RPC timeout for replica replication RPC's
    this.operationTimeout = conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
      HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
    try {
      connection = (ClusterConnection) ConnectionFactory.createConnection(this.conf);
      this.pool = getDefaultThreadPool(conf);
      outputSink = new RegionReplicaOutputSink(controller, tableDescriptors, entryBuffers,
          connection, pool, numWriterThreads, operationTimeout);
       outputSink.startWriterThreads();
    } catch (IOException ex) {
      LOG.warn("Received exception while creating connection :" + ex);
    }
    this.replicationTimeoutNs = TimeUnit.MILLISECONDS.toNanos(
      conf.getLong("hbase.regionserver.mutations.sync.timeout", DEFAULT_WAL_SYNC_TIMEOUT_MS));
  }

  /**
   * Exception handler to pass the disruptor ringbuffer. Same as native implementation only it logs
   * using our logger instead of java native logger.
   */
  /**
   * Returns a Thread pool for the RPC's to region replicas. Similar to Connection's thread pool.
   */
  private ExecutorService getDefaultThreadPool(Configuration conf) {
    int maxThreads = conf.getInt("hbase.region.replica.replication.threads.max", 256);
    if (maxThreads == 0) {
      maxThreads = Runtime.getRuntime().availableProcessors() * 8;
    }
    long keepAliveTime = conf.getLong("hbase.region.replica.replication.threads.keepalivetime", 60);
    LinkedBlockingQueue<Runnable> workQueue =
        new LinkedBlockingQueue<>(maxThreads * conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
          HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS));
    ThreadPoolExecutor tpe =
        new ThreadPoolExecutor(maxThreads, maxThreads, keepAliveTime, TimeUnit.SECONDS, workQueue,
            Threads.newDaemonThreadFactory(this.getClass().getSimpleName() + "-rpc-shared-"));
    tpe.allowCoreThreadTimeOut(true);
    return tpe;
  }

  @Override
  public void replicate(MemstoreReplicationKey memstoreReplicationKey, MemstoreEdits memstoreEdits,
      boolean replay, int replicaId, RegionLocations locations)
      throws IOException, InterruptedException, ExecutionException {
    // TODO : It is better we have one to one mapping on the result of this replication
    // We have a blocking mechanism here but we may need to create a future that is mapped per
    // thread doing the
    /// actual replication. (TODO)
    MemstoreReplicationEntry entry =
        new MemstoreReplicationEntry(memstoreReplicationKey, memstoreEdits, replay, replicaId);
    CompletedFuture future = this.entryBuffers.appendEntry(entry, locations);
    // wait for the result here
    future.get(replicationTimeoutNs);
    // TODO :Very important. Probably return back if there is no replica here instead of putting
    // them in a pool etc.
  }

  static class RegionReplicaOutputSink extends OutputSink {
    private final RegionReplicaSinkWriter sinkWriter;
    private final TableDescriptors tableDescriptors;

    public RegionReplicaOutputSink(PipelineController controller, TableDescriptors tableDescriptors,
        EntryBuffers entryBuffers, ClusterConnection connection, ExecutorService pool,
        int numWriters, int operationTimeout) {
      super(controller, entryBuffers, numWriters);
      this.sinkWriter = new RegionReplicaSinkWriter(this, connection, pool, operationTimeout);
      this.tableDescriptors = tableDescriptors;
    }

    @Override
    public void append(RegionEntryBuffer buffer) throws IOException {
      List<MemstoreReplicationEntry> entries = buffer.getEntryBuffer();
      List<CompletedFuture> futures = buffer.getFutures();
      RegionLocations locations = buffer.getLocations();

      if (entries.isEmpty() || entries.get(0).getMemstoreEdits().getCells().isEmpty()) {
        return;
      }
      boolean replay = entries.get(0).isReplay();
      int replicaId = entries.get(0).getReplicaId();
      sinkWriter.append(buffer.getTableName(), buffer.getEncodedRegionName(),
        CellUtil.cloneRow(entries.get(0).getMemstoreEdits().getCells().get(0)), futures, entries,
        replay, replicaId, locations);
    }

    @Override
    public boolean flush() throws IOException {
      // nothing much to do for now. Wait for the Writer threads to finish up
      // append()'ing the data.
      entryBuffers.waitUntilDrained();
      return super.flush();
    }

    @Override
    public boolean keepRegionEvent(Entry entry) {
      return true;
    }

    @Override
    public List<Path> finishWritingAndClose() throws IOException {
      finishWriting(true);
      return null;
    }

    @Override
    public Map<byte[], Long> getOutputCounts() {
      return null; // only used in tests
    }

    @Override
    public int getNumberOfRecoveredRegions() {
      return 0;
    }
  }

  /**
   * Class which accumulates edits and separates them into a buffer per region while simultaneously
   * accounting RAM usage. Blocks if the RAM usage crosses a predefined threshold. Writer threads
   * then pull region-specific buffers from this class.
   */
  static class EntryBuffers {
    // Check how to make use of this in ringbuffer
    PipelineController controller;

    Map<byte[], RegionEntryBuffer> buffers = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    // holds the region entries in the order in which they arrive
    // TODO : don grow infinite. Have some upper limit here
    LinkedBlockingQueue<byte[]> regionQueue = new LinkedBlockingQueue<>();

    /*
     * Track which regions are currently in the middle of writing. We don't allow an IO thread to
     * pick up bytes from a region if we're already writing data for that region in a different IO
     * thread.
     */
    Set<byte[]> currentlyWriting = new TreeSet<>(Bytes.BYTES_COMPARATOR);

    long totalBuffered = 0;
    long maxHeapUsage;

    public EntryBuffers(PipelineController controller, long maxHeapUsage) {
      this.controller = controller;
      this.maxHeapUsage = maxHeapUsage;
    }

    /**
     * Append a log entry into the corresponding region buffer. Blocks if the total heap usage has
     * crossed the specified threshold.
     * @param locations 
     * @throws InterruptedException
     * @throws IOException
     * @return CompletedFuture the future associated with this entry
     */
    public CompletedFuture appendEntry(MemstoreReplicationEntry entry, RegionLocations locations)
        throws InterruptedException, IOException {
      MemstoreReplicationKey key = entry.getMemstoreReplicationKey();

      RegionEntryBuffer buffer;
      long incrHeap;
      CompletedFuture future;
      synchronized (this) {
        buffer = buffers.get(key.getEncodedRegionNameInBytes());
        future = new CompletedFuture();
        regionQueue.add(key.getEncodedRegionNameInBytes());
        if (buffer == null) {
          // add the region here.
          buffer = new RegionEntryBuffer(key.getTableName(), key.getEncodedRegionNameInBytes(),
              entry.getReplicaId(), locations);
          buffers.put(key.getEncodedRegionNameInBytes(), buffer);
        }
        incrHeap = buffer.appendEntry(entry);
        buffer.appendFuture(future);
      }

      // If we crossed the chunk threshold, wait for more space to be available
/*      synchronized (controller.dataAvailable) {
        totalBuffered += incrHeap;
        while (totalBuffered > maxHeapUsage && controller.thrown.get() == null) {
          LOG.debug(
            "Used " + totalBuffered + " bytes of buffered edits, waiting for IO threads...");
          controller.dataAvailable.wait(2000);
        }
        controller.dataAvailable.notifyAll();
      }
      controller.checkForErrors();*/
      return future;
    }

    /**
     * @return RegionEntryBuffer a buffer of edits to be written or replayed.
     */
    synchronized RegionEntryBuffer getChunkToWrite() {

      // TODO : Change this. We have to make it queue based. Means for a given region the writes
      // will be sequential
      // and in the order they appear
      // take the head of the queue
      byte[] regionInQueue = regionQueue.poll();
      if (regionInQueue != null && !currentlyWriting.contains(regionInQueue)) {
        RegionEntryBuffer regionEntryBuffer = buffers.get(regionInQueue);
        if (regionEntryBuffer == null) {
          return null;
        }
        regionEntryBuffer = buffers.remove(regionInQueue);
        currentlyWriting.add(regionInQueue);
        return regionEntryBuffer;
      }
      return null;
    }

    void doneWriting(RegionEntryBuffer buffer) {
      synchronized (this) {
        boolean removed = currentlyWriting.remove(buffer.encodedRegionName);
        assert removed;
      }
      long size = buffer.heapSize();

/*      synchronized (controller.dataAvailable) {
        totalBuffered -= size;
        // We may unblock writers
        controller.dataAvailable.notifyAll();
      }*/
    }

    synchronized boolean isRegionCurrentlyWriting(byte[] region) {
      return currentlyWriting.contains(region);
    }

    public void waitUntilDrained() {
      synchronized (controller.dataAvailable) {
        while (totalBuffered > 0) {
          try {
            controller.dataAvailable.wait(2000);
          } catch (InterruptedException e) {
            LOG.warn("Got interrupted while waiting for EntryBuffers is drained");
            Thread.interrupted();
            break;
          }
        }
      }
    }
  }
}

 
