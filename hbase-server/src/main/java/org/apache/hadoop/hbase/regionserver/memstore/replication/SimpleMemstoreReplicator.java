package org.apache.hadoop.hbase.regionserver.memstore.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionAdminServiceCallable;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ReplicationProtbufUtil;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.memstore.replication.codec.KVCodecWithSeqId;
import org.apache.hadoop.hbase.regionserver.memstore.replication.v2.RegionReplicaReplicator;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MemstoreReplicaProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MemstoreReplicaProtos.ReplicateMemstoreRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MemstoreReplicaProtos.ReplicateMemstoreResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.Pair;

public class SimpleMemstoreReplicator implements MemstoreReplicator {
  private static final Log LOG = LogFactory.getLog(SimpleMemstoreReplicator.class);
  private static final String MEMSTORE_REPLICATION_THREAD_COUNT = 
      "hbase.regionserver.memstore.replication.threads";
  private final Configuration conf;
  // TODO this is a global level Q for all the ReplicationThreads? Should we have individual Qs for
  // each of the Threads. Discuss pros and cons and arrive
  private ClusterConnection connection;
  private final int operationTimeout;
  private final ReplicationThread[] replicationThreads;
  private final long replicationTimeoutNs;
  protected static final int DEFAULT_WAL_SYNC_TIMEOUT_MS = 5 * 60 * 1000;
  protected final RegionServerServices rs;
  private int threadIndex = 0;
  private final RpcControllerFactory rpcControllerFactory;
  private final RpcRetryingCallerFactory rpcRetryingCallerFactory;
  
  public SimpleMemstoreReplicator(Configuration conf, RegionServerServices rs) {
    this.conf = HBaseConfiguration.create(conf);
    // Adjusting the client retries number. This defaults to 31. (Multiplied by 10?)
    // The more this retries, the more latency we will have when we have some region replica write
    // fails. Adding a new config may be needed. As of now just making this to 2. And the multiplier to 1.
    this.conf.setInt("hbase.client.serverside.retries.multiplier", 1);
    this.conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    this.conf.set(HConstants.RPC_CODEC_CONF_KEY, KVCodecWithSeqId.class.getCanonicalName());

    // TODO : Better math considering Regions count also? As per the cur parallel model, this is enough
    // use the regular RPC timeout for replica replication RPC's
    this.operationTimeout = this.conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
      HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
    
    this.replicationTimeoutNs = TimeUnit.MILLISECONDS.toNanos(this.conf
        .getLong("hbase.regionserver.mutations.sync.timeout", DEFAULT_WAL_SYNC_TIMEOUT_MS));

    try {
      this.connection = (ClusterConnection) ConnectionFactory.createConnection(this.conf);
    } catch (IOException ex) {
    }
    int numWriterThreads = this.conf.getInt(MEMSTORE_REPLICATION_THREAD_COUNT,
        HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT);
    this.rpcRetryingCallerFactory = RpcRetryingCallerFactory
        .instantiate(connection.getConfiguration());
    this.rpcControllerFactory = RpcControllerFactory.instantiate(connection.getConfiguration());
    this.replicationThreads = new ReplicationThread[numWriterThreads];
    for (int i = 0; i < numWriterThreads; i++) {
      this.replicationThreads[i] = new ReplicationThread();
      this.replicationThreads[i].start();
    }
    this.rs = rs;
  }

  @Override
  public ReplicateMemstoreResponse replicate(MemstoreReplicationKey memstoreReplicationKey,
      MemstoreEdits memstoreEdits, RegionReplicaReplicator regionReplicator)
      throws IOException, InterruptedException, ExecutionException {
    MemstoreReplicationEntry entry = new MemstoreReplicationEntry(memstoreReplicationKey,
        memstoreEdits);
    CompletedFuture future = regionReplicator.append(entry);
    offer(regionReplicator, entry);
    return future.get(replicationTimeoutNs);
  }

  @Override
  public CompletableFuture<ReplicateMemstoreResponse> replicateAsync(
      MemstoreReplicationKey memstoreReplicationKey, MemstoreEdits memstoreEdits,
      RegionReplicaReplicator regionReplicaReplicator)
      throws IOException, InterruptedException, ExecutionException {
    return wrap(replicate(memstoreReplicationKey, memstoreEdits, regionReplicaReplicator));
  }

  private CompletableFuture<ReplicateMemstoreResponse>
      wrap(ReplicateMemstoreResponse replicateMemstoreResponse) {
    CompletableFuture<ReplicateMemstoreResponse> asyncFuture = new CompletableFuture<>();
    return asyncFuture;
  }
  @Override
  // directly waiting on this? Is it better to go with the rep threads here too???
  public ReplicateMemstoreResponse replicate(ReplicateMemstoreRequest request, List<Cell> allCells,
      RegionReplicaReplicator replicator)
      throws TimeoutIOException, InterruptedException, ExecutionException {
    CompletedFuture future = new CompletedFuture();
    replicate(new RequestEntryHolder(request, allCells, future), null, replicator);
    return future.get(replicationTimeoutNs);
  }


  public void stop() {
    for (ReplicationThread thread : this.replicationThreads) {
      thread.stop();
    }
  }

  public void offer(RegionReplicaReplicator replicator, MemstoreReplicationEntry entry) {
    int index = replicator.getReplicationThreadIndex();
    this.replicationThreads[index].regionQueue.offer(new Entry(replicator, entry.getSeq()));
  }

  // called only when the region replicator is created
  @Override
  public synchronized int getNextReplicationThread() {
    this.threadIndex = (threadIndex + 1) % this.replicationThreads.length;
    return threadIndex;
  }

  void replicate(RequestEntryHolder request, List<MemstoreReplicationEntry> replicationEntries,
      RegionReplicaReplicator replicator) {
    int curRegionReplicaId = replicator.getCurRegionReplicaId();
    HRegionLocation[] regionLocations = replicator.getLocations().getRegionLocations();
    List<Integer> replicaIds = null;
    List<Integer> replicaIdsInReq = null;
    // for primary region
    if(curRegionReplicaId == RegionReplicaUtil.DEFAULT_REPLICA_ID) {
      replicaIds = new ArrayList<Integer>(regionLocations.length);
      for (HRegionLocation location : regionLocations) {
        replicaIds.add(location.getRegionInfo().getReplicaId());
      }
    } else {
      if (request.getRequest().getReplicasCount() > 0) {
        // we have a request with replicas set.
        replicaIdsInReq = new ArrayList<Integer>(request.getRequest().getReplicasCount());
        for (int replicaId : request.getRequest().getReplicasList()) {
          replicaIdsInReq.add(replicaId);
        }
      }
    }
    ReplicateMemstoreResponse.Builder builder = ReplicateMemstoreResponse.newBuilder();
    builder.setReplicasCommitted(1);
    try {
      // The write pipeline for replication will always be R1 -> R2 ->.. Rn
      // When there is a failure for any node, the current replica will try with its next and so on
      // Replica ids are like 0, 1, 2...
      //TODO : If a RS goes down we assign the region to the same RS. So two replicas in same RS is 
      // not right. Fix it in LB.
      for (int i = curRegionReplicaId + 1; i < replicator.getReplicasCount(); i++) {
        // Getting the location of the next Region Replica (in pipeline)
        HRegionLocation nextRegionLocation = replicator.getRegionLocation(i);
        if (nextRegionLocation != null) {
          // if request is null then it is primary region
          if (request == null || (replicaIdsInReq != null
              && replicaIdsInReq.contains(nextRegionLocation.getRegionInfo().getReplicaId()))) {
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                "Replicating from region " + replicator.getRegionLocation(curRegionReplicaId)
                    + "  to the next replica " + nextRegionLocation);
            }
            RegionReplicaReplayCallable callable = new RegionReplicaReplayCallable(connection,
                rpcControllerFactory, replicator.getTableName(), nextRegionLocation,
                nextRegionLocation.getRegionInfo(), null, request, replicationEntries, replicaIds);
            try {
              ReplicateMemstoreResponse response =
                  rpcRetryingCallerFactory.<ReplicateMemstoreResponse> newCaller()
                      .callWithRetries(callable, operationTimeout);
              // we need this because we may have a success after some failures.
              builder.setReplicasCommitted(response.getReplicasCommitted() + 1);// Adding this write
                                                                                // itelf as success.
              if (response.getFailedReplicasCount() > 0) {
                // Since only primary takes the decision of marking the META as bad we need
                // to pass on this information till the primary
                for (int replicaId : response.getFailedReplicasList()) {
                  builder.addFailedReplicas(replicaId);
                }
              }
              break;// Break the inner for loop
            } catch (IOException | RuntimeException e) {
              // There may be other parallel handlers also trying to write to that replica.
              builder.addFailedReplicas(i);
              // We should mark the future with exception only after retrying with the other
              // Replicas
              // so that the write is successful??
            }
          } else {
            LOG.warn("The primary region "
                + RegionReplicaUtil.getRegionInfoForDefaultReplica(
                  nextRegionLocation.getRegionInfo())
                + " " + "has not specified the replica "
                + nextRegionLocation.getRegionInfo().getReplicaId()+"  probably due to a write issue");
          }
        } else {
          // TODO - This is unexpected situation. Should not mark as success
          // markEntriesSuccess(entries);
          builder.addFailedReplicas(i);
        }
      }
    } finally {
      markResponse(request, replicationEntries, builder.build());
    }
  }

  private void markResponse(RequestEntryHolder request,
      List<MemstoreReplicationEntry> replicationEntries, ReplicateMemstoreResponse response) {
    if (request != null) {
      request.getCompletedFuture().markResponse(response);
    } else {
      for (MemstoreReplicationEntry entry : replicationEntries) {
        entry.getFuture().markResponse(response);
      }
    }
  }
  private class Entry {
    private final RegionReplicaReplicator replicator;
    private final long seq;

    Entry(RegionReplicaReplicator replicator, long seq) {
      this.replicator = replicator;
      this.seq = seq;
    }
  }

  private class ReplicationThread extends HasThread {
    
    private volatile boolean closed = false;
    private final BlockingQueue<Entry> regionQueue;
    
    // TODO : create thread affinity here.
    public ReplicationThread() {
      regionQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void run() {
      while (!this.closed) {
        try {
          Entry entry = regionQueue.take();// TODO Check whether this call
                                           // will make the thread under wait
                                           // or whether consume CPU
          replicate(entry);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    private void replicate(Entry entry) {
      // TODO : Handle requests directly that comes for replica regions
      RegionReplicaReplicator replicator = entry.replicator;
      List<MemstoreReplicationEntry> entries = replicator.pullEntries(entry.seq);
      if (entries == null || entries.isEmpty()) {
        return;
      }
      SimpleMemstoreReplicator.this.replicate(null, entries, replicator);
    }
 
    public void stop() {
      this.closed = true;
      this.interrupt();
    }
  }

  private static class RequestEntryHolder {
    private ReplicateMemstoreRequest request;
    private List<Cell> cells;
    private CompletedFuture future;

    public RequestEntryHolder(ReplicateMemstoreRequest request, List<Cell> allCells,
        CompletedFuture future) {
      this.request = request;
      this.cells = allCells;
      this.future = future;
    }

    public void setRequest(ReplicateMemstoreRequest request) {
      this.request = request;
    }

    public ReplicateMemstoreRequest getRequest() {
      return this.request;
    }

    public CompletedFuture getCompletedFuture() {
      return this.future;
    }

    public List<Cell> getCells() {
      return this.cells;
    }
  }
  private static class RegionReplicaReplayCallable
      extends RegionAdminServiceCallable<ReplicateMemstoreResponse> {
    private RequestEntryHolder request;
    private final byte[] initialEncodedRegionName;
    private boolean primaryRegion = false;
    private List<Integer> replicaIds;

    public RegionReplicaReplayCallable(ClusterConnection connection,
        RpcControllerFactory rpcControllerFactory, TableName tableName, HRegionLocation location,
        HRegionInfo regionInfo, byte[] row,
        RequestEntryHolder request, List<MemstoreReplicationEntry> entries, List<Integer> replicaIds) {
      super(connection, rpcControllerFactory, location, tableName, row, regionInfo.getReplicaId());
      this.initialEncodedRegionName = regionInfo.getEncodedNameAsBytes();
      if (request != null) {
        this.request = request;
      } else {
        primaryRegion = true;
        // only for primary regions we will have this
        this.replicaIds = replicaIds;
        if (entries != null && !entries.isEmpty()) {
          MemstoreReplicationEntry[] entriesArray = new MemstoreReplicationEntry[entries.size()];
          entriesArray = entries.toArray(entriesArray);
          Pair<ReplicateMemstoreRequest, List<Cell>> pair = ReplicationProtbufUtil
              .buildReplicateMemstoreEntryRequest(entriesArray, initialEncodedRegionName, replicaIds);
          this.request = new RequestEntryHolder(pair.getFirst(), pair.getSecond(), null);
        }
      }
    }

    @Override
    public ReplicateMemstoreResponse call(HBaseRpcController controller) throws Exception {
      // Check whether we should still replay this entry. If the regions are changed, or the
      // entry is not coming form the primary region, filter it out because we do not need it.
      // Regions can change because of (1) region split (2) region merge (3) table recreated
      boolean skip = false;
      if (!Bytes.equals(location.getRegionInfo().getEncodedNameAsBytes(),
        initialEncodedRegionName)) {
        skip = true;
      }
      controller.setCellScanner(ReplicationProtbufUtil.getCellScannerOnCells(request.getCells()));
      if (primaryRegion) {
        // already request is created
        return stub.replicateMemstore(controller, request.getRequest());
      } else {
        MemstoreReplicaProtos.ReplicateMemstoreRequest.Builder reqBuilder =
            MemstoreReplicaProtos.ReplicateMemstoreRequest.newBuilder();
        for (int i = 0; i < request.getRequest().getEntryCount(); i++) {
          reqBuilder.addEntry(request.getRequest().getEntry(i));
        }
        reqBuilder.setEncodedRegionName(
          UnsafeByteOperations.unsafeWrap(location.getRegionInfo().getEncodedNameAsBytes()));
        reqBuilder.setReplicasOffered(request.getRequest().getReplicasOffered() + 1);
        return stub.replicateMemstore(controller, reqBuilder.build());
      }
    }
  }
}
