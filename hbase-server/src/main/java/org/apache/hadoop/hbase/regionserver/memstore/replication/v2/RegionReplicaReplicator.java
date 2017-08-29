// TODO temp new package name
package org.apache.hadoop.hbase.regionserver.memstore.replication.v2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.RegionAdminServiceCallable;
import org.apache.hadoop.hbase.regionserver.memstore.replication.CompletedFuture;
import org.apache.hadoop.hbase.regionserver.memstore.replication.MemstoreEdits;
import org.apache.hadoop.hbase.regionserver.memstore.replication.MemstoreReplicationEntry;
import org.apache.hadoop.hbase.regionserver.memstore.replication.MemstoreReplicationKey;
import org.apache.hadoop.hbase.regionserver.memstore.replication.MemstoreReplicator;

// This is a per Region instance 
//TODO better name
public class RegionReplicaReplicator implements MemstoreReplicator {

  private final MemstoreReplicationService replicationService;
  private final HRegionInfo curRegion;
  private volatile RegionLocations locations;
  private final ClusterConnection connection;
  private List<MemstoreReplicationEntry> entryBuffer;
  private volatile long curSeq = 0;
  private volatile long curMaxConsumedSeq = -1;

  public RegionReplicaReplicator(HRegionInfo primaryRegion, ClusterConnection connection,
      MemstoreReplicationService replicationService) {
    this.replicationService = replicationService;
    this.curRegion = primaryRegion;
    this.connection = connection;
    this.entryBuffer = new ArrayList<>();
  }

  // Seems no way to avoid this sync
  private CompletedFuture append(MemstoreReplicationEntry entry) throws IOException {
    this.loadRegionLocationFromMeta();
    CompletedFuture future = new CompletedFuture();
    entry.attachFuture(future, curSeq++);
    addToBuffer(entry);
    return future;
  }

  private synchronized void addToBuffer(MemstoreReplicationEntry entry) {
    this.entryBuffer.add(entry);
  }
  
  /**
   * @param minSeq
   *          The min sequence number we expect to return. If that is not there (already retrieved
   *          and processed by some one else), we will return null.
   */
  public synchronized List<MemstoreReplicationEntry> pullEntries(long minSeq) {
    if (this.curMaxConsumedSeq >= minSeq) {
      return null;
    }
    List<MemstoreReplicationEntry> local = this.entryBuffer;
    this.entryBuffer = new ArrayList<>();
    this.curMaxConsumedSeq = this.curSeq;
    return local;
  }

  public HRegionLocation getRegionLocation(int replicaId) {
    return this.locations.getRegionLocation(replicaId);
  }

  public int getCurRegionReplicaId() {
    return this.curRegion.getReplicaId();
  }

  public TableName getTableName() {
    return this.curRegion.getTable();
  }

  public HRegionInfo getRegionInfo() {
    return this.curRegion;
  }

  public int getReplicasCount() {
    return this.locations.size();
  }

  private void loadRegionLocationFromMeta() throws IOException {
    // The replica id been passed have not much relevance than some checks. We will get all replica
    // loactions for this region. Not using the RegionLocation cache. We are on a fresh cluster
    // connection here. We will call this only once for a region and use that throughout unless we
    // have some replica region move. That anyway will be under our control and there is no point
    // in using cached location then.
    if (this.locations == null) {
      synchronized (this) {
        if (this.locations == null) {
          this.locations = RegionAdminServiceCallable.getRegionLocations(connection,
              this.curRegion.getTable(), this.curRegion.getStartKey(), false,
              this.curRegion.getReplicaId());
        }
      }
    }
  }

  @Override
  public void replicate(MemstoreReplicationKey memstoreReplicationKey, MemstoreEdits memstoreEdits,
      boolean replay, int replicaId, RegionLocations locations)
      throws IOException, InterruptedException, ExecutionException {
    MemstoreReplicationEntry entry =
        new MemstoreReplicationEntry(memstoreReplicationKey, memstoreEdits, replay, replicaId);
    CompletedFuture future = append(entry);
    this.replicationService.offer(this, entry);
  }
}
