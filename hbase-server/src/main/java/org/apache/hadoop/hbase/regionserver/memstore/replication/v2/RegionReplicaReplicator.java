// TODO temp new package name
package org.apache.hadoop.hbase.regionserver.memstore.replication.v2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.RegionAdminServiceCallable;
import org.apache.hadoop.hbase.regionserver.memstore.replication.CompletedFuture;
import org.apache.hadoop.hbase.regionserver.memstore.replication.MemstoreReplicationEntry;

// This is a per Region instance 
//TODO better name
public class RegionReplicaReplicator {

  private final HRegionInfo curRegion;
  private volatile RegionLocations locations;
  private final ClusterConnection connection;
  
  private List<MemstoreReplicationEntry> entryBuffer;
  private List<CompletedFuture> futures;

  public RegionReplicaReplicator(HRegionInfo primaryRegion, ClusterConnection connection) {
    this.curRegion = primaryRegion;
    this.connection = connection;
    this.entryBuffer = new ArrayList<>();
    this.futures = new ArrayList<>();
  }

  // Seems no way to avoid this sync
  public synchronized CompletedFuture append(MemstoreReplicationEntry entry) {
    this.entryBuffer.add(entry);
    CompletedFuture future = new CompletedFuture();
    this.futures.add(future);
    return future;
  }

  public synchronized List<MemstoreReplicationEntry> pullEntries() {
    List<MemstoreReplicationEntry> local = this.entryBuffer;
    this.entryBuffer = new ArrayList<>();
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
}
