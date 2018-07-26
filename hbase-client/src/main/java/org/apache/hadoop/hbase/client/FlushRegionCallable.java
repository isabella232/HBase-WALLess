/**
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

package org.apache.hadoop.hbase.client;

import java.io.IOException;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionResponse;

/**
 * A Callable for flushRegion() RPC.
 */
@InterfaceAudience.Private
public class FlushRegionCallable extends RegionAdminServiceCallable<FlushRegionResponse> {
  private static final Logger LOG = LoggerFactory.getLogger(FlushRegionCallable.class);
  private final byte[] regionName;
  private final boolean writeFlushWalMarker;
  private boolean reload;
  private int requestingRegionReplica;
  // we need this servername because in case of a flush triggered by the replica on a primary
  // the location may not be still updated in META. So it may still point to the old replica's replication
  // (which was actually switched over). It is better we send the current servername directly.
  // TODO : check if we have issues during a failure or retry
  private ServerName serverName;

  public FlushRegionCallable(ClusterConnection connection,
      RpcControllerFactory rpcControllerFactory, TableName tableName, byte[] regionName,
      byte[] regionStartKey, boolean writeFlushWalMarker, RegionInfo requestingRegion, ServerName serverName) {
    super(connection, rpcControllerFactory, tableName, regionStartKey);
    this.regionName = regionName;
    this.writeFlushWalMarker = writeFlushWalMarker;
    if (requestingRegion != null) {
      this.requestingRegionReplica = requestingRegion.getReplicaId();
      this.serverName = serverName;
    }
  }

  public FlushRegionCallable(ClusterConnection connection,
      RpcControllerFactory rpcControllerFactory, RegionInfo regionInfo,
      boolean writeFlushWalMarker) {
    this(connection, rpcControllerFactory, regionInfo.getTable(), regionInfo.getRegionName(),
      regionInfo.getStartKey(), writeFlushWalMarker, null, null);
  }

  public FlushRegionCallable(ClusterConnection connection,
      RpcControllerFactory rpcControllerFactory, RegionInfo regionInfo, boolean writeFlushWalMarker,
      RegionInfo requestingRegionReplica, ServerName serverName) {
    this(connection, rpcControllerFactory, regionInfo.getTable(), regionInfo.getRegionName(),
        regionInfo.getStartKey(), writeFlushWalMarker, requestingRegionReplica, serverName);
  }

  @Override
  public void prepare(boolean reload) throws IOException {
    super.prepare(reload);
    this.reload = reload;
  }

  @Override
  protected FlushRegionResponse call(HBaseRpcController controller) throws Exception {
    // Check whether we should still do the flush to this region. If the regions are changed due
    // to splits or merges, etc return success
    if (!Bytes.equals(location.getRegionInfo().getRegionName(), regionName)) {
      if (!reload) {
        throw new IOException("Cached location seems to be different than requested region.");
      }
      LOG.info("Skipping flush region, because the located region "
          + Bytes.toStringBinary(location.getRegionInfo().getRegionName()) + " is different than "
          + " requested region " + Bytes.toStringBinary(regionName));
      return FlushRegionResponse.newBuilder()
          .setLastFlushTime(EnvironmentEdgeManager.currentTime())
          .setFlushed(false)
          .setWroteFlushWalMarker(false)
          .build();
    }

    FlushRegionRequest request = RequestConverter.buildFlushRegionRequest(regionName,
      writeFlushWalMarker, this.requestingRegionReplica, serverName);
    return stub.flushRegion(controller, request);
  }
}
