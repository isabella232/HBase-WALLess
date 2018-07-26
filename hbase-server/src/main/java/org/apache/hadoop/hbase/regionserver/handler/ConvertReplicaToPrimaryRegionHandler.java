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
package org.apache.hadoop.hbase.regionserver.handler;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.handler.OpenRegionHandler.PostOpenDeployTasksThread;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class ConvertReplicaToPrimaryRegionHandler extends OpenPriorityRegionHandler {

  private static final Log LOG = LogFactory.getLog(ConvertReplicaToPrimaryRegionHandler.class);
  private final RegionInfo replicaRegionInfo;
  private HRegion replicaRegion;

  public ConvertReplicaToPrimaryRegionHandler(Server server, RegionServerServices rsServices,
      RegionInfo regionInfo, RegionInfo replicaRegion, TableDescriptor htd,
      long masterSystemTime) {
    super(server, rsServices, regionInfo, htd, masterSystemTime);
    this.replicaRegionInfo = replicaRegion;
    LOG.info("Converting replica region "+replicaRegion+ " to primary region "+regionInfo);
  }

  protected HRegion openRegion() {
    HRegion region = (HRegion) ((HRegionServer)this.rsServices)
        .getRegion(this.replicaRegionInfo.getEncodedName());
    this.replicaRegion = region;
    LOG.debug("Converting region "+region.getRegionInfo() +  "  to primary region");
    // First remove the replica region from online regions.
    // TODO we are not following the regular way of close of a region where 1st it is in RIT
    // This typecasting is ok as in Trunk we will do many changes and here in handler we will have
    // HRS
    boolean removeRegion = ((HRegionServer) this.rsServices).removeRegion(this.replicaRegion, null);
    LOG.debug("Removed replica region " + replicaRegion + " from server " + removeRegion);        
    region.convertAsPrimaryRegion();
    return region;
  }

  @Override
  protected boolean postRegionOpenSteps(HRegion region) {
    // Successful region open, and add it to MutableOnlineRegions
    addToOnlineRegions(region);
    // Open region won't fail here in this handler because we are just doing a simple conversion.
    // Why we do this?
    // When a replica region is converted to primary we inform the master on the transition
    // completion
    // and that starts another procedure which assigns the actual replica region to another server.
    // Sometimes it so happens that the RPC to open the replica happens to reach the same
    // server(current one)
    // before it does the addToOnlineregions(). (the RPC reaching this server itself is wrong) but
    // when that happens we are just not able to do anything and that region keeps hanging in RIT
    // (*).
    // So here we add that new converted region and remove the replica region from this server's
    // memory
    // and then update the transition to maser.
    // Pls see the RsRpcServices#openRegion changes to see how (*) is handled : TODO
    if (!updateMeta(region, masterSystemTime) || this.server.isStopped()
        || this.rsServices.isStopping()) {
      return false;
    }
    return true;
  }

  @Override
  protected void addToOnlineRegions(HRegion region) {
    this.rsServices.addRegion(region);
  }
}
