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
package org.apache.hadoop.hbase.master.memstore.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicaRegionHealthProtos.RSRegionReplicaHealthChangeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicaRegionHealthProtos.RegionNameMasterProcessingTimePair;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

@InterfaceAudience.Private
public class ReplicaHealthCorrectionHandler extends EventHandler {

  private static final Log LOG = LogFactory.getLog(ReplicaHealthCorrectionHandler.class.getName());

  private final RegionReplicaHealthManager replicaHealthManager;
  private final HMaster master;
  private final ServerName sn;
  private final List<Pair<RegionInfo, Long>> regions;

  public ReplicaHealthCorrectionHandler(RegionReplicaHealthManager replicaHealthManager,
      HMaster master, EventType eventType, ServerName sn, List<Pair<RegionInfo, Long>> regions) {
    super(master, eventType);
    this.replicaHealthManager = replicaHealthManager;
    this.master = master;
    this.sn = sn;
    this.regions = regions;
  }

  @Override
  public void process() throws IOException {
    try {
      AdminService.BlockingInterface rsAdmin = this.master.getServerManager().getRsAdmin(this.sn);
      LOG.info(
          "Trying to contact " + sn + " for processing of bad health replica regions " + regions);
      // TODO - What is the socket timeout value here? If this is too long, we will make the reqs
      // to other server to wait longer.
      // When this chore is in run, the next schedule comes, what to do? Just cancel?
      rsAdmin.handleBadRegions(null, buildHealthChangeReq(this.regions));
    } catch (RegionServerStoppedException rsse) {
      // We know these regions are from a down server. So no use of repeating the RPC for them.
      // Later we will reassign these regions to other RSs and end of it, we will add them to this
      // 'badRegions' list and will process then.
      List<RegionInfo> downRegions = new ArrayList<>(this.regions.size());
      this.regions.forEach((region) -> {
        downRegions.add(region.getFirst());
      });
      this.replicaHealthManager.markRegionsDown(downRegions, this.sn);
    } catch (IOException|ServiceException e) {
    }
  }
  
  private RSRegionReplicaHealthChangeRequest buildHealthChangeReq(
      List<Pair<RegionInfo, Long>> regions) {
    RSRegionReplicaHealthChangeRequest.Builder healthChangeReqBuilder = RSRegionReplicaHealthChangeRequest
        .newBuilder();
    RegionNameMasterProcessingTimePair.Builder builder = RegionNameMasterProcessingTimePair
        .newBuilder();
    regions.forEach(regionTsPair -> {
      builder.setEncodedRegionName(
          UnsafeByteOperations.unsafeWrap(regionTsPair.getFirst().getEncodedNameAsBytes()));
      builder.setMasterTs(regionTsPair.getSecond());
      healthChangeReqBuilder.addRegionNameProcessingTimePair(builder.build());
      builder.clear();
    });
    RSRegionReplicaHealthChangeRequest healthChangeReq = healthChangeReqBuilder.build();
    return healthChangeReq;
  }
}
