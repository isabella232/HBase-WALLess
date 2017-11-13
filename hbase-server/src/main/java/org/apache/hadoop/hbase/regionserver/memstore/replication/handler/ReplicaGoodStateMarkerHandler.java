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
package org.apache.hadoop.hbase.regionserver.memstore.replication.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;

@InterfaceAudience.Private
public class ReplicaGoodStateMarkerHandler extends EventHandler {

  private final HRegionServer hrs;
  private final HRegionInfo replicaRegion;
  private final HRegion primaryRegion;

  public ReplicaGoodStateMarkerHandler(HRegionServer server, HRegionInfo replicaRegion,
      HRegion primaryRegion) {
    super(server, EventType.RS_REGION_REPLICA_GOOD_HEALTH_MARKER);
    this.hrs = server;
    this.replicaRegion = replicaRegion;
    this.primaryRegion = primaryRegion;
  }

  @Override
  public void process() throws IOException {
    List<HRegionInfo> lst = new ArrayList<>(1);
    lst.add(this.replicaRegion);
    boolean success = this.hrs.reportReplicaRegionHealthChange(lst, true);
    if (success) {
      this.primaryRegion.markReplicaBackToGoodInMeta(this.replicaRegion);
    }
  }
}
