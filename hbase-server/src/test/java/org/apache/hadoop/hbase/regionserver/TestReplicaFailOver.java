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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.SimpleRpcServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestReplicaFailOver {
  private static final Log LOG = LogFactory.getLog(TestReplicaFailOver.class);

  private static final int NB_SERVERS = 4;
  private static Table table;
  private static final byte[] row = "TestScanFailoverWithReplicas".getBytes();

  private static HRegionInfo hriPrimary;
  private static HRegionInfo hriSecondary;
  private static HRegionInfo hriTertiary;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final byte[] f = HConstants.CATALOG_FAMILY;
// Testcase won't work as Balancer says this is same host multi server case and the logic for getting max cost
  // in that case is totally a mess
  @BeforeClass
  public static void before() throws Exception {
    // Reduce the hdfs block size and prefetch to trigger the file-link reopen
    // when the file is moved to archive (e.g. compaction)
    HTU.getConfiguration().setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 8192);
    HTU.getConfiguration().setInt(DFSConfigKeys.DFS_CLIENT_READ_PREFETCH_SIZE_KEY, 1);
    HTU.getConfiguration().setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 128 * 1024 * 1024);
    HTU.getConfiguration().setLong("hbase.client.scanner.caching", 20l);
    HTU.getConfiguration().set("hbase.rpc.server.impl", SimpleRpcServer.class.getName());
    HTU.getConfiguration().setBoolean("hbase.master.loadbalance.bytable", true);
    HTU.startMiniCluster(NB_SERVERS);
    HTU.getAdmin().setBalancerRunning(false, true);
    
    final TableName tableName = TableName.valueOf(TestScanFailoverWithReplicas.class.getSimpleName());

    // Create table then get the single region for our new table.
    createTableDirectlyFromHTD(tableName);
  }

  private static void createTableDirectlyFromHTD(final TableName tableName) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.setRegionReplication(3);
    int noofRegions = 10;
    // create a table with 3 replication
    byte[][] split = new byte[100][];
    for(int i =0 ; i < 100; i++) {
      split[i] = Bytes.toBytes(String.valueOf(i));
    }
    table = HTU.createTable(htd, new byte[][] { f }, split,
      new Configuration(HTU.getConfiguration()));
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    HRegionServer.TEST_SKIP_REPORTING_TRANSITION = false;
    table.close();
    HTU.shutdownMiniCluster();
  }

  private HRegionServer getRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(0);
  }

  private HRegionServer getSecondaryRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(1);
  }

  private HRegionServer getTertiaryRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(2);
  }

  @Test(timeout = 300000000)
  public void testScanFailOver() throws Exception {
    ServerName metaServer = metaServer();
    List<RegionInfo> assignedRegions = HTU.getHBaseCluster().getMasterThread().getMaster().getAssignmentManager().getAssignedRegions();
    System.out.println("the num of assigned regions are "+assignedRegions.size());
    for(RegionInfo info : assignedRegions) {
      System.out.println("The region that were assigned were "+info);
    }
    abortPrimary(metaServer);

    Thread.sleep(2000);
    assignedRegions = HTU.getHBaseCluster().getMasterThread().getMaster().getAssignmentManager().getAssignedRegions();
    System.out.println("the num of assigned regions after scp are "+assignedRegions.size());
    for(RegionInfo info : assignedRegions) {
      System.out.println("The region that were assigned were after scp "+info);
    }
    // wait for balancer;
    HTU.getAdmin().balance(true);
    // Ideally no balance should happen
  }

  private ServerName metaServer() {
    for (RegionServerThread rs : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
      for (Region r : rs.getRegionServer().getRegions()) {
        if (r.getRegionInfo().isMetaRegion()){
          // now see how does the assignment happen
          return rs.getRegionServer().getServerName();
        }
      }
    }
    return null;
  }
  
  private void abortPrimary(ServerName serverName) {
    
    for (RegionServerThread rs : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
      List<HRegion> regions = rs.getRegionServer().getRegions(table.getName());
      System.out.println("The region counts are "+regions.size());  
    }
    
    for (RegionServerThread rs : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
      if (!rs.getRegionServer().getServerName().equals(serverName)) {
        for (Region r : rs.getRegionServer().getRegions(table.getName())) {
          if (r.getRegionInfo().getReplicaId() == RegionReplicaUtil.DEFAULT_REPLICA_ID) {
            // now see how does the assignment happen
            rs.getRegionServer().abort("for test");
            // aborted = true;
            return;
          }
        }
      }
    }
  }
}
