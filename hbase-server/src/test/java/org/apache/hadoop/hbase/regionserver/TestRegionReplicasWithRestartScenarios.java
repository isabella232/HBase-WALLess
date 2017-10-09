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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.TestRegionReplicasWith3Replicas.OpenedIn;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestRegionReplicasWithRestartScenarios {

  private static final Log LOG = LogFactory.getLog(TestRegionReplicasWithRestartScenarios.class);

  private static final int NB_SERVERS = 3;
  private static Table table;
  private static final byte[] row = "TestRegionReplicasWithRestartScenarios".getBytes();

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final byte[] f = HConstants.CATALOG_FAMILY;

  @BeforeClass
  public static void before() throws Exception {
    // Reduce the hdfs block size and prefetch to trigger the file-link reopen
    // when the file is moved to archive (e.g. compaction)
    HTU.getConfiguration().setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 8192);
    HTU.getConfiguration().setInt(DFSConfigKeys.DFS_CLIENT_READ_PREFETCH_SIZE_KEY, 1);
    HTU.getConfiguration().setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 128 * 1024 * 1024);

    HTU.startMiniCluster(NB_SERVERS);
    final TableName tableName = TableName.valueOf(TestRegionReplicasWithRestartScenarios.class.getSimpleName());

    // Create table then get the single region for our new table.
    createTableDirectlyFromHTD(tableName);
    //enableReplicationByModification(tableName);
  }

  private static void enableReplicationByModification(final TableName tableName)
      throws IOException, InterruptedException {
    table = HTU.createTable(tableName, f);
    HBaseTestingUtility.setReplicas(HTU.getAdmin(), table.getName(), 3);
  }

  private static void createTableDirectlyFromHTD(final TableName tableName) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.setRegionReplication(3);
    // create a table with 3 replication
    table = HTU.createTable(htd, new byte[][] { f }, (byte[][]) null,
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

  @Test(timeout = 6000000)
  public void testRegionReplicasCreated() throws Exception {
    List<Region> onlineRegions = getRS().getOnlineRegions();
    List<Region> onlineRegions2 = getSecondaryRS().getOnlineRegions();
    List<Region> onlineRegions3 = getTertiaryRS().getOnlineRegions();
    int totalRegions = onlineRegions.size() + onlineRegions2.size() + onlineRegions3.size();
    assertEquals("the number of regions should be more than 1", totalRegions, 3);
  }
  
  //@Test(timeout = 6000000)
  public void testGetOnTargetRegionReplica() throws Exception {
    try {
      // load some data to primary
      HTU.loadNumericRows(table, f, 0, 199);
      // assert that we can read back from primary
      Assert.assertEquals(199, HTU.countRows(table));
      // flush so that region replica can read
      byte[] row = Bytes.toBytes(String.valueOf(67));
      Get get = new Get(row);
      get.setReplicaId(2);
      Result result = table.get(get);
      Assert.assertArrayEquals(row, result.getValue(f, null));
      get = new Get(row);
      get.setReplicaId(1);
      result = table.get(get);
      Assert.assertArrayEquals(row, result.getValue(f, null));
    } finally {
      //HTU.deleteNumericRows(table, HConstants.CATALOG_FAMILY, 0, 0);
    }
  }

  @Test(timeout = 6000000)
  public void testFailWriteOnTertiaryWhenTertiaryIsDown() throws Exception {
    Pair<OpenedIn, OpenedIn> pair = null;
    OpenedIn tertiaryOpenedIn = null;
    HRegion tertiaryRegion = null;
    try {
      List<Region> onlineRegions = getRS().getOnlineRegions();
      HRegionInfo region = onlineRegions.get(0).getRegionInfo();
      for (RegionServerThread rs : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
        for (Region r : rs.getRegionServer().getOnlineRegions(table.getName())) {
          if (r.getRegionInfo().getReplicaId() == 2) {
            ((HRegion)r).throwErrorOnMemstoreReplay(true);
            tertiaryRegion = (HRegion)r;
            //rs.getRegionServer().abort("for test");
            break;
          }
        }
      }
      byte[] data = Bytes.toBytes(String.valueOf(100));
      Put put = new Put(data);
      put.setDurability(Durability.SKIP_WAL);
      put.addColumn(f, null, data);
      table.put(put);
      // just sleeping to see if the value is visible
      // try directly Get against region replica
      byte[] row = Bytes.toBytes(String.valueOf(100));
      Get get = new Get(row);
      get.setConsistency(Consistency.TIMELINE);
      get.setReplicaId(1);
      Result result = table.get(get);
      Assert.assertArrayEquals(row, result.getValue(f, null));
      // Getting the same row from replica 3 should throw an exception to the client
      row = Bytes.toBytes(String.valueOf(100));
      get = new Get(row);
      get.setConsistency(Consistency.TIMELINE);
      get.setReplicaId(2);
      try {
        table.get(get);
        fail("Test should have got an exception");
      } catch (Exception e) {
        assertTrue(e instanceof DoNotRetryIOException);
      }
      // again trying to write - this time tertiary should be avoided if the cache was actually cleared
      data = Bytes.toBytes(String.valueOf(101));
      put = new Put(data);
      put.setDurability(Durability.SKIP_WAL);
      put.addColumn(f, null, data);
      table.put(put);
      row = Bytes.toBytes(String.valueOf(101));
      get = new Get(row);
      get.setConsistency(Consistency.TIMELINE);
      get.setReplicaId(1);
      result = table.get(get);
      Assert.assertArrayEquals(row, result.getValue(f, null));
    } finally {
      if (tertiaryRegion != null) {
        tertiaryRegion.throwErrorOnMemstoreReplay(false);
      }
    }
  }

}
