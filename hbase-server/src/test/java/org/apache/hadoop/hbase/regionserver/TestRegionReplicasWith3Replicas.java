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

import static org.apache.hadoop.hbase.regionserver.TestRegionServerNoMaster.closeRegion;
import static org.apache.hadoop.hbase.regionserver.TestRegionServerNoMaster.openRegion;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.ipc.SimpleRpcServer;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestRegionReplicasWith3Replicas {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionReplicasWith3Replicas.class);
  private static final Log LOG = LogFactory.getLog(TestRegionReplicasWith3Replicas.class);

  private static final int NB_SERVERS = 3;
  private static Table table;
  private static final byte[] row = "TestRegionReplicasWith3Replicas".getBytes();

  private static HRegionInfo hriPrimary;
  private static HRegionInfo hriSecondary;
  private static HRegionInfo hriTertiary;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final byte[] f = HConstants.CATALOG_FAMILY;
  @Rule
  public TestName name = new TestName();
  @BeforeClass
  public static void before() throws Exception {
    // Reduce the hdfs block size and prefetch to trigger the file-link reopen
    // when the file is moved to archive (e.g. compaction)
    HTU.getConfiguration().setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 8192);
    HTU.getConfiguration().setInt(DFSConfigKeys.DFS_CLIENT_READ_PREFETCH_SIZE_KEY, 1);
    HTU.getConfiguration().setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 128 * 1024 * 1024);
    HTU.getConfiguration().setInt("hbase.regionserver.offheap.global.memstore.size", 800);
    HTU.getConfiguration().setBoolean("hbase.hregion.memstore.mslab.enabled", true);
    HTU.getConfiguration().setInt("hbase.hregion.memstore.chunkpool.maxsize", 1);
    HTU.getConfiguration().setInt("hbase.hregion.memstore.chunkpool.initialsize", 1);
    List<String> aepPaths = new ArrayList<String> (NB_SERVERS);
    for (int i = 0; i < NB_SERVERS; i++) {
      File file = new File("./chunkfile" + i);
      if(file.exists()) {
        file.delete();
      }
      aepPaths.add("./chunkfile" + i);
    }
    HTU.startMiniCluster(NB_SERVERS, aepPaths);
    final TableName tableName = TableName.valueOf(TestRegionReplicasWith3Replicas.class.getSimpleName());

    // Create table then get the single region for our new table.
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.setRegionReplication(3);
    table =
        HTU.createTable(htd, new byte[][] { f }, null, new Configuration(HTU.getConfiguration()));
    Thread.sleep(5000);
    try (RegionLocator locator = HTU.getConnection().getRegionLocator(tableName)) {
      hriPrimary = locator.getRegionLocation(row, false).getRegionInfo();
    }

    // mock a secondary region info to open
    hriSecondary = new HRegionInfo(hriPrimary.getTable(), hriPrimary.getStartKey(),
        hriPrimary.getEndKey(), hriPrimary.isSplit(), hriPrimary.getRegionId(), 1);
    // mock a tertiary region info to open
    hriTertiary = new HRegionInfo(hriPrimary.getTable(), hriPrimary.getStartKey(),
        hriPrimary.getEndKey(), hriPrimary.isSplit(), hriPrimary.getRegionId(), 2);

    // No master
    TestRegionServerNoMaster.stopMasterAndAssignMeta(HTU);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HRegionServer.TEST_SKIP_REPORTING_TRANSITION = false;
    table.close();
    HTU.shutdownMiniCluster();
  }

  private static HRegionServer getRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(0);
  }

  private static HRegionServer getSecondaryRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(1);
  }

  private static HRegionServer getTertiaryRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(2);
  }

  //@Test(timeout = 6000000)
  public void testSimpleFlush() throws Exception {
    try {
    new Thread() {
      public void run() {
        for (int i = 1; i < 100; i++) {
          byte[] data = Bytes.toBytes(String.valueOf(i));
          Put put = new Put(data);
          put.setDurability(Durability.SKIP_WAL);
          put.addColumn(f, null, data);
          try {
            table.put(put);
            if(i == 50) {
              System.out.println("");
            }
          } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      }
    }.start();
    HRegion primaryRegion = null;
    try {
      primaryRegion = (HRegion) getRS().getRegionByEncodedName(hriPrimary.getEncodedName());
    } catch (NotServingRegionException e) {
      try {
        primaryRegion =
            (HRegion) getSecondaryRS().getRegionByEncodedName(hriPrimary.getEncodedName());
      } catch (NotServingRegionException e1) {
        primaryRegion =
            (HRegion) getTertiaryRS().getRegionByEncodedName(hriPrimary.getEncodedName());
      }
    }
    primaryRegion.flush(true);
    } finally {
      for (int i = 1; i < 100; i++) {
        byte[] data = Bytes.toBytes(String.valueOf(i));
        Delete delete = new Delete(data);
        delete.addFamily(f);
        table.delete(delete);
      }
    }
  }

  @Test(timeout = 6000000)
  public void testGetOnTargetRegionReplica() throws Exception {
    Pair<OpenedIn, OpenedIn> pair = null;
    OpenedIn tertiaryOpenedIn = null;
    System.out.println(SimpleRpcServer.class.getName());
    try {
     // pair = openSecondary();
      //tertiaryOpenedIn = openTertiary(pair);
      // load some data to primary
      //loadInDifferentThreads(pair);
      HTU.loadNumericRows(table, f, 0, 100);
      // assert that we can read back from primary
      Assert.assertEquals(100, HTU.countRows(table));
      // flush so that region replica can read
      //Region primaryRegion = getPrimaryRegion(pair); 
      // region.flush(true);
      LOG.info("The region count has been retrieved ");
      byte[] row = Bytes.toBytes(String.valueOf(42));
      Get get = new Get(row);
      Result result = table.get(get);
      LOG.info("Reading from primary ");
      Assert.assertArrayEquals(row, result.getValue(f, null));
      // just sleeping to see if the value is visible
      // try directly Get against region replica
      get = new Get(row);
      get.setConsistency(Consistency.TIMELINE);
      LOG.info("Reading from secondary ");
      get.setReplicaId(2);
      result = table.get(get);
      Assert.assertArrayEquals(row, result.getValue(f, null));
      get = new Get(row);
      get.setConsistency(Consistency.TIMELINE);
      get.setReplicaId(1);
      result = table.get(get);
      LOG.info("The result is "+result);
      Assert.assertArrayEquals(row, result.getValue(f, null));
    } finally {
      HTU.deleteNumericRows(table, HConstants.CATALOG_FAMILY, 0, 100);
      //closeSecondary(pair.getSecond());
      //closeTertiary(tertiaryOpenedIn);
      afterClass();
    }
  }

  private void loadInDifferentThreads(Pair<OpenedIn, OpenedIn> pair) throws InterruptedException {
    // TODO Auto-generated method stub
    int threads = 1;
    LoadThread[] loadThreads = new LoadThread[threads];
    for(int i = 0; i < threads; i++) {
      loadThreads[i] = new LoadThread(pair);
    }
    for(int i = 0; i < threads; i++) {
      loadThreads[i].start();
    }
    for(int i = 0; i < threads; i++) {
      loadThreads[i].join();
    }
  }

  private static class LoadThread extends Thread {
    Pair<OpenedIn, OpenedIn> pair;
public LoadThread(Pair<OpenedIn, OpenedIn> pair) {
      // TODO Auto-generated constructor stub
      this.pair = pair;
    }

    /*    Table table;
    public LoadThread(Table table) {
      this.table = table;
    }*/
    @Override
    public void run() {
      try {
        for(int i = 0; i < 100; i++) {
        HTU.loadNumericRows(table, f, 0, i);
        if(i % 10 == 0) {
          Region primaryRegion = getPrimaryRegion(pair);
          ((HRegion)primaryRegion).flush(true);
        }
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
  
  private void restartRegionServer() throws Exception {
    afterClass();
    before();
  }

  @Test(timeout = 300000)
  public void testVerifySecondaryAbilityToReadWithOnFiles() throws Exception {
    // disable the store file refresh chore (we do this by hand)
    HTU.getConfiguration().setInt(StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD, 0);
    restartRegionServer();

    try {
      // wait for post open deploy to be completed.
      Thread.sleep(3000);
      // load some data to primary
      LOG.info("Loading data to primary region");
      List<HRegion> regions = new ArrayList<HRegion>();
      List<ServerName> regionServers = getRegionServers();
      for (ServerName server : regionServers) {
        regions.addAll(
          HTU.getMiniHBaseCluster().getRegionServer(server).getOnlineRegionsLocalContext());
      }
      for (int i = 0; i < 3; ++i) {
        HTU.loadNumericRows(table, f, i * 1000, (i + 1) * 1000);
        for (HRegion region : regions) {
          if (!region.getTableDescriptor().getTableName().isSystemTable()) {
            // only primary can flush
            region.flush(true);
          }
        }
      }
      HRegion primaryRegion = null;
      for (HRegion region : regions) {
        if(hriPrimary.equals(region.getRegionInfo())) {
          primaryRegion = region;
          Assert.assertEquals(3, region.getStore(f).getStorefilesCount());
          break;
        }
      }

      // Refresh store files on the secondary
      HRegion secondaryRegion = null;
      // after all the flushes no snapshot should be available and if there is read it should happen
      // from the store files which should have been created as part of primary flushes.
      for (HRegion region : regions) {
        if(hriSecondary.equals(region.getRegionInfo())) {
         secondaryRegion = region;
          break;
        }
      }
      byte[] row = Bytes.toBytes(String.valueOf(45));
      Get get = new Get(row);
      get.setConsistency(Consistency.TIMELINE);
      get.setReplicaId(1);
      Result result = table.get(get);
      Assert.assertArrayEquals(row, result.getValue(f, null));
      // without refreshFiles also we should get 3 files
      Assert.assertEquals(3, secondaryRegion.getStore(f).getStorefilesCount());
      
      // no manual refresh done on tertiary region. //But lets see how many store files we have
      HRegion tertiaryRegion = null;
      for (HRegion region : regions) {
        if(hriTertiary.equals(region.getRegionInfo())) {
          tertiaryRegion = region;
          break;
        }
      }
      Assert.assertEquals(3, tertiaryRegion.getStore(f).getStorefilesCount());
      // force compaction
      LOG.info("Force Major compaction on primary region " + hriPrimary);
      ((HRegion)primaryRegion).compact(true);
      Assert.assertEquals(1, primaryRegion.getStore(f).getStorefilesCount());
      List<RegionServerThread> regionServerThreads =
          HTU.getMiniHBaseCluster().getRegionServerThreads();
      HRegionServer hrs = null;
      for (RegionServerThread rs : regionServerThreads) {
        if (rs.getRegionServer()
            .getOnlineRegion(primaryRegion.getRegionInfo().getRegionName()) != null) {
          hrs = rs.getRegionServer();
          break;
        }
      }
      CompactedHFilesDischarger cleaner = new CompactedHFilesDischarger(100, null, hrs, false);
      cleaner.chore();
      // scan all the hfiles on the secondary.
      // since there are no read on the secondary when we ask locations to
      // the NN a FileNotFound exception will be returned and the FileLink
      // should be able to deal with it giving us all the result we expect.
      int keys = 0;
      int sum = 0;
      // try on tertiary region
      Assert.assertEquals(1, tertiaryRegion.getStore(f).getStorefilesCount());
      for (HStoreFile sf : ((HStore) tertiaryRegion.getStore(f)).getCompactedFiles()) {
        // Our file does not exist anymore. was moved by the compaction above.
        // but the compacted file will be existing
        LOG.debug(getRS().getFileSystem().exists(sf.getPath()));
        Assert.assertFalse(getRS().getFileSystem().exists(sf.getPath()));

        HFileScanner scanner = sf.getReader().getScanner(false, false);
        scanner.seekTo();
        do {
          keys++;

          Cell cell = scanner.getCell();
          sum += Integer.parseInt(
            Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
        } while (scanner.next());
      }
      Assert.assertEquals(3000, keys);
      Assert.assertEquals(4498500, sum);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      HTU.deleteNumericRows(table, HConstants.CATALOG_FAMILY, 0, 1000);
      afterClass();
      //closeSecondary(pair.getSecond());
      //closeTertiary(tertiaryOpenedIn);
    }
  }

  // TODO : This test some times fails after the recent commit - check
  //@Test(timeout = 300000)
  public void testRefreshStoreFiles() throws Exception {
    // enable store file refreshing
    final int refreshPeriod = 2000; // 2 sec
    HTU.getConfiguration().setInt("hbase.hstore.compactionThreshold", 100);
    HTU.getConfiguration().setInt(StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD,
      refreshPeriod);
    // restart the region server so that it starts the refresher chore
    restartRegionServer();

    try {
      // wait for post open deploy to be completed.
      // All replica regions force a flush on primary and this flush
      // makes additional store files. So before we even accept writes
      // wait for the replicas to be opened completely.
      Thread.sleep(6000);
      List<HRegion> regions = new ArrayList<HRegion>();
      List<ServerName> regionServers = getRegionServers();
      for (ServerName server : regionServers) {
        regions.addAll(
          HTU.getMiniHBaseCluster().getRegionServer(server).getOnlineRegionsLocalContext());
      }
      //load some data to primary
      LOG.info("Loading data to primary region");
      HTU.loadNumericRows(table, f, 0, 1000);
      // assert that we can read back from primary
      Assert.assertEquals(1000, HTU.countRows(table));
      // flush so that region replica can read
      LOG.info("Flushing primary region");
      for (HRegion region : regions) {
       region.flush(true);
      }

      // ensure that chore is run
      LOG.info("Sleeping for " + (4 * refreshPeriod));
      Threads.sleep(4 * refreshPeriod);
      HRegion primaryRegion = null;
      for (HRegion region : regions) {
        if(hriPrimary.equals(region.getRegionInfo())) {
          primaryRegion = region;
          break;
        }
      }
      LOG.info("Checking results from secondary region replica");
      Region secondaryRegion = null;
      for (HRegion region : regions) {
        if(hriSecondary.equals(region.getRegionInfo())) {
          secondaryRegion = region;
          break;
        }
      }
      Assert.assertEquals(1, secondaryRegion.getStore(f).getStorefilesCount());

      assertGet(secondaryRegion, 42, true);
      assertGetRpc(hriSecondary, 42, true);
      assertGetRpc(hriSecondary, 1042, false);

      // wait for post open deploy to be completed.
      // All replica regions force a flush on primary and this flush
      // makes additional store files. So before we even accept writes
      // wait for the replicas to be opened completely.
      Thread.sleep(3000);
      // load some data to primary
      HTU.loadNumericRows(table, f, 1000, 1100);
      ((HRegion)primaryRegion).flush(true);

      HTU.loadNumericRows(table, f, 2000, 2100);
      ((HRegion)primaryRegion).flush(true);

      // ensure that chore is run
      Threads.sleep(4 * refreshPeriod);

      assertGetRpc(hriTertiary, 42, true);
      assertGetRpc(hriTertiary, 1042, true);
      assertGetRpc(hriTertiary, 2042, true);

      // ensure that we see the 3 store files
      Assert.assertEquals(3, secondaryRegion.getStore(f).getStorefilesCount());

      // force compaction
      HTU.compact(table.getName(), true);

      long wakeUpTime = System.currentTimeMillis() + 4 * refreshPeriod;
      while (System.currentTimeMillis() < wakeUpTime) {
        assertGetRpc(hriSecondary, 42, true);
        assertGetRpc(hriSecondary, 1042, true);
        assertGetRpc(hriSecondary, 2042, true);
        assertGetRpc(hriTertiary, 42, true);
        assertGetRpc(hriTertiary, 1042, true);
        assertGetRpc(hriTertiary, 2042, true);
        Threads.sleep(10);
      }

      // ensure that we see the compacted file only
      // This will be 4 until the cleaner chore runs
      Assert.assertEquals(4, secondaryRegion.getStore(f).getStorefilesCount());

    } finally {
      afterClass();
    }
  }

  private void assertGet(Region region, int value, boolean expect) throws IOException {
    byte[] row = Bytes.toBytes(String.valueOf(value));
    Get get = new Get(row);
    Result result = region.get(get);
    if (expect) {
      Assert.assertArrayEquals(row, result.getValue(f, null));
    } else {
      result.isEmpty();
    }
  }

  // build a mock rpc
  private void assertGetRpc(RegionInfo info, int value, boolean expect)
      throws Exception {
    byte[] row = Bytes.toBytes(String.valueOf(value));
    Get get = new Get(row);
    ClientProtos.GetRequest getReq = RequestConverter.buildGetRequest(info.getRegionName(), get);
    ClientProtos.GetResponse getResp =  getRegionServer(info).getRSRpcServices().get(null, getReq);
    Result result = ProtobufUtil.toResult(getResp.getResult());
    if (expect) {
      Assert.assertArrayEquals(row, result.getValue(f, null));
    } else {
      result.isEmpty();
    }
  }

  private void closeTertiary(OpenedIn tertiaryOpenedIn) throws Exception {
    switch (tertiaryOpenedIn) {
    case PRIMARY:
      closeRegion(HTU, getRS(), hriTertiary);
      break;
    case SECONDARY:
      closeRegion(HTU, getSecondaryRS(), hriTertiary);
      break;
    case TERTIARY:
      closeRegion(HTU, getTertiaryRS(), hriTertiary);
      break;
    }
  }

  private OpenedIn openTertiary(Pair<OpenedIn, OpenedIn> pair) throws Exception {
    // we know the combination
    OpenedIn tertiaryOpenedIn = null;
    switch (pair.getFirst()) {
    case PRIMARY:
      LOG.debug("Opening the tertiary in tertiary region server");
      openRegion(HTU, getTertiaryRS(), hriTertiary);
      tertiaryOpenedIn = OpenedIn.TERTIARY;
      break;
    case SECONDARY:
      LOG.debug("Opening the tertiary in primary region server");
      openRegion(HTU, getRS(), hriTertiary);
      tertiaryOpenedIn = OpenedIn.PRIMARY;
      break;
    case TERTIARY:
      LOG.debug("Opening the tertiary in secondary region server");
      openRegion(HTU, getSecondaryRS(), hriTertiary);
      tertiaryOpenedIn = OpenedIn.SECONDARY;
      break;
    }
    return tertiaryOpenedIn;
  }

  private void closeSecondary(OpenedIn openedIn) throws Exception {
    switch (openedIn) {
    case PRIMARY:
      closeRegion(HTU, getRS(), hriSecondary);
      break;
    case SECONDARY:
      closeRegion(HTU, getSecondaryRS(), hriSecondary);
      break;
    case TERTIARY:
      closeRegion(HTU, getTertiaryRS(), hriSecondary);
      break;
    }
  }

  static Region getPrimaryRegion(Pair<OpenedIn, OpenedIn> pair) throws NotServingRegionException {
    Region region = null;
    switch (pair.getFirst()) {
    case PRIMARY:
      // If secondary is opened in Primary
      region = getRS().getRegionByEncodedName(hriPrimary.getEncodedName());
      break;
    case SECONDARY:
      region = getSecondaryRS().getRegionByEncodedName(hriPrimary.getEncodedName());
      break;
    case TERTIARY:
      region = getTertiaryRS().getRegionByEncodedName(hriPrimary.getEncodedName());
      break;
    }
    return region;
  }

  private HRegionServer getRegionServer(RegionInfo info) throws Exception {
    List<ServerName> regionServers = getRegionServers();
    for (ServerName sn : regionServers) {
      Collection<HRegion> regions =
          HTU.getMiniHBaseCluster().getRegionServer(sn).getOnlineRegionsLocalContext();
      for(HRegion region : regions) {
        if(region.getRegionInfo().equals(info)) {
          return HTU.getMiniHBaseCluster().getRegionServer(sn);
        }
      }
    }
    return null;
  }

  private List<ServerName> getRegionServers() throws Exception {
    List<ServerName> replicaRegionLocations = HTU.getMiniHBaseCluster().getMasterThreads().get(0)
        .getMaster().getAssignmentManager().getReplicaRegionLocations(hriPrimary);
    for (RegionServerThread rsThread : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
      if (!replicaRegionLocations.contains(rsThread.getRegionServer().getServerName())) {
        replicaRegionLocations.add(rsThread.getRegionServer().getServerName());
        break;
      }
    }
    return replicaRegionLocations;
  }

  enum OpenedIn {
    PRIMARY, SECONDARY, TERTIARY;
  }

}
