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

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestRegionReplicasWith3Replicas {

  private static final Log LOG = LogFactory.getLog(TestRegionReplicasWith3Replicas.class);

  private static final int NB_SERVERS = 3;
  private static Table table;
  private static final byte[] row = "TestRegionReplicasWith3Replicas".getBytes();

  private static HRegionInfo hriPrimary;
  private static HRegionInfo hriSecondary;
  private static HRegionInfo hriTertiary;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final byte[] f = HConstants.CATALOG_FAMILY;

  @BeforeClass
  public static void before() throws Exception {
    // Reduce the hdfs block size and prefetch to trigger the file-link reopen
    // when the file is moved to archive (e.g. compaction)
    HTU.getConfiguration().setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 8192);
    HTU.getConfiguration().setInt(DFSConfigKeys.DFS_CLIENT_READ_PREFETCH_SIZE_KEY, 1);
    HTU.getConfiguration().setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 128 * 1024 * 1024);
    HTU.getConfiguration().set(HRegionServer.HBASE_REGIONSERVER_MEMSTORE_REPLICATOR_CLASS, "default");

    HTU.startMiniCluster(NB_SERVERS);
    final TableName tableName = TableName.valueOf(TestRegionReplicasWith3Replicas.class.getSimpleName());

    // Create table then get the single region for our new table.
    table = HTU.createTable(tableName, f);

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
  public void testGetOnTargetRegionReplica() throws Exception {
    Pair<OpenedIn, OpenedIn> pair = null;
    OpenedIn tertiaryOpenedIn = null;
    try {
      pair = openSecondary();
      tertiaryOpenedIn = openTertiary(pair);
      // load some data to primary
      HTU.loadNumericRows(table, f, 0, 100);
      // assert that we can read back from primary
      Assert.assertEquals(100, HTU.countRows(table));
      // flush so that region replica can read
      Region primaryRegion = getPrimaryRegion(pair);
      // region.flush(true);

      // just sleeping to see if the value is visible
      // try directly Get against region replica
      byte[] row = Bytes.toBytes(String.valueOf(42));
      Get get = new Get(row);
      get.setConsistency(Consistency.TIMELINE);
      get.setReplicaId(2);
      Result result = table.get(get);
      Assert.assertArrayEquals(row, result.getValue(f, null));
      get = new Get(row);
      get.setConsistency(Consistency.TIMELINE);
      get.setReplicaId(1);
      result = table.get(get);
      Assert.assertArrayEquals(row, result.getValue(f, null));
    } finally {
      HTU.deleteNumericRows(table, HConstants.CATALOG_FAMILY, 0, 100);
      closeSecondary(pair.getSecond());
      closeTertiary(tertiaryOpenedIn);
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

    Pair<OpenedIn, OpenedIn> pair = null;
    OpenedIn tertiaryOpenedIn = null;
    try {
      pair = openSecondary();
      tertiaryOpenedIn = openTertiary(pair);

      // load some data to primary
      LOG.info("Loading data to primary region");
      for (int i = 0; i < 3; ++i) {
        HTU.loadNumericRows(table, f, i * 1000, (i + 1) * 1000);
        Region primaryRegion = getPrimaryRegion(pair);
        primaryRegion.flush(true);
        Region secRegion = getSecondaryRegion(pair);
        // this flush should not happen at any cost - Assert it some how
        secRegion.flush(true);
        
        Region tertiaryRegion = getTertiaryRegion(tertiaryOpenedIn);
        // this flush should not happen at any cost
        tertiaryRegion.flush(true);
      }

      Region primaryRegion = getPrimaryRegion(pair);
      Assert.assertEquals(3, primaryRegion.getStore(f).getStorefilesCount());

      // Refresh store files on the secondary
      Region secondaryRegion = getSecondaryRegion(pair);
      // after all the flushes no snapshot should be available and if there is read it should happen
      // from the store files which should have been created as part of primary flushes.
      byte[] row = Bytes.toBytes(String.valueOf(45));
      Get get = new Get(row);
      get.setConsistency(Consistency.TIMELINE);
      get.setReplicaId(1);
      Result result = table.get(get);
      Assert.assertArrayEquals(row, result.getValue(f, null));
      // without refreshFiles also we should get 3 files
      Assert.assertEquals(3, secondaryRegion.getStore(f).getStorefilesCount());
      
      // no manual refresh done on tertiary region. //But lets see how many store files we have
      Region tertiaryRegion = getTertiaryRegion(tertiaryOpenedIn);
      Assert.assertEquals(3, tertiaryRegion.getStore(f).getStorefilesCount());
      // force compaction
      LOG.info("Force Major compaction on primary region " + hriPrimary);
      primaryRegion.compact(true);
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
      for (StoreFile sf : tertiaryRegion.getStore(f).getStorefiles()) {
        // Our file does not exist anymore. was moved by the compaction above.
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
      closeSecondary(pair.getSecond());
      closeTertiary(tertiaryOpenedIn);
    }
  }

  // TODO : This test some times fails after the recent commit - check
  @Test(timeout = 300000)
  public void testRefreshStoreFiles() throws Exception {
    // enable store file refreshing
    final int refreshPeriod = 2000; // 2 sec
    HTU.getConfiguration().setInt("hbase.hstore.compactionThreshold", 100);
    HTU.getConfiguration().setInt(StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD,
      refreshPeriod);
    // restart the region server so that it starts the refresher chore
    restartRegionServer();

    Pair<OpenedIn, OpenedIn> pair = null;
    OpenedIn tertiaryOpenedIn = null;
    try {
      LOG.info("Opening the secondary");
      pair = openSecondary();
      tertiaryOpenedIn = openTertiary(pair);

      //load some data to primary
      LOG.info("Loading data to primary region");
      HTU.loadNumericRows(table, f, 0, 1000);
      // assert that we can read back from primary
      Assert.assertEquals(1000, HTU.countRows(table));
      // flush so that region replica can read
      LOG.info("Flushing primary region");
      Region region = getPrimaryRegion(pair);
      region.flush(true);
      HRegion primaryRegion = (HRegion) region;

      // ensure that chore is run
      LOG.info("Sleeping for " + (4 * refreshPeriod));
      Threads.sleep(4 * refreshPeriod);

      LOG.info("Checking results from secondary region replica");
      Region secondaryRegion = getSecondaryRegion(pair);
      Assert.assertEquals(1, secondaryRegion.getStore(f).getStorefilesCount());

      assertGet(secondaryRegion, 42, true);
      assertGetRpc(hriSecondary, 42, true, pair.getSecond());
      assertGetRpc(hriSecondary, 1042, false, pair.getSecond());

      // load some data to primary
      HTU.loadNumericRows(table, f, 1000, 1100);
      region = getPrimaryRegion(pair);
      region.flush(true);

      HTU.loadNumericRows(table, f, 2000, 2100);
      region = getPrimaryRegion(pair);
      region.flush(true);

      // ensure that chore is run
      Threads.sleep(4 * refreshPeriod);

      assertGetRpc(hriTertiary, 42, true, tertiaryOpenedIn);
      assertGetRpc(hriTertiary, 1042, true, tertiaryOpenedIn);
      assertGetRpc(hriTertiary, 2042, true, tertiaryOpenedIn);

      // ensure that we see the 3 store files
      Assert.assertEquals(3, secondaryRegion.getStore(f).getStorefilesCount());

      // force compaction
      HTU.compact(table.getName(), true);

      long wakeUpTime = System.currentTimeMillis() + 4 * refreshPeriod;
      while (System.currentTimeMillis() < wakeUpTime) {
        assertGetRpc(hriSecondary, 42, true, pair.getSecond());
        assertGetRpc(hriSecondary, 1042, true, pair.getSecond());
        assertGetRpc(hriSecondary, 2042, true, pair.getSecond());
        assertGetRpc(hriTertiary, 42, true, tertiaryOpenedIn);
        assertGetRpc(hriTertiary, 1042, true, tertiaryOpenedIn);
        assertGetRpc(hriTertiary, 2042, true, tertiaryOpenedIn);
        Threads.sleep(10);
      }

      // ensure that we see the compacted file only
      // This will be 4 until the cleaner chore runs
      Assert.assertEquals(4, secondaryRegion.getStore(f).getStorefilesCount());

    } finally {
      HTU.deleteNumericRows(table, HConstants.CATALOG_FAMILY, 0, 1000);
      closeSecondary(pair.getSecond());
      closeTertiary(tertiaryOpenedIn);
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
  private void assertGetRpc(HRegionInfo info, int value, boolean expect, OpenedIn openedIn)
      throws IOException, org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException {
    byte[] row = Bytes.toBytes(String.valueOf(value));
    Get get = new Get(row);
    ClientProtos.GetRequest getReq = RequestConverter.buildGetRequest(info.getRegionName(), get);
    ClientProtos.GetResponse getResp =  getRegionServer(openedIn).getRSRpcServices().get(null, getReq);
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

  private Region getPrimaryRegion(Pair<OpenedIn, OpenedIn> pair) throws NotServingRegionException {
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

  private HRegionServer getRegionServer(OpenedIn openedIn)
      throws NotServingRegionException {
    HRegionServer regionServer = null;
    switch (openedIn) {
    case PRIMARY:
      // If secondary is opened in Primary
      regionServer = getRS();
      break;
    case SECONDARY:
      regionServer = getSecondaryRS();
      break;
    case TERTIARY:
      regionServer = getTertiaryRS();
      break;
    }
    return regionServer;
  }

  private Region getTertiaryRegion(OpenedIn openedIn) throws NotServingRegionException {
    Region region = null;
    switch (openedIn) {
    case PRIMARY:
      // If secondary is opened in Primary
      region = getRS().getRegionByEncodedName(hriTertiary.getEncodedName());
      break;
    case SECONDARY:
      region = getSecondaryRS().getRegionByEncodedName(hriTertiary.getEncodedName());
      break;
    case TERTIARY:
      region = getTertiaryRS().getRegionByEncodedName(hriTertiary.getEncodedName());
      break;
    }
    return region;
  }

  private Region getSecondaryRegion(Pair<OpenedIn, OpenedIn> pair)
      throws NotServingRegionException {
    Region region = null;
    switch (pair.getSecond()) {
    case PRIMARY:
      // If secondary is opened in Primary
      region = getRS().getRegionByEncodedName(hriSecondary.getEncodedName());
      break;
    case SECONDARY:
      region = getSecondaryRS().getRegionByEncodedName(hriSecondary.getEncodedName());
      break;
    case TERTIARY:
      region = getTertiaryRS().getRegionByEncodedName(hriSecondary.getEncodedName());
      break;
    }
    return region;
  }

  private Pair<OpenedIn, OpenedIn> openSecondary() throws Exception {
    OpenedIn primaryOpenedIn = OpenedIn.PRIMARY;
    OpenedIn secondaryOpenedIn = OpenedIn.SECONDARY;
    Pair<OpenedIn, OpenedIn> pair = new Pair<OpenedIn, OpenedIn>();
    try {
      getRS().getRegion(hriPrimary.getRegionName());
    } catch (NotServingRegionException e) {
      try {
        getSecondaryRS().getRegion(hriPrimary.getRegionName());
        // primary is found in 2nd region server so open secondary in either 1 or 2
        primaryOpenedIn = OpenedIn.SECONDARY;
      } catch (NotServingRegionException e1) {
        // it is found in 3rd regionserver
        try {
          getTertiaryRS().getRegion(hriPrimary.getRegionName());
          primaryOpenedIn = OpenedIn.TERTIARY;
        } catch (NotServingRegionException e2) {
          throw new Exception("Region no where to be found ");
        }
      }
    }
    switch (primaryOpenedIn) {
    case PRIMARY:
      // open in secondary RS
      openRegion(HTU, getSecondaryRS(), hriSecondary);
      secondaryOpenedIn = OpenedIn.SECONDARY;
      break;
    case SECONDARY:
      // so open it in PRIMARY
      openRegion(HTU, getTertiaryRS(), hriSecondary);
      secondaryOpenedIn = OpenedIn.TERTIARY;
      break;
    case TERTIARY:
      openRegion(HTU, getRS(), hriSecondary);
      secondaryOpenedIn = OpenedIn.PRIMARY;
      break;
    }
    pair.setFirst(primaryOpenedIn);
    pair.setSecond(secondaryOpenedIn);
    return pair;
  }

  enum OpenedIn {
    PRIMARY, SECONDARY, TERTIARY;
  }

}
