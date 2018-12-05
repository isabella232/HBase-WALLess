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

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.yetus.audience.InterfaceAudience;

import com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Private
public class ExtendedHRegion extends HRegion {

  private ChunkCreator chunkCreator;

  @VisibleForTesting
  public ExtendedHRegion(HRegionFileSystem fs, WAL wal, Configuration confParam,
      TableDescriptor htd, RegionServerServices rsServices, ChunkCreator chunkCreator) {
    super(fs, wal, confParam, htd, rsServices);
    this.chunkCreator = chunkCreator;
  }

  @VisibleForTesting
  public ExtendedHRegion(final Path tableDir, final WAL wal, final FileSystem fs,
      final Configuration confParam, final RegionInfo regionInfo, final TableDescriptor htd,
      final RegionServerServices rsServices, ChunkCreator chunkCreator) {
    this(new HRegionFileSystem(confParam, fs, tableDir, regionInfo), wal, confParam, htd,
        rsServices, chunkCreator);
  }
  
  @Override
  public ChunkCreator getChunkCreator() {
    if (this.chunkCreator != null) {
      return this.chunkCreator;
    }
    return ChunkCreatorFactory.createChunkCreator(MemStoreLABImpl.CHUNK_SIZE_DEFAULT, false, 0, 0,
      0, null, null);
  }

  /**
   * A utility method to create new instances of HRegion based on the {@link HConstants#REGION_IMPL}
   * configuration property.
   * @param tableDir qualified path of directory where region should be located, usually the table
   *          directory.
   * @param wal The WAL is the outbound log for any updates to the HRegion The wal file is a logfile
   *          from the previous execution that's custom-computed for this HRegion. The HRegionServer
   *          computes and sorts the appropriate wal info for this HRegion. If there is a previous
   *          file (implying that the HRegion has been written-to before), then read it from the
   *          supplied path.
   * @param fs is the filesystem.
   * @param conf is global configuration settings.
   * @param regionInfo - RegionInfo that describes the region is new), then read them from the
   *          supplied path.
   * @param htd the table descriptor
   * @return the new instance
   */
  static HRegion newHRegion(Path tableDir, WAL wal, FileSystem fs, Configuration conf,
      RegionInfo regionInfo, final TableDescriptor htd, RegionServerServices rsServices,
      ChunkCreator chunkCreator) {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends HRegion> regionClass =
          (Class<? extends HRegion>) conf.getClass(HConstants.REGION_IMPL, ExtendedHRegion.class);

      Constructor<? extends HRegion> c = regionClass.getConstructor(Path.class, WAL.class,
        FileSystem.class, Configuration.class, RegionInfo.class, TableDescriptor.class,
        RegionServerServices.class, ChunkCreator.class);

      return c.newInstance(tableDir, wal, fs, conf, regionInfo, htd, rsServices, chunkCreator);
    } catch (Throwable e) {
      // todo: what should I throw here?
      throw new IllegalStateException("Could not instantiate a region instance.", e);
    }
  }

  public static HRegion createHRegion(final RegionInfo info, final Path rootDir,
      final Configuration conf, final TableDescriptor hTableDescriptor, final WAL wal)
      throws IOException {
    return createHRegion(info, rootDir, conf, hTableDescriptor, wal, true, null);
  }

  public static HRegion createHRegion(final RegionInfo info, final Path rootDir,
      final Configuration conf, final TableDescriptor hTableDescriptor, final WAL wal,
      final boolean initialize) throws IOException {
    return createHRegion(info, rootDir, conf, hTableDescriptor, wal, initialize, null);
  }

  public static HRegion createHRegion(final RegionInfo info, final Path rootDir,
      final Configuration conf, final TableDescriptor hTableDescriptor, final WAL wal,
      final boolean initialize, final ChunkCreator chunkCreator) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path tableDir = FSUtils.getTableDir(rootDir, info.getTable());
    HRegionFileSystem.createRegionOnFileSystem(conf, fs, tableDir, info);
    HRegion region =
        newHRegion(tableDir, wal, fs, conf, info, hTableDescriptor, null, chunkCreator);
    if (initialize) region.initialize(null);
    return region;
  }

  public static HRegion openHRegion(Path rootDir, final RegionInfo info, final TableDescriptor htd,
      final WAL wal, final Configuration conf) throws IOException {
    return openHRegion(rootDir, info, htd, wal, conf, null, null);
  }

  /**
   * Open a Region.
   * @param rootDir Root directory for HBase instance
   * @param info Info for region to be opened.
   * @param htd the table descriptor
   * @param wal WAL for region to use. This method will call
   * WAL#setSequenceNumber(long) passing the result of the call to
   * HRegion#getMinSequenceId() to ensure the wal id is properly kept
   * up.  HRegionStore does this every time it opens a new region.
   * @param conf The Configuration object to use.
   * @param rsServices An interface we can request flushes against.
   * @param reporter An interface we can report progress against.
   * @return new HRegion
   * @throws IOException
   */
  public static HRegion openHRegion(final Path rootDir, final RegionInfo info,
      final TableDescriptor htd, final WAL wal, final Configuration conf,
      final RegionServerServices rsServices, final CancelableProgressable reporter)
      throws IOException {
    FileSystem fs = null;
    if (rsServices != null) {
      fs = rsServices.getFileSystem();
    }
    if (fs == null) {
      fs = FileSystem.get(conf);
    }
    return openHRegion(conf, fs, rootDir, info, htd, wal, rsServices, reporter);
  }

  /**
   * Open a Region.
   * @param conf The Configuration object to use.
   * @param fs Filesystem to use
   * @param rootDir Root directory for HBase instance
   * @param info Info for region to be opened.
   * @param htd the table descriptor
   * @param wal WAL for region to use. This method will call WAL#setSequenceNumber(long) passing the
   *          result of the call to HRegion#getMinSequenceId() to ensure the wal id is properly kept
   *          up. HRegionStore does this every time it opens a new region.
   * @return new HRegion
   * @throws IOException
   */
  public static HRegion openHRegion(final Configuration conf, final FileSystem fs,
      final Path rootDir, final RegionInfo info, final TableDescriptor htd, final WAL wal)
      throws IOException {
    return openHRegion(conf, fs, rootDir, info, htd, wal, null, null);
  }

  /**
   * Open a Region.
   * @param conf The Configuration object to use.
   * @param fs Filesystem to use
   * @param rootDir Root directory for HBase instance
   * @param info Info for region to be opened.
   * @param htd the table descriptor
   * @param wal WAL for region to use. This method will call WAL#setSequenceNumber(long) passing the
   *          result of the call to HRegion#getMinSequenceId() to ensure the wal id is properly kept
   *          up. HRegionStore does this every time it opens a new region.
   * @param rsServices An interface we can request flushes against.
   * @param reporter An interface we can report progress against.
   * @return new HRegion
   * @throws IOException
   */
  public static HRegion openHRegion(final Configuration conf, final FileSystem fs,
      final Path rootDir, final RegionInfo info, final TableDescriptor htd, final WAL wal,
      final RegionServerServices rsServices, final CancelableProgressable reporter)
      throws IOException {
    Path tableDir = FSUtils.getTableDir(rootDir, info.getTable());
    return openHRegion(conf, fs, rootDir, tableDir, info, htd, wal, rsServices, reporter);
  }

  public static HRegion openHRegion(final Configuration conf, final FileSystem fs,
      final Path rootDir, final Path tableDir, final RegionInfo info, final TableDescriptor htd,
      final WAL wal, final RegionServerServices rsServices, final CancelableProgressable reporter)
      throws IOException {
    if (info == null) throw new NullPointerException("Passed region info is null");
    HRegion r = newHRegion(tableDir, wal, fs, conf, info, htd, rsServices, null);
    return r.openHRegion(reporter);
  }
}
