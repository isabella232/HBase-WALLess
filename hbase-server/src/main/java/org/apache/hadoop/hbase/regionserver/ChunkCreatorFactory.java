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

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class ChunkCreatorFactory {

  // TODO better name?
  public static final String MSLAB_DURABLE_PATH_KEY = "hbase.memstore.mslab.durable.path";

  private static ChunkCreator chunkCreator = null;

  private ChunkCreatorFactory() {
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "LI_LAZY_INIT_STATIC",
      justification = "Method is called by single thread at the starting of RS")
  public synchronized static void createChunkCreator(int chunkSize, boolean offheap,
      long globalMemStoreSize, float poolSizePercentage, float initialCountPercentage,
      HeapMemoryManager heapMemoryManager, String durablePath) {
    if (durablePath != null) {
      assert offheap;// When working with Durbale chunks, it has to be marked as off heap.
      if (poolSizePercentage != 1.0 || initialCountPercentage != 1.0) {
        // When Durable chunks in place, we need to have the entire global memstore size has to be
        // from pool.
        throw new RuntimeException(
            "When Durable chunks in place, we need to have the entire global memstore size has"
                + " to be from pool");
      }
      chunkCreator = new DurableChunkCreator(chunkSize, globalMemStoreSize, durablePath);
    } else {
      chunkCreator = new ChunkCreator(chunkSize, offheap, globalMemStoreSize, poolSizePercentage,
          initialCountPercentage, heapMemoryManager,
          MemStoreLABImpl.INDEX_CHUNK_PERCENTAGE_DEFAULT);
    }
    ChunkCreator.instance = chunkCreator;
  }

  public static ChunkCreator getChunkCreator() {
    return chunkCreator;
  }
}
