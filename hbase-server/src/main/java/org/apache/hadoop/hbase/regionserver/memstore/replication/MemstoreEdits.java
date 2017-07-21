/**
 *
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
package org.apache.hadoop.hbase.regionserver.memstore.replication;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * This is similar to WALEdit, but mainly encapsulates all the cells corresponding to a transaction
 * that goes into the memstore. TODO : USe this for now
 */
@InterfaceAudience.Private
public class MemstoreEdits {
  private static final Log LOG = LogFactory.getLog(MemstoreEdits.class);
  private ArrayList<Cell> cells = null;

  public MemstoreEdits(int cellCount) {
    cells = new ArrayList<>(cellCount);
  }

  public MemstoreEdits() {
    cells = new ArrayList<>();
  }

  public MemstoreEdits add(Cell cell) {
    this.cells.add(cell);
    return this;
  }

  public boolean isEmpty() {
    return cells.isEmpty();
  }

  public int size() {
    return cells.size();
  }

  public ArrayList<Cell> getCells() {
    return cells;
  }

  public long heapSize() {
    long ret = ClassSize.ARRAYLIST;
    for (Cell cell : cells) {
      ret += CellUtil.estimatedHeapSizeOf(cell);
    }
    return ret;
  }
}
