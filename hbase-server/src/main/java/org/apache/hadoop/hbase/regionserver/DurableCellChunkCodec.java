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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

// this class will encode the cells into the chunk's buffer and also updates the end preamble
@InterfaceAudience.Private
public class DurableCellChunkCodec {

  /**
   * Every cell is written with following format
   * 1) an int representing the len of the cell (excludes seqId)
   * 2) the actual cell
   * Note that to recreate the cell only use the len of the cell and read the seqId
   * per cell to be used while recreating the cells from the chunks
   * @param cell
   * @param offset
   * @param chunkBuffer
   */
  public void encode(ExtendedCell cell, int offset, ByteBuffer chunkBuffer, int length) {
    ByteBufferUtils.putInt(chunkBuffer, offset, length);
    cell.write(chunkBuffer, offset + Bytes.SIZEOF_INT);
    // write the seqId also
    /*ByteBufferUtils.putLong(chunkBuffer, offset + Bytes.SIZEOF_INT + length,
      cell.getSequenceId());*/
    // lets write the end offset at the beginning of the chunk after the chunk id and the inUse byte.
    // TODO : Update this after every persist operation
  }

  public List<Cell> decode(int startOffset, ByteBuffer chunkBuffer, int endOffset) {
    List<Cell> cells = new ArrayList<Cell>();
    int off = startOffset;
    for (; off < endOffset + startOffset;) {
      long seqID = ByteBufferUtils.toLong(chunkBuffer, off);
      off += Bytes.SIZEOF_LONG;
      int noOfCells = ByteBufferUtils.toInt(chunkBuffer, off);
      off += Bytes.SIZEOF_INT;
      int batchStart =  off;
      for (int i = 0; i < noOfCells; i++) {
        int size = ByteBufferUtils.toInt(chunkBuffer, batchStart);
        // get the seqID
        // we need to deepClone here. Otherwise the new chunkCreator for this
        // RS will overwrite and corrupt the exisitng Mnemonic chunk
        Cell cell =
            new ByteBufferKeyValue(chunkBuffer, batchStart + Bytes.SIZEOF_INT, size).deepClone();
        try {
          CellUtil.setSequenceId(cell, seqID);
        } catch (IOException e) {
          // won't haappen
        }
        cells.add(cell);
        batchStart += Bytes.SIZEOF_INT + size;
      }
      off = batchStart;
    }
    return cells;
  }

  public int getCellMetaDataSize(int size) {
    return (size + Bytes.SIZEOF_INT + Bytes.SIZEOF_LONG);
  }

  public int getChunkMetaDataSize(int regionSize) {
    return 3 * Bytes.SIZEOF_INT + Bytes.SIZEOF_BYTE + regionSize;
  }
}
