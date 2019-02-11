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
package org.apache.hadoop.hbase.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class KeyValueCodecWithTagsAndSeqNo implements Codec {

  public static class KeyValueEncoder extends KeyValueCodecWithTags.KeyValueEncoder {

    public KeyValueEncoder(OutputStream out) {
      super(out);
    }

    @Override
    public void write(Cell cell) throws IOException {
      super.write(cell);
      // Write seqNo
      // TODO add putInt kind of optimized path...
      ByteBufferUtils.putLong(out, cell.getSequenceId(), Bytes.SIZEOF_LONG);
    }
  }

  public static class KeyValueDecoder extends KeyValueCodecWithTags.KeyValueDecoder {

    public KeyValueDecoder(InputStream in) {
      super(in);
    }

    @Override
    protected Cell parseCell() throws IOException {
      Cell cell = super.parseCell();
      long seqNo = StreamUtils.readLong(in);
      CellUtil.setSequenceId(cell, seqNo);
      return cell;
    }
  }

  public static class ByteBuffKeyValueDecoder extends KeyValueCodecWithTags.ByteBuffKeyValueDecoder {

    public ByteBuffKeyValueDecoder(ByteBuff buf) {
      super(buf);
    }

    @Override
    protected Cell createCell(byte[] buf, int offset, int len) {
      Cell cell = super.createCell(buf, offset, len);
      setSeqNo(cell);
      return cell;
    }

    @Override
    protected Cell createCell(ByteBuffer bb, int pos, int len) {
      Cell cell = super.createCell(bb, pos, len);
      setSeqNo(cell);
      return cell;
    }

    private void setSeqNo(Cell cell) {
      long seqNo = this.buf.getLong();
      try {
        CellUtil.setSequenceId(cell, seqNo);
      } catch (IOException e) {
        // Will never get to this.
      }
    }
  }

  @Override
  public Decoder getDecoder(InputStream is) {
    return new KeyValueDecoder(is);
  }

  @Override
  public Decoder getDecoder(ByteBuff buf) {
    return new ByteBuffKeyValueDecoder(buf);
  }

  @Override
  public Encoder getEncoder(OutputStream os) {
    return new KeyValueEncoder(os);
  }
}