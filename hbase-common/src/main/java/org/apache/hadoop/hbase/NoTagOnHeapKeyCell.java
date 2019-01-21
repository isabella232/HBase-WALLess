/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
/**
 * A new variety of cell where the key is onheap. Used specifically when we deal with a cell created
 * out of DurableMemstore
 */
public class NoTagOnHeapKeyCell extends ByteBufferKeyValue {

  // the onheap reference of the key
  private final ByteBuffer key;
  private final int keyLength;
  private final long seqId;
  private final short rowLen;

  public NoTagOnHeapKeyCell(ByteBuffer buf, int offset, int length) {
    super(buf, offset, length);
    this.keyLength = getKeyLength();
    this.seqId = ByteBufferUtils.toLong(this.buf, this.offset + this.length);
    key = ByteBuffer.allocate(this.keyLength);
    rowLen = ByteBufferUtils.toShort(this.buf, this.offset + KeyValue.ROW_OFFSET);
    ByteBufferUtils.copyFromBufferToBuffer(buf, key, getKeyOffset(), 0, this.keyLength);
  }

  private int getKeyLength() {
    return ByteBufferUtils.toInt(this.buf, this.offset);
  }

  @Override
  public long getSequenceId() {
    // TODO Auto-generated method stub
    return seqId;
  }

  private int getKeyOffset() {
    return this.offset + KeyValue.ROW_OFFSET;
  }

  @Override
  public byte[] getRowArray() {
    return CellUtil.cloneRow(this);
  }

  @Override
  public int getRowOffset() {
    return 0;
  }

  @Override
  public short getRowLength() {
    return this.rowLen;
  }

  @Override
  public byte[] getFamilyArray() {
    return CellUtil.cloneFamily(this);
  }

  @Override
  public int getFamilyOffset() {
    return 0;
  }

  @Override
  public byte getFamilyLength() {
    return getFamilyLength(getFamilyLengthPosition());
  }

  private int getFamilyLengthPosition() {
    return KeyValue.ROW_LENGTH_SIZE + +this.rowLen;
  }

  private byte getFamilyLength(int famLenPos) {
    return ByteBufferUtils.toByte(this.key, famLenPos);
  }

  @Override
  public byte[] getQualifierArray() {
    return CellUtil.cloneQualifier(this);
  }

  @Override
  public int getQualifierOffset() {
    return 0;
  }

  @Override
  public int getQualifierLength() {
    return getQualifierLength(getRowLength(), getFamilyLength());
  }

  private int getQualifierLength(int rlength, int flength) {
    return this.keyLength - (int) KeyValue.getKeyDataStructureSize(rlength, flength, 0);
  }

  @Override
  public long getTimestamp() {
    int offset = getTimestampOffset(this.keyLength);
    return ByteBufferUtils.toLong(this.key, offset);
  }

  private int getTimestampOffset(int keyLen) {
    return keyLen - KeyValue.TIMESTAMP_TYPE_SIZE;
  }

  @Override
  public byte getTypeByte() {
    return ByteBufferUtils.toByte(this.key, this.keyLength - 1);
  }

  @Override
  public byte[] getValueArray() {
    return CellUtil.cloneValue(this);
  }

  @Override
  public int getValueOffset() {
    return 0;
  }

  @Override
  public int getValueLength() {
    return length - KeyValue.ROW_OFFSET - keyLength;
  }

  @Override
  public byte[] getTagsArray() {
    return CellUtil.cloneTags(this);
  }

  @Override
  public int getTagsOffset() {
    return 0;
  }

  @Override
  public int getTagsLength() {
    return 0;
  }

  @Override
  public ByteBuffer getRowByteBuffer() {
    return this.key;
  }

  @Override
  public int getRowPosition() {
    return KeyValue.ROW_LENGTH_SIZE;
  }

  @Override
  public ByteBuffer getFamilyByteBuffer() {
    return this.key;
  }

  @Override
  public int getFamilyPosition() {
    return getFamilyLengthPosition() + Bytes.SIZEOF_BYTE;
  }

  @Override
  public ByteBuffer getQualifierByteBuffer() {
    return this.key;
  }

  @Override
  public int getQualifierPosition() {
    return getFamilyPosition() + getFamilyLength();
  }

  @Override
  public ByteBuffer getValueByteBuffer() {
    return this.buf;
  }

  @Override
  public int getValuePosition() {
    return this.offset + KeyValue.ROW_OFFSET + this.keyLength;
  }

  @Override
  public ByteBuffer getTagsByteBuffer() {
    return this.buf;
  }

  @Override
  public int getTagsPosition() {
    int tagsLen = getTagsLength();
    if (tagsLen == 0) {
      return this.offset + this.length;
    }
    return this.offset + this.length - tagsLen;
  }

  @Override
  public int write(OutputStream out, boolean withTags) throws IOException {
    int length = getSerializedSize(withTags);
    ByteBufferUtils.copyBufferToStream(out, this.buf, this.offset, length);
    return length;
  }

  @Override
  public int getSerializedSize(boolean withTags) {
    if (withTags) {
      return this.length;
    }
    return this.keyLength + this.getValueLength() + KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE;
  }

  @Override
  public void write(ByteBuffer buf, int offset) {
    ByteBufferUtils.copyFromBufferToBuffer(this.buf, buf, this.offset, offset, this.length);
  }

  @Override
  public String toString() {
    return CellUtil.toString(this, true);
  }

  @Override
  public void setTimestamp(long ts) throws IOException {
    ByteBufferUtils.copyFromArrayToBuffer(this.buf, this.getTimestampOffset(), Bytes.toBytes(ts), 0,
      Bytes.SIZEOF_LONG);
  }

  private int getTimestampOffset() {
    return this.offset + KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE + this.keyLength
        - KeyValue.TIMESTAMP_TYPE_SIZE;
  }

  @Override
  public void setTimestamp(byte[] ts) throws IOException {
    ByteBufferUtils.copyFromArrayToBuffer(this.buf, this.getTimestampOffset(), ts, 0,
      Bytes.SIZEOF_LONG);
  }

  /**
   * Needed doing 'contains' on List. Only compares the key portion, not the value.
   */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Cell)) {
      return false;
    }
    return CellUtil.equals(this, (Cell) other);
  }

  /**
   * In line with {@link #equals(Object)}, only uses the key portion, not the value.
   */
  @Override
  public int hashCode() {
    return calculateHashForKey(this);
  }

  private int calculateHashForKey(ByteBufferExtendedCell cell) {
    int rowHash = ByteBufferUtils.hashCode(cell.getRowByteBuffer(), cell.getRowPosition(),
      cell.getRowLength());
    int familyHash = ByteBufferUtils.hashCode(cell.getFamilyByteBuffer(), cell.getFamilyPosition(),
      cell.getFamilyLength());
    int qualifierHash = ByteBufferUtils.hashCode(cell.getQualifierByteBuffer(),
      cell.getQualifierPosition(), cell.getQualifierLength());

    int hash = 31 * rowHash + familyHash;
    hash = 31 * hash + qualifierHash;
    hash = 31 * hash + (int) cell.getTimestamp();
    hash = 31 * hash + cell.getTypeByte();
    return hash;
  }

  @Override
  public ExtendedCell deepClone() {
    byte[] copy = new byte[this.length];
    ByteBufferUtils.copyFromBufferToArray(copy, this.buf, this.offset, 0, this.length);
    KeyValue kv = new KeyValue(copy, 0, copy.length);
    kv.setSequenceId(this.getSequenceId());
    return kv;
  }

  @Override
  public long heapSize() {
    return super.heapSize() + ClassSize.OBJECT + ClassSize.REFERENCE + ClassSize.align(keyLength)
        + 2 * Bytes.SIZEOF_INT + Bytes.SIZEOF_LONG;
  }
}
