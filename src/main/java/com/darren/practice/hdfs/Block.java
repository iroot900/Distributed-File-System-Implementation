package com.darren.parctice.hdfs;

import java.io.Serializable;

public class Block implements Serializable {
  private long blockId;
  private long length;

  public Block(long blockId, long length) {
    this.blockId = blockId;
    this.length = length;
  }

  public Block(long blockId) {
    this(blockId, 0);
  }

  public long getBlockId() {
    return blockId;
  }

  public long getLength() {
    return length;
  }
   
  void setLength(long length) {
    this.length = length;
  }

  @Override 
  public int hashCode() {
    return Long.valueOf(blockId).hashCode();  
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof Block) {
      return blockId == ((Block)that).blockId;
    } else {
      return false;
    }
  }
}
