package com.darren.parctice.hdfs;

import java.io.Serializable;

public class LocatedBlock implements Serializable {
  private Block block;
  private DataNodeId[] dataNodeIds;

  public LocatedBlock(Block block, DataNodeId[] dataNodeIds) {
    this.block = block;
    this.dataNodeIds = dataNodeIds;
  }

  public Block getBlock() {
    return block;
  }

  public DataNodeId[] getDataNodeIds() {
    return dataNodeIds;
  }
}

