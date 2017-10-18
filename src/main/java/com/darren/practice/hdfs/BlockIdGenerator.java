package com.darren.parctice.hdfs;

//should be able to get the curMax Id issues.
class BlockIdGenerator {
  private long curId = Integer.MAX_VALUE;

  synchronized long nextId() {
    curId += 1;
    if (curId == Long.MAX_VALUE) curId = Integer.MAX_VALUE; 
    return curId;
  }
}
