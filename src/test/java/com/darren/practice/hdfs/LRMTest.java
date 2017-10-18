package com.darren.parctice.hdfs;

import org.testng.Assert;
import org.testng.annotations.Test;

public class LRMTest {

  class HeartBeat extends TimedItem {
    HeartBeat() {
      super();
    }
  }

  @Test
  public void Test1() {
    LRM<String, HeartBeat> lrm = new LRM<>();

    //test add    1 3 2
    lrm.put("node1", new HeartBeat()); 
    Assert.assertEquals(lrm.getFirstKey(), "node1");
    lrm.put("node3", new HeartBeat());
    Assert.assertEquals(lrm.getFirstKey(), "node1");
    lrm.put("node2", new HeartBeat());
    Assert.assertEquals(lrm.getFirstKey(), "node1");

    //test update   3 2  1
    HeartBeat heartBeat = lrm.get("node1");
    Assert.assertEquals(lrm.getFirstKey(), "node1");
    //heartBeat.updateTimeStamp(System.currentTimeMillis());
    lrm.put("node1", heartBeat);
    Assert.assertEquals(lrm.getFirstKey(), "node3");

    //mix add update   2 1 3
    heartBeat = lrm.get("node3");
    //heartBeat.updateTimeStamp(System.currentTimeMillis());
    lrm.put("node3", heartBeat);
    Assert.assertEquals(lrm.getFirstKey(), "node2");

    //test first remove 1 3 
    String node2 = lrm.getFirstKey();
    Assert.assertEquals(lrm.remove(node2), true);
    Assert.assertEquals(lrm.getFirstKey(), "node1");

    Assert.assertEquals(lrm.size(), 2);
    //random remove.
    Assert.assertEquals(lrm.remove("node2"), false);
    Assert.assertEquals(lrm.remove("node3"), true);

    Assert.assertEquals(lrm.size(), 1);
    Assert.assertEquals(lrm.getFirstKey(), "node1");

    lrm.remove("node1");
    Assert.assertEquals(lrm.size(), 0);
  }
}
