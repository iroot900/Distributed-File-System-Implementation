package com.darren.parctice.hdfs;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.darren.parctice.rpc.RPC;

import java.net.InetSocketAddress;
import java.io.IOException;

public class NameNodeTest {

  @Test
  public void startStopTest() throws IOException {
    NameNode namenode = new NameNode("./namenodeTest", true, 8080);
    namenode.start();
    namenode.stop();
  }

  @Test
  public void clientProtocolTest() throws IOException {
    NameNode namenode = new NameNode("./namenodeTest", true, 8081);
    namenode.start();

    InetSocketAddress address = new InetSocketAddress("localhost", 8081);
    ClientProtocol client = (ClientProtocol) RPC.getProxy(ClientProtocol.class, address);


    DatanodeProtocol datanode1 = (DatanodeProtocol) RPC.getProxy(DatanodeProtocol.class, address);

    DataNodeId[] dataNodeIds = new DataNodeId[3];
    dataNodeIds[0] = new DataNodeId("localhost", 9981);
    dataNodeIds[1] = new DataNodeId("localhost", 9982);
    dataNodeIds[2] = new DataNodeId("localhost", 9983);

    LocatedBlock[] fileBlcoks = client.openFile("/missing_file");
    Assert.assertEquals(fileBlcoks, null);

    //add three2 datanode

    datanode1.blockReport(new Block[0], dataNodeIds[0]);
    datanode1.blockReport(new Block[0], dataNodeIds[1]);
    datanode1.blockReport(new Block[0], dataNodeIds[1]);

    client.mkdir("/dir");
    try {
      LocatedBlock locatedBlock1 = client.startFile("/dir/file1");
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    //data anothere datanode
    datanode1.blockReport(new Block[0], dataNodeIds[2]);
    LocatedBlock locatedBlock1 = client.startFile("/dir/file1");
    Assert.assertEquals(locatedBlock1.getDataNodeIds().length, 3);

    fileBlcoks = client.openFile("/dir/file1");
    Assert.assertEquals(fileBlcoks, null);

    client.blockReceived(locatedBlock1);
    //complete the file
    client.completeFile("/dir/file1");

    fileBlcoks = client.openFile("/dir/file1");
    Assert.assertEquals(fileBlcoks.length, 1);


    //then delete a file
    client.delete("/dir/file1");
    Assert.assertEquals(client.exist("/dir/file1"), false);
    namenode.stop();
  }
}
