package com.darren.parctice.hdfs;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

import com.darren.parctice.rpc.RPC;

import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.net.Socket;
import java.net.InetSocketAddress;

public class DataNodeTest {
  private DataNode dataNode1 = null;
  private DataNode dataNode2 = null; 
  private DataNode dataNode3 = null;
  private DataNodeId[] dataNodeIds = new DataNodeId[3];
  private NameNode namenode = null; 

  @BeforeClass
  public void initDataNodes() throws IOException, InterruptedException {
    namenode = new NameNode("./namenodeTest", true, 9999);
    namenode.start();
    Thread.sleep(1000 * 1);

    InetSocketAddress namenodeAddress = new InetSocketAddress("localhost", 9999);
    dataNode1 = new DataNode(9991, "datanode1", namenodeAddress); 
    dataNode2 = new DataNode(9992, "datanode2", namenodeAddress);
    dataNode3 = new DataNode(9993, "datanode3", namenodeAddress); 
    dataNodeIds[0] = new DataNodeId("localhost", 9991);
    dataNodeIds[1] = new DataNodeId("localhost", 9992);
    dataNodeIds[2] = new DataNodeId("localhost", 9993);
    new DataNodeThread(dataNode1).start();
    new DataNodeThread(dataNode2).start();
    new DataNodeThread(dataNode3).start();
    Thread.sleep(1000 * 1);
  }

  static private class DataNodeThread extends Thread {
    private DataNode dataNode;

    public DataNodeThread(DataNode dataNode) {
      this.dataNode = dataNode;
    }

    @Override
    public void run() {
      try {
        dataNode.start();
        dataNode.join();
      } catch (Exception e) {
        System.out.println("shit happens");
      }
    }
  }

  @Test 
  public void testDataNodeService() throws IOException,InterruptedException { //
    InetSocketAddress namenodeAddress = new InetSocketAddress("localhost", 9999);
    //datanode should be able to register -- sending block report.
    ClientProtocol client = (ClientProtocol) RPC.getProxy(ClientProtocol.class, namenodeAddress);

    //block report is done for start creation
    LocatedBlock locatedBlock1 = client.startFile("/file1");
    client.blockReceived(locatedBlock1);
    client.completeFile("/file1");
    Assert.assertEquals(namenode.getBlockNumber(), 1);
    LocatedBlock[] locatedBlocks = client.openFile("/file1");
    Assert.assertNotEquals(locatedBlocks, null);

    //heatbeat and removeblock to get deletedblock done
    client.delete("/file1");
    Thread.sleep(1000 * 3);
    Assert.assertEquals(namenode.getBlockNumber(), 0);
  }

  @Test
  public void testWriteAndRead() throws IOException {
    Socket socket = new Socket("localhost", 9991);
    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
    DataInputStream in = new DataInputStream(socket.getInputStream());


    out.writeByte(1);
    out.writeLong(12345);
    out.writeInt(3); 
    out.flush();
    ObjectOutputStream outO = new ObjectOutputStream(out);
    for (int i = 0; i < 3; ++i) {
      outO.writeObject(dataNodeIds[i]);
    }   
    outO.flush();
    outO = null;

    for (int i = 0; i < 10;  ++i) {
        String line = "line" + i + "\n";
        byte[] bytes = line.getBytes();
        out.writeInt(bytes.length);
        out.write(bytes);
    }
    out.writeInt(-1);
    int STATUS = in.readByte();
    out.close();
    socket.close();
    Assert.assertEquals(STATUS, 1);

    socket = new Socket("localhost", 9992);
    out = new DataOutputStream(socket.getOutputStream());
    in = new DataInputStream(socket.getInputStream());
    out.writeByte(0);
    out.writeLong(12345);
    out.writeLong(6);
    out.writeLong(12);
    out.flush();

    String expectedReadBytes = "line1\nline2\n";
    byte[] bts = expectedReadBytes.getBytes();
    for (int i = 0; i < 12; ++i) {
        Assert.assertEquals(bts[i], in.read());
    }

    socket.close();
  }
}
