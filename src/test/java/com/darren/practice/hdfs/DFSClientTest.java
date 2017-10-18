package com.darren.parctice.hdfs;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

import java.net.InetSocketAddress;
import java.io.IOException;

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

public class DFSClientTest {
  private DataNode dataNode1 = null;
  private DataNode dataNode2 = null; 
  private DataNode dataNode3 = null;
  private DataNodeId[] dataNodeIds = new DataNodeId[3];
  private NameNode namenode = null; 
  private String testFile = "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20";

  @BeforeClass
  public void initDataNodes() throws IOException, InterruptedException {
    namenode = new NameNode("./namenodeTest", true, 9909);
    namenode.start();
    Thread.sleep(1000 * 1);

    InetSocketAddress namenodeAddress = new InetSocketAddress("localhost", 9909);
    dataNode1 = new DataNode(9901, "datanode1", namenodeAddress); 
    dataNode2 = new DataNode(9902, "datanode2", namenodeAddress);
    dataNode3 = new DataNode(9903, "datanode3", namenodeAddress); 
    dataNodeIds[0] = new DataNodeId("localhost", 9901);
    dataNodeIds[1] = new DataNodeId("localhost", 9902);
    dataNodeIds[2] = new DataNodeId("localhost", 9903);
    new DataNodeThread(dataNode1).start();
    new DataNodeThread(dataNode2).start();
    new DataNodeThread(dataNode3).start();
    Thread.sleep(1000 * 2);
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
  public void outputStreamTest() throws IOException { //
    InetSocketAddress namenodeAddress = new InetSocketAddress("localhost", 9909);
    DFSClient dfsClient = new DFSClient(namenodeAddress);
    OutputStream fileOutSteam = dfsClient.newOutputStream("/file1");

    byte[] bytes = testFile.getBytes();

    for (int i = 0; i < bytes.length; ++i) {
      fileOutSteam.write(bytes[i]);
    }
    fileOutSteam.close();

    Assert.assertEquals(dfsClient.exist("/file1"), true);

    fileOutSteam = dfsClient.newOutputStream("/file2");
    fileOutSteam.write(49);
    fileOutSteam.write(50);
    fileOutSteam.close();
    Assert.assertEquals(dfsClient.exist("/file2"), true);
  }


  @Test(dependsOnMethods={"outputStreamTest"})
  public void inputStreamTest() throws IOException { //
    InetSocketAddress namenodeAddress = new InetSocketAddress("localhost", 9909);
    DFSClient dfsClient = new DFSClient(namenodeAddress);
    InputStream fileInputStream = dfsClient.newInputStream("/file1");
    byte[] bytes = testFile.getBytes();
    for (int i = 0; i < bytes.length; ++i) {
      Assert.assertEquals(fileInputStream.read(), bytes[i]);
    }
    System.out.println();
    fileInputStream.close();
  }

  @Test(dependsOnMethods={"inputStreamTest"})
  public void restartTest() throws IOException, InterruptedException { //
    namenode.stop();
    namenode = new NameNode("./namenodeTest", false, 9919);
    namenode.start();
    Thread.sleep(1000 * 1);
    dataNode1.stop();
    dataNode2.stop();
    dataNode3.stop();

    InetSocketAddress namenodeAddress = new InetSocketAddress("localhost", 9919);
    dataNode1 = new DataNode(9911, "datanode1", false, namenodeAddress); 
    dataNode2 = new DataNode(9912, "datanode2", false, namenodeAddress);
    dataNode3 = new DataNode(9913, "datanode3", false, namenodeAddress); 
    dataNodeIds[0] = new DataNodeId("localhost", 9911);
    dataNodeIds[1] = new DataNodeId("localhost", 9912);
    dataNodeIds[2] = new DataNodeId("localhost", 9913);
    new DataNodeThread(dataNode1).start();
    new DataNodeThread(dataNode2).start();
    new DataNodeThread(dataNode3).start();
    Thread.sleep(1000 * 3);

    DFSClient dfsClient = new DFSClient(namenodeAddress);
    InputStream fileInputStream = dfsClient.newInputStream("/file1");
    byte[] bytes = testFile.getBytes();
    for (int i = 0; i < bytes.length; ++i) {
      Assert.assertEquals(fileInputStream.read(), bytes[i]);
    }
    System.out.println();
    fileInputStream.close();
   
    OutputStream fileOutSteam = dfsClient.newOutputStream("/file3");

    for (int i = 0; i < bytes.length; ++i) {
      fileOutSteam.write(bytes[i]);
    }
    fileOutSteam.close();

    Assert.assertEquals(dfsClient.exist("/file3"), true);

  }

}

