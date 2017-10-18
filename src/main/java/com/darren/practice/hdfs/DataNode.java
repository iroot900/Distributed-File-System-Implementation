package com.darren.parctice.hdfs;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.darren.parctice.rpc.RPC;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetSocketAddress;

import java.io.IOException;
import java.io.InputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedInputStream;

import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.Files;

import java.util.List;
import java.util.ArrayList;

/**
 * DataNode should do simple things. 
 * Write block and Read block. Other logic should be maintain in nameNode
 * Join and Report blocks.
 * Stateful but logicless.
 * 2. send heartbeat 2.send block report 3. receive cmd. invalidate block...
 */
public class DataNode {
  static private final Logger logger = LogManager.getLogger(DataNode.class);
  static private final byte OP_READ_BLOCK = 0;
  static private final byte OP_WRITE_BLOCK = 1;
  static private final long HEART_BEAT_INTERVAL = 1000 * 2;
  static private final long BLOCK_REPORT_INTERVAL =  1000 * 60 * 60 * 6; 
  private boolean shouldRun = true;

  private DatanodeProtocol datanodeProtocol = null;
  private InetSocketAddress namenodeAddress = null;
  private DataNodeId dataNodeId = null;

  private int port;
  private String dir;

  private FSDataSet dataSet = null; 

  private DataXceiveServer dataXceiveServer = null;

  //TODO : should use a config to start -- hard code namenode information. for now
  public DataNode(int port, String dir, boolean format, InetSocketAddress namenodeAddress) throws IOException {
    this.port = port;
    this.dir = dir;
    dataSet = new FSDataSet(dir, format);
    dataXceiveServer = new DataXceiveServer(port);

    this.namenodeAddress = namenodeAddress; 
    this.datanodeProtocol = (DatanodeProtocol) RPC.getProxy(DatanodeProtocol.class, namenodeAddress);
    this.dataNodeId = new DataNodeId("localhost", port); 
  }

  public DataNode(int port, String dir, InetSocketAddress namenodeAddress) throws IOException {
    this.port = port;
    this.dir = dir;
    dataSet = new FSDataSet(dir, true);
    dataXceiveServer = new DataXceiveServer(port);

    this.namenodeAddress = namenodeAddress; 
    this.datanodeProtocol = (DatanodeProtocol) RPC.getProxy(DatanodeProtocol.class, namenodeAddress);
    this.dataNodeId = new DataNodeId("localhost", port); 
  }

  public void stop() {
    try {
      shouldRun = false;
      dataXceiveServer.interrupt();
    } catch (Exception e) {
      logger.warn("error while shutdown service ", e);
    }
  }

  void run() {
    while (shouldRun) { //reader only 
      try {
        offerService();
      } catch (Exception e) {
        logger.warn("unknow error ", e);
      }
    }
  }


  void offerService() throws Exception {
    //first need to do a block report.  -- must do a success block report .. 
    long lastHeartBeatStamp = System.currentTimeMillis();
    long lastBlockReportStamp = 0; 

    while (shouldRun) {
      //HEART_BEAT_INTERVAL
      long currentTimeStamp = System.currentTimeMillis();
      if (currentTimeStamp - lastBlockReportStamp > BLOCK_REPORT_INTERVAL) {

        try {

          Block[]  blocksOnDataNode = dataSet.getBlockReport();
          datanodeProtocol.blockReport(blocksOnDataNode, dataNodeId);
          lastBlockReportStamp = currentTimeStamp;
          lastHeartBeatStamp = currentTimeStamp;
        } catch (IOException e) {
          //block report fail ?  what to do ?
          logger.warn("block report went wrong", e);
          Thread.sleep(1000);
        }

      } else if (currentTimeStamp - lastHeartBeatStamp > HEART_BEAT_INTERVAL) {

        try  {
          Block[] blocksToRemove = datanodeProtocol.sendHeartBeat(dataNodeId);
          lastHeartBeatStamp = currentTimeStamp;
          if (blocksToRemove == null) continue;

          List<Block> blocksRemovedOrNotExist = new ArrayList<>();
          for (Block block : blocksToRemove) {
            try {
                dataSet.removeBlock(block.getBlockId());
            } catch (IOException ie) {
              logger.info("io error when remove " + block.getBlockId(), ie);
            }
            blocksRemovedOrNotExist.add(block);
          }
          datanodeProtocol.blockRemoved(blocksRemovedOrNotExist.toArray(new Block[0]), dataNodeId);  
        } catch (IOException e) { //heartbeat not send out.
          logger.warn("heart beat went wrong", e);
        }

      } else {
        Thread.sleep(HEART_BEAT_INTERVAL);
      }

    }
  }

  public void start() {
    dataXceiveServer.start();
    run();
  }

  public void join() throws InterruptedException{
    dataXceiveServer.join();
  }

  private class DataXceiveServer extends Thread {
    private ServerSocket serverSocket = null;
    private int port;

    public DataXceiveServer(int port) throws IOException {
      this.port = port;
      serverSocket = new ServerSocket(port);
    }

    @Override
    public void run() {
      logger.info("DataNode on " + port + " started");
      while (shouldRun) {
        try {
            Socket socket = serverSocket.accept();
            DataXceiver dataXceiver = new DataXceiver(socket);
            dataXceiver.setDaemon(true);
            dataXceiver.start();
        } catch (IOException e) {
          //one accept broke or acceptor down?
          e.printStackTrace();
          logger.warn("One connection broke while accept incoming client connection", e);
        }
      }
    }
  }

  private class DataXceiver extends Thread {
    private Socket socket = null;
    private DataInputStream in = null; 
    private DataOutputStream out = null;
    static private final int MAX_CHUNK_SIZE = 1024;
    
    public DataXceiver (Socket socket) throws IOException {
      this.socket = socket;
      this.in = new DataInputStream(socket.getInputStream());
      this.out = new DataOutputStream(socket.getOutputStream());
    }

    void writeBlock() throws IOException, ClassNotFoundException {
      long block_id = in.readLong(); //blockId. and also need to write to others maybe. -- if end or not
      logger.info("start write block " + block_id + " on " + socket);
      int numTarget = in.readInt(); 
      ObjectInputStream objIn = new ObjectInputStream(in);
      DataNodeId[] dataNodeIds = new DataNodeId[numTarget];
      for (int i = 0; i < numTarget; ++i) {
        dataNodeIds[i] = (DataNodeId) objIn.readObject();
      }

      //get downstream + local disk stream

      DataInputStream downStreamIn = null;
      ObjectOutputStream downStreamOutO = null;
      DataOutputStream downStreamOutD = null;
      Socket downStreamSocket = null; 

      if (numTarget > 1) {
        downStreamSocket = new Socket(dataNodeIds[1].getAddress(), dataNodeIds[1].getPort()); 
        downStreamIn = new DataInputStream(downStreamSocket.getInputStream());
        downStreamOutD = new DataOutputStream(downStreamSocket.getOutputStream());

        //set up downStream
        downStreamOutD.writeByte(OP_WRITE_BLOCK);
        downStreamOutD.writeLong(block_id);
        downStreamOutD.writeInt(numTarget - 1);
        downStreamOutD.flush();
        downStreamOutO = new ObjectOutputStream(downStreamSocket.getOutputStream());
        for (int i = 1; i < numTarget; ++i) {
          downStreamOutO.writeObject(dataNodeIds[i]);
        }
        downStreamOutO.flush();
        downStreamOutO = null;
      }
      
      BufferedOutputStream localStreamOut = new BufferedOutputStream(dataSet.writeBlock(block_id));
      
      int numBytes = 0;
      byte[] buffer = new byte[MAX_CHUNK_SIZE];
      //should read a chunk, then another chunk, in this way we'll if an end is reached.
      while ((numBytes = in.readInt()) != -1) {
        if (numBytes < 0 || numBytes > MAX_CHUNK_SIZE) { 
          throw new IOException("data transfer chunk length can't be negative or greater than " + MAX_CHUNK_SIZE);
        }

        int numReads = 0;
        while (numBytes > 0) {
          int curNumReads = in.read(buffer, numReads, numBytes);
          numBytes -= curNumReads; 
          numReads += curNumReads;
        }
          
        localStreamOut.write(buffer, 0, numReads);
        if (downStreamOutD != null) {
            downStreamOutD.writeInt(numReads);
            downStreamOutD.write(buffer, 0, numReads);
        }
      }
      
      localStreamOut.flush();
      localStreamOut.close();
      if (downStreamOutD != null) {
        downStreamOutD.writeInt(-1);
        downStreamOutD.flush();
      }

      //all the transfer is done, wait for response if any; -- 
      //if just status, then we done really have to -- denote done or not. broken will have exception fine! 
      byte STATUS = 1; 
      if (downStreamIn != null) { //need to go to down stream;
        STATUS = downStreamIn.readByte();  
      }

      if (STATUS == 1) {
        dataSet.addBlock(block_id);
      }
      out.write(STATUS);
      out.flush();
      in.close();
      out.close();
    }

    void readBlock() throws IOException { //could read from a offset. --- block went bad or datanode went bad ?
      long block_id = in.readLong(); //
      long offset = in.readLong(); 
      long length = in.readLong();

      Block block = dataSet.getBlock(block_id);
      if (block == null) {
        throw new IOException("no block " + block_id);
      }

      if ((offset < 0 || length <= 0) || offset + length > block.getLength()) {
        throw new IOException("out of file range!");
      }

      BufferedInputStream localBlockStream = new BufferedInputStream(dataSet.readBlock(block_id));
      localBlockStream.skip(offset);

      //write length then write something.    
      byte[] buffer = new byte[MAX_CHUNK_SIZE];
      int totalSent = 0;
      while (totalSent < length) {
        int numReads = 0;
        int numBytesToRead = (int)Math.min((long)MAX_CHUNK_SIZE, length - totalSent);
        while (numReads < numBytesToRead) {
          int curNumReads = localBlockStream.read(buffer, numReads, numBytesToRead - numReads);
          if (curNumReads == -1) {
            throw new IOException ("end of file");
          }
          numReads += curNumReads;
        }
          
        //-- we know how many we need. can just sent it over.
        //out.writeInt(numReads);
        out.write(buffer, 0, numReads);
        totalSent += numReads;
      }
      localBlockStream.close();
      out.flush();
      out.close();
    }

    @Override
    public void run() {
      logger.info("got connection from" + socket);
      byte op = -1;
      try {
        op = in.readByte(); 
        System.out.println(op);
        if (op == OP_READ_BLOCK) {
          readBlock();
        } else if (op == OP_WRITE_BLOCK) { //write a block. what do we know -- pipeline
          writeBlock();
        } else { //not valid operation - should fail and notify client?
          logger.debug("invalid op code from client");
        }
      } catch (IOException e) { //IO exception while streaming data

        if (op == -1) {
             logger.warn("connection broke for datanode op", e);
        } else logger.warn("error while handle block  " + (op == 0 ? " read " : " write  "), e);

      } catch (Exception e) {
        
      } finally {
        //probabaly should do file clean up here
      }
    }
  }

  static public void main(String[] args) throws IOException, InterruptedException {
    DataNode dataNode = new DataNode(9988, "datanode_test", true, new InetSocketAddress("localhost", 9900));
    dataNode.start();
    dataNode.join();
  }
}
