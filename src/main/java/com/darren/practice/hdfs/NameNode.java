package com.darren.parctice.hdfs;

import com.darren.parctice.rpc.Protocol;
import com.darren.parctice.rpc.RPC;
import com.darren.parctice.rpc.Server;

import java.util.Set;

import java.io.IOException;

public class NameNode implements ClientProtocol, DatanodeProtocol {
  private FSNamesystem fsNameSystem = null;
  private int port;
  Server RPCServer = null; 

  NameNode(String dir, boolean format, int port) throws IOException {
    this.fsNameSystem = new FSNamesystem(dir, format);
    this.port = port;
    this.RPCServer = RPC.getProtocolServer(port, this);
  }

  void start() {
    RPCServer.start();
  }

  void stop() throws IOException {
    RPCServer.stop();
    fsNameSystem.close();
  }

  long getBlockNumber() {
    Set<Block> blocks = fsNameSystem.getAllBlocks(); 
    return blocks == null ? 0 : blocks.size();
  }

  @Override
  public boolean exist(String path) throws IOException {
    return fsNameSystem.exist(path);
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    return fsNameSystem.isDirectory(path);
  }

  @Override
  public boolean mkdir(String path) throws IOException {
    return fsNameSystem.mkdir(path);
  }

  @Override
  public LocatedBlock[] openFile(String filePath) throws IOException {
    return fsNameSystem.openFile(filePath);
  }

  @Override
  public LocatedBlock startFile(String filePath) throws IOException {
    return fsNameSystem.startFile(filePath);
  }

  @Override
  public void blockReceived(LocatedBlock locatedBlock) throws IOException {
    fsNameSystem.blockReceived(locatedBlock);
  }

  @Override
  public LocatedBlock getAdditionalBlock(String filePath) throws IOException {
    return fsNameSystem.getAdditionalBlock(filePath);
  }

  @Override
  public boolean abandonBlock(Block block, String filePath) throws IOException {
    return fsNameSystem.abandonBlock(block, filePath);
  }

  @Override
  public void completeFile(String filePath) throws IOException {
    fsNameSystem.completeFile(filePath);
  }

  @Override
  public boolean abandonFileInProgress(String filePath) throws IOException {
    return fsNameSystem.abandonFileInProgress(filePath);
  }
  
  @Override
  public boolean delete(String filePath) throws IOException {
    return fsNameSystem.delete(filePath);
  }


  //DatanodeProtocol
   public Block[] sendHeartBeat(DataNodeId dataNodeId) throws IOException {
     Block[] blocks = fsNameSystem.gotHeartBeat(dataNodeId);
     return blocks;
   }

   public void blockReport(Block[] blocks, DataNodeId dataNodeId) throws IOException {
     fsNameSystem.processReport(blocks, dataNodeId);
   }

   public void blockRemoved(Block[] blocks, DataNodeId dataNodeId) throws IOException {
     fsNameSystem.removeStoreBlock(blocks, dataNodeId);
   }
}
