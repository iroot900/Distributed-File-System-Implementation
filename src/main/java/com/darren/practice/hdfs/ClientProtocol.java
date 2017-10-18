package com.darren.parctice.hdfs;

import com.darren.parctice.rpc.Protocol;

import java.io.IOException;

public interface ClientProtocol extends Protocol {
  boolean exist(String path) throws IOException;
  boolean isDirectory(String path) throws IOException;
  boolean mkdir(String path) throws IOException;

  LocatedBlock[] openFile(String filePath) throws IOException;

  LocatedBlock startFile(String filePath) throws IOException;
  void blockReceived(LocatedBlock locatedBlock) throws IOException;
  LocatedBlock getAdditionalBlock(String filePath) throws IOException;
  boolean abandonBlock(Block block, String filePath) throws IOException;

  void completeFile(String filePath) throws IOException;
  boolean abandonFileInProgress(String filePath) throws IOException;
  
  boolean delete(String filePath) throws IOException;
}
