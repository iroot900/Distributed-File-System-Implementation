package com.darren.parctice.hdfs;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.darren.parctice.util.Util;

import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;

import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.DirectoryStream;

import java.util.List;
import java.util.ArrayList;

/*
 * FSDataSet manages the current block files in local directory
 * //Let's do this append in a silly way. if you append, basically let's create another block.
 * scan to report current valid block. and received block to invalid. plus scan underconstruct, how could we know if we should delete it?
 */

public class FSDataSet {
  private String dir;
  static private final String cur = "cur"; 
  static private final String tmp = "tmp"; 

  private Path curPath = null;
  private Path tmpPath = null;
  private Path dirPath = null;

  public FSDataSet (String dir) throws IOException {
    this(dir, false);
  }

  public FSDataSet (String dir, boolean shouldFormat) throws IOException {

    this.dir = dir;
    curPath = Paths.get(dir, cur);
    tmpPath = Paths.get(dir, tmp);
    dirPath = Paths.get(dir);

    if (shouldFormat) {
      format(dir);
    }

    if (!Files.isDirectory(dirPath) || !Files.isDirectory(curPath) || !Files.isDirectory(tmpPath)) {
      throw new IOException("No valid directory for FSDataSet");
    }
  }
  
  public void format(String dir) throws IOException {
    Util.deleteDirectory(Paths.get(dir));
    Files.createDirectory(dirPath);
    Files.createDirectory(curPath);
    Files.createDirectory(tmpPath);
  }

  synchronized Block[] getBlockReport() throws IOException {
    DirectoryStream<Path> directoryStream = Files.newDirectoryStream(curPath, "**block_[0-9]*");
    List<Block> blocks = new ArrayList<>();

    for (Path path : directoryStream) {
      String blockIdStr = path.toString().replaceFirst(".*block_","");
      long fileSize = Files.size(path);
      blocks.add(new Block(Long.parseLong(blockIdStr), fileSize));
    }

    return blocks.toArray(new Block[0]);
  }

  //--------//
  /*CRUD*/
  synchronized public Block addBlock(long blockId) throws IOException {//no tmp or cur exist -- should give exception. 
    Path curBlockPath = getCurBlockPath(blockId);
    Path tmpBlockPath = getTmpBlockPath(blockId);
    try {
        Files.move(tmpBlockPath, curBlockPath);
        Block block = new Block(blockId, Files.size(curBlockPath));
        return block;
    } catch (Exception e) {
      throw new IOException();
    }
  }

  //should move, then delete
  synchronized public Block removeBlock(long blockId) throws IOException {//might not exist
    if (!exist(blockId)) return null;
    try { 
        Path curBlockPath = getCurBlockPath(blockId);
        Block block = new Block(blockId, Files.size(curBlockPath));
        Files.delete(curBlockPath);
        return block;
    } catch (IOException e) {
      throw new IOException();
    }
  }

  synchronized public OutputStream writeBlock(long blockId) throws IOException { //can't exist and can't underconstruct, if we fail and didn't clean up!
    if (exist(blockId) || underConstruction(blockId)) {
      throw new IOException("block already exist on datanode " + blockId);
    }
    try { 
        return Files.newOutputStream(getTmpBlockPath(blockId));
    } catch (IOException e) {
      throw new IOException();
    }
  }

  synchronized public InputStream readBlock(long blockId) throws IOException {
    Path curBlockPath = getCurBlockPath(blockId); 
    try {
      return Files.newInputStream(curBlockPath);
    } catch (Exception e) {
      System.out.println(e);
      throw new IOException();
    }
  }

  synchronized public Block getBlock(long blockId) { //if not there , return null;
    if (!exist(blockId)) return null;
    Path curBlockPath = getCurBlockPath(blockId);
    try {
        return new Block(blockId, Files.size(curBlockPath));
    } catch (IOException e) {
      return null;
    }
  }

  //read write
  //--------//

  synchronized public boolean exist(long blockId) {
    Path curBlockPath = getCurBlockPath(blockId);
    return Files.exists(curBlockPath);
  }

  synchronized public boolean underConstruction(long blockId) {
    Path tmpBlockPath = getTmpBlockPath(blockId);
    return Files.exists(tmpBlockPath);
  }

  Path getTmpBlockPath(long blockId) {
    return Paths.get(tmpPath.toString(), "block_" + blockId);
  }

  Path getCurBlockPath(long blockId) {
    return Paths.get(curPath.toString(), "block_" + blockId);
  }
 
}
