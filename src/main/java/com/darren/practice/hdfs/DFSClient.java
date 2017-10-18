package com.darren.parctice.hdfs;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.darren.parctice.rpc.RPC;

import java.net.InetSocketAddress;
import java.net.Socket;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.ObjectOutputStream;

import java.util.Random;

//clent.openfile (read).    creatFile(write).
//remove file
//check status
//
//
//client should do another thing. renew lease -- heartbeat and access guarrantee.
public class DFSClient {
  private static final Logger logger = LogManager.getLogger(DFSClient.class); 
  
  private InetSocketAddress namenodeAddress = null;
  private ClientProtocol clientProtocol = null;
  private Random random = new Random();
  private long MAX_BLOCK_SIZE = 10;

  public DFSClient(InetSocketAddress namenodeAddress) throws IOException {
    this.namenodeAddress = namenodeAddress;
    this.clientProtocol = (ClientProtocol) RPC.getProxy(ClientProtocol.class, namenodeAddress);
  }

  public boolean exist(String path) throws IOException {
    return clientProtocol.exist(path);
  }

  public boolean isDirectory(String path) throws IOException {
    return clientProtocol.isDirectory(path);
  }

  public boolean mkdir(String path) throws IOException {
    return clientProtocol.mkdir(path);
  }
  
  public boolean delete(String path) throws IOException {
    return clientProtocol.delete(path);
  }

  public InputStream newInputStream(String filePath) throws IOException {
    //return null;
    return new DFSInputStream(filePath);
  }

  public OutputStream newOutputStream(String filePath) throws IOException {
    return new DFSOutputStream(filePath);
  }

    //TODO buffering   appending --empty file 
  //write a block. then write another block.
  public class DFSOutputStream extends OutputStream {
    private String filePath;
    LocatedBlock locatedBlock = null;
    Socket curSocket = null;
    DataOutputStream curBlockOutputStream = null;
    DataInputStream curBlockInputStream = null;
    long fileLength = 0;
    long blockLength = 0;

    public DFSOutputStream (String filePath) throws IOException { //only support overwrite here.
      this.filePath = filePath;
    }

    public void close() throws IOException {
      closeCurrentBlock();
      clientProtocol.completeFile(filePath); 
    } 

    public void flush() throws IOException { 
      if (curBlockOutputStream != null) {
        curBlockOutputStream.flush();
      }
    }

    private void nextBlock() throws IOException {//report current block then get nextBlock.
      if (fileLength != 0) {
        flush();
        closeCurrentBlock(); 
      }

      if (fileLength == 0) {
        locatedBlock = clientProtocol.startFile(filePath);
      } else {
        locatedBlock = clientProtocol.getAdditionalBlock(filePath);
      }
      //setup. 

      DataNodeId[] dataNodeIds = locatedBlock.getDataNodeIds();
      if (dataNodeIds == null || dataNodeIds.length == 0) throw new IOException("fail to get next block");

      curSocket = new Socket(dataNodeIds[0].getAddress(), dataNodeIds[0].getPort());

      curBlockOutputStream = new DataOutputStream(curSocket.getOutputStream());
      curBlockInputStream = new DataInputStream(curSocket.getInputStream());


      curBlockOutputStream.writeByte(1);
      curBlockOutputStream.writeLong(locatedBlock.getBlock().getBlockId());
      curBlockOutputStream.writeInt(dataNodeIds.length);
      curBlockOutputStream.flush();

      ObjectOutputStream outO = new ObjectOutputStream(curBlockOutputStream);
      for (int i = 0; i < dataNodeIds.length ; ++i) {
        outO.writeObject(dataNodeIds[i]);
      }   

      outO.flush();
      outO = null;
      blockLength = 0;
    }
    
    private void closeCurrentBlock() throws IOException {
      if (curBlockOutputStream == null) throw new IOException();
      flush();
      curBlockOutputStream.writeInt(-1);
      int STATUS = curBlockInputStream.readByte();
      if (STATUS != 1) throw new IOException();
      locatedBlock.getBlock().setLength(blockLength);
      clientProtocol.blockReceived(locatedBlock);

      try {
        curBlockOutputStream.close();
        curBlockInputStream.close();
        curSocket.close();
      } catch (IOException e) {
        logger.warn(e);
      }
    }


    public void write(byte[] b) { 
      throw new UnsupportedOperationException();
    }

    public void write(byte[] b, int off, int len) {
      throw new UnsupportedOperationException();
    }

    public void write(int b) throws IOException {
      //next block haapens before write
      try {
        if (fileLength == 0 || blockLength >= MAX_BLOCK_SIZE) {
          nextBlock();
        }
      } catch (Exception e) {
        logger.warn("something in next block went wrong", e);
        throw new IOException();
      }
      curBlockOutputStream.writeInt(1);
      curBlockOutputStream.write(b);
      ++blockLength; ++fileLength;
    }
  }

  public class DFSInputStream extends InputStream  { //eacho block here should have bytes number.
    private LocatedBlock[] locatedBlocks = null;
    private long fileLength = 0; 
    private long curPos = 0;
    private long curBlockEnd = -1;
    DataInputStream curBlockInputStream = null; 
    DataOutputStream curBlockOutputStream = null;
    Socket  curSocket = null;

    public DFSInputStream(String filePath) throws IOException {
      LocatedBlock[] locatedBlocks = clientProtocol.openFile(filePath);
      if (locatedBlocks == null) {
        throw new IOException("file not exist : " + filePath);
      }

      this.locatedBlocks = locatedBlocks;

      for (LocatedBlock locatedBlock : locatedBlocks) {
        fileLength += locatedBlock.getBlock().getLength();
      }

      System.out.println("file leng : "  + fileLength);
      System.out.println("number of block "  + locatedBlocks.length);
    }

    public int available() { //this is not good. g file
      return (int) (fileLength - curPos);
    }

    public void close() {
      try {
        if (curSocket != null) {
          curSocket.close();    
          curBlockOutputStream.close();
          curBlockInputStream.close();
        }
      } catch (IOException e) {
        logger.warn("error while close one block reading connection" + curSocket);
      }
    }

    public void mark(int readlimit) {
      throw new UnsupportedOperationException();
    }

    public boolean markSupported() {
      throw new UnsupportedOperationException();
    }

    public int read() throws IOException {
      //current position.  beyond  currentBlock must go to Another block.
      //something went to go to block
      // curPos is the position we gonna to read.
      if (curPos >= fileLength) return -1; //end of file reached!

      if (curPos > curBlockEnd) {
        seekToPosition(curPos);
      }
      ++curPos;
      return curBlockInputStream.read();
    }

    private void seekToPosition(long curPos) throws IOException {
      close(); //close connection for last block;

      if (curPos < 0 || curPos >= fileLength) throw new IOException("out of file range");
      long blockStart = -1; long blockEnd = -1; int blockIndex = 0;

      for (; blockIndex < locatedBlocks.length; ++blockIndex) {
        LocatedBlock locatedBlock = locatedBlocks[blockIndex];
        long blockLength = locatedBlock.getBlock().getLength();
        
        blockStart = blockEnd + 1;
        blockEnd = blockStart + blockLength - 1;
        if (blockStart <= curPos && curPos <= blockEnd) {
          break;
        }
      }

      if (blockIndex == locatedBlocks.length) throw new IOException("out of file range");

      LocatedBlock curBlock = locatedBlocks[blockIndex];
      DataNodeId[] dataNodeIds = curBlock.getDataNodeIds();
      int numNodes = dataNodeIds.length;
      if (dataNodeIds == null || numNodes == 0) {
        throw new IOException("block has no serving datanode : " + curBlock.getBlock().getBlockId());
      }

      
      int startDataNode = random.nextInt(numNodes);
      long offset = curPos - blockStart;
      long length = blockEnd - curPos + 1; //blockEnd - curPos + 1
      long triedNodes = 0;
      while (triedNodes < numNodes) {
        DataNodeId dataNodeId = dataNodeIds[startDataNode];

        try {
          curSocket = new Socket(dataNodeId.getAddress(), dataNodeId.getPort());
          curBlockOutputStream = new DataOutputStream(curSocket.getOutputStream());
          curBlockInputStream = new DataInputStream(curSocket.getInputStream());
          curBlockOutputStream.writeByte(0);
          curBlockOutputStream.writeLong(curBlock.getBlock().getBlockId());
          curBlockOutputStream.writeLong(offset);
          curBlockOutputStream.writeLong(length);
          curBlockOutputStream.flush();
          break;

        } catch (IOException e) {
          close();
          logger.info(dataNodeId.getAddress() + " no block " + curBlock.getBlock().getBlockId() , e);
        }

        dataNodeId.getAddress();
        ++triedNodes;
        ++startDataNode; 
        startDataNode %= numNodes;
      } 
      
      if (triedNodes == numNodes) throw new IOException("no datanode response to block read for : " + curBlock.getBlock().getBlockId());
    }

    public int read(byte[] b) {
      throw new UnsupportedOperationException();
    }

    public int read(byte[] b, int off, int len) {
      throw new UnsupportedOperationException();
    }

    public void reset() {
      throw new UnsupportedOperationException();
    }

    public long skip(long n) {
      if (n < 0) return 0;
      long remain = fileLength - curPos;
      long skipLen = Math.min(remain, n);
      curPos += skipLen;
      return skipLen;
    }
  }

}
