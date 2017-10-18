package com.darren.parctice.hdfs;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

import java.io.IOException;

//need to undertand lease monitor... read write file ownership gurrantte

/**
 * Combined block management with namespace 
 *
 * block create:  when block initiated it will be tracked, and it either sucess to become a file or be gone in the last. 
 * state transition : (block, source of truth, datanode (report) -- then might + / - it's mapping in namenode  
 *  in process, in datanode not in file, in datanode and in file (final status, a block id created, then end with a file or delete)
 *  pendingTable,  fileInProgress,  blockMap  
 *  created --> to datanode or not (to blockMap, remove from pendin)
 *  datanode --> to file or not (to file, pending removed, modify namespace, or fail, it will be detected by tracker of clients. clients file --> all the blocks will be gone)  
 *
 *  block delete:
 *  abandon file in progress : status of block --   file --- and all the block associated, either pending or in block  -- to delete it  
 *  delete file : all the blocks to delete it ( in toRemoveBlock and block to Remove)...  here could fail. then when start, need to do a one time sync 
 *      
 *      * heart beat, something to remove..  after remove..  need to tell remove is done..!
 *      datanode toremoveBlockMap -- heart beat, will get blocks to remove, when removed is done, will call to tell remove is done, then we could remove blocks from to Delete? 
 *       (to remove block, here should be another state transition) now  make it simple.      remove it send once, then we're done  --- but do call, remove (status unknow-bad) 
 *                  --- toRemoved -- heart beat, get it will do remove, then call, we modify blockMap and blockToRemove -- could be clock already removed, then?- should report?
 *
 *                  heartbeat, report, blockremoved -- when we add. -- add success.  remove. let's remove. --- i say remove. for block in 
 *                  block to Map. (here it's in our blockMap)  tell node to remove it. after remove  call I've removed. then we know removed.  we clean. -- it could be..  
 *                  we say remove. but that information is lost. so  datanode didn't remove it.  let's say it again.   datanode will remove it or say there is none, 
 *                  o
 *                  already remove shouldn't be a big mess.  so what should we do
 *                  
 *                  blocks in map.  blockTomoved.  ---  client. call. remove.   then get it out from blockToremove and blockMap. nodeMap.--- when report. remove  
 *
 *                  so, when delete.  make sure message will be sent to block. so let's send it many times ok. when call which is sent. --- not there or delete both ok    
 *
 *                  heart beat is 3 second.
 *
 *
 *          //clinet blockReceived
 *          //datanoe report * block removed , heartbeat
 *
 *          //plus thread to do lease check.  client - pendingFiles.
 *
 *  //why do we need datanode -> block..   datanode capacity to get datanode 1), 2) datanode dies need to know which block is there. 
 */

class FSNamesystem {

    //namespace managetment
    private FSDirectory fsDirectory; 

    private int BLOCK_REPLICA_NUM = 3;
    private long HEART_BEAT_EXPIRE_INTERVAL = 1000 * 60; //one minute.
    static private final Logger logger = LogManager.getLogger(FSNamesystem.class);
    BlockIdGenerator blockIdGenerator = new BlockIdGenerator();  

    //block management 
    private Map<Block, Set<DataNodeId>> blockMap = new HashMap<>(); //  fact, which DataNode to get.
    private Map<DataNodeId, Set<Block>> dataNodeMap = new HashMap<>(); // --- should get more info from datanode. -- valid or not
    //datanode also need management. ---- baseOn there infomation.

    private Map<String, Set<Block>>  pendingFiles = new HashMap<>(); //create  staging area  --- single place of how they work together ... 
    private Map<DataNodeId, Set<Block>> blocksToRemove = new HashMap<>(); //remove stagin area

    LRM<DataNodeId, HeartBeat> heartbeats = new LRM<>();

    private Map<Block, Long> sizeMap = new HashMap<>();


    public FSNamesystem(String dir, boolean format) throws IOException {
      fsDirectory = new FSDirectory(dir, format);
    }

    public void close() throws IOException {
      //here we should invalidate all blocks. -- or start check
      //file Underconstruction should be gone.
        fsDirectory.close();
    }


    //need access to heartbea.
    private static class HeartBeat extends TimedItem {

    }

    private class HeartBeatMonitor extends Thread { //if one heart beat is expire, then that node is down..
      int checkInterval = 1000;
      //dataNode update it's heartbeat; 
      //LRU<>

      public HeartBeatMonitor(int checkInterval) {
        this.checkInterval = checkInterval;
      }

      @Override
      public void run() {
        while (true) {
            checkDataNodesHeartBeat(); 
            try {
              Thread.sleep(checkInterval);
            } catch (InterruptedException e) {
              logger.warn(e);
            }
        }
      }

      void checkDataNodesHeartBeat() {
        synchronized(heartbeats) {
          while (heartbeats.size() > 0 && (System.currentTimeMillis() - heartbeats.get(heartbeats.getFirstKey()).getTimeStamp() > HEART_BEAT_EXPIRE_INTERVAL)) {
            logger.info("lost heart beat update from :" + heartbeats.getFirstKey().getAddress()); 
            //remove a datanode. --- all the blocks in a node. need to remove it. 
            
            //TO DO. without ok. can survive  
          }
        }
      }

    }

    //add this for testing purpose
    Set<Block> getAllBlocks() {
      return blockMap.keySet();
    }
    
    Set<DataNodeId> getDataNodesWithBlockToRemove() {
      return blocksToRemove.keySet();
    }

    synchronized public boolean exist(String path) {
      return fsDirectory.exist(path);
    } 

    synchronized public boolean isDirectory(String path) {
      return fsDirectory.isDirectory(path);
    }

    synchronized public boolean mkdir(String path) throws IOException {//directory not exist or exist.
      return fsDirectory.mkdir(path);
    }


    //need to get all the block information, we should randomly get one.
    //don't worry performance or balancing... just get it work run first : )... enginerring.    solve problem blocking by blcoking.. :)
    public LocatedBlock[] openFile(String filePath) throws IOException { //mightbe file is corrupted. 
       Block[] blocks = fsDirectory.getFileInfo(filePath);
       if (blocks == null) { //no such file
         return null;
       }

       int numBlocks = blocks.length;     
       LocatedBlock[] locatedBlocks = new LocatedBlock[numBlocks];
       for (int i = 0; i < numBlocks; ++i) {
         Set<DataNodeId> dataNodeIds = blockMap.get(blocks[i]);
         if (dataNodeIds == null || dataNodeIds.size() == 0) {
           throw new IOException("file " + filePath + " has missing blocks");
         } else {
           DataNodeId[] blockNodes = dataNodeIds.toArray(new DataNodeId[dataNodeIds.size()]);
           locatedBlocks[i] = new LocatedBlock(blocks[i], blockNodes);
         }
       }

       return locatedBlocks;
    }

    synchronized public LocatedBlock startFile(String filePath) throws IOException { //is this over synced? 
      //nobody is creating and not exist and parent exist? 
      if (!fsDirectory.validToCreate(filePath) || pendingFiles.containsKey(filePath)) { //path should be normalized.
        throw new IOException("not valid to create");
      }

      Set<Block> blocks = new HashSet<>();
      Block block = allocateABlock();
      blocks.add(block);


      DataNodeId[] dataNodeIds = chooseDataNodes(BLOCK_REPLICA_NUM); //should be a global setting.
      if (dataNodeIds == null) {
        throw new IOException("not enough data nodes for replica");
      }
      pendingFiles.put(filePath, blocks);
      return new LocatedBlock(block, dataNodeIds);
    }

    //client report block stored  ---  client need to say a block is write to node?
    //also when block transfer data node will say this
    //when report datanode 
    //node has a report saying there is the sameblock again.. based on how block on disk, this should not happen i
    //same id? not possbiel!
    //one node is done. this file fail,  or we just fail block. --- can't must fail block. not good! 
    public synchronized void blockReceived(LocatedBlock locatedBlock) throws IOException { //for a block for a file is received.

        Block block = locatedBlock.getBlock();
        sizeMap.put(block, block.getLength());

        DataNodeId[] newDataNodeIdsForBlock = locatedBlock.getDataNodeIds();

        Set<DataNodeId> dataNodesSet = blockMap.get(block); 
        if (dataNodesSet == null) {
          dataNodesSet = new HashSet<DataNodeId> ();
          blockMap.put(block, dataNodesSet);
        } 

        //update block -> nodes
        for (DataNodeId dataNodeId : newDataNodeIdsForBlock) { //
          
          Set<Block> blocksOnNode = dataNodeMap.get(dataNodeId);
          if (blocksOnNode == null) {
            logger.debug("datanode " + dataNodeId.getAddress() +  " " + dataNodeId.getPort() + "done"); 
            throw new IOException("one datanode done!");
          } else if (!dataNodesSet.add(dataNodeId) || !blocksOnNode.add(block)) {
            logger.debug("block " + block.getBlockId() + " already exist on node " + dataNodeId.getAddress() +  " " + dataNodeId.getPort()); 
          }
        }
    }

    //for file -- let's something call getAdditionalBlocks for file. if not pending wrong
    public synchronized LocatedBlock getAdditionalBlock(String filePath) throws IOException {

      if (!pendingFiles.containsKey(filePath)) { //path should be normalized.
        throw new IOException("no such file in construction" + filePath);
      }

      Set<Block> blocks = pendingFiles.get(filePath); //one block is in construction now. if we abond. we have block number, then to see if it's store or not
      Block block = allocateABlock();
      blocks.add(block);

      DataNodeId[] dataNodeIds = chooseDataNodes(BLOCK_REPLICA_NUM); //should be a global setting.
      if (dataNodeIds == null) {
        throw new IOException("not enough data nodes for replica");
      }
      return new LocatedBlock(block, dataNodeIds);
    }

    public synchronized boolean abandonBlock(Block block, String filePath) throws IOException { //file not there, or block not there.
        Set<Block> blocks = pendingFiles.get(filePath);
        if (blocks == null) {
          throw new IOException("not such file under construction " + filePath);
        } else {
          //should check if we should remove it or not.
        }

        return blocks.remove(block); 
    }


    //while create someone might do delete or mess up the directory..
    public synchronized void completeFile(String filePath) throws IOException { //what we need to do to complete file
      //all blocks are in, we complete. yes.
      Set<Block> blocks = pendingFiles.get(filePath);
      for (Block block : blocks) {
        Set<DataNodeId> dataNodes = blockMap.get(block);
        if (dataNodes == null || dataNodes.size() < 1) { //set something here should
          throw new IOException("file not valid, missing block"); //if fail here should abandon this file 
        }
        long length = sizeMap.get(block);
        block.setLength(length);
      }

      if (fsDirectory.addFile(filePath, blocks.toArray(new Block[0]))) {
        pendingFiles.remove(filePath);
      } else {
          throw new IOException("file not valid, missing block"); 
          //----let client to cann abandonFileInProgress, or in the end when client went disconnect, clean all pending files----//
      }
    }

    public synchronized boolean abandonFileInProgress(String filePath) throws IOException {
      Set<Block> blocks = pendingFiles.get(filePath);
      if (blocks == null) {
        return false;
      } {
        pendingFiles.remove(filePath);
        addBlockToRemove(blocks.toArray(new Block[0]));
        return true;
      }
    }

    public synchronized boolean delete(String filePath) throws IOException { //not exist, or something went wrong.
      Block[] blocks = fsDirectory.removeFile(filePath);
      if (blocks == null) { //no such file
        return false;
      } else {
        addBlockToRemove(blocks);
        return true;
      }
    }

    //block on some datanode. now should remove it!
    //gurrantte remove, then datanode should notify the block is remove. (by block report. certainly will, or just remomved, then gone from blocksToRemove)
    //block remove,   datanode will report it's not there,  or report delte.   either way. we can remove it from  toremove and datanode. other wise not sucess keep send. 
    //at least by block report, then we'll know
    //return.. for each block. we'll say block removed. 
    public synchronized void addBlockToRemove(Block[] blocks) {
      for (Block block : blocks) {
        sizeMap.remove(block);
        Set<DataNodeId> dataNodeIds = blockMap.get(block);
        for (DataNodeId dataNodeId : dataNodeIds) { //currently still on these dataNode; 
            Set<Block> blockToRemoveOnNode = blocksToRemove.get(dataNodeId);
            if (blockToRemoveOnNode == null) {
              blockToRemoveOnNode = new HashSet<Block>();
              blocksToRemove.put(dataNodeId, blockToRemoveOnNode);
            }
            blockToRemoveOnNode.add(block);
        }
      }
    }

    //MUST DO
    synchronized private Block allocateABlock() { //should have a place fsdirectory to have all blockIds.
      long blockId = blockIdGenerator.nextId();
      Block block = new Block(blockId);
      while(blockMap.containsKey(block)) {
        block = new Block(blockIdGenerator.nextId());
      }
      return block;
    }

    //MUST DO:
    private DataNodeId[] chooseDataNodes(int replica) {//might not able to get some....
      Set<DataNodeId> allDataNodeIds = dataNodeMap.keySet();
      if (allDataNodeIds.size() < replica) {
        return null;
      }
      //don't bother
      DataNodeId[] dataNodeIds = new DataNodeId[replica]; 
      int i = 0;
      for (DataNodeId dataNodeId : allDataNodeIds) {
        dataNodeIds[i++] = dataNodeId;
        if (i == replica) break; 
      }
      return dataNodeIds;
    }

    //could send more info here. 
    synchronized public Block[] gotHeartBeat(DataNodeId dataNodeId) { // what cmd send back blocks To remove.
      HeartBeat heartbeat = heartbeats.get(dataNodeId);
      if (heartbeat == null) {
        heartbeat = new HeartBeat();
      }
      //update something, then put it back.
      heartbeats.put(dataNodeId, heartbeat);
      Set<Block> blocks = blocksToRemove.get(dataNodeId); 
      if (blocks == null) return null;
      else return blocks.toArray(new Block[0]);
    }

     synchronized public void processReport(Block[] blocks, DataNodeId dataNodeId) { //create this node maybe.

       Set<Block> blocksOnNodeReported = new HashSet<Block>();
       for (Block block : blocks) {
         blocksOnNodeReported.add(block);
       }

       Set<Block> blocksOnNode = dataNodeMap.get(dataNodeId);
       if (blocksOnNode == null) {
         logger.info(" register a data node on name node : " + dataNodeId.getAddress() + "on port " + dataNodeId.getPort());
       }
       if (blocksOnNode == null || blocksOnNode.size() == 0) { //no old block nothing to remove
         dataNodeMap.put(dataNodeId, blocksOnNodeReported);
         //addblockMap
         for (Block block : blocksOnNodeReported) {
           sizeMap.put(block, block.getLength());
           Set<DataNodeId> dataNodeIds = blockMap.get(block);
           if (dataNodeIds == null) {
             dataNodeIds = new HashSet<DataNodeId>();
             blockMap.put(block, dataNodeIds);
           }
           dataNodeIds.add(dataNodeId);
         }
       } else { //ok there are some block removed.

         Set<Block> blocksToRemoveOnDataNode = blocksToRemove.get(dataNodeId);
         for (Block block : blocksOnNode) { //blocks. some needs to be removed and some needs to be added. --- all old block if not then remove. for block if new then add
            if (!blocksOnNodeReported.contains(block)) {
              //blocks to remove
              if (blocksToRemoveOnDataNode != null) {
                blocksToRemoveOnDataNode.remove(block); //already remove
              }
              Set<DataNodeId> dataNodeIds = blockMap.get(block);
              dataNodeIds.remove(dataNodeId); //at least on dataNode;
              if (dataNodeIds.size() == 0) {
                blockMap.remove(block);
              }
            }
         }

         for (Block block : blocksOnNodeReported) {
           if (!blocksOnNode.contains(block)) {
             //add this block
              sizeMap.put(block, block.getLength());
              Set<DataNodeId> dataNodeIds = blockMap.get(block);
              if (dataNodeIds == null) {
                dataNodeIds = new HashSet<DataNodeId>();
                blockMap.put(block, dataNodeIds);
              }
              dataNodeIds.add(dataNodeId); //at least on dataNode;
            }
         }
         dataNodeMap.put(dataNodeId, blocksOnNodeReported);
       }
     }
     //dataNode could die before remove.. so,,  if die. still need to remove
     //if node is delete clean up this. but now still need to keep it.
     synchronized public void removeStoreBlock(Block[] blocks, DataNodeId dataNodeId) { //might already be removed  report come first. -- but as a response to toremove, shoureturn all. f
       Set<Block> blocksOnDataNode = dataNodeMap.get(dataNodeId);
       Set<Block> blocksToRemoveOnDataNode = blocksToRemove.get(dataNodeId);
         if (blocksOnDataNode != null) {

           for (Block block :  blocks) {
             //dataNodeMap
             blocksOnDataNode.remove(block);

             //blockMap
             Set<DataNodeId> dataNodeIds = blockMap.get(block);
             if (dataNodeIds != null) {
               dataNodeIds.remove(dataNodeId);
               if (dataNodeIds.size() == 0) {
                 blockMap.remove(block);
               }
             }
              
             //block to remove
             if (blocksToRemoveOnDataNode != null) {
               blocksToRemoveOnDataNode.remove(block);
               if (blocksToRemoveOnDataNode.size() == 0) {
                 blocksToRemove.remove(dataNodeId);
               }
             }
           }
        }
     }
}
