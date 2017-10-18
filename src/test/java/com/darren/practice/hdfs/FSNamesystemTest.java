package com.darren.parctice.hdfs;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

import java.io.IOException;

public class FSNamesystemTest {

  @Test(expectedExceptions = IOException.class)
  public void testConstruct() throws IOException {
    //just started, nothing is there.
    FSNamesystem fSNamesystem = new FSNamesystem("./test_namespace", true);
    //no namenode registered.

    //no file 
    LocatedBlock[] locatedBlocks = fSNamesystem.openFile("/file_not_exist");

    //can't create no dataNode
    LocatedBlock locatedBlock = fSNamesystem.startFile("/create_a_file");
  }

  @Test
  public void testCreateReadFile() throws IOException {
    //register some node first.
    FSNamesystem fSNamesystem = new FSNamesystem("./test_namespace", true);

    DataNodeId dataNodeId1 = new DataNodeId("localhost", 9001); 
    DataNodeId dataNodeId2 = new DataNodeId("localhost", 9002); 
    DataNodeId dataNodeId3 = new DataNodeId("localhost", 9003); 

    fSNamesystem.processReport(new Block[0] , dataNodeId1);
    fSNamesystem.processReport(new Block[0] , dataNodeId2);
    fSNamesystem.processReport(new Block[0] , dataNodeId3);
    
    String fileName = "/create_a_file";
    //assume default replica is 3.
    LocatedBlock locatedBlock1 = fSNamesystem.startFile(fileName);
    Assert.assertEquals(locatedBlock1.getDataNodeIds().length, 3);
    fSNamesystem.blockReceived(locatedBlock1);
    LocatedBlock locatedBlock2 = fSNamesystem.getAdditionalBlock(fileName);
    Assert.assertEquals(locatedBlock2.getDataNodeIds().length, 3);
    fSNamesystem.blockReceived(locatedBlock2);
    fSNamesystem.completeFile(fileName);

    LocatedBlock[] locatedBlocks = fSNamesystem.openFile(fileName);
    Assert.assertEquals(locatedBlocks.length, 2); 

    Assert.assertEquals(locatedBlocks[0].getBlock(), locatedBlock1.getBlock()); 
    Assert.assertEquals(locatedBlocks[1].getBlock(), locatedBlock2.getBlock()); 

    Assert.assertEquals(locatedBlocks[0].getDataNodeIds().length, 3);
    Assert.assertEquals(locatedBlocks[1].getDataNodeIds().length, 3);

    Assert.assertEquals(locatedBlocks[0].getBlock().getBlockId(), 2147483648L); 
    Assert.assertEquals(locatedBlocks[1].getBlock().getBlockId(), 2147483649L); 
  }

  @Test
  public void testRemoveFile() throws IOException {
    //register some node first.
    FSNamesystem fSNamesystem = new FSNamesystem("./test_namespace", true);

    DataNodeId dataNodeId1 = new DataNodeId("localhost", 9001); 
    DataNodeId dataNodeId2 = new DataNodeId("localhost", 9002); 
    DataNodeId dataNodeId3 = new DataNodeId("localhost", 9003); 

    fSNamesystem.processReport(new Block[0] , dataNodeId1);
    fSNamesystem.processReport(new Block[0] , dataNodeId2);
    fSNamesystem.processReport(new Block[0] , dataNodeId3);
    
    String fileName = "/create_a_file";
    //assume default replica is 3.
    LocatedBlock locatedBlock1 = fSNamesystem.startFile(fileName);
    fSNamesystem.blockReceived(locatedBlock1);
    LocatedBlock locatedBlock2 = fSNamesystem.getAdditionalBlock(fileName);
    fSNamesystem.blockReceived(locatedBlock2);
    fSNamesystem.completeFile(fileName);

    LocatedBlock[] locatedBlocks = fSNamesystem.openFile(fileName);
    Assert.assertEquals(locatedBlocks.length, 2); 

    //exist or not those things. 

    Assert.assertEquals(fSNamesystem.delete(fileName), true);
    Assert.assertEquals(fSNamesystem.getDataNodesWithBlockToRemove().size(), 3);
    Assert.assertEquals(fSNamesystem.openFile(fileName), null);

    //block are still there
    Assert.assertEquals(fSNamesystem.getAllBlocks().size(), 2);

    Block[] blocksForFile = new Block[locatedBlocks.length];
    for (int i = 0; i < blocksForFile.length; ++i) {
      blocksForFile[i] = locatedBlocks[i].getBlock();  
    }


    //few node remove
    fSNamesystem.removeStoreBlock(blocksForFile, dataNodeId1);
    Assert.assertEquals(fSNamesystem.getDataNodesWithBlockToRemove().size(), 2);
    fSNamesystem.removeStoreBlock(blocksForFile, dataNodeId2);
    Assert.assertEquals(fSNamesystem.getAllBlocks().size(), 2);

    //last node gone
    fSNamesystem.removeStoreBlock(blocksForFile, dataNodeId3);
    Assert.assertEquals(fSNamesystem.getAllBlocks().size(), 0);
    Assert.assertEquals(fSNamesystem.getDataNodesWithBlockToRemove().size(), 0);
  }

  @Test(expectedExceptions = IOException.class)
  public void testProcessReport() throws IOException {
    //create a file.
    //then drop some by report. 
    //then read the file
    FSNamesystem fSNamesystem = new FSNamesystem("./test_namespace", true);

    DataNodeId dataNodeId1 = new DataNodeId("localhost", 9001); 
    DataNodeId dataNodeId2 = new DataNodeId("localhost", 9002); 
    DataNodeId dataNodeId3 = new DataNodeId("localhost", 9003); 

    fSNamesystem.processReport(new Block[0] , dataNodeId1);
    fSNamesystem.processReport(new Block[0] , dataNodeId2);
    fSNamesystem.processReport(new Block[0] , dataNodeId3);
    
    String fileName = "/create_a_file";
    //assume default replica is 3.
    LocatedBlock locatedBlock1 = fSNamesystem.startFile(fileName);
    fSNamesystem.blockReceived(locatedBlock1);
    LocatedBlock locatedBlock2 = fSNamesystem.getAdditionalBlock(fileName);
    fSNamesystem.blockReceived(locatedBlock2);
    fSNamesystem.completeFile(fileName);

    LocatedBlock[] locatedBlocks = fSNamesystem.openFile(fileName);
    Assert.assertEquals(locatedBlocks.length, 2); 


    //do a block report -- then check the blockMap;
    fSNamesystem.processReport(new Block[0], dataNodeId1);
    Assert.assertEquals(fSNamesystem.getAllBlocks().size(), 2);

    locatedBlocks = fSNamesystem.openFile(fileName);
    Assert.assertEquals(locatedBlocks.length, 2); 
    Assert.assertEquals(locatedBlocks[0].getDataNodeIds().length, 2); 

    fSNamesystem.processReport(new Block[0], dataNodeId2);
    fSNamesystem.processReport(new Block[0], dataNodeId3);
    Assert.assertEquals(fSNamesystem.getAllBlocks().size(), 0); 

    //do a file open missing block  
    locatedBlocks = fSNamesystem.openFile(fileName);
  }

}
