package com.darren.parctice.hdfs;

import com.darren.parctice.util.Util;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.AfterClass;

import java.io.OutputStream;
import java.io.InputStream;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;

public class FSDataSetTest {
  private FSDataSet dataSet = null; 
  private final String dir = "./testDataSet";


  @BeforeClass
  public void init() throws IOException {
    dataSet = new FSDataSet(dir, true); 
  }

  @Test(expectedExceptions = IOException.class)
  public void testEmptyAdd() throws IOException {
    Block block = null;
    block = dataSet.addBlock(123456);
  }

  @Test
  public void testWriteRemove() throws IOException {
    Block block = null;
    OutputStream out = dataSet.writeBlock(123456); 
    out.write(1);
    out.close();
    block = dataSet.addBlock(123456);
    Assert.assertEquals(block.getBlockId(), 123456);
    Assert.assertEquals(block.getLength(), 1);
    Assert.assertEquals(dataSet.exist(123456), true);

    block = dataSet.removeBlock(123456);
    Assert.assertEquals(block.getLength(), 1);
    Assert.assertEquals(dataSet.exist(123456), false);
  }

  @Test 
  public void testBlockReport() throws IOException {
    Block block = null;
    OutputStream out = dataSet.writeBlock(123456); 
    out.write(1);
    out.close();
    block = dataSet.addBlock(123456);
    Block[] blocks = dataSet.getBlockReport();
    Assert.assertEquals(blocks.length, 1);
    Assert.assertEquals(blocks[0].getBlockId(), 123456);
    block = dataSet.removeBlock(123456);
  }
  
  @Test
  public void testRead() throws IOException {
    Block block = null;
    OutputStream out = dataSet.writeBlock(123456); 
    out.write(1);
    out.close();
    block = dataSet.addBlock(123456);

    InputStream in = dataSet.readBlock(123456); 
    int bt = in.read(); 
    in.close();

    Assert.assertEquals(bt, 1);
    block = dataSet.removeBlock(123456);
    Assert.assertEquals(block.getLength(), 1);
  }

  @AfterClass
  public void cleanUp() throws IOException {
    Util.deleteDirectory(dir);
  }
}
