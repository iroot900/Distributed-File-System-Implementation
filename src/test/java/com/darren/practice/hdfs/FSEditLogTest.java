package com.darren.parctice.hdfs;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FSEditLogTest {

  @Test(expectedExceptions = IOException.class)
  public void testConstructor() throws IOException {
    FSEditLog fsEditLog = new FSEditLog("./dir/fsEdits"); 
  }

  @Test
  public void testLoadEditLog() throws IOException {
    String editLogFile = "./fs_edit_log";
    FSEditLog fsEditLog = new FSEditLog(editLogFile);

    fsEditLog.logAddINode("/dir");
    fsEditLog.logAddINode("/dir/dir2");

    fsEditLog.logAddINode("/dir/dir2/file22", new Block[2]);
    fsEditLog.logAddINode("/dir/dir2/file21", new Block[2]);
    fsEditLog.logAddINode("/dir/file11", new Block[3]);
    fsEditLog.logAddINode("/file01", new Block[1]);
    fsEditLog.logAddINode("/file02", new Block[1]);

    fsEditLog.logRemoveINode("/dir/dir2/file21");
    fsEditLog.logRemoveINode("/file02");
    fsEditLog.close();


    //load edit log
    INodeTree fsTree = new INodeTree(); 
    FSEditLog.loadEditLog(fsTree, editLogFile);

    //get root
    INodeTree.INode node = fsTree.getINode("/dir");
    Assert.assertEquals(node.name, "dir");

    node = fsTree.getINode("/dir/dir2");
    Assert.assertEquals(node.name, "dir2");

    node = fsTree.getINode("/dir/dir2/file22");
    Assert.assertEquals(node.name, "file22");
    Assert.assertEquals(node.blocks.length, 2);

    node = fsTree.getINode("/dir/file11");
    Assert.assertEquals(node.name, "file11");
    Assert.assertEquals(node.blocks.length, 3);

    node = fsTree.getINode("/file01");
    Assert.assertEquals(node.name, "file01");
    Assert.assertEquals(node.blocks.length, 1);

    //get nonExist
    node = fsTree.getINode("/dir/dir2/file21");
    Assert.assertEquals(node, null);

    node = fsTree.getINode("/file02");
    Assert.assertEquals(node, null);

        Files.delete(Paths.get(editLogFile));
  }

}
