package com.darren.parctice.hdfs;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;

import java.nio.file.*;

public class INodeTreeTest {

  @Test
  public void testConstructor() {
    INodeTree fsTree = new INodeTree(); 
    INodeTree.INode node = fsTree.getINode("/");
    Assert.assertEquals(node.name, "/");
  }

  @Test
  public void testGetINode() {
    INodeTree fsTree = new INodeTree(); 
    //get root
    INodeTree.INode node = fsTree.getINode("//./.");
    Assert.assertEquals(node.name, "/");

    //get nonExist
    node = fsTree.getINode("/not/exist");
    Assert.assertEquals(node, null);

    //get invalidPath
    node = fsTree.getINode("not/exist");
    Assert.assertEquals(node, null);
  }

  @Test
  public void testAddINode() {
    INodeTree fsTree = new INodeTree(); 

    //add dir
    INodeTree.INode node = fsTree.addINode("/./dir1");
    Assert.assertEquals(node.name, "dir1");
    //retrived added dir
    node = fsTree.getINode("/dir1/././");
    Assert.assertEquals(node.name, "dir1");

    //add root -not ok
    node = fsTree.addINode("/");
    Assert.assertEquals(node, null);


    //add dir without parent 
    node = fsTree.addINode("/parent/dir");
    Assert.assertEquals(node, null);

    //add dir with parent
    node = fsTree.addINode("//dir1/dir2");
    Assert.assertEquals(node.name, "dir2");
    node = fsTree.addINode("//dir1/dir2/dir3");
    Assert.assertEquals(node.name, "dir3");
    //retrived added dir
    node = fsTree.getINode("/dir1/dir2");
    Assert.assertEquals(node.name, "dir2");
    Assert.assertEquals(node.isDirectory, true);

    //readd
    node = fsTree.addINode("/dir1/dir2");
    Assert.assertEquals(node, null);


    //add file
    node = fsTree.addINode("/file1", new Block[2]);
    Assert.assertEquals(node.name, "file1");
    Assert.assertEquals(node.blocks.length, 2);

    //add file to file
    node = fsTree.addINode("/file1/file2");
    Assert.assertEquals(node, null);
  }

  @Test
  public void testRemoveINode() {
    INodeTree fsTree = new INodeTree(); 
    INodeTree.INode node = null; 

    //remove root
    node = fsTree.removeINode("/");
    Assert.assertEquals(node, null);

    //remove not exist
    node = fsTree.removeINode("/not/exist");
    Assert.assertEquals(node, null);

    node = fsTree.addINode("/dir");
    node = fsTree.addINode("/dir/file", new Block[2]);
    node = fsTree.getINode("/dir/file");
    Assert.assertEquals(node.name, "file");

    //remove not empty dir 
    node = fsTree.removeINode("/dir");
    Assert.assertEquals(node, null);

    //remove file 
    node = fsTree.removeINode("/dir/file");
    Assert.assertEquals(node.name, "file");
    node = fsTree.getINode("/dir/file");
    Assert.assertEquals(node, null);

    //remove empty dir 
    node = fsTree.removeINode("/dir");
    Assert.assertEquals(node.name, "dir");
    node = fsTree.getINode("/dir");
    Assert.assertEquals(node, null);
  }


  @Test
  public void testSaveImageTest() throws IOException {
    INodeTree fsTree = new INodeTree(); 
    fsTree.addINode("/dir1");
    fsTree.addINode("/dir2");
    fsTree.addINode("/dir3");
    fsTree.addINode("/dir2/dir22");
    fsTree.addINode("/dir3/dir33");
    fsTree.addINode("/dir3/dir33/dir333");
    fsTree.addINode("/dir2/dir22//file", new Block[2]);

    FileOutputStream out = null;
    FileInputStream in = null;
    try {
        out = new FileOutputStream("./fsimage");
        fsTree.saveImage(out);

        in = new FileInputStream("./fsimage");
        INodeTree fsTreeLoaded = new INodeTree();
        fsTreeLoaded.loadImage(in);

        INodeTree.INode node = fsTreeLoaded.getINode("/dir1");
        Assert.assertEquals(node.name, "dir1");

        node = fsTreeLoaded.getINode("/dir2");
        Assert.assertEquals(node.name, "dir2");

        node = fsTreeLoaded.getINode("/dir3");
        Assert.assertEquals(node.name, "dir3");

        node = fsTreeLoaded.getINode("/dir2/dir22");
        Assert.assertEquals(node.name, "dir22");

        node = fsTreeLoaded.getINode("/dir3/dir33");
        Assert.assertEquals(node.name, "dir33");


        node = fsTreeLoaded.getINode("/dir3/dir33/dir333");
        Assert.assertEquals(node.name, "dir333");

        node = fsTreeLoaded.getINode("/dir1/dir11");
        Assert.assertEquals(node, null);

        node = fsTreeLoaded.getINode("/");
        Assert.assertEquals(node.name, "/");


        node = fsTreeLoaded.getINode("/dir2/dir22//file");
        Assert.assertEquals(node.name, "file");
        Assert.assertEquals(node.blocks.length, 2);

        Files.delete(Paths.get("fsimage"));

    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (out != null) {
        try {
            out.close();
        } catch (IOException ex) {
          //
        }
      }

      if (in != null) {
        try {
            in.close();
        } catch (IOException ex) {
          //
        }
      }

    }
  }

}
