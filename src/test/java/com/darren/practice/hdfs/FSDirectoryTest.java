package com.darren.parctice.hdfs;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;

import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.Files;

public class FSDirectoryTest {

  static private final String dir = "./image";
  static private String CUR_FS_IMAGE = "fsimage.cur";
  static private String NEW_FS_IMAGE = "fsimage.new";
  static private String OLD_FS_IMAGE = "fsimage.old";
  static private String FS_EDIT_LOG = "fsEditLog";

  private Path curImagePath;
  private Path newImagePath;
  private Path oldImagePath;
  private Path fsEditLogPath;

  @BeforeClass
  void setUp() {
    curImagePath = Paths.get(dir, CUR_FS_IMAGE);
    newImagePath = Paths.get(dir, NEW_FS_IMAGE);
    oldImagePath = Paths.get(dir, OLD_FS_IMAGE);
    fsEditLogPath = Paths.get(dir, FS_EDIT_LOG);
  }

  @Test
  public void testFormat() throws IOException {
    FSDirectory fsDirectory = new FSDirectory(dir, true);
    Assert.assertEquals(Files.isDirectory(Paths.get(dir)), true);
    Assert.assertEquals(Files.exists(Paths.get(dir, CUR_FS_IMAGE)), true);

    fsDirectory.close();

    fsDirectory = new FSDirectory(dir, false);
    Assert.assertEquals(Files.exists(Paths.get(dir, CUR_FS_IMAGE)), true);
  }

 @Test
  public void testLoadFailure() throws IOException {
    FSDirectory fsDirectory = new FSDirectory(dir, true);
    fsDirectory.close();

    //cur and newFile 
    Files.createFile(newImagePath);
    fsDirectory = new FSDirectory(dir, false);
    fsDirectory.close();
    Assert.assertEquals(Files.exists(curImagePath), true);
    Assert.assertEquals(!Files.exists(newImagePath), true);
    Assert.assertEquals(!Files.exists(oldImagePath), true);

    //old and newFile 
    Files.move(curImagePath, newImagePath);
    Files.createFile(oldImagePath);
    fsDirectory = new FSDirectory(dir, false);
    fsDirectory.close();
    Assert.assertEquals(Files.exists(curImagePath), true);
    Assert.assertEquals(!Files.exists(newImagePath), true);
    Assert.assertEquals(!Files.exists(oldImagePath), true);

    //old and cur;
    Files.createFile(oldImagePath);
    fsDirectory = new FSDirectory(dir, false);
    fsDirectory.close();
    Assert.assertEquals(Files.exists(curImagePath), true);
    Assert.assertEquals(!Files.exists(newImagePath), true);
    Assert.assertEquals(!Files.exists(oldImagePath), true);
  }

  @Test
  public void testCRUD() throws IOException {
    FSDirectory fsDirectory = new FSDirectory(dir, true);
    Assert.assertEquals(fsDirectory.addFile("/file1", new Block[2]), true);
    Assert.assertEquals(fsDirectory.addFile("/file2", new Block[3]), true);
    Assert.assertEquals(fsDirectory.removeFile("/file2").length, 3);
    Assert.assertEquals(fsDirectory.removeFile("/file2"), null);
    Assert.assertEquals(fsDirectory.exist("/file2"), false);
    Assert.assertEquals(fsDirectory.getFileInfo("/file2"), null);

    Assert.assertEquals(fsDirectory.mkdir("/dir2/"), true);
    Assert.assertEquals(fsDirectory.mkdir("/dir20"), true);
    Assert.assertEquals(fsDirectory.removeFile("/dir20").length, 0);
    Assert.assertEquals(fsDirectory.removeFile("/dir20"), null);
    Assert.assertEquals(fsDirectory.isDirectory("/dir2"), true);

    Assert.assertEquals(fsDirectory.addFile("/dir2/file20", new Block[4]), true);
    Assert.assertEquals(fsDirectory.getFileInfo("/dir2/file20").length, 4);
    fsDirectory.close();

    fsDirectory = new FSDirectory(dir, false);
    Assert.assertEquals(fsDirectory.exist("/file2"), false);
    Assert.assertEquals(fsDirectory.getFileInfo("/file1").length, 2);

    Assert.assertEquals(fsDirectory.isDirectory("/dir2"), true);
    Assert.assertEquals(fsDirectory.isDirectory("/dir20"), false);
    Assert.assertEquals(fsDirectory.getFileInfo("/dir2/file20").length, 4);
    fsDirectory.close();
  }
}
