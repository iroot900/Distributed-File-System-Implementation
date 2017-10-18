package com.darren.parctice.util;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

import java.io.IOException;

import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.FileVisitor;
import java.nio.file.FileVisitResult;
import java.nio.file.DirectoryStream;

public class UtilTest {

  @Test
  public void deleteDirectoryTest() throws IOException {
    String dirPath = "./testDir";
    Files.createDirectory(Paths.get(dirPath));
    Assert.assertEquals(Files.isDirectory(Paths.get(dirPath)), true);

    String file = "file";
    Files.createFile(Paths.get(dirPath, file));
    Files.createDirectory(Paths.get(dirPath, dirPath));
    Files.createFile(Paths.get(dirPath, dirPath, file));
    
    Util.deleteDirectory(Paths.get(dirPath));
    Assert.assertEquals(Files.isDirectory(Paths.get(dirPath)), false);
  }
}
