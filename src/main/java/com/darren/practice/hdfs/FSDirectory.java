package com.darren.parctice.hdfs;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;

import java.io.IOException;

/**
 *  Maintain file system status in memory and disk  (data, api) -- > functionality!
 *  1. a tree of the file system hiearchy 
 *  2. persistent fs image with help of editlog
 *  (let's be simple and stupid first, then add things:
 *      exception handling. replica/load balancing. concurrent access !)
 */

class FSDirectory {
    //there should be no manual operation under this directory.
    private String dir;

    static private String CUR_FS_IMAGE = "fsimage.cur";
    static private String NEW_FS_IMAGE = "fsimage.new";
    static private String OLD_FS_IMAGE = "fsimage.old";
    static private String FS_EDIT_LOG = "fsEditLog";

    private Path curImagePath;
    private Path newImagePath;
    private Path oldImagePath;
    private Path fsEditLogPath;

    //in memoery
    private INodeTree fsTree = new INodeTree();
    //in disc
    private FSEditLog fsEditLog = null;  

    public FSDirectory(String dir, boolean format) throws IOException {
      this.dir = dir;
      curImagePath = Paths.get(dir, CUR_FS_IMAGE);
      newImagePath = Paths.get(dir, NEW_FS_IMAGE);
      oldImagePath = Paths.get(dir, OLD_FS_IMAGE);
      fsEditLogPath = Paths.get(dir, FS_EDIT_LOG);

      if (format) {
        formatImageDirectory(dir);
      }
      
      Path path = Paths.get(dir);
      if (!Files.isDirectory(path)) {
        throw new IOException("not a valid diretory");
      }

      loadFSImage(dir);
    }

    //TO DO: all the public method should give more specific failure information!
    synchronized public boolean exist(String path) {
      return fsTree.getINode(path) != null;
    } 

    synchronized public boolean isDirectory(String path) {
      INodeTree.INode node = fsTree.getINode(path);
      return node != null && node.isDirectory;
    }

    synchronized public Block[] getFileInfo(String path) { //file not exist
        INodeTree.INode node = fsTree.getINode(path);
        return node == null ? null : node.blocks;
    }

    synchronized public boolean addFile(String path, Block[] blocks) throws IOException { //file exist, directory not exist
      INodeTree.INode node = fsTree.addINode(path, blocks);
      if (node != null) { 
        fsEditLog.logAddINode(path, blocks);
        return true;
      } else {
        return false;
      }
    }

    synchronized public boolean validToCreate(String path) {
      return fsTree.validToAdd(path);
    }

    synchronized public boolean mkdir(String path) throws IOException {//directory not exist or exist.
      INodeTree.INode node = fsTree.addINode(path);
      if (node != null) { 
        fsEditLog.logAddINode(path);
        return true;
      } else {
        return false;
      }
    }

    synchronized public Block[] removeFile(String path) throws IOException { //nothing to remove or remove a directory 
      INodeTree.INode node = fsTree.removeINode(path);
      if (node != null) {// 
        fsEditLog.logRemoveINode(path);
        if (node.isDirectory) {
          return new Block[0]; //remove a directory
        } else {
          return node.blocks; //remove a file
        }
      } else {
        return null;
      }
    }

    private void formatImageDirectory(String dir) throws IOException {
      //TODO delete recursively

      Files.deleteIfExists(curImagePath);
      Files.deleteIfExists(newImagePath);
      Files.deleteIfExists(oldImagePath);
      Files.deleteIfExists(fsEditLogPath);

      Path imageDir = Paths.get(dir);
      Files.deleteIfExists(imageDir);
      Files.createDirectory(imageDir);

      fsTree.saveImage(Files.newOutputStream(curImagePath));
    }

    private void loadFSImage(String dir) throws IOException {

      if (Files.exists(curImagePath) && Files.exists(newImagePath)) {
        Files.delete(newImagePath);
      } else if (Files.exists(oldImagePath) && Files.exists(newImagePath)) { //old and new. we can remove old now actually. 
        //old and new.
        Files.deleteIfExists(fsEditLogPath);
        Files.move(newImagePath, curImagePath); //now  old and cur
        Files.delete(oldImagePath);
      } else if (Files.exists(oldImagePath) && Files.exists(curImagePath)) {
        Files.delete(oldImagePath);
      }

      if (Files.exists(curImagePath)) {
        fsTree.loadImage(Files.newInputStream(curImagePath));
      }

      if (Files.exists(fsEditLogPath)) {
        FSEditLog.loadEditLog(fsTree, fsEditLogPath.toString());
        saveFSImage(dir);
      }

      fsEditLog = new FSEditLog(fsEditLogPath.toString());
    }

    private void saveFSImage(String dir) throws IOException {
      //fail here, remove new.
      fsTree.saveImage(Files.newOutputStream(newImagePath)); 
      
      Files.move(curImagePath, oldImagePath);

      //fail here, old ,new (remove edits if there, then rename new -> cur);
      Files.delete(fsEditLogPath);
      Files.move(newImagePath, curImagePath);
      Files.delete(oldImagePath);
    }


    void close() throws IOException {
      fsEditLog.close();
    }
}
