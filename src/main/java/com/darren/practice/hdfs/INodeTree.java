package com.darren.parctice.hdfs;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.nio.file.Path;
import java.nio.file.Paths;

import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.Serializable;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

import java.util.TreeMap;
import java.util.Queue;
import java.util.ArrayDeque;

/**
 *  a tree of the file system hiearchy 
 */

//All the path should be normalized and checked before

//limit support: (these are well defined, will expanded later) --
//could make it client / userd side or modify api. [get the basic and fundamental right] 
//remove and add only leaf node
class INodeTree {

  private INode root = null; 

  //The tree is serilized by a BFS. lightweight O(n) 
  //Tree node is using defaul ser/des, this could be easily optimized
  //for now, leave it as it is, coz INode info might change frequently while dev
  static class INode implements Serializable {
    //ok private, it's inner class 
    String name; 
    TreeMap<String, INode> children;
    Block[] blocks;
    boolean isDirectory;
    int numChildren = 0;

    public INode(String name, Block[] blocks) {
      this.name = name;
      this.blocks = blocks;
      this.isDirectory = false;
    }

    public INode(String name) {
      this.name = name;
      this.isDirectory = true;
      this.children = new TreeMap<String, INode>();
    }
  }

  public INodeTree() {
    root = new INode("/");
  }
  
  public void saveImage(OutputStream out) throws IOException {
    Queue<INode> que = new ArrayDeque<>();
    que.add(root);

    ObjectOutputStream objOut = new ObjectOutputStream(out);

    while (!que.isEmpty()) {
      INode node = que.poll();
      objOut.writeObject(node);

      if (node.children == null) {
        continue;
      }

      for (INode child : node.children.values()) {
        que.offer(child);
      }
    }

    objOut.close();
  }

  public void loadImage(InputStream in) throws IOException {
    Queue<INode> que = new ArrayDeque<>();
    ObjectInputStream objIn = new ObjectInputStream(in);

    try {
      //load root..
      root = (INode) objIn.readObject();
      que.offer(root);

      while (!que.isEmpty()) {
        INode node = que.poll();
        int numChildren = node.numChildren;
        for (int i = 0; i < numChildren; ++i) {
          INode childNode = (INode) objIn.readObject();
          node.children.put(childNode.name, childNode);
          que.offer(childNode);
        }
      }
    } catch (ClassNotFoundException ex) {
      throw new IOException("fail to loadImage", ex);
    }
    objIn.close();
  }

  public boolean validToAdd(String path) {
    String[] names = pathComponents(path);
    if (names == null || names.length == 0) return false; 

    int namesLen = names.length;
    INode parent = getINode(names, namesLen - 1);
    if (parent == null || !parent.isDirectory 
        || parent.children.containsKey(names[namesLen - 1])) return false;

    return true;
  }

  //for add for file
  public INode addINode(String path, Block[] blocks) {
    String[] names = pathComponents(path);
    if (names == null || names.length == 0) return null; 

    int namesLen = names.length;
    INode parent = getINode(names, namesLen - 1);
    if (parent == null || !parent.isDirectory 
        || parent.children.containsKey(names[namesLen - 1])) return null;

    INode node = null;
    if (blocks != null) {
        node = new INode(names[namesLen - 1], blocks);
    } else {
        node = new INode(names[namesLen - 1]);
    }
    parent.children.put(node.name, node); 
    parent.numChildren++;
    return node;
  }

  //for add a diretory.
  public INode addINode(String path) {
    return addINode(path, null);
  }

  public INode getINode(String path) { //this will get root ok
    String[] names = pathComponents(path);
    if (names == null) return null;

    return getINode(names, names.length);
  }


  private INode getINode(String[] names, int end) {
    INode cur = root;
    for (int i = 0; i < end; ++i) {
      if (cur.children == null || !cur.children.containsKey(names[i])) return null;
      cur = cur.children.get(names[i]);
    }
    return cur;
  }

  //exit of not?
  public INode removeINode(String path) {
    String[] names = pathComponents(path);
    if (names == null || names.length == 0) return null; //can't remove root

    int namesLen = names.length;
    INode parent = getINode(names, namesLen - 1);
    if (parent == null || !parent.isDirectory) return null; //parent should be a directry
    
    INode node = parent.children.get(names[namesLen - 1]);
    if (node == null || (node.isDirectory && node.children.size() > 0)) return null;

    parent.children.remove(node.name);
    parent.numChildren--;
    return node;
  }

  private String[] pathComponents(String path) {
    if (path == null) return null;
    String normPath = Paths.get(path).normalize().toString();
    if (!normPath.startsWith("/")) return null;
    if (normPath.length() == 1) return new String[0]; // for "root" return nothing.
    return normPath.substring(1).split("/");
  }
}
