package com.darren.parctice.hdfs;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;

/**
 *  All the namespace modification should be logged and persistent
 *  1.data change should be done first.
 *  2.when it's complete, modify namespace and editlog the change
 *  3.return success to client. (once client got success then it's there and persisted)
 *
 *  if fail, then? 
 *  data change fail, then there should be some dameon to clean it up, no namespace change ok!
 *  data change succed, namespace not changed. same as data change fail. 
 *
 *  data change succed, namespace changed. (client fail, when client check, it will say it's there, then client will handle this inconsistency by delete)
 *  if logged, then in namespace -- ?? 
 *
 *  when fail :: namespace, editlog, data, client. ? 
 *  once in namespace, it should be in editlog. others will acess this and say it's there, but it gone? then bad ok it should be there.
 *  editlog first, namespace then.. if namespace change is done. then it's there! -- even client see failure. but it actually succeed.
 *  editlog success, namespace fail. --  namespace fail.. the system is done. when it up, it recover it. ok.
 */

class FSEditLog {

    private static final byte OP_ADD = 0;
    //private static final byte OP_RENAME = 1;
    private static final byte OP_DELETE = 2;
    private static final byte OP_MKDIR = 3;
    
    private String editLog;
    private ObjectOutputStream out; 
    
    public FSEditLog(String editLog) throws IOException {
      this.editLog = editLog;
      out = new ObjectOutputStream(new FileOutputStream(editLog, true)); //direcotry append./should not overwrite if it's there
    }

    //if EditLog file then, this operation should not be return as success.  so operation fail due to interal (framework issue); not app logic
    synchronized void logEdit(byte OP, Object arg1, Object arg2) throws IOException {
      out.write(OP);
      if (arg1 != null) {
        out.writeObject(arg1);
      }

      if (arg2 != null) {
        out.writeObject(arg2);
      }
    }

    synchronized public void logAddINode(String dir) throws IOException { //add directory
      logEdit(OP_MKDIR, dir, null); 
    }

    synchronized public void logAddINode(String fileName, Block[] blocks) throws IOException {
      logEdit(OP_ADD, fileName, blocks); 
    }

    synchronized public void logRemoveINode(String path) throws IOException {
      logEdit(OP_DELETE, path, null);
    }

    //load all operation and apply it to a INodeTree
    static void loadEditLog(INodeTree fsTree, String editLog) throws IOException {
      ObjectInputStream in = new ObjectInputStream(new FileInputStream(editLog));

      while (in.available() > 0) {
        byte OP = in.readByte();

        try {
          switch(OP) {
            case OP_MKDIR: {
                String dir = (String) in.readObject();
                fsTree.addINode(dir);
                break; 
            }
            case OP_ADD: { 
                String fileName = (String) in.readObject();
                Block[] blocks = (Block[]) in.readObject();
                fsTree.addINode(fileName, blocks);
                break; 
            }
            case OP_DELETE: { 
                String path = (String) in.readObject();
                fsTree.removeINode(path);
                break; 
            }
            default: {
               throw new IOException("no such operation defined");
            }
          }
        } catch (ClassNotFoundException e) {
          throw new IOException("io error in edit log", e);
        }
      }

      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          //
        }
      }
    }

    void close() throws IOException {
        out.close();
    }
}
