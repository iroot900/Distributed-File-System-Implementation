package com.darren.parctice.hdfs;

import java.io.Serializable;
import java.io.IOException;

import java.net.InetSocketAddress;
import java.net.InetAddress;

public class DataNodeId implements Serializable {
  InetSocketAddress socketAddress;

  public DataNodeId(String host, int port) throws IOException {
    socketAddress = new InetSocketAddress(InetAddress.getByName(host), port);
  }

  public DataNodeId(InetSocketAddress socketAddress) {
    this.socketAddress = socketAddress;
  }
  
  public int getPort() {
    return socketAddress.getPort();
  }

  public InetAddress getAddress() {
    return socketAddress.getAddress();
  }

  @Override 
  public int hashCode() {
    return socketAddress.hashCode();
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof DataNodeId) {
      return socketAddress.equals(((DataNodeId)that).socketAddress);
    } else {
      return false;
    }   
  }
}
