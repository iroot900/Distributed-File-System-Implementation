package com.darren.parctice.hdfs;

import com.darren.parctice.rpc.Protocol;

import java.io.IOException;

public interface DatanodeProtocol extends Protocol {
  Block[] sendHeartBeat(DataNodeId dataNodeId) throws IOException;
  void blockReport(Block[] blocks, DataNodeId dataNodeId) throws IOException; 
  void blockRemoved(Block[] blocks, DataNodeId dataNodeId) throws IOException; 
}

/*
class Namenode implements DatanodeProtocol {

}
*/



