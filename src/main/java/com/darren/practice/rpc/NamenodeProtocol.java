package com.darren.parctice.rpc;

import java.io.IOException;

public interface NamenodeProtocol extends Protocol {
  String getFileStatus(String path) throws IOException;
  double getDouble(Integer arg) throws IOException;
  int getFileSize(String file) throws Exception;
}

class NamenodeMock implements NamenodeProtocol {
  public String getFileStatus(String path) throws IOException {
    return path + " hello world!";
  }

  public double getDouble(Integer arg) throws IOException {
    return 12.3456;
  }

  public int getFileSize(String file) throws Exception {
    if (file.equals("good")) return 1024;
    if (file.equals("bad")) throw new Exception("there is not such file");
    return -1;
  }
}



