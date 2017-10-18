package com.darren.parctice.util;

import java.io.IOException;

import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.DirectoryStream;

public abstract class Util {
  static public void deleteDirectory(String path) throws IOException {
    deleteDirectory(Paths.get(path));
  }

  static public void deleteDirectory(Path path) throws IOException {
    if (!Files.exists(path)) return;
    if (!Files.isDirectory(path)) throw new IOException("not a directory");
    DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path);

    for (Path file : directoryStream) {
        if (Files.isDirectory(file)) {
            deleteDirectory(file);
        } else {
            Files.delete(file);
        } 
    }
    Files.delete(path);
  }
}
