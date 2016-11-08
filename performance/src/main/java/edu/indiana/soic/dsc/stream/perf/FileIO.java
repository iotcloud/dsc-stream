package edu.indiana.soic.dsc.stream.perf;

import java.io.*;

public class FileIO {
  private PrintWriter pw;

  private BufferedReader br;

  public FileIO(String file, boolean write) {
    try {
      if (write) {
        pw = new PrintWriter(new FileWriter(file));
      } else {
        br = new BufferedReader(new FileReader(file));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void writeResult(String value) {
    pw.printf("%s\n", value);
    pw.flush();
  }
}

