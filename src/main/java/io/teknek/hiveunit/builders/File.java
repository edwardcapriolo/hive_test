package io.teknek.hiveunit.builders;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class File {

  private BufferedWriter bw;
  
  public File(FileSystem fs, Path path) throws IOException{
    bw = new BufferedWriter(new OutputStreamWriter(fs.create(path)));
  }
  
  public File withRow (Row r) throws IOException{
    bw.write(r.toString());
    bw.write("\n");
    return this;
  }
  
  public void build() throws IOException {
    bw.close();
  }
  
}
