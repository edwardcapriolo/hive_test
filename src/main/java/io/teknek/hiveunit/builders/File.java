package io.teknek.hiveunit.builders;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class File {

  private BufferedWriter bw;

  public File(FileSystem fs, Path path) throws IOException {
    bw = new BufferedWriter(new OutputStreamWriter(fs.create(path)));
  }

  public static File File(FileSystem fs, Path path) throws IOException {
    return new File(fs, path);
  }

  /**
   * Writes a row to output file with a newline after it.
   *
   * @param row
   * @return this instance
   * @throws IOException
   */
  public File withRow(Row row) throws IOException {
    bw.write(row.toString());
    bw.write("\n");
    return this;
  }

  /**
   * Closes the underlying resources flushing the file and closing it. Note this
   * method can only be called once in the lifetime of a File Instance.
   *
   * @throws IOException
   */
  public void build() throws IOException {
    bw.flush();
    bw.close();
  }

}
