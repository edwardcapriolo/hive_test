package io.teknek.hiveunit.builders;

public class Row {
  private StringBuffer sb = new StringBuffer();

  public Row() {
  }

  ;

  public Row withColumn(String data) {
    sb.append(data).append("\t");
    return this;
  }

  public String toString() {
    if (sb.length() > 0 && sb.charAt(sb.length() - 1) == '\t') {
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }
}