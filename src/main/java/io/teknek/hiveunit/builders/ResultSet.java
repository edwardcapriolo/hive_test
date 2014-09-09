package io.teknek.hiveunit.builders;

import java.util.ArrayList;
import java.util.List;

public class ResultSet {

  private List<Row> rows = new ArrayList<Row>();

  public ResultSet() {

  }

  public ResultSet withRow(Row row) {
    rows.add(row);
    return this;
  }

  public List<String> build() {
    List<String> res = new ArrayList<String>();
    for (Row r : rows) {
      res.add(r.toString());
    }
    return res;
  }
}
