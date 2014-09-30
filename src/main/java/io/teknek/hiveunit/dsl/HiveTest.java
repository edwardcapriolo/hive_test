package io.teknek.hiveunit.dsl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.teknek.hiveunit.client.HiveClient;
import io.teknek.hiveunit.common.Response;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Provides methods for easier Hive scripts testing
 */
public class HiveTest {
  private Hive hive;
  private String scriptFile;
  private Map<String, String> params;
  private HiveClient hiveClient;

  protected HiveTest(HiveClient hiveClient, String scriptFile) {
    this(hiveClient, scriptFile, null);
  }

  protected HiveTest(HiveClient hiveClient, String scriptFile, Map<String, String> params) {
    if (params == null) {
      params = Maps.newHashMap();
    }
    this.hiveClient = hiveClient;
    this.scriptFile = scriptFile;
    this.params = params;
  }

  /**
   * Enables the user to get the output for a specific input in
   * order to be able to perform assertions.
   *
   * @param input Map<String, List<String>> variables referring to files to be loaded.
   * @return Response;
   */
  public Response outputForInput(Map<String, List<String>> input) {
    List<File> inputs = Lists.newArrayList();
    Map<String, File> inputFiles = Maps.newHashMap();
    for (String inputKey : input.keySet()) {
      try {
        File file = File.createTempFile(UUID.randomUUID().toString(), ".tmp");
        PrintWriter writer = new PrintWriter(file, "UTF-8");
        for (String line : input.get(inputKey)) {
          writer.println(line);
        }
        writer.flush();
        inputs.add(file);
        inputFiles.put(inputKey, file);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    Response response = outputForInputFiles(inputFiles);
    //cleanup aux files to avoid filling too much space
    for (File file : inputs) {
      file.delete();
    }
    return response;
  }

  /**
   * Enables the user to get the output for a specific input in
   * order to be able to perform assertions.
   *
   * @param input - Map<String, File> input
   * @return Response;
   */
  public Response outputForInputFiles(Map<String, File> input) {
    Map<String, String> params = Maps.<String, String>newHashMap();
    params.putAll(this.params);
    for (String inputKey : input.keySet()) {
      params.put(inputKey, input.get(inputKey).getAbsolutePath());
    }
    this.hive = new Hive(hiveClient, scriptFile, params);

    return hive.execute();
  }
}
