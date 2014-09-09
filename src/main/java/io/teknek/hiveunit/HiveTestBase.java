/*
Copyright 2011 Edward Capriolo

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package io.teknek.hiveunit;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class HiveTestBase extends HadoopTestCase {

  protected final static Logger LOGGER = Logger.getLogger(HiveTestBase.class.getName());
  protected static final Path ROOT_DIR = new Path("testing");
  protected List<File> toCleanUp = new ArrayList<File>();

  public HiveTestBase() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 1, 1);
    Map<String, String> env = new HashMap<String, String>();
    env.putAll(System.getenv());

    if (System.getenv("HADOOP_HOME") == null) {
      String shome = System.getProperty("user.home");
      if (shome != null) {
        File hadoopHome = new File(new File(shome, "hadoop"), "hadoop-1.2.1_local");
        if (hadoopHome.exists()) {
          env.put("HADOOP_HOME", hadoopHome.getAbsolutePath());
          EnvironmentHack.setEnv(env);
          LOGGER.info(String.format("HADOOP_HOME was set by hiveunit to %1$s.", System.getenv("HADOOP_HOME")));
        }
        File target = new File("target/hadoop-1.2.1");
        if (target.exists()) {
          env.put("HADOOP_HOME", target.getAbsolutePath());
          EnvironmentHack.setEnv(env);
          LOGGER.info(String.format("HADOOP_HOME was set by hiveunit to %1$s.", System.getenv("HADOOP_HOME")));
        }
      } else {
        LOGGER.info(String.format("HADOOP_HOME was found in environment as %1$s. Using that for hiveunit.", System.getenv("HADOOP_HOME")));
      }
      //detect variable or hadoop in path and throw exception here?
    }
  }

  protected Path getDir(Path dir) {
    if (isLocalFS()) {
      String localPathRoot = System.getProperty("test.build.data", "/tmp").replace(' ', '+');
      dir = new Path(localPathRoot, dir);
    }
    return dir;
  }

  /**
   * Hive InputFormats and Serdes may require some classes to exist in the auxlib or lib folder. This method
   * finds the jar file for a given class and copies that jar to the hadoop lib directory. (this only works with a writable
   * hadoop_home) the teardown method of this class should automatically remove this file.
   *
   * @param cl
   */
  public void addJarFileToLib(Class cl) {
    try {
      File source = new File(cl.getProtectionDomain().getCodeSource().getLocation().toURI());
      File destination = new File(System.getenv("HADOOP_HOME"), "lib");
      if (destination.exists() && destination.isDirectory() && destination.canWrite()) {
        LOGGER.info("copy " + source.getPath() + "destination " + destination.getPath());
        File destinationFile = new File(destination, source.getName());
        Files.copy(source, destinationFile);
        toCleanUp.add(destinationFile);
      } else {
        throw new RuntimeException("Did not add jar file to " + destination + " Permissions?");
      }
    } catch (IOException e) {
      throw new RuntimeException("unable to copy to hadoop_home" + e);
    } catch (URISyntaxException e) {
      throw new RuntimeException("unable to copy to hadoop_home" + e);
    }
  }

  public void setUp() throws Exception {
    super.setUp();
    String jarFile = org.apache.hadoop.hive.ql.exec.CopyTask.class.getProtectionDomain().getCodeSource().getLocation().getFile();
    System.setProperty(HiveConf.ConfVars.HIVEJAR.toString(), jarFile);
    Path rootDir = getDir(ROOT_DIR);
    Configuration conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);
    fs.delete(rootDir, true);
    Path metastorePath = new Path("/tmp/metastore_db");
    fs.delete(metastorePath, true);
    Path warehouse = new Path("/tmp/warehouse");
    fs.delete(warehouse, true);
    fs.mkdirs(warehouse);
  }

  public void tearDown() throws Exception {
    super.tearDown();
    for (File f : this.toCleanUp) {
      boolean result = f.delete();
      if (!result) {
        LOGGER.warn("count not delete " + f);
      }
    }
  }
}
