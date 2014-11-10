/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.slider.core.launch;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.api.RoleKeys;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.tools.CoreFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.MapOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Launcher of applications: base class
 */
public abstract class AbstractLauncher extends Configured {
  private static final Logger log =
    LoggerFactory.getLogger(AbstractLauncher.class);
  /**
   * Filesystem to use for the launch
   */
  protected final CoreFileSystem coreFileSystem;
  /**
   * Env vars; set up at final launch stage
   */
  protected final Map<String, String> envVars = new HashMap<String, String>();
  protected final MapOperations env = new MapOperations("env", envVars);
  protected final ContainerLaunchContext containerLaunchContext =
    Records.newRecord(ContainerLaunchContext.class);
  protected final List<String> commands = new ArrayList<String>(20);
  protected final Map<String, LocalResource> localResources =
    new HashMap<String, LocalResource>();
  private final Map<String, ByteBuffer> serviceData =
    new HashMap<String, ByteBuffer>();
  // security
  Credentials credentials = new Credentials();


  protected AbstractLauncher(Configuration conf,
                             CoreFileSystem fs) {
    super(conf);
    this.coreFileSystem = fs;
  }

  public AbstractLauncher(CoreFileSystem fs) {
    this.coreFileSystem = fs;
  }

  /**
   * Get the container. Until "completed", this isn't valid to launch.
   * @return the container to launch
   */
  public ContainerLaunchContext getContainerLaunchContext() {
    return containerLaunchContext;
  }

  /**
   * Get the env vars to work on
   * @return env vars
   */
  public MapOperations getEnv() {
    return env;
  }

  public List<String> getCommands() {
    return commands;
  }

  public Map<String, LocalResource> getLocalResources() {
    return localResources;
  }

  public void addLocalResource(String subpath, LocalResource resource) {
    localResources.put(subpath, resource);
  }

  /**
   * Add a set of local resources
   * @param resourceMap map of name:resource to add
   */
  public void addLocalResources(Map<String, LocalResource> resourceMap) {
    localResources.putAll(resourceMap);
  }


  public Map<String, ByteBuffer> getServiceData() {
    return serviceData;
  }


  /**
   * Add a command line. It is converted to a single command before being
   * added.
   * @param cmd
   */
  public void addCommandLine(CommandLineBuilder cmd) {
    commands.add(cmd.build());
  }

  public void addCommand(String cmd) {
    commands.add(cmd);
  }

  /**
   * Add a list of commands. Each element in the list becomes a single command
   * @param commandList
   */
  public void addCommands(List<String> commandList) {
    commands.addAll(commandList);
  }

  /**
   * Get all commands as a string, separated by ";". This is for diagnostics
   * @return a string descriptionof the commands
   */
  public String getCommandsAsString() {
    return SliderUtils.join(getCommands(), "; ");
  }

  /**
   * Complete the launch context (copy in env vars, etc).
   * @return the container to launch
   */
  public ContainerLaunchContext completeContainerLaunch() throws IOException {
    dumpLocalResources();

    String cmdStr = SliderUtils.join(commands, " ", false);
    log.debug("Completed setting up container command {}", cmdStr);
    containerLaunchContext.setCommands(commands);

    //fix the env variables
    containerLaunchContext.setEnvironment(env);
    //service data
    containerLaunchContext.setServiceData(serviceData);
    containerLaunchContext.setLocalResources(localResources);


    DataOutputBuffer dob = new DataOutputBuffer();
    if (this.getConf().get("mapreduce.job.credentials.binary") != null) {
      // use delegation tokens, i.e. from Oozie
      Credentials creds = UserGroupInformation.getLoginUser().getCredentials();
      creds.writeTokenStorageToStream(dob);
    } else {
      // normal auth
      credentials.writeTokenStorageToStream(dob);
    }

    ByteBuffer tokenBuffer = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    containerLaunchContext.setTokens(tokenBuffer);


    return containerLaunchContext;
  }

  /**
   * Dump local resources at debug level
   */
  private void dumpLocalResources() {
    if (log.isDebugEnabled()) {
      log.debug("{} resources: ", localResources.size());
      for (Map.Entry<String, LocalResource> entry : localResources.entrySet()) {

        String key = entry.getKey();
        LocalResource val = entry.getValue();
        log.debug(key + "=" + SliderUtils.stringify(val.getResource()));
      }
    }
  }

  /**
   * This is critical for an insecure cluster -it passes
   * down the username to YARN, and so gives the code running
   * in containers the rights it needs to work with
   * data.
   * @throws IOException problems working with current user
   */
  protected void propagateUsernameInInsecureCluster() throws IOException {
    //insecure cluster: propagate user name via env variable
    String userName = UserGroupInformation.getCurrentUser().getUserName();
    env.put(SliderKeys.HADOOP_USER_NAME, userName);
  }

  /**
   * Extract any resource requirements from this component's settings.
   * All fields that are set will override the existing values -if
   * unset that resource field will be left unchanged.
   *
   * Important: the configuration must already be fully resolved 
   * in order to pick up global options.
   * @param resource resource to configure
   * @param map map of options
   */
  public void extractResourceRequirements(Resource resource,
                                          Map<String, String> map) {


    if (map != null) {
      MapOperations options = new MapOperations("", map);
      resource.setMemory(options.getOptionInt(ResourceKeys.YARN_MEMORY,
                                              resource.getMemory()));
      resource.setVirtualCores(options.getOptionInt(ResourceKeys.YARN_CORES,
                                                    resource.getVirtualCores()));
    }
  }


  /**
   * Utility method to set up the classpath
   * @param classpath classpath to use
   */
  public void setClasspath(ClasspathConstructor classpath) {
    setEnv("CLASSPATH", classpath.buildClasspath());
  }
  public void setEnv(String var, String value) {
    Preconditions.checkArgument(var != null, "null variable name");
    Preconditions.checkArgument(value != null, "null value");
    env.put(var, value);
  }

  public void putEnv(Map<String, String> map) {
    env.putAll(map);
  }

  /**
   * Important: the configuration must already be fully resolved 
   * in order to pick up global options
   * Copy env vars into the launch context.
   */
  public boolean copyEnvVars(MapOperations options) {
    if (options == null) {
      return false;
    }
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(RoleKeys.ENV_PREFIX)) {
        key = key.substring(RoleKeys.ENV_PREFIX.length());
        env.put(key, entry.getValue());
      }
    }
    return true;
  }

  public String[] dumpEnvToString() {

    List<String> nodeEnv = new ArrayList<String>();

    for (Map.Entry<String, String> entry : env.entrySet()) {
      String envElt = String.format("%s=\"%s\"",
                                    entry.getKey(),
                                    entry.getValue());
      log.debug(envElt);
      nodeEnv.add(envElt);
    }
    String[] envDescription = nodeEnv.toArray(new String[nodeEnv.size()]);

    return envDescription;
  }

  /**
   * Suubmit an entire directory
   * @param srcDir src path in filesystem
   * @param destRelativeDir relative path under destination local dir
   * @throws IOException IO problems
   */
  public void submitDirectory(Path srcDir, String destRelativeDir) throws
                                                                   IOException {
    //add the configuration resources
    Map<String, LocalResource> confResources;
    confResources = coreFileSystem.submitDirectory(
      srcDir,
      destRelativeDir);
    addLocalResources(confResources);
  }


}
