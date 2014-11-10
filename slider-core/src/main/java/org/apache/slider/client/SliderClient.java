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

package org.apache.slider.client;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.ClusterNode;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.api.OptionKeys;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.api.SliderClusterProtocol;
import org.apache.slider.api.proto.Messages;
import org.apache.slider.common.Constants;
import org.apache.slider.common.SliderExitCodes;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.params.AbstractClusterBuildingActionArgs;
import org.apache.slider.common.params.ActionAMSuicideArgs;
import org.apache.slider.common.params.ActionCreateArgs;
import org.apache.slider.common.params.ActionEchoArgs;
import org.apache.slider.common.params.ActionFlexArgs;
import org.apache.slider.common.params.ActionFreezeArgs;
import org.apache.slider.common.params.ActionGetConfArgs;
import org.apache.slider.common.params.ActionKillContainerArgs;
import org.apache.slider.common.params.ActionRegistryArgs;
import org.apache.slider.common.params.ActionStatusArgs;
import org.apache.slider.common.params.ActionThawArgs;
import org.apache.slider.common.params.Arguments;
import org.apache.slider.common.params.ClientArgs;
import org.apache.slider.common.params.LaunchArgsAccessor;
import org.apache.slider.common.tools.ConfigHelper;
import org.apache.slider.common.tools.Duration;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.common.tools.SliderVersionInfo;
import org.apache.slider.core.build.InstanceBuilder;
import org.apache.slider.core.build.InstanceIO;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.conf.ResourcesInputPropertiesValidator;
import org.apache.slider.core.conf.TemplateInputPropertiesValidator;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.ErrorStrings;
import org.apache.slider.core.exceptions.NoSuchNodeException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException;
import org.apache.slider.core.exceptions.WaitTimeoutException;
import org.apache.slider.core.launch.AppMasterLauncher;
import org.apache.slider.core.launch.ClasspathConstructor;
import org.apache.slider.core.launch.CommandLineBuilder;
import org.apache.slider.core.launch.JavaCommandLineBuilder;
import org.apache.slider.core.launch.LaunchedApplication;
import org.apache.slider.core.launch.RunningApplication;
import org.apache.slider.core.main.RunService;
import org.apache.slider.core.persist.ConfPersister;
import org.apache.slider.core.persist.LockAcquireFailedException;
import org.apache.slider.core.registry.YARNRegistryClient;
import org.apache.slider.core.registry.docstore.ConfigFormat;
import org.apache.slider.core.registry.docstore.PublishedConfigSet;
import org.apache.slider.core.registry.docstore.PublishedConfiguration;
import org.apache.slider.core.registry.docstore.PublishedConfigurationOutputter;
import org.apache.slider.core.registry.info.RegisteredEndpoint;
import org.apache.slider.core.registry.info.ServiceInstanceData;
import org.apache.slider.core.registry.retrieve.RegistryRetriever;
import org.apache.slider.core.zk.BlockingZKWatcher;
import org.apache.slider.core.zk.ZKIntegration;
import org.apache.slider.core.zk.ZKPathBuilder;
import org.apache.slider.providers.AbstractClientProvider;
import org.apache.slider.providers.SliderProviderFactory;
import org.apache.slider.providers.agent.AgentKeys;
import org.apache.slider.providers.slideram.SliderAMClientProvider;
import org.apache.slider.server.appmaster.SliderAppMaster;
import org.apache.slider.server.appmaster.rpc.RpcBinder;
import org.apache.slider.server.services.curator.CuratorServiceInstance;
import org.apache.slider.server.services.registry.SliderRegistryService;
import org.apache.slider.server.services.utility.AbstractSliderLaunchedService;

import static org.apache.slider.common.params.SliderActions.*;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Client service for Slider
 */

public class SliderClient extends AbstractSliderLaunchedService implements RunService,
    SliderExitCodes, SliderKeys, ErrorStrings {
  private static final Logger log = LoggerFactory.getLogger(SliderClient.class);

  private ClientArgs serviceArgs;
  public ApplicationId applicationId;
  
  private String deployedClusterName;
  /**
   * Cluster opaerations against the deployed cluster -will be null
   * if no bonding has yet taken place
   */
  private SliderClusterOperations sliderClusterOperations;

  private SliderFileSystem sliderFileSystem;

  /**
   * Yarn client service
   */
  private SliderYarnClientImpl yarnClient;
  private YARNRegistryClient YARNRegistryClient;
  private AggregateConf launchedInstanceDefinition;
  private SliderRegistryService registry;

  /**
   * Constructor
   */
  public SliderClient() {
    super("Slider Client");
    new HdfsConfiguration();
    new YarnConfiguration();
  }

  @Override
  public Configuration bindArgs(Configuration config, String... args) throws Exception {
    config = super.bindArgs(config, args);
    serviceArgs = new ClientArgs(args);
    serviceArgs.parse();
    // yarn-ify
    YarnConfiguration yarnConfiguration = new YarnConfiguration(config);
    return SliderUtils.patchConfiguration(yarnConfiguration);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    Configuration clientConf = SliderUtils.loadClientConfigurationResource();
    ConfigHelper.mergeConfigurations(conf, clientConf, CLIENT_RESOURCE);
    serviceArgs.applyDefinitions(conf);
    serviceArgs.applyFileSystemBinding(conf);
    // init security with our conf
    if (SliderUtils.isHadoopClusterSecure(conf)) {
      SliderUtils.forceLogin();
      SliderUtils.initProcessSecurity(conf);
    }
    //create the YARN client
    yarnClient = new SliderYarnClientImpl();
    addService(yarnClient);

    super.serviceInit(conf);
    
    //here the superclass is inited; getConfig returns a non-null value
    sliderFileSystem = new SliderFileSystem(getConfig());
    YARNRegistryClient =
      new YARNRegistryClient(yarnClient, getUsername(), getConfig());
  }

  /**
   * this is where the work is done.
   * @return the exit code
   * @throws Throwable anything that went wrong
   */
/* JDK7

  @Override
  public int runService() throws Throwable {

    // choose the action
    String action = serviceArgs.getAction();
    int exitCode = EXIT_SUCCESS;
    String clusterName = serviceArgs.getClusterName();
    // actions
    switch (action) {
      case ACTION_BUILD:
        exitCode = actionBuild(clusterName, serviceArgs.getActionBuildArgs());
        break;
      case ACTION_UPDATE:
        exitCode = actionUpdate(clusterName, serviceArgs.getActionUpdateArgs());
        break;
      case ACTION_CREATE:
        exitCode = actionCreate(clusterName, serviceArgs.getActionCreateArgs());
        break;
      case ACTION_FREEZE:
        exitCode = actionFreeze(clusterName, serviceArgs.getActionFreezeArgs());
        break;
      case ACTION_THAW:
        exitCode = actionThaw(clusterName, serviceArgs.getActionThawArgs());
        break;
      case ACTION_DESTROY:
        exitCode = actionDestroy(clusterName);
        break;
      case ACTION_EXISTS:
        exitCode = actionExists(clusterName,
            serviceArgs.getActionExistsArgs().live);
        break;
      case ACTION_FLEX:
        exitCode = actionFlex(clusterName, serviceArgs.getActionFlexArgs());
        break;
      case ACTION_GETCONF:
        exitCode =
            actionGetConf(clusterName, serviceArgs.getActionGetConfArgs());
        break;
      case ACTION_HELP:
      case ACTION_USAGE:
        log.info(serviceArgs.usage());
        break;
      case ACTION_KILL_CONTAINER:
        exitCode = actionKillContainer(clusterName,
            serviceArgs.getActionKillContainerArgs());
        break;
      case ACTION_AM_SUICIDE:
        exitCode = actionAmSuicide(clusterName,
            serviceArgs.getActionAMSuicideArgs());
        break;
      case ACTION_LIST:
        exitCode = actionList(clusterName);
        break;
      case ACTION_REGISTRY:
        exitCode = actionRegistry(
            serviceArgs.getActionRegistryArgs());
        break;
      case ACTION_STATUS:
        exitCode = actionStatus(clusterName,
            serviceArgs.getActionStatusArgs());
        break;
      case ACTION_VERSION:
        exitCode = actionVersion();
        break;
      default:
        throw new SliderException(EXIT_UNIMPLEMENTED,
            "Unimplemented: " + action);
    }

    return exitCode;
  }

*/
  @Override
  public int runService() throws Throwable {

    // choose the action
    String action = serviceArgs.getAction();
    int exitCode = EXIT_SUCCESS;
    String clusterName = serviceArgs.getClusterName();
    // actions
    if (ACTION_BUILD.equals(action)) {
      exitCode = actionBuild(clusterName, serviceArgs.getActionBuildArgs());
    } else if (ACTION_CREATE.equals(action)) {
      exitCode = actionCreate(clusterName, serviceArgs.getActionCreateArgs());
    } else if (ACTION_FREEZE.equals(action)) {
      exitCode = actionFreeze(clusterName,
          serviceArgs.getActionFreezeArgs());
    } else if (ACTION_THAW.equals(action)) {
      exitCode = actionThaw(clusterName, serviceArgs.getActionThawArgs());
    } else if (ACTION_DESTROY.equals(action)) {
      exitCode = actionDestroy(clusterName);
    } else if (ACTION_EXISTS.equals(action)) {
      exitCode = actionExists(clusterName,
          serviceArgs.getActionExistsArgs().live);
    } else if (ACTION_FLEX.equals(action)) {
      exitCode = actionFlex(clusterName, serviceArgs.getActionFlexArgs());
    } else if (ACTION_GETCONF.equals(action)) {
      exitCode = actionGetConf(clusterName, serviceArgs.getActionGetConfArgs());
    } else if (ACTION_HELP.equals(action) ||
               ACTION_USAGE.equals(action)) {
      log.info(serviceArgs.usage());

    } else if (ACTION_KILL_CONTAINER.equals(action)) {
      exitCode = actionKillContainer(clusterName,
          serviceArgs.getActionKillContainerArgs());

    } else if (ACTION_AM_SUICIDE.equals(action)) {
      exitCode = actionAmSuicide(clusterName,
          serviceArgs.getActionAMSuicideArgs());

    } else if (ACTION_LIST.equals(action)) {
      exitCode = actionList(clusterName);
    } else if (ACTION_REGISTRY.equals(action)) {
      exitCode = actionRegistry(
          serviceArgs.getActionRegistryArgs());
    } else if (ACTION_STATUS.equals(action)) {
      exitCode = actionStatus(clusterName,
          serviceArgs.getActionStatusArgs());
    } else if (ACTION_UPDATE.equals(action)) {
      exitCode = actionUpdate(clusterName, serviceArgs.getActionUpdateArgs());

    } else if (ACTION_VERSION.equals(action)) {

      exitCode = actionVersion();
    } else {
      throw new SliderException(EXIT_UNIMPLEMENTED,
          "Unimplemented: " + action);
    }

    return exitCode;
  }
  /**
   * Delete the zookeeper node associated with the calling user and the cluster
   **/
  protected boolean deleteZookeeperNode(String clusterName) throws YarnException, IOException {
    String user = getUsername();
    String zkPath = ZKIntegration.mkClusterPath(user, clusterName);
    Exception e = null;
    try {
      Configuration config = getConfig();
      if (!SliderUtils.isHadoopClusterSecure(config)) {
        ZKIntegration client = getZkClient(clusterName, user);
        if (client != null) {
          if (client.exists(zkPath)) {
            log.info("Deleting zookeeper path {}", zkPath);
          }
          client.deleteRecursive(zkPath);
          return true;
        }
      } else {
        log.warn("Default zookeeper node is not available for secure cluster");
      }
    } catch (InterruptedException ignored) {
      e = ignored;
    } catch (KeeperException ignored) {
      e = ignored;
    } catch (BadConfigException ignored) {
      e = ignored;
    }
    if (e != null) {
      log.warn("Unable to recursively delete zk node {}", zkPath);
      log.debug("Reason: ", e);
    }

    return false;
  }

  /**
   * Create the zookeeper node associated with the calling user and the cluster
   */
  protected String createZookeeperNode(String clusterName, Boolean nameOnly) throws YarnException, IOException {
    String user = getUsername();
    String zkPath = ZKIntegration.mkClusterPath(user, clusterName);
    if(nameOnly) {
      return zkPath;
    }
    Configuration config = getConfig();
    if (!SliderUtils.isHadoopClusterSecure(config)) {
      ZKIntegration client = getZkClient(clusterName, user);
      if (client != null) {
        try {
          client.createPath(zkPath, "", ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
          return zkPath;
          
          //JDK7
//        } catch (InterruptedException | KeeperException e) {
        } catch (InterruptedException e) {
          log.warn("Unable to create zk node {}", zkPath, e);
        } catch ( KeeperException e) {
          log.warn("Unable to create zk node {}", zkPath, e);
        }
      }
    }

    return null;
  }

  /**
   * Gets a zookeeper client, returns null if it cannot connect to zookeeper
   **/
  protected ZKIntegration getZkClient(String clusterName, String user) throws YarnException {
    String registryQuorum = lookupZKQuorum();
    ZKIntegration client = null;
    try {
      BlockingZKWatcher watcher = new BlockingZKWatcher();
      client = ZKIntegration.newInstance(registryQuorum, user, clusterName, true, false, watcher);
      client.init();
      watcher.waitForZKConnection(2 * 1000);
    } catch (InterruptedException e) {
      client = null;
      log.warn("Unable to connect to zookeeper quorum {}", registryQuorum, e);
    } catch (IOException e) {
      log.warn("Unable to connect to zookeeper quorum {}", registryQuorum, e);
    }
    return client;
  }

  /**
   * Destroy a cluster. There's two race conditions here
   * #1 the cluster is started between verifying that there are no live
   * clusters of that name.
   */
  public int actionDestroy(String clustername) throws YarnException,
                                                      IOException {
    // verify that a live cluster isn't there
    SliderUtils.validateClusterName(clustername);
    //no=op, it is now mandatory. 
    verifyBindingsDefined();
    verifyNoLiveClusters(clustername);

    // create the directory path
    Path clusterDirectory = sliderFileSystem.buildClusterDirPath(clustername);
    // delete the directory;
    boolean exists = sliderFileSystem.getFileSystem().exists(clusterDirectory);
    if (exists) {
      log.info("Application Instance {} found at {}: destroying", clustername, clusterDirectory);
    } else {
      log.info("Application Instance {} already destroyed", clustername);
    }
    boolean deleted =
      sliderFileSystem.getFileSystem().delete(clusterDirectory, true);
    if (!deleted) {
      log.warn("Filesystem returned false from delete() operation");
    }

    if(!deleteZookeeperNode(clustername)) {
      log.warn("Unable to perform node cleanup in Zookeeper.");
    }

    List<ApplicationReport> instances = findAllLiveInstances(clustername);
    // detect any race leading to cluster creation during the check/destroy process
    // and report a problem.
    if (!instances.isEmpty()) {
      throw new SliderException(EXIT_APPLICATION_IN_USE,
                              clustername + ": "
                              + E_DESTROY_CREATE_RACE_CONDITION
                              + " :" +
                              instances.get(0));
    }
    log.info("Destroyed cluster {}", clustername);
    return EXIT_SUCCESS;
  }
  
  /**
   * AM to commit an asynchronous suicide
   */
  public int actionAmSuicide(String clustername,
                                 ActionAMSuicideArgs args) throws
                                                              YarnException,
                                                              IOException {
    SliderClusterOperations clusterOperations =
      createClusterOperations(clustername);
    clusterOperations.amSuicide(args.message, args.exitcode, args.waittime);
    return EXIT_SUCCESS;
  }

  /**
   * Get the provider for this cluster
   * @param provider the name of the provider
   * @return the provider instance
   * @throws SliderException problems building the provider
   */
  private AbstractClientProvider createClientProvider(String provider)
    throws SliderException {
    SliderProviderFactory factory =
      SliderProviderFactory.createSliderProviderFactory(provider);
    return factory.createClientProvider();
  }

  /**
   * Create the cluster -saving the arguments to a specification file first
   * @param clustername cluster name
   * @return the status code
   * @throws YarnException Yarn problems
   * @throws IOException other problems
   * @throws BadCommandArgumentsException bad arguments.
   */
  public int actionCreate(String clustername, ActionCreateArgs createArgs) throws
                                               YarnException,
                                               IOException {

    actionBuild(clustername, createArgs);
    return startCluster(clustername, createArgs);
  }

  /**
   * Build up the cluster specification/directory
   *
   * @param clustername cluster name
   * @param buildInfo the arguments needed to build the cluster
   * @throws YarnException Yarn problems
   * @throws IOException other problems
   * @throws BadCommandArgumentsException bad arguments.
   */
  public int actionBuild(String clustername,
                           AbstractClusterBuildingActionArgs buildInfo) throws
                                               YarnException,
                                               IOException {

    buildInstanceDefinition(clustername, buildInfo, false, false);
    return EXIT_SUCCESS; 
  }

  /**
   * Update the cluster specification
   *
   * @param clustername cluster name
   * @param buildInfo the arguments needed to update the cluster
   * @throws YarnException Yarn problems
   * @throws IOException other problems
   */
  public int actionUpdate(String clustername, AbstractClusterBuildingActionArgs buildInfo) throws
      YarnException, IOException {
    buildInstanceDefinition(clustername, buildInfo, true, true);
    return EXIT_SUCCESS; 
  }

  /**
   * Build up the AggregateConfiguration for an application instance then
   * persists it
   * @param clustername name of the cluster
   * @param buildInfo the arguments needed to build the cluster
   * @param overwrite true if existing cluster directory can be overwritten
   * @param liveClusterAllowed true if live cluster can be modified
   * @throws YarnException
   * @throws IOException
   */
  
  public void buildInstanceDefinition(String clustername,
      AbstractClusterBuildingActionArgs buildInfo, boolean overwrite, boolean liveClusterAllowed)
        throws YarnException, IOException {
    // verify that a live cluster isn't there
    SliderUtils.validateClusterName(clustername);
    verifyBindingsDefined();
    if (!liveClusterAllowed) verifyNoLiveClusters(clustername);

    Configuration conf = getConfig();
    String registryQuorum = lookupZKQuorum();

    Path appconfdir = buildInfo.getConfdir();
    // Provider
    String providerName = buildInfo.getProvider();
    requireArgumentSet(Arguments.ARG_PROVIDER, providerName);
    log.debug("Provider is {}", providerName);
    SliderAMClientProvider sliderAM = new SliderAMClientProvider(conf);
    AbstractClientProvider provider =
      createClientProvider(providerName);
    InstanceBuilder builder =
      new InstanceBuilder(sliderFileSystem, 
                          getConfig(),
                          clustername);
    
    AggregateConf instanceDefinition = new AggregateConf();
    ConfTreeOperations appConf = instanceDefinition.getAppConfOperations();

    String tokenCredentials = conf.get("mapreduce.job.credentials.binary");
    if (tokenCredentials != null)
      appConf.set("mapreduce.job.credentials.binary", tokenCredentials);

    ConfTreeOperations resources = instanceDefinition.getResourceOperations();
    ConfTreeOperations internal = instanceDefinition.getInternalOperations();
    //initial definition is set by the providers 
    sliderAM.prepareInstanceConfiguration(instanceDefinition);
    provider.prepareInstanceConfiguration(instanceDefinition);

    //load in any specified on the command line
    if (buildInfo.resources != null) {
      try {
        resources.mergeFile(buildInfo.resources,
                            new ResourcesInputPropertiesValidator());

      } catch (IOException e) {
        throw new BadConfigException(e,
               "incorrect argument to %s: \"%s\" : %s ", 
                                     Arguments.ARG_RESOURCES,
                                     buildInfo.resources,
                                     e.toString());
      }
    }
    if (buildInfo.template != null) {
      try {
        appConf.mergeFile(buildInfo.template,
                          new TemplateInputPropertiesValidator());
      } catch (IOException e) {
        throw new BadConfigException(e,
                                     "incorrect argument to %s: \"%s\" : %s ",
                                     Arguments.ARG_TEMPLATE,
                                     buildInfo.template,
                                     e.toString());
      }
    }

    //get the command line options
    ConfTree cmdLineAppOptions = buildInfo.buildAppOptionsConfTree();
    ConfTree cmdLineResourceOptions = buildInfo.buildResourceOptionsConfTree();

    appConf.merge(cmdLineAppOptions);

    // put the role counts into the resources file
    Map<String, String> argsRoleMap = buildInfo.getComponentMap();
    for (Map.Entry<String, String> roleEntry : argsRoleMap.entrySet()) {
      String count = roleEntry.getValue();
      String key = roleEntry.getKey();
      log.debug("{} => {}", key, count);
      resources.getOrAddComponent(key)
                 .put(ResourceKeys.COMPONENT_INSTANCES, count);
    }

    //all CLI role options
    Map<String, Map<String, String>> appOptionMap =
      buildInfo.getCompOptionMap();
    appConf.mergeComponents(appOptionMap);

    //internal picks up core. values only
    internal.propagateGlobalKeys(appConf, "slider.");
    internal.propagateGlobalKeys(appConf, "internal.");

    //copy over role. and yarn. values ONLY to the resources
    if (PROPAGATE_RESOURCE_OPTION) {
      resources.propagateGlobalKeys(appConf, "component.");
      resources.propagateGlobalKeys(appConf, "role.");
      resources.propagateGlobalKeys(appConf, "yarn.");
      resources.mergeComponentsPrefix(appOptionMap, "component.", true);
      resources.mergeComponentsPrefix(appOptionMap, "yarn.", true);
      resources.mergeComponentsPrefix(appOptionMap, "role.", true);
    }

    // resource component args
    appConf.merge(cmdLineResourceOptions);
    resources.merge(cmdLineResourceOptions);
    resources.mergeComponents(buildInfo.getResourceCompOptionMap());

    builder.init(providerName, instanceDefinition);
    builder.propagateFilename();
    builder.propagatePrincipals();
    builder.setImageDetails(buildInfo.getImage(), buildInfo.getAppHomeDir());


    String quorum = buildInfo.getZKhosts();
    if (SliderUtils.isUnset(quorum)) {
      quorum = registryQuorum;
    }
    if (isUnset(quorum)) {
      throw new BadConfigException("No Zookeeper quorum defined");
    }
    ZKPathBuilder zkPaths = new ZKPathBuilder(getAppName(),
        getUsername(),
        clustername,
        registryQuorum,
        quorum);
    String zookeeperRoot = buildInfo.getAppZKPath();

    if (isSet(zookeeperRoot)) {
      zkPaths.setAppPath(zookeeperRoot);
    } else {
      String createDefaultZkNode = appConf.getGlobalOptions().getOption(AgentKeys.CREATE_DEF_ZK_NODE, "false");
      if (createDefaultZkNode.equals("true")) {
        String defaultZKPath = createZookeeperNode(clustername, false);
        log.info("ZK node created for application instance: {}.", defaultZKPath);
        if (defaultZKPath != null) {
          zkPaths.setAppPath(defaultZKPath);
        }
      } else {
        // create AppPath if default is being used
        String defaultZKPath = createZookeeperNode(clustername, true);
        log.info("ZK node assigned to application instance: {}.", defaultZKPath);
        zkPaths.setAppPath(defaultZKPath);
      }
    }

    builder.addZKBinding(zkPaths);

    //then propagate any package URI
    if (buildInfo.packageURI != null) {
      appConf.set(AgentKeys.PACKAGE_PATH, buildInfo.packageURI);
    }

    // provider to validate what there is
    try {
      sliderAM.validateInstanceDefinition(builder.getInstanceDescription());
      provider.validateInstanceDefinition(builder.getInstanceDescription());
    } catch (SliderException e) {
      //problem, reject it
      log.info("Error {} validating application instance definition ", e.toString());
      log.debug("Error validating application instance definition ", e);
      log.info(instanceDefinition.toString());
      throw e;
    }
    try {
      builder.persist(appconfdir, overwrite);
    } catch (LockAcquireFailedException e) {
      log.warn("Failed to get a Lock on {} : {}", builder, e);
      throw new BadClusterStateException("Failed to save " + clustername
                                         + ": " + e);
    }
  }
  
  public FsPermission getClusterDirectoryPermissions(Configuration conf) {
    String clusterDirPermsOct =
      conf.get(CLUSTER_DIRECTORY_PERMISSIONS,
               DEFAULT_CLUSTER_DIRECTORY_PERMISSIONS);
    return new FsPermission(clusterDirPermsOct);
  }

  /**
   * Verify that the Resource MAnager is configured, if not fail
   * with a useful error message
   * @throws BadCommandArgumentsException the exception raised on an invalid config
   */
  public void verifyBindingsDefined() throws BadCommandArgumentsException {
    InetSocketAddress rmAddr = SliderUtils.getRmAddress(getConfig());
    if (!SliderUtils.isAddressDefined(rmAddr)) {
      throw new BadCommandArgumentsException(
        "No valid Resource Manager address provided in the argument "
        + Arguments.ARG_MANAGER
        + " or the configuration property "
        + YarnConfiguration.RM_ADDRESS 
        + " value :" + rmAddr);
    }

  }

  /**
   * Load and start a cluster specification.
   * This assumes that all validation of args and cluster state
   * have already taken place
   *
   * @param clustername name of the cluster.
   * @param launchArgs launch arguments
   * @return the exit code
   * @throws YarnException
   * @throws IOException
   */
  private int startCluster(String clustername,
                           LaunchArgsAccessor launchArgs) throws
                                                          YarnException,
                                                          IOException {
    Path clusterDirectory = sliderFileSystem.buildClusterDirPath(clustername);
    AggregateConf instanceDefinition = loadInstanceDefinitionUnresolved(
      clustername,
      clusterDirectory);

    LaunchedApplication launchedApplication =
      launchApplication(clustername, clusterDirectory, instanceDefinition,
                        serviceArgs.isDebug());
    applicationId = launchedApplication.getApplicationId();

    return waitForAppAccepted(launchedApplication, launchArgs.getWaittime());
  }

  /**
   * Load the instance definition. It is not resolved at this point
   * @param name cluster name
   * @param clusterDirectory cluster dir
   * @return the loaded configuration
   * @throws IOException
   * @throws SliderException
   * @throws UnknownApplicationInstanceException if the file is not found
   */
  public AggregateConf loadInstanceDefinitionUnresolved(String name,
                                                         Path clusterDirectory) throws
                                                                      IOException,
      SliderException {

    try {
      AggregateConf definition =
        InstanceIO.loadInstanceDefinitionUnresolved(sliderFileSystem,
                                                    clusterDirectory);
      return definition;
    } catch (FileNotFoundException e) {
      throw UnknownApplicationInstanceException.unknownInstance(name, e);
    }
  }
    /**
   * Load the instance definition. 
   * @param name cluster name
   * @param resolved flag to indicate the cluster should be resolved
   * @return the loaded configuration
   * @throws IOException IO problems
   * @throws SliderException slider explicit issues
   * @throws UnknownApplicationInstanceException if the file is not found
   */
    public AggregateConf loadInstanceDefinition(String name,
        boolean resolved) throws
        IOException,
        SliderException {

    Path clusterDirectory = sliderFileSystem.buildClusterDirPath(name);
    AggregateConf instanceDefinition = loadInstanceDefinitionUnresolved(
      name,
      clusterDirectory);
    if (resolved) {
      instanceDefinition.resolve();
    }
    return instanceDefinition;

  }

  /**
   *
   * @param clustername
   * @param clusterDirectory
   * @param instanceDefinition
   * @param debugAM
   * @return the launched application
   * @throws YarnException
   * @throws IOException
   */
  public LaunchedApplication launchApplication(String clustername,
                                               Path clusterDirectory,
                                               AggregateConf instanceDefinition,
                                               boolean debugAM)
    throws YarnException, IOException {


    deployedClusterName = clustername;
    SliderUtils.validateClusterName(clustername);
    verifyNoLiveClusters(clustername);
    Configuration config = getConfig();
    lookupZKQuorum();
    boolean clusterSecure = SliderUtils.isHadoopClusterSecure(config);
    //create the Slider AM provider -this helps set up the AM
    SliderAMClientProvider sliderAM = new SliderAMClientProvider(config);

    instanceDefinition.resolve();
    launchedInstanceDefinition = instanceDefinition;

    ConfTreeOperations internalOperations =
      instanceDefinition.getInternalOperations();
    MapOperations internalOptions = internalOperations.getGlobalOptions();
    ConfTreeOperations resourceOperations =
      instanceDefinition.getResourceOperations();
    ConfTreeOperations appOperations =
      instanceDefinition.getAppConfOperations();
    Path generatedConfDirPath =
      createPathThatMustExist(internalOptions.getMandatoryOption(
        InternalKeys.INTERNAL_GENERATED_CONF_PATH));
    Path snapshotConfPath =
      createPathThatMustExist(internalOptions.getMandatoryOption(
        InternalKeys.INTERNAL_SNAPSHOT_CONF_PATH));


    // cluster Provider
    AbstractClientProvider provider = createClientProvider(
      internalOptions.getMandatoryOption(
        InternalKeys.INTERNAL_PROVIDER_NAME));
    // make sure the conf dir is valid;
    
    // now build up the image path
    // TODO: consider supporting apps that don't have an image path
    Path imagePath =
      SliderUtils.extractImagePath(sliderFileSystem, internalOptions);
    if (log.isDebugEnabled()) {
      log.debug(instanceDefinition.toString());
    }
    MapOperations sliderAMResourceComponent =
      resourceOperations.getOrAddComponent(SliderKeys.COMPONENT_AM);

    // add the tags if available
    Set<String> applicationTags = provider.getApplicationTags(sliderFileSystem,
        appOperations.getGlobalOptions().get(AgentKeys.APP_DEF));
    AppMasterLauncher amLauncher = new AppMasterLauncher(clustername,
        SliderKeys.APP_TYPE,
        config,
        sliderFileSystem,
        yarnClient,
        clusterSecure,
        sliderAMResourceComponent,
        applicationTags);

    ApplicationId appId = amLauncher.getApplicationId();
    // set the application name;
    amLauncher.setKeepContainersOverRestarts(true);

    int maxAppAttempts = config.getInt(KEY_AM_RESTART_LIMIT, 0);
    amLauncher.setMaxAppAttempts(maxAppAttempts);

    sliderFileSystem.purgeAppInstanceTempFiles(clustername);
    Path tempPath = sliderFileSystem.createAppInstanceTempPath(
        clustername,
        appId.toString() + "/am");
    String libdir = "lib";
    Path libPath = new Path(tempPath, libdir);
    sliderFileSystem.getFileSystem().mkdirs(libPath);
    log.debug("FS={}, tempPath={}, libdir={}", sliderFileSystem.toString(),
              tempPath, libPath);
    // set local resources for the application master
    // local files or archives as needed
    // In this scenario, the jar file for the application master is part of the local resources
    Map<String, LocalResource> localResources = amLauncher.getLocalResources();
    // conf directory setup
    Path remoteConfPath = null;
    String relativeConfDir = null;
    String confdirProp =
      System.getProperty(SliderKeys.PROPERTY_CONF_DIR);
    if (confdirProp == null || confdirProp.isEmpty()) {
      log.debug("No local configuration directory provided as system property");
    } else {
      File confDir = new File(confdirProp);
      if (!confDir.exists()) {
        throw new BadConfigException(E_CONFIGURATION_DIRECTORY_NOT_FOUND,
                                     confDir);
      }
      Path localConfDirPath = SliderUtils.createLocalPath(confDir);
      log.debug("Copying AM configuration data from {}", localConfDirPath);
      remoteConfPath = new Path(clusterDirectory,
                                    SliderKeys.SUBMITTED_CONF_DIR);
      SliderUtils.copyDirectory(config, localConfDirPath, remoteConfPath,
          null);
    }
    // the assumption here is that minimr cluster => this is a test run
    // and the classpath can look after itself

    boolean usingMiniMRCluster = getUsingMiniMRCluster();
    if (!usingMiniMRCluster) {

      log.debug("Destination is not a MiniYARNCluster -copying full classpath");

      // insert conf dir first
      if (remoteConfPath != null) {
        relativeConfDir = SliderKeys.SUBMITTED_CONF_DIR;
        Map<String, LocalResource> submittedConfDir =
          sliderFileSystem.submitDirectory(remoteConfPath,
                                         relativeConfDir);
        SliderUtils.mergeMaps(localResources, submittedConfDir);
      }
    }
    // build up the configuration 
    // IMPORTANT: it is only after this call that site configurations
    // will be valid.

    propagatePrincipals(config, instanceDefinition);
    Configuration clientConfExtras = new Configuration(false);
    // then build up the generated path.
    FsPermission clusterPerms = getClusterDirectoryPermissions(config);
    SliderUtils.copyDirectory(config, snapshotConfPath, generatedConfDirPath,
        clusterPerms);


    // add AM and provider specific artifacts to the resource map
    Map<String, LocalResource> providerResources;
    // standard AM resources
    sliderAM.prepareAMAndConfigForLaunch(sliderFileSystem,
                                       config,
                                       amLauncher,
                                       instanceDefinition,
                                       snapshotConfPath,
                                       generatedConfDirPath,
                                       clientConfExtras,
                                       libdir,
                                       tempPath,
                                       usingMiniMRCluster);
    //add provider-specific resources
    provider.prepareAMAndConfigForLaunch(sliderFileSystem,
                                         config,
                                         amLauncher,
                                         instanceDefinition,
                                         snapshotConfPath,
                                         generatedConfDirPath,
                                         clientConfExtras,
                                         libdir,
                                         tempPath,
                                         usingMiniMRCluster);

    // now that the site config is fully generated, the provider gets
    // to do a quick review of them.
    log.debug("Preflight validation of cluster configuration");


    sliderAM.preflightValidateClusterConfiguration(sliderFileSystem,
                                                 clustername,
                                                 config,
                                                 instanceDefinition,
                                                 clusterDirectory,
                                                 generatedConfDirPath,
                                                 clusterSecure
                                                );

    provider.preflightValidateClusterConfiguration(sliderFileSystem,
                                                   clustername,
                                                   config,
                                                   instanceDefinition,
                                                   clusterDirectory,
                                                   generatedConfDirPath,
                                                   clusterSecure
                                                  );


    // now add the image if it was set
    if (sliderFileSystem.maybeAddImagePath(localResources, imagePath)) {
      log.debug("Registered image path {}", imagePath);
    }


    // build the environment
    amLauncher.putEnv(
      SliderUtils.buildEnvMap(sliderAMResourceComponent));
    ClasspathConstructor classpath = SliderUtils.buildClasspath(relativeConfDir,
        libdir,
        getConfig(),
        usingMiniMRCluster);
    amLauncher.setClasspath(classpath);
    if (log.isDebugEnabled()) {
      log.debug("AM classpath={}", classpath);
      log.debug("Environment Map:\n{}",
                SliderUtils.stringifyMap(amLauncher.getEnv()));
      log.debug("Files in lib path\n{}", sliderFileSystem.listFSDir(libPath));
    }

    // rm address

    InetSocketAddress rmSchedulerAddress;
    try {
      rmSchedulerAddress = SliderUtils.getRmSchedulerAddress(config);
    } catch (IllegalArgumentException e) {
      throw new BadConfigException("%s Address invalid: %s",
                                   YarnConfiguration.RM_SCHEDULER_ADDRESS,
                                   config.get(
                                     YarnConfiguration.RM_SCHEDULER_ADDRESS)
      );

    }
    String rmAddr = NetUtils.getHostPortString(rmSchedulerAddress);

    JavaCommandLineBuilder commandLine = new JavaCommandLineBuilder();
    // insert any JVM options);
    sliderAM.addJVMOptions(instanceDefinition, commandLine);
    // enable asserts if the text option is set
    commandLine.enableJavaAssertions();
    // add the AM sevice entry point
    commandLine.add(SliderAppMaster.SERVICE_CLASSNAME);

    // create action and the cluster name
    commandLine.add(ACTION_CREATE, clustername);

    // debug
    if (debugAM) {
      commandLine.add(Arguments.ARG_DEBUG);
    }

    // set the cluster directory path
    commandLine.add(Arguments.ARG_CLUSTER_URI, clusterDirectory.toUri());

    if (!isUnset(rmAddr)) {
      commandLine.add(Arguments.ARG_RM_ADDR, rmAddr);
    }

    if (serviceArgs.getFilesystemBinding() != null) {
      commandLine.add(Arguments.ARG_FILESYSTEM, serviceArgs.getFilesystemBinding());
    }
    
    addConfOptionToCLI(commandLine, config, REGISTRY_PATH,
        DEFAULT_REGISTRY_PATH);
    addMandatoryConfOptionToCLI(commandLine, config, REGISTRY_ZK_QUORUM);
    
    if (clusterSecure) {
      // if the cluster is secure, make sure that
      // the relevant security settings go over
/*
      addConfOptionToCLI(commandLine, config, KEY_SECURITY);
*/
      addConfOptionToCLI(commandLine,
          config,
          DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY);
    }
    // write out the path output
    commandLine.addOutAndErrFiles(STDOUT_AM, STDERR_AM);

    String cmdStr = commandLine.build();
    log.debug("Completed setting up app master command {}", cmdStr);

    amLauncher.addCommandLine(commandLine);

    // the Slider AM gets to configure the AM requirements, not the custom provider
    sliderAM.prepareAMResourceRequirements(sliderAMResourceComponent,
        amLauncher.getResource());


    // Set the priority for the application master

    int amPriority = config.getInt(KEY_YARN_QUEUE_PRIORITY,
                                   DEFAULT_YARN_QUEUE_PRIORITY);


    amLauncher.setPriority(amPriority);

    // Set the queue to which this application is to be submitted in the RM
    // Queue for App master
    String amQueue = config.get(KEY_YARN_QUEUE, DEFAULT_YARN_QUEUE);

    amLauncher.setQueue(amQueue);

    // Submit the application to the applications manager
    // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
    // Ignore the response as either a valid response object is returned on success
    // or an exception thrown to denote some form of a failure
    

    // submit the application
    LaunchedApplication launchedApplication = amLauncher.submitApplication();
    return launchedApplication;
  }
  
  
  /**
   * Wait for the launched app to be accepted
   * @param waittime time in millis
   * @return exit code
   * @throws YarnException
   * @throws IOException
   */
  public int waitForAppAccepted(LaunchedApplication launchedApplication, 
                                int waittime) throws
                                              YarnException,
                                              IOException {
    assert launchedApplication != null;
    int exitCode;
    // wait for the submit state to be reached
    ApplicationReport report = launchedApplication.monitorAppToState(
      YarnApplicationState.ACCEPTED,
      new Duration(Constants.ACCEPT_TIME));


    // may have failed, so check that
    if (SliderUtils.hasAppFinished(report)) {
      exitCode = buildExitCode(report);
    } else {
      // exit unless there is a wait
      exitCode = EXIT_SUCCESS;

      if (waittime != 0) {
        // waiting for state to change
        Duration duration = new Duration(waittime * 1000);
        duration.start();
        report = launchedApplication.monitorAppToState(
          YarnApplicationState.RUNNING, duration);
        if (report != null &&
            report.getYarnApplicationState() == YarnApplicationState.RUNNING) {
          exitCode = EXIT_SUCCESS;
        } else {

          launchedApplication.kill("");
          exitCode = buildExitCode(report);
        }
      }
    }
    return exitCode;
  }


  /**
   * Propagate any critical principals from the current site config down to the HBase one.
   * @param config config to read from
   * @param clusterSpec cluster spec
   */
  private void propagatePrincipals(Configuration config,
                                   AggregateConf clusterSpec) {
    String dfsPrincipal = config.get(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY);
    if (dfsPrincipal != null) {
      String siteDfsPrincipal = OptionKeys.SITE_XML_PREFIX +
                                DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY;
      clusterSpec.getAppConfOperations().getGlobalOptions().putIfUnset(
        siteDfsPrincipal,
        dfsPrincipal);
    }
  }


  private boolean addConfOptionToCLI(CommandLineBuilder cmdLine,
      Configuration conf,
      String key) {
    String val = conf.get(key);
    if (val != null) {
      cmdLine.add(Arguments.ARG_DEFINE, key + "=" + val);
      return true;
    } else {
      return false;
    }
  }
  
  private void addConfOptionToCLI(CommandLineBuilder cmdLine,
      Configuration conf,
      String key, String defVal) {
    String val = conf.get(key, defVal);
      cmdLine.add(Arguments.ARG_DEFINE, key + "=" + val);
  }

  private void addMandatoryConfOptionToCLI(CommandLineBuilder cmdLine,
      Configuration conf,
      String key) throws BadConfigException {
    if (!addConfOptionToCLI(cmdLine, conf, key)) {
      throw new BadConfigException("Missing configuration option: " + key);
    }
  }
  
  /**
   * Create a path that must exist in the cluster fs
   * @param uri uri to create
   * @return the path
   * @throws FileNotFoundException if the path does not exist
   */
  public Path createPathThatMustExist(String uri) throws
      SliderException,
                                                  IOException {
    return sliderFileSystem.createPathThatMustExist(uri);
  }

  /**
   * verify that a live cluster isn't there
   * @param clustername cluster name
   * @throws SliderException with exit code EXIT_CLUSTER_LIVE
   * if a cluster of that name is either live or starting up.
   */
  public void verifyNoLiveClusters(String clustername) throws
                                                       IOException,
                                                       YarnException {
    List<ApplicationReport> existing = findAllLiveInstances(clustername);

    if (!existing.isEmpty()) {
      throw new SliderException(EXIT_APPLICATION_IN_USE,
                              clustername + ": " + E_CLUSTER_RUNNING + " :" +
                              existing.get(0));
    }
  }

  public String getUsername() throws IOException {
    return UserGroupInformation.getCurrentUser().getShortUserName();
  }

  /**
   * Get the name of any deployed cluster
   * @return the cluster name
   */
  public String getDeployedClusterName() {
    return deployedClusterName;
  }

  @VisibleForTesting
  public void setDeployedClusterName(String deployedClusterName) {
    this.deployedClusterName = deployedClusterName;
  }

  /**
   * ask if the client is using a mini MR cluster
   * @return true if they are
   */
  private boolean getUsingMiniMRCluster() {
    return getConfig().getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER,
                                  false);
  }

  /**
   * Get the application name used in the zookeeper root paths
   * @return an application-specific path in ZK
   */
  private String getAppName() {
    return "slider";
  }

  /**
   * Wait for the app to start running (or go past that state)
   * @param duration time to wait
   * @return the app report; null if the duration turned out
   * @throws YarnException YARN or app issues
   * @throws IOException IO problems
   */
  @VisibleForTesting
  public ApplicationReport monitorAppToRunning(Duration duration)
    throws YarnException, IOException {
    return monitorAppToState(YarnApplicationState.RUNNING, duration);
  }

  /**
   * Build an exit code for an application Id and its report.
   * If the report parameter is null, the app is killed
   * @param report report
   * @return the exit code
   */
  private int buildExitCode(ApplicationReport report) throws
                                                      IOException,
                                                      YarnException {
    if (null == report) {
      forceKillApplication("Reached client specified timeout for application");
      return EXIT_TIMED_OUT;
    }

    YarnApplicationState state = report.getYarnApplicationState();
    FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
    switch (state) {
      case FINISHED:
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          log.info("Application has completed successfully");
          return EXIT_SUCCESS;
        } else {
          log.info("Application finished unsuccessfully." +
                   "YarnState = {}, DSFinalStatus = {} Breaking monitoring loop",
                   state, dsStatus);
          return EXIT_YARN_SERVICE_FINISHED_WITH_ERROR;
        }

      case KILLED:
        log.info("Application did not finish. YarnState={}, DSFinalStatus={}",
                 state, dsStatus);
        return EXIT_YARN_SERVICE_KILLED;

      case FAILED:
        log.info("Application Failed. YarnState={}, DSFinalStatus={}", state,
                 dsStatus);
        return EXIT_YARN_SERVICE_FAILED;
      default:
        //not in any of these states
        return EXIT_SUCCESS;
    }
  }

  /**
   * Monitor the submitted application for reaching the requested state.
   * Will also report if the app reaches a later state (failed, killed, etc)
   * Kill application if duration!= null & time expires. 
   * Prerequisite: the applicatin was launched.
   * @param desiredState desired state.
   * @param duration how long to wait -must be more than 0
   * @return the application report -null on a timeout
   * @throws YarnException
   * @throws IOException
   */
  @VisibleForTesting
  public ApplicationReport monitorAppToState(
    YarnApplicationState desiredState,
    Duration duration)
    throws YarnException, IOException {
    LaunchedApplication launchedApplication =
      new LaunchedApplication(applicationId, yarnClient);
    return launchedApplication.monitorAppToState(desiredState, duration);
  }

  /**
   * Get the report of a this application
   * @return the app report or null if it could not be found.
   * @throws IOException
   * @throws YarnException
   */
  public ApplicationReport getApplicationReport() throws
                                                  IOException,
                                                  YarnException {
    return getApplicationReport(applicationId);
  }

  /**
   * Kill the submitted application by sending a call to the ASM
   * @throws YarnException
   * @throws IOException
   */
  public boolean forceKillApplication(String reason)
    throws YarnException, IOException {
    if (applicationId != null) {
      new LaunchedApplication(applicationId, yarnClient).forceKill(reason);
      return true;
    }
    return false;
  }

  /**
   * List Slider instances belonging to a specific user
   * @param user user: "" means all users
   * @return a possibly empty list of Slider AMs
   */
  @VisibleForTesting
  public List<ApplicationReport> listSliderInstances(String user)
    throws YarnException, IOException {
    return YARNRegistryClient.listInstances();
  }

  /**
   * Implement the list action: list all nodes
   * @return exit code of 0 if a list was created
   */
  @VisibleForTesting
  public int actionList(String clustername) throws IOException, YarnException {
    verifyBindingsDefined();

    String user = UserGroupInformation.getCurrentUser().getUserName();
    List<ApplicationReport> instances = listSliderInstances(user);

    if (isUnset(clustername)) {
      log.info("Instances for {}: {}",
               (user != null ? user : "all users"),
               instances.size());
      for (ApplicationReport report : instances) {
        logAppReport(report);
      }
      return EXIT_SUCCESS;
    } else {
      SliderUtils.validateClusterName(clustername);
      log.debug("Listing cluster named {}", clustername);
      ApplicationReport report =
        findClusterInInstanceList(instances, clustername);
      if (report != null) {
        logAppReport(report);
        return EXIT_SUCCESS;
      } else {
        throw unknownClusterException(clustername);
      }
    }
  }

  /**
   * Log the application report at INFO
   * @param report report to log
   */
  public void logAppReport(ApplicationReport report) {
    log.info(SliderUtils.appReportToString(report, "\n"));
  }

  /**
   * Implement the islive action: probe for a cluster of the given name existing
   * @return exit code
   */
  @VisibleForTesting
  public int actionFlex(String name, ActionFlexArgs args) throws YarnException, IOException {
    verifyBindingsDefined();
    SliderUtils.validateClusterName(name);
    log.debug("actionFlex({})", name);
    Map<String, Integer> roleInstances = new HashMap<String, Integer>();
    Map<String, String> roleMap = args.getComponentMap();
    for (Map.Entry<String, String> roleEntry : roleMap.entrySet()) {
      String key = roleEntry.getKey();
      String val = roleEntry.getValue();
      try {
        roleInstances.put(key, Integer.valueOf(val));
      } catch (NumberFormatException e) {
        throw new BadCommandArgumentsException("Requested count of role %s" +
                                               " is not a number: \"%s\"",
                                               key, val);
      }
    }
    return flex(name, roleInstances);
  }

  /**
   * Test for a cluster existing probe for a cluster of the given name existing
   * in the filesystem. If the live param is set, it must be a live cluster
   * @return exit code
   */
  @VisibleForTesting
  public int actionExists(String name, boolean live) throws YarnException, IOException {
    verifyBindingsDefined();
    SliderUtils.validateClusterName(name);
    log.debug("actionExists({}, {})", name, live);

    //initial probe for a cluster in the filesystem
    Path clusterDirectory = sliderFileSystem.buildClusterDirPath(name);
    if (!sliderFileSystem.getFileSystem().exists(clusterDirectory)) {
      throw unknownClusterException(name);
    }
    
    //test for liveness if desired

    if (live) {
      ApplicationReport instance = findInstance(name);
      if (instance == null) {
        log.info("cluster {} not running", name);
        return EXIT_FALSE;
      } else {
        // the app exists, but it may be in a terminated state
        SliderUtils.OnDemandReportStringifier report =
          new SliderUtils.OnDemandReportStringifier(instance);
        YarnApplicationState state =
          instance.getYarnApplicationState();
        if (state.ordinal() >= YarnApplicationState.FINISHED.ordinal()) {
          //cluster in the list of apps but not running
          log.info("Cluster {} found but is in state {}", name, state);
          log.debug("State {}", report);
          return EXIT_FALSE;
        }
        log.info("Cluster {} is running:\n{}", name, report);
      }
    } else {
      log.info("Cluster {} exists but is not running", name);

    }
    return EXIT_SUCCESS;
  }


  /**
   * Kill a specific container of the cluster
   * @param name cluster name
   * @param args arguments
   * @return exit code
   * @throws YarnException
   * @throws IOException
   */
  public int actionKillContainer(String name,
                                 ActionKillContainerArgs args) throws
                                                               YarnException,
                                                               IOException {
    String id = args.id;
    if (SliderUtils.isUnset(id)) {
      throw new BadCommandArgumentsException("Missing container id");
    }
    log.info("killingContainer {}:{}", name, id);
    SliderClusterOperations clusterOps =
      new SliderClusterOperations(bondToCluster(name));
    try {
      clusterOps.killContainer(id);
    } catch (NoSuchNodeException e) {
      throw new BadClusterStateException("Container %s not found in cluster %s",
                                         id, name);
    }
    return EXIT_SUCCESS;
  }

  /**
   * Echo operation (not currently wired up to command line)
   * @param name cluster name
   * @param args arguments
   * @return the echoed text
   * @throws YarnException
   * @throws IOException
   */
  public String actionEcho(String name, ActionEchoArgs args) throws
                                                             YarnException,
                                                             IOException {
    String message = args.message;
    if (message == null) {
      throw new BadCommandArgumentsException("missing message");
    }
    SliderClusterOperations clusterOps =
      new SliderClusterOperations(bondToCluster(name));
    return clusterOps.echo(message);
  }

  /**
   * Get at the service registry operations
   * @return registry client -valid after the service is inited.
   */
  public YARNRegistryClient getYARNRegistryClient() {
    return YARNRegistryClient;
  }

  /**
   * Find an instance of an application belonging to the current user
   * @param appname application name
   * @return the app report or null if none is found
   * @throws YarnException YARN issues
   * @throws IOException IO problems
   */
  private ApplicationReport findInstance(String appname) throws
                                                        YarnException,
                                                        IOException {
    return YARNRegistryClient.findInstance(appname);
  }
  
  private RunningApplication findApplication(String appname) throws
                                                                      YarnException,
                                                                      IOException {
    ApplicationReport applicationReport = findInstance(appname);
    return applicationReport != null ? new RunningApplication(yarnClient, applicationReport): null; 
      
  }

  /**
   * find all live instances of a specific app -if there is >1 in the cluster,
   * this returns them all. State should be running or less
   * @param appname application name
   * @return the list of all matching application instances
   */
  private List<ApplicationReport> findAllLiveInstances(String appname)
    throws YarnException, IOException {
    
    return YARNRegistryClient.findAllLiveInstances(appname);
  }


  public ApplicationReport findClusterInInstanceList(List<ApplicationReport> instances,
                                                     String appname) {
    return yarnClient.findClusterInInstanceList(instances, appname);
  }

  /**
   * Connect to a Slider AM
   * @param app application report providing the details on the application
   * @return an instance
   * @throws YarnException
   * @throws IOException
   */
  private SliderClusterProtocol connect(ApplicationReport app) throws
                                                              YarnException,
                                                              IOException {

    try {
      return RpcBinder.getProxy(getConfig(),
                                yarnClient.getRmClient(),
                                app,
                                Constants.CONNECT_TIMEOUT,
                                Constants.RPC_TIMEOUT);
    } catch (InterruptedException e) {
      throw new SliderException(SliderExitCodes.EXIT_TIMED_OUT,
                              e,
                              "Interrupted waiting for communications with the Slider AM");
    }
  }

  /**
   * Status operation
   *
   * @param clustername cluster name
   * @param statusArgs status arguments
   * @return 0 -for success, else an exception is thrown
   * @throws YarnException
   * @throws IOException
   */
  @VisibleForTesting
  public int actionStatus(String clustername, ActionStatusArgs statusArgs) throws
                                              YarnException,
                                              IOException {
    verifyBindingsDefined();
    SliderUtils.validateClusterName(clustername);
    String outfile = statusArgs.getOutput();
    ClusterDescription status = getClusterDescription(clustername);
    String text = status.toJsonString();
    if (outfile == null) {
      log.info(text);
    } else {
      status.save(new File(outfile).getAbsoluteFile());
    }
    return EXIT_SUCCESS;
  }

  /**
   * Version Details
   * @return exit code
   */
  public int actionVersion() {
    SliderVersionInfo.loadAndPrintVersionInfo(log);
    return EXIT_SUCCESS;
  }

  /**
   * Freeze the cluster
   *
   * @param clustername cluster name
   * @param freezeArgs arguments to the freeze
   * @return EXIT_SUCCESS if the cluster was not running by the end of the operation
   */
  public int actionFreeze(String clustername,
                          ActionFreezeArgs freezeArgs) throws
                                                            YarnException,
                                                            IOException {
    verifyBindingsDefined();
    SliderUtils.validateClusterName(clustername);
    int waittime = freezeArgs.getWaittime();
    String text = freezeArgs.message;
    boolean forcekill = freezeArgs.force;
    log.debug("actionFreeze({}, reason={}, wait={}, force={})", clustername,
              text,
              waittime,
              forcekill);
    
    //is this actually a known cluster?
    sliderFileSystem.locateInstanceDefinition(clustername);
    ApplicationReport app = findInstance(clustername);
    if (app == null) {
      // exit early
      log.info("Cluster {} not running", clustername);
      // not an error to freeze a frozen cluster
      return EXIT_SUCCESS;
    }
    log.debug("App to freeze was found: {}:\n{}", clustername,
              new SliderUtils.OnDemandReportStringifier(app));
    if (app.getYarnApplicationState().ordinal() >=
        YarnApplicationState.FINISHED.ordinal()) {
      log.info("Cluster {} is a terminated state {}", clustername,
               app.getYarnApplicationState());
      return EXIT_SUCCESS;
    }
    LaunchedApplication application = new LaunchedApplication(yarnClient, app);
    applicationId = application.getApplicationId();
    

    if (forcekill) {
      //escalating to forced kill
      application.kill("Forced freeze of " + clustername +
                       ": " + text);
    } else {
      try {
        SliderClusterProtocol appMaster = connect(app);
        Messages.StopClusterRequestProto r =
          Messages.StopClusterRequestProto
                  .newBuilder()
                  .setMessage(text)
                  .build();
        appMaster.stopCluster(r);

        log.debug("Cluster stop command issued");

      } catch (YarnException e) {
        log.warn("Exception while trying to terminate {}: {}", clustername, e);
        return EXIT_FALSE;
      } catch (IOException e) {
        log.warn("Exception while trying to terminate {}: {}", clustername, e);
        return EXIT_FALSE;
      }
    }

    //wait for completion. We don't currently return an exception during this process
    //as the stop operation has been issued, this is just YARN.
    try {
      if (waittime > 0) {
        ApplicationReport applicationReport =
          application.monitorAppToState(YarnApplicationState.FINISHED,
                                        new Duration(waittime * 1000));
        if (applicationReport == null) {
          log.info("application did not shut down in time");
          return EXIT_FALSE;
        }
      }

// JDK7    } catch (YarnException | IOException e) {
    } catch (YarnException e) {
      log.warn("Exception while waiting for the cluster {} to shut down: {}",
               clustername, e);
    } catch ( IOException e) {
      log.warn("Exception while waiting for the cluster {} to shut down: {}",
               clustername, e);
    }

    return EXIT_SUCCESS;
  }

  /*
   * Creates a site conf with entries from clientProperties of ClusterStatus
   * @param desc ClusterDescription, can be null
   * @param clustername, can be null
   * @return site conf
   */
  public Configuration getSiteConf(ClusterDescription desc, String clustername)
      throws YarnException, IOException {
    if (desc == null) {
      desc = getClusterDescription();
    }
    if (clustername == null) {
      clustername = getDeployedClusterName();
    }
    String description = "Slider Application Instance " + clustername;
    
    Configuration siteConf = new Configuration(false);
    for (String key : desc.clientProperties.keySet()) {
      siteConf.set(key, desc.clientProperties.get(key), description);
    }
    return siteConf;
  }


  /**
   * get the cluster configuration
   * @param clustername cluster name
   * @return the cluster name
   */

  @SuppressWarnings(
    {"UseOfSystemOutOrSystemErr", "IOResourceOpenedButNotSafelyClosed"})
  public int actionGetConf(String clustername, ActionGetConfArgs confArgs) throws
                                               YarnException,
                                               IOException {
    File outfile = null;
    
    if (confArgs.getOutput() != null) {
      outfile = new File(confArgs.getOutput());
    }

    String format = confArgs.getFormat();
    verifyBindingsDefined();
    SliderUtils.validateClusterName(clustername);
    ClusterDescription status = getClusterDescription(clustername);
    Writer writer;
    boolean toPrint;
    if (outfile != null) {
      writer = new FileWriter(outfile);
      toPrint = false;
    } else {
      writer = new StringWriter();
      toPrint = true;
    }
    try {
      String description = "Slider Application Instance " + clustername;
// JDK7      
/*
      switch (format) {
        case Arguments.FORMAT_XML:
          Configuration siteConf = getSiteConf(status, clustername);
          siteConf.writeXml(writer);
          break;
        case Arguments.FORMAT_PROPERTIES:
          Properties props = new Properties();
          props.putAll(status.clientProperties);
          props.store(writer, description);
          break;
        default:
          throw new BadCommandArgumentsException("Unknown format: " + format);
      }
*/
      if (Arguments.FORMAT_XML.equals(format)) {
        Configuration siteConf = getSiteConf(status, clustername);
        siteConf.writeXml(writer);
      } else if (Arguments.FORMAT_PROPERTIES.equals(format)) {
        Properties props = new Properties();
        props.putAll(status.clientProperties);
        props.store(writer, description);
      } else {
          throw new BadCommandArgumentsException("Unknown format: " + format);
      }
    } finally {
      // data is written.
      // close the file
      writer.close();
    }
    // then, if this is not a file write, print it
    if (toPrint) {
      // not logged
      System.err.println(writer.toString());
    }
    return EXIT_SUCCESS;
  }

  /**
   * Restore a cluster
   */
  public int actionThaw(String clustername, ActionThawArgs thaw) throws YarnException, IOException {
    SliderUtils.validateClusterName(clustername);
    // see if it is actually running and bail out;
    verifyBindingsDefined();
    verifyNoLiveClusters(clustername);


    //start the cluster
    return startCluster(clustername, thaw);
  }

  /**
   * Implement flexing
   * @param clustername name of the cluster
   * @param roleInstances map of new role instances
   * @return EXIT_SUCCESS if the #of nodes in a live cluster changed
   * @throws YarnException
   * @throws IOException
   */
  public int flex(String clustername,
                  Map<String, Integer> roleInstances) throws
                                   YarnException,
                                   IOException {
    verifyBindingsDefined();
    SliderUtils.validateClusterName(clustername);
    Path clusterDirectory = sliderFileSystem.buildClusterDirPath(clustername);
    AggregateConf instanceDefinition = loadInstanceDefinitionUnresolved(
      clustername,
      clusterDirectory);

    ConfTreeOperations resources =
      instanceDefinition.getResourceOperations();
    for (Map.Entry<String, Integer> entry : roleInstances.entrySet()) {
      String role = entry.getKey();
      int count = entry.getValue();
      if (count < 0) {
        throw new BadCommandArgumentsException("Requested number of " + role
            + " instances is out of range");
      }
      resources.getOrAddComponent(role).put(ResourceKeys.COMPONENT_INSTANCES,
                                            Integer.toString(count));


      log.debug("Flexed cluster specification ( {} -> {}) : \n{}",
                role,
                count,
                resources);
    }
    int exitCode = EXIT_FALSE;
    // save the specification
    try {
      InstanceIO.updateInstanceDefinition(sliderFileSystem, clusterDirectory,instanceDefinition);
    } catch (LockAcquireFailedException e) {
      // lock failure
      log.debug("Failed to lock dir {}", clusterDirectory, e);
      log.warn("Failed to save new resource definition to {} : {}", clusterDirectory,
               e.toString());
      

    }

    // now see if it is actually running and tell it about the update if it is
    ApplicationReport instance = findInstance(clustername);
    if (instance != null) {
      log.info("Flexing running cluster");
      SliderClusterProtocol appMaster = connect(instance);
      SliderClusterOperations clusterOps = new SliderClusterOperations(appMaster);
      if (clusterOps.flex(instanceDefinition.getResources())) {
        log.info("Cluster size updated");
        exitCode = EXIT_SUCCESS;
      } else {
        log.info("Requested size is the same as current size: no change");
      }
    } else {
      log.info("No running instance to update");
    }
    return exitCode;
  }


  /**
   * Load the persistent cluster description
   * @param clustername name of the cluster
   * @return the description in the filesystem
   * @throws IOException any problems loading -including a missing file
   */
  @VisibleForTesting
  public AggregateConf loadPersistedClusterDescription(String clustername)
      throws IOException, SliderException, LockAcquireFailedException {
    Path clusterDirectory = sliderFileSystem.buildClusterDirPath(clustername);
    ConfPersister persister = new ConfPersister(sliderFileSystem, clusterDirectory);
    AggregateConf instanceDescription = new AggregateConf();
    persister.load(instanceDescription);
    return instanceDescription;
  }

    /**
     * Connect to a live cluster and get its current state
     * @param clustername the cluster name
     * @return its description
     */
  @VisibleForTesting
  public ClusterDescription getClusterDescription(String clustername) throws
                                                                 YarnException,
                                                                 IOException {
    SliderClusterOperations clusterOperations =
      createClusterOperations(clustername);
    return clusterOperations.getClusterDescription();
  }

  /**
   * Connect to the cluster and get its current state
   * @return its description
   */
  @VisibleForTesting
  public ClusterDescription getClusterDescription() throws
                                               YarnException,
                                               IOException {
    return getClusterDescription(getDeployedClusterName());
  }

  /**
   * List all node UUIDs in a role
   * @param role role name or "" for all
   * @return an array of UUID strings
   * @throws IOException
   * @throws YarnException
   */
  @VisibleForTesting
  public String[] listNodeUUIDsByRole(String role) throws
                                               IOException,
                                               YarnException {
    return createClusterOperations()
              .listNodeUUIDsByRole(role);
  }

  /**
   * List all nodes in a role. This is a double round trip: once to list
   * the nodes in a role, another to get their details
   * @param role component/role to look for
   * @return an array of ContainerNode instances
   * @throws IOException
   * @throws YarnException
   */
  @VisibleForTesting
  public List<ClusterNode> listClusterNodesInRole(String role) throws
                                               IOException,
                                               YarnException {
    return createClusterOperations().listClusterNodesInRole(role);
  }

  /**
   * Get the details on a list of uuids
   * @param uuids uuids to ask for 
   * @return a possibly empty list of node details
   * @throws IOException
   * @throws YarnException
   */
  @VisibleForTesting
  public List<ClusterNode> listClusterNodes(String[] uuids) throws
                                               IOException,
                                               YarnException {

    if (uuids.length == 0) {
      // short cut on an empty list
      return new LinkedList<ClusterNode>();
    }
    return createClusterOperations().listClusterNodes(uuids);
  }

  /**
   * Get a node from the AM
   * @param uuid uuid of node
   * @return deserialized node
   * @throws IOException IO problems
   * @throws NoSuchNodeException if the node isn't found
   */
  @VisibleForTesting
  public ClusterNode getNode(String uuid) throws IOException, YarnException {
    return createClusterOperations().getNode(uuid);
  }
  
  /**
   * Get the instance definition from the far end
   */
  @VisibleForTesting
  public AggregateConf getLiveInstanceDefinition() throws IOException, YarnException {
    return createClusterOperations().getInstanceDefinition();
  }

  /**
   * Bond to a running cluster
   * @param clustername cluster name
   * @return the AM RPC client
   * @throws SliderException if the cluster is unkown
   */
  private SliderClusterProtocol bondToCluster(String clustername) throws
                                                                  YarnException,
                                                                  IOException {
    verifyBindingsDefined();
    if (clustername == null) {
      throw unknownClusterException("(undefined)");
    }
    ApplicationReport instance = findInstance(clustername);
    if (null == instance) {
      throw unknownClusterException(clustername);
    }
    return connect(instance);
  }

  /**
   * Create a cluster operations instance against a given cluster
   * @param clustername cluster name
   * @return a bonded cluster operations instance
   * @throws YarnException YARN issues
   * @throws IOException IO problems
   */
  private SliderClusterOperations createClusterOperations(String clustername) throws
                                                                            YarnException,
                                                                            IOException {
    SliderClusterProtocol sliderAM = bondToCluster(clustername);
    return new SliderClusterOperations(sliderAM);
  }

  /**
   * Create a cluster operations instance against the active cluster
   * -returning any previous created one if held.
   * @return a bonded cluster operations instance
   * @throws YarnException YARN issues
   * @throws IOException IO problems
   */
  public SliderClusterOperations createClusterOperations() throws
                                                         YarnException,
                                                         IOException {
    if (sliderClusterOperations == null) {
      sliderClusterOperations =
        createClusterOperations(getDeployedClusterName());
    }
    return sliderClusterOperations;
  }

  /**
   * Wait for an instance of a named role to be live (or past it in the lifecycle)
   * @param role role to look for
   * @param timeout time to wait
   * @return the state. If still in CREATED, the cluster didn't come up
   * in the time period. If LIVE, all is well. If >LIVE, it has shut for a reason
   * @throws IOException IO
   * @throws SliderException Slider
   * @throws WaitTimeoutException if the wait timed out
   */
  @VisibleForTesting
  public int waitForRoleInstanceLive(String role, long timeout)
    throws WaitTimeoutException, IOException, YarnException {
    return createClusterOperations().waitForRoleInstanceLive(role, timeout);
  }

  /**
   * Generate an exception for an unknown cluster
   * @param clustername cluster name
   * @return an exception with text and a relevant exit code
   */
  public UnknownApplicationInstanceException unknownClusterException(String clustername) {
    return UnknownApplicationInstanceException.unknownInstance(clustername);
  }

  @Override
  public String toString() {
    return "Slider Client in state " + getServiceState()
           + " and Slider Application Instance " + deployedClusterName;
  }

  /**
   * Get all YARN applications
   * @return a possibly empty list
   * @throws YarnException
   * @throws IOException
   */
  @VisibleForTesting
  public List<ApplicationReport> getApplications() throws YarnException, IOException {
    return yarnClient.getApplications();
  }

  @VisibleForTesting
  public ApplicationReport getApplicationReport(ApplicationId appId)
    throws YarnException, IOException {
    return new LaunchedApplication(appId, yarnClient).getApplicationReport();

  }

  /**
   * The configuration used for deployment (after resolution)
   * @return
   */
  @VisibleForTesting
  public AggregateConf getLaunchedInstanceDefinition() {
    return launchedInstanceDefinition;
  }


  /**
   * Registry operation
   *
   * @param registryArgs registry Arguments
   * @return 0 for success, -1 for some issues that aren't errors, just failures
   * to retrieve information (e.g. no configurations for that entry)
   * @throws YarnException YARN problems
   * @throws IOException Network or other problems
   */
  @VisibleForTesting
  public int actionRegistry(ActionRegistryArgs registryArgs) throws
      YarnException,
      IOException {
    // as this is also a test entry point, validate
    // the arguments
    registryArgs.validate();
    try {
      if (registryArgs.list) {
        actionRegistryList(registryArgs);
      } else if (registryArgs.listConf) {
        // list the configurations
        actionRegistryListConfigs(registryArgs);
      } else if (SliderUtils.isSet(registryArgs.getConf)) {
        // get a configuration
        PublishedConfiguration publishedConfiguration =
            actionRegistryGetConfig(registryArgs);
        outputConfig(publishedConfiguration, registryArgs);
      } else {
        // it's an unknown command
        throw new BadCommandArgumentsException(
            "Bad command arguments for " + ACTION_REGISTRY + " " +
            registryArgs);
      }
    } catch (FileNotFoundException e) {
      log.info("{}", e.toString());
      log.debug("{}",e, e);
      return EXIT_NOT_FOUND;
    }
    return EXIT_SUCCESS;
  }

  /**
   * Registry operation
   *
   * @param registryArgs registry Arguments
   * @return the instances (for tests)
   * @throws YarnException YARN problems
   * @throws IOException Network or other problems
   */
  @VisibleForTesting
  public List<ServiceInstanceData> actionRegistryList(
      ActionRegistryArgs registryArgs)
      throws YarnException, IOException {
    SliderRegistryService registryService = getRegistry();
    String serviceType = registryArgs.serviceType;
    String name = registryArgs.name;
    List<CuratorServiceInstance<ServiceInstanceData>> instances =
        registryService.findInstances(serviceType, name);
    int size = instances.size();
    if (size == 0) {
      throw new FileNotFoundException("No entries for servicetype "
                                      + serviceType
                                      + " name " + name);
    }
    List<ServiceInstanceData> sids = new ArrayList<ServiceInstanceData>(size);
    for (CuratorServiceInstance<ServiceInstanceData> instance : instances) {
      ServiceInstanceData payload = instance.payload;
      logInstance(payload, registryArgs.verbose);
      sids.add(payload);
    }
    return sids;
  }

  private void logInstance(ServiceInstanceData instance,
      boolean verbose) {
    if (!verbose) {
      log.info("{}", instance.id);
    } else {
      log.info("{}: ", instance.id);
      logEndpoints(instance);
    }
  }
  
  private void logEndpoints(ServiceInstanceData instance) {
      Map<String, RegisteredEndpoint> endpoints =
          instance.listEndpoints(true);
      for (Map.Entry<String, RegisteredEndpoint> entry : endpoints.entrySet()) {
        String name = entry.getKey();
        RegisteredEndpoint endpoint = entry.getValue();
        log.info("  {}", endpoint);
    }
  }

  /**
   * list configs available for an instance
   *
   * @param registryArgs registry Arguments
   * @throws YarnException YARN problems
   * @throws IOException Network or other problems
   */
  public void actionRegistryListConfigs(ActionRegistryArgs registryArgs)
      throws YarnException, IOException {
    ServiceInstanceData instance = lookupInstance(registryArgs);

    RegistryRetriever retriever = new RegistryRetriever(instance);
    PublishedConfigSet configurations =
        retriever.getConfigurations(!registryArgs.internal);

    for (String configName : configurations.keys()) {
      if (!registryArgs.verbose) {
        log.info("{}", configName);
      } else {
        PublishedConfiguration published =
            configurations.get(configName);
        log.info("{} : {}",
            configName,
            published.description);
      }
    }
  }

  /**
   * list configs available for an instance
   *
   * @param registryArgs registry Arguments
   * @throws YarnException YARN problems
   * @throws IOException Network or other problems
   * @throws FileNotFoundException if the config is not found
   */
  @VisibleForTesting
  public PublishedConfiguration actionRegistryGetConfig(ActionRegistryArgs registryArgs)
      throws YarnException, IOException {
    ServiceInstanceData instance = lookupInstance(registryArgs);

    RegistryRetriever retriever = new RegistryRetriever(instance);
    boolean external = !registryArgs.internal;
    PublishedConfigSet configurations =
        retriever.getConfigurations(external);

    PublishedConfiguration published = retriever.retrieveConfiguration(configurations,
            registryArgs.getConf,
            external);
    return published;
  }

  /**
   * write out the config
   * @param published
   * @param registryArgs
   * @throws BadCommandArgumentsException
   * @throws IOException
   */
  private void outputConfig(PublishedConfiguration published,
      ActionRegistryArgs registryArgs) throws
      BadCommandArgumentsException,
      IOException {
    // decide whether or not to print
    String entry = registryArgs.getConf;
    String format = registryArgs.format;
    ConfigFormat configFormat = ConfigFormat.resolve(format);
    if (configFormat == null) {
      throw new BadCommandArgumentsException(
          "Unknown/Unsupported format %s ", format);
    }
    PublishedConfigurationOutputter outputter =
        PublishedConfigurationOutputter.createOutputter(configFormat,
            published);
    boolean print = registryArgs.dest == null;
    if (!print) {
      File destFile;
      destFile = registryArgs.dest;
      if (destFile.isDirectory()) {
        // creating it under a directory
        destFile = new File(destFile, entry + "." + format);
      }
      log.info("Destination path: {}", destFile);
      outputter.save(destFile);
    } else {
      print(outputter.asString());
    }
    
  }

  /**
   * Look up an instance
   * @return instance data
   * @throws UnknownApplicationInstanceException no match
   * @throws SliderException other failures
   * @throws IOException IO problems or wrapped exceptions
   */
  private ServiceInstanceData lookupInstance(ActionRegistryArgs registryArgs) throws
      UnknownApplicationInstanceException,
      SliderException,
      IOException {
    return lookupInstance(registryArgs.name, registryArgs.serviceType);
  }

  /**
   * Look up an instance
   * @param id instance ID
   * @param serviceType service type
   * @return instance data
   * @throws UnknownApplicationInstanceException no match
   * @throws SliderException other failures
   * @throws IOException IO problems or wrapped exceptions
   */
  private ServiceInstanceData lookupInstance(String id,
      String serviceType) throws
      IOException {
    try {
      CuratorServiceInstance<ServiceInstanceData> csi =
          getRegistry().queryForInstance(serviceType, id);
      if (csi == null) {
        throw new FileNotFoundException(
            String.format("instance %s of type %s not found",
            id, serviceType));
      }
      return csi.getPayload();
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  } 
  
  /**
   * List instances in the registry
   * @return
   * @throws IOException
   * @throws YarnException
   */
  public List<CuratorServiceInstance<ServiceInstanceData>> listRegistryInstances()
      throws IOException, YarnException {
    maybeStartRegistry();
    return registry.listInstances(SliderKeys.APP_TYPE);
  }

  /**
   * List instances in the registry
   * @return the instance IDs
   * @throws IOException
   * @throws YarnException
   */
  public List<String> listRegisteredSliderInstances() throws
      IOException,
      YarnException {
    try {
      maybeStartRegistry();
      return registry.instanceIDs(SliderKeys.APP_TYPE);
/// JDK7    } catch (YarnException | IOException e) {
    } catch (IOException e) {
      throw e;
    } catch (YarnException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Start the registry if it is not there yet
   * @return the registry service
   * @throws SliderException
   * @throws IOException
   */
  private synchronized SliderRegistryService maybeStartRegistry() throws
      SliderException,
      IOException {

    if (registry == null) {
      registry = startRegistrationService();
    }
    return registry;
  }

  /**
   * Get the registry binding. As this may start the registry, it can take time
   * and fail
   * @return registry the registry service
   * @throws SliderException slider-specific failures
   * @throws IOException other failures
   */
  @VisibleForTesting

  public SliderRegistryService getRegistry() throws
      SliderException,
      IOException {
    return maybeStartRegistry();
  }

  /**
   * Output to standard out/stderr (implementation specific detail)
   * @param src source
   */
  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  private static void print(CharSequence src) {
    System.out.append(src);
  }
}
