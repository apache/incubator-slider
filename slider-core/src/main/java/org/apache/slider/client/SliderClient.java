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
import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.registry.client.api.RegistryOperations;

import static org.apache.hadoop.registry.client.binding.RegistryUtils.*;

import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.exceptions.NoRecordException;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.apache.hadoop.yarn.util.ConverterUtils;
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
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.common.params.AbstractActionArgs;
import org.apache.slider.common.params.AbstractClusterBuildingActionArgs;
import org.apache.slider.common.params.ActionDiagnosticArgs;
import org.apache.slider.common.params.ActionExistsArgs;
import org.apache.slider.common.params.ActionInstallKeytabArgs;
import org.apache.slider.common.params.ActionInstallPackageArgs;
import org.apache.slider.common.params.ActionAMSuicideArgs;
import org.apache.slider.common.params.ActionCreateArgs;
import org.apache.slider.common.params.ActionEchoArgs;
import org.apache.slider.common.params.ActionFlexArgs;
import org.apache.slider.common.params.ActionFreezeArgs;
import org.apache.slider.common.params.ActionKillContainerArgs;
import org.apache.slider.common.params.ActionListArgs;
import org.apache.slider.common.params.ActionLookupArgs;
import org.apache.slider.common.params.ActionRegistryArgs;
import org.apache.slider.common.params.ActionResolveArgs;
import org.apache.slider.common.params.ActionStatusArgs;
import org.apache.slider.common.params.ActionThawArgs;
import org.apache.slider.common.params.Arguments;
import org.apache.slider.common.params.ClientArgs;
import org.apache.slider.common.params.CommonArgs;
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
import org.apache.slider.core.exceptions.NotFoundException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException;
import org.apache.slider.core.exceptions.WaitTimeoutException;
import org.apache.slider.core.launch.AppMasterLauncher;
import org.apache.slider.core.launch.ClasspathConstructor;
import org.apache.slider.core.launch.CommandLineBuilder;
import org.apache.slider.core.launch.JavaCommandLineBuilder;
import org.apache.slider.core.launch.LaunchedApplication;
import org.apache.slider.core.launch.RunningApplication;
import org.apache.slider.core.launch.SerializedApplicationReport;
import org.apache.slider.core.main.RunService;
import org.apache.slider.core.persist.ApplicationReportSerDeser;
import org.apache.slider.core.persist.ConfPersister;
import org.apache.slider.core.persist.LockAcquireFailedException;
import org.apache.slider.core.registry.SliderRegistryUtils;
import org.apache.slider.core.registry.YarnAppListClient;
import org.apache.slider.core.registry.docstore.ConfigFormat;
import org.apache.slider.core.registry.docstore.PublishedConfigSet;
import org.apache.slider.core.registry.docstore.PublishedConfiguration;
import org.apache.slider.core.registry.docstore.PublishedConfigurationOutputter;
import org.apache.slider.core.registry.docstore.PublishedExports;
import org.apache.slider.core.registry.docstore.PublishedExportsOutputter;
import org.apache.slider.core.registry.docstore.PublishedExportsSet;
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
import org.apache.slider.server.services.utility.AbstractSliderLaunchedService;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.slider.common.params.SliderActions.*;

/**
 * Client service for Slider
 */

public class SliderClient extends AbstractSliderLaunchedService implements RunService,
    SliderExitCodes, SliderKeys, ErrorStrings, SliderClientAPI {
  private static final Logger log = LoggerFactory.getLogger(SliderClient.class);

  private ClientArgs serviceArgs;
  public ApplicationId applicationId;
  
  private String deployedClusterName;
  /**
   * Cluster opaerations against the deployed cluster -will be null
   * if no bonding has yet taken place
   */
  private SliderClusterOperations sliderClusterOperations;

  protected SliderFileSystem sliderFileSystem;

  /**
   * Yarn client service
   */
  private SliderYarnClientImpl yarnClient;
  private YarnAppListClient YarnAppListClient;
  private AggregateConf launchedInstanceDefinition;

  /**
   * The YARN registry service
   */
  private RegistryOperations registryOperations;

  /**
   * Constructor
   */
  public SliderClient() {
    super("Slider Client");
    new HdfsConfiguration();
    new YarnConfiguration();
  }

  /**
   * This is called <i>Before serviceInit is called</i>
   * @param config the initial configuration build up by the
   * service launcher.
   * @param args argument list list of arguments passed to the command line
   * after any launcher-specific commands have been stripped.
   * @return the post-binding configuration to pass to the <code>init()</code>
   * operation.
   * @throws Exception
   */
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
    ConfigHelper.mergeConfigurations(conf, clientConf, CLIENT_RESOURCE, true);
    serviceArgs.applyDefinitions(conf);
    serviceArgs.applyFileSystemBinding(conf);
    // init security with our conf
    if (SliderUtils.isHadoopClusterSecure(conf)) {
      SliderUtils.forceLogin();
      SliderUtils.initProcessSecurity(conf);
    }
    AbstractActionArgs coreAction = serviceArgs.getCoreAction();
    if (coreAction.getHadoopServicesRequired()) {
      initHadoopBinding();
    }
    super.serviceInit(conf);
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
        exitCode = actionList(clusterName, serviceArgs.getActionListArgs());
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

  /**
   * Launched service execution. This runs {@link #exec()}
   * then catches some exceptions and converts them to exit codes
   * @return an exit code
   * @throws Throwable
   */
  @Override
  public int runService() throws Throwable {
    try {
      return exec();
    } catch (FileNotFoundException nfe) {
      throw new NotFoundException(nfe, nfe.toString());
    } catch (PathNotFoundException nfe) {
      throw new NotFoundException(nfe, nfe.toString());
    }
  }

  /**
   * Execute the command line
   * @return an exit code
   * @throws Throwable on a failure
   */
  public int exec() throws Throwable {

    // choose the action
    String action = serviceArgs.getAction();

    int exitCode = EXIT_SUCCESS;
    String clusterName = serviceArgs.getClusterName();
    // actions
    if (ACTION_INSTALL_PACKAGE.equals(action)) {
      exitCode = actionInstallPkg(serviceArgs.getActionInstallPackageArgs());
    } else if (ACTION_INSTALL_KEYTAB.equals(action)) {
      exitCode = actionInstallKeytab(serviceArgs.getActionInstallKeytabArgs());
    } else if (ACTION_BUILD.equals(action)) {
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
    } else if (ACTION_DIAGNOSTICS.equals(action)) {
      exitCode = actionDiagnostic(serviceArgs.getActionDiagnosticArgs());
    } else if (ACTION_EXISTS.equals(action)) {
      exitCode = actionExists(clusterName,
          serviceArgs.getActionExistsArgs());
    } else if (ACTION_FLEX.equals(action)) {
      exitCode = actionFlex(clusterName, serviceArgs.getActionFlexArgs());
    } else if (ACTION_HELP.equals(action)) {
      log.info(serviceArgs.usage());
    } else if (ACTION_KILL_CONTAINER.equals(action)) {
      exitCode = actionKillContainer(clusterName,
          serviceArgs.getActionKillContainerArgs());
    } else if (ACTION_AM_SUICIDE.equals(action)) {
      exitCode = actionAmSuicide(clusterName,
          serviceArgs.getActionAMSuicideArgs());
    } else if (ACTION_LIST.equals(action)) {
      exitCode = actionList(clusterName, serviceArgs.getActionListArgs());
    } else if (ACTION_LOOKUP.equals(action)) {
      exitCode = actionLookup(serviceArgs.getActionLookupArgs());
    } else if (ACTION_REGISTRY.equals(action)) {
      exitCode = actionRegistry(serviceArgs.getActionRegistryArgs());
    } else if (ACTION_RESOLVE.equals(action)) {
      exitCode = actionResolve(serviceArgs.getActionResolveArgs());
    } else if (ACTION_STATUS.equals(action)) {
      exitCode = actionStatus(clusterName,
          serviceArgs.getActionStatusArgs());
    } else if (ACTION_UPDATE.equals(action)) {
      exitCode = actionUpdate(clusterName, serviceArgs.getActionUpdateArgs());
    } else if (ACTION_VERSION.equals(action)) {
      exitCode = actionVersion();
    } else if (SliderUtils.isUnset(action)) {
        throw new SliderException(EXIT_USAGE,
                serviceArgs.usage());
    } else {
      throw new SliderException(EXIT_UNIMPLEMENTED,
          "Unimplemented: " + action);
    }

    return exitCode;
  }

/**
   * Perform everything needed to init the hadoop binding.
   * This assumes that the service is already  in inited or started state
   * @throws IOException
   * @throws SliderException
   */
  protected void initHadoopBinding() throws IOException, SliderException {
    // validate the client
    SliderUtils.validateSliderClientEnvironment(null);
    //create the YARN client
    yarnClient = new SliderYarnClientImpl();
    yarnClient.init(getConfig());
    if (getServiceState() == STATE.STARTED) {
      yarnClient.start();
    }
    addService(yarnClient);
    YarnAppListClient =
        new YarnAppListClient(yarnClient, getUsername(), getConfig());
    // create the filesystem
    sliderFileSystem = new SliderFileSystem(getConfig());    
  }

  /**
   * Delete the zookeeper node associated with the calling user and the cluster
   * TODO: YARN registry operations
   **/
  @VisibleForTesting
  public boolean deleteZookeeperNode(String clusterName) throws YarnException, IOException {
    String user = getUsername();
    String zkPath = ZKIntegration.mkClusterPath(user, clusterName);
    Exception e = null;
    try {
      Configuration config = getConfig();
      ZKIntegration client = getZkClient(clusterName, user);
      if (client != null) {
        if (client.exists(zkPath)) {
          log.info("Deleting zookeeper path {}", zkPath);
        }
        client.deleteRecursive(zkPath);
        return true;
      }
    } catch (InterruptedException ignored) {
      e = ignored;
    } catch (KeeperException ignored) {
      e = ignored;
    } catch (BadConfigException ignored) {
      e = ignored;
    }
    if (e != null) {
      log.debug("Unable to recursively delete zk node {}", zkPath);
      log.debug("Reason: ", e);
    }

    return false;
  }

  /**
   * Create the zookeeper node associated with the calling user and the cluster
   */
  @VisibleForTesting
  public String createZookeeperNode(String clusterName, Boolean nameOnly) throws YarnException, IOException {
    String user = getUsername();
    String zkPath = ZKIntegration.mkClusterPath(user, clusterName);
    if (nameOnly) {
      return zkPath;
    }
    Configuration config = getConfig();
    ZKIntegration client = getZkClient(clusterName, user);
    if (client != null) {
      try {
        client.createPath(zkPath, "", ZooDefs.Ids.OPEN_ACL_UNSAFE,
                          CreateMode.PERSISTENT);
        return zkPath;
      } catch (InterruptedException e) {
        log.warn("Unable to create default zk node {}", zkPath, e);
      } catch (KeeperException e) {
        log.warn("Unable to create default zk node {}", zkPath, e);
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

  @Override
  public int actionDestroy(String clustername) throws YarnException,
                                                      IOException {
    // verify that a live cluster isn't there
    SliderUtils.validateClusterName(clustername);
    //no=op, it is now mandatory. 
    verifyBindingsDefined();
    verifyNoLiveClusters(clustername, "Destroy");

    // create the directory path
    Path clusterDirectory = sliderFileSystem.buildClusterDirPath(clustername);
    // delete the directory;
    FileSystem fs = sliderFileSystem.getFileSystem();
    boolean exists = fs.exists(clusterDirectory);
    if (exists) {
      log.debug("Application Instance {} found at {}: destroying", clustername, clusterDirectory);
      boolean deleted =
          fs.delete(clusterDirectory, true);
      if (!deleted) {
        log.warn("Filesystem returned false from delete() operation");
      }

      if(!deleteZookeeperNode(clustername)) {
        log.warn("Unable to perform node cleanup in Zookeeper.");
      }

      if (fs.exists(clusterDirectory)) {
        log.warn("Failed to delete {}", clusterDirectory);
      }

    } else {
      log.debug("Application Instance {} already destroyed", clustername);
    }

    // rm the registry entry —do not let this block the destroy operations
    String registryPath = SliderRegistryUtils.registryPathForInstance(
        clustername);
    try {
      getRegistryOperations().delete(registryPath, true);
    } catch (IOException e) {
      log.warn("Error deleting registry entry {}: {} ", registryPath, e, e);
    } catch (SliderException e) {
      log.warn("Error binding to registry {} ", e, e);
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
  
  @Override
  public int actionAmSuicide(String clustername,
      ActionAMSuicideArgs args) throws YarnException, IOException {
    SliderClusterOperations clusterOperations =
      createClusterOperations(clustername);
    clusterOperations.amSuicide(args.message, args.exitcode, args.waittime);
    return EXIT_SUCCESS;
  }

  @Override
  public AbstractClientProvider createClientProvider(String provider)
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
    Path clusterDirectory = sliderFileSystem.buildClusterDirPath(clustername);
    AggregateConf instanceDefinition = loadInstanceDefinitionUnresolved(
        clustername, clusterDirectory);
    try {
      checkForCredentials(getConfig(), instanceDefinition.getAppConf());
    } catch (IOException e) {
      sliderFileSystem.getFileSystem().delete(clusterDirectory, true);
      throw e;
    }
    return startCluster(clustername, createArgs);
  }

  private void checkForCredentials(Configuration conf,
      ConfTree tree) throws IOException {
    if (tree.credentials == null || tree.credentials.size()==0) {
      log.info("No credentials requested");
      return;
    }

    for (Entry<String, List<String>> cred : tree.credentials.entrySet()) {
      String provider = cred.getKey();
      List<String> aliases = cred.getValue();
      if (aliases == null || aliases.size()==0) {
        continue;
      }
      Configuration c = new Configuration(conf);
      c.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, provider);
      CredentialProvider credentialProvider =
          CredentialProviderFactory.getProviders(c).get(0);
      Set<String> existingAliases = new HashSet<String>(credentialProvider.getAliases());
      for (String alias : aliases) {
        if (!existingAliases.contains(alias.toLowerCase(Locale.ENGLISH))) {
          throw new IOException("Specified credentials have not been " +
              "initialized in provider " + provider + ": " + alias);
        }
      }
    }
  }

  @Override
  public int actionBuild(String clustername,
      AbstractClusterBuildingActionArgs buildInfo) throws
                                               YarnException,
                                               IOException {

    buildInstanceDefinition(clustername, buildInfo, false, false);
    return EXIT_SUCCESS; 
  }

  @Override
  public int actionInstallKeytab(ActionInstallKeytabArgs installKeytabInfo)
      throws YarnException, IOException {

    Path srcFile = null;
    if (StringUtils.isEmpty(installKeytabInfo.folder)) {
      throw new BadCommandArgumentsException(
          "A valid destination keytab sub-folder name is required (e.g. 'security').\n"
              + CommonArgs.usage(serviceArgs, ACTION_INSTALL_KEYTAB));
    }

    if (StringUtils.isEmpty(installKeytabInfo.keytabUri)) {
      throw new BadCommandArgumentsException("A valid local keytab location is required.");
    } else {
      File keytabFile = new File(installKeytabInfo.keytabUri);
      if (!keytabFile.exists() || keytabFile.isDirectory()) {
        throw new BadCommandArgumentsException("Unable to access supplied keytab file at " +
                                               keytabFile.getAbsolutePath());
      } else {
        srcFile = new Path(keytabFile.toURI());
      }
    }

    Path pkgPath = sliderFileSystem.buildKeytabInstallationDirPath(installKeytabInfo.folder);
    sliderFileSystem.getFileSystem().mkdirs(pkgPath);
    sliderFileSystem.getFileSystem().setPermission(pkgPath, new FsPermission(
        FsAction.ALL, FsAction.NONE, FsAction.NONE));

    Path fileInFs = new Path(pkgPath, srcFile.getName());
    log.info("Installing keytab {} at {} and overwrite is {}.", srcFile, fileInFs, installKeytabInfo.overwrite);
    if (sliderFileSystem.getFileSystem().exists(fileInFs) && !installKeytabInfo.overwrite) {
      throw new BadCommandArgumentsException("Keytab exists at " +
                                             fileInFs.toUri().toString() +
                                             ". Use --overwrite to overwrite.");
    }

    sliderFileSystem.getFileSystem().copyFromLocalFile(false, installKeytabInfo.overwrite, srcFile, fileInFs);
    sliderFileSystem.getFileSystem().setPermission(fileInFs, new FsPermission(
        FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));

    return EXIT_SUCCESS;
  }

  @Override
  public int actionInstallPkg(ActionInstallPackageArgs installPkgInfo) throws
      YarnException,
      IOException {

    Path srcFile = null;
    if (StringUtils.isEmpty(installPkgInfo.name)) {
      throw new BadCommandArgumentsException(
          "A valid application type name is required (e.g. HBASE).\n"
              + CommonArgs.usage(serviceArgs, ACTION_INSTALL_PACKAGE));
    }

    if (StringUtils.isEmpty(installPkgInfo.packageURI)) {
      throw new BadCommandArgumentsException("A valid application package location required.");
    } else {
      File pkgFile = new File(installPkgInfo.packageURI);
      if (!pkgFile.exists() || pkgFile.isDirectory()) {
        throw new BadCommandArgumentsException("Unable to access supplied pkg file at " +
                                               pkgFile.getAbsolutePath());
      } else {
        srcFile = new Path(pkgFile.toURI());
      }
    }

    Path pkgPath = sliderFileSystem.buildPackageDirPath(installPkgInfo.name);
    sliderFileSystem.getFileSystem().mkdirs(pkgPath);

    Path fileInFs = new Path(pkgPath, srcFile.getName());
    log.info("Installing package {} at {} and overwrite is {}.", srcFile, fileInFs, installPkgInfo.replacePkg);
    if (sliderFileSystem.getFileSystem().exists(fileInFs) && !installPkgInfo.replacePkg) {
      throw new BadCommandArgumentsException("Pkg exists at " +
                                             fileInFs.toUri().toString() +
                                             ". Use --replacepkg to overwrite.");
    }

    sliderFileSystem.getFileSystem().copyFromLocalFile(false, installPkgInfo.replacePkg, srcFile, fileInFs);
    return EXIT_SUCCESS;
  }

  @Override
  public int actionUpdate(String clustername,
      AbstractClusterBuildingActionArgs buildInfo) throws
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
    if (!liveClusterAllowed) {
      verifyNoLiveClusters(clustername, "Create");
    }

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
    builder.setImageDetailsIfAvailable(buildInfo.getImage(),
                                       buildInfo.getAppHomeDir());
    builder.setQueue(buildInfo.queue);

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
        log.debug("ZK node created for application instance: {}", defaultZKPath);
        if (defaultZKPath != null) {
          zkPaths.setAppPath(defaultZKPath);
        }
      } else {
        // create AppPath if default is being used
        String defaultZKPath = createZookeeperNode(clustername, true);
        log.debug("ZK node assigned to application instance: {}", defaultZKPath);
        zkPaths.setAppPath(defaultZKPath);
      }
    }

    builder.addZKBinding(zkPaths);

    //then propagate any package URI
    if (buildInfo.packageURI != null) {
      appConf.set(AgentKeys.PACKAGE_PATH, buildInfo.packageURI);
    }

    propagatePythonExecutable(conf, instanceDefinition);

    // make any substitutions needed at this stage
    replaceTokens(appConf.getConfTree(), getUsername(), clustername);

    // providers to validate what there is
    AggregateConf instanceDescription = builder.getInstanceDescription();
    validateInstanceDefinition(sliderAM, instanceDescription, sliderFileSystem);
    validateInstanceDefinition(provider, instanceDescription, sliderFileSystem);
    try {
      persistInstanceDefinition(overwrite, appconfdir, builder);
    } catch (LockAcquireFailedException e) {
      log.warn("Failed to get a Lock on {} : {}", builder, e);
      throw new BadClusterStateException("Failed to save " + clustername
                                         + ": " + e);
    }
  }

  protected void persistInstanceDefinition(boolean overwrite,
                                         Path appconfdir,
                                         InstanceBuilder builder)
      throws IOException, SliderException, LockAcquireFailedException {
    builder.persist(appconfdir, overwrite);
  }

  @VisibleForTesting
  public static void replaceTokens(ConfTree conf,
      String userName, String clusterName) throws IOException {
    Map<String,String> newglobal = new HashMap<String,String>();
    for (Entry<String,String> entry : conf.global.entrySet()) {
      newglobal.put(entry.getKey(), replaceTokens(entry.getValue(),
          userName, clusterName));
    }
    conf.global.putAll(newglobal);

    Map<String,List<String>> newcred = new HashMap<String,List<String>>();
    for (Entry<String,List<String>> entry : conf.credentials.entrySet()) {
      List<String> resultList = new ArrayList<String>();
      for (String v : entry.getValue()) {
        resultList.add(replaceTokens(v, userName, clusterName));
      }
      newcred.put(replaceTokens(entry.getKey(), userName, clusterName),
          resultList);
    }
    conf.credentials.clear();
    conf.credentials.putAll(newcred);
  }

  private static String replaceTokens(String s, String userName,
      String clusterName) throws IOException {
    return s.replaceAll(Pattern.quote("${USER}"), userName)
        .replaceAll(Pattern.quote("${USER_NAME}"), userName)
        .replaceAll(Pattern.quote("${CLUSTER_NAME}"), clusterName);
  }

  public FsPermission getClusterDirectoryPermissions(Configuration conf) {
    String clusterDirPermsOct =
      conf.get(CLUSTER_DIRECTORY_PERMISSIONS,
          DEFAULT_CLUSTER_DIRECTORY_PERMISSIONS);
    return new FsPermission(clusterDirPermsOct);
  }

  /**
   * Verify that the Resource Manager is configured (on a non-HA cluster).
   * with a useful error message
   * @throws BadCommandArgumentsException the exception raised on an invalid config
   */
  public void verifyBindingsDefined() throws BadCommandArgumentsException {
    InetSocketAddress rmAddr = SliderUtils.getRmAddress(getConfig());
    if (!getConfig().getBoolean(YarnConfiguration.RM_HA_ENABLED, false)
     && !SliderUtils.isAddressDefined(rmAddr)) {
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

    if (launchArgs.getOutputFile() != null) {
      // output file has been requested. Get the app report and serialize it
      ApplicationReport report =
          launchedApplication.getApplicationReport();
      SerializedApplicationReport sar = new SerializedApplicationReport(report);
      sar.submitTime = System.currentTimeMillis();
      ApplicationReportSerDeser serDeser = new ApplicationReportSerDeser();
      serDeser.save(sar, launchArgs.getOutputFile());
    }
    int waittime = launchArgs.getWaittime();
    if (waittime > 0) {
      return waitForAppRunning(launchedApplication, waittime, waittime);
    } else {
      // no waiting
      return EXIT_SUCCESS;
    }
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
      definition.setName(name);
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
   * @param clustername name of the cluster
   * @param clusterDirectory cluster dir
   * @param instanceDefinition the instance definition
   * @param debugAM enable debug AM options
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
    verifyNoLiveClusters(clustername, "Launch");
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
    
    if (log.isDebugEnabled()) {
      log.debug(instanceDefinition.toString());
    }
    MapOperations sliderAMResourceComponent =
      resourceOperations.getOrAddComponent(SliderKeys.COMPONENT_AM);
    MapOperations resourceGlobalOptions = resourceOperations.getGlobalOptions();

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
        resourceGlobalOptions,
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
    
    // look for the configuration directory named on the command line
    boolean hasServerLog4jProperties = false;
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
      remoteConfPath = new Path(clusterDirectory, SliderKeys.SUBMITTED_CONF_DIR);
      log.debug("Slider configuration directory is {}; remote to be {}",
    		  localConfDirPath, remoteConfPath);
      SliderUtils.copyDirectory(config, localConfDirPath, remoteConfPath, null);

      File log4jserver =
          new File(confDir, SliderKeys.LOG4J_SERVER_PROP_FILENAME);
      hasServerLog4jProperties = log4jserver.isFile();
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
    // validate security data

/*
    // turned off until tested
    SecurityConfiguration securityConfiguration =
        new SecurityConfiguration(config,
            instanceDefinition, clustername);
    
*/
    Configuration clientConfExtras = new Configuration(false);
    // then build up the generated path.
    FsPermission clusterPerms = getClusterDirectoryPermissions(config);
    SliderUtils.copyDirectory(config, snapshotConfPath, generatedConfDirPath,
        clusterPerms);


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


    // TODO: consider supporting apps that don't have an image path
    Path imagePath =
        SliderUtils.extractImagePath(sliderFileSystem, internalOptions);
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
    
    // if the conf dir has a log4j-server.properties, switch to that
    if (hasServerLog4jProperties) {
      commandLine.sysprop(SYSPROP_LOG4J_CONFIGURATION, LOG4J_SERVER_PROP_FILENAME);
      commandLine.sysprop(SYSPROP_LOG_DIR, ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    }
    
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

    /**
     * pass the registry binding
     */
    addConfOptionToCLI(commandLine, config, REGISTRY_PATH,
        DEFAULT_REGISTRY_PATH);
    addMandatoryConfOptionToCLI(commandLine, config,
        RegistryConstants.KEY_REGISTRY_ZK_QUORUM);

    if (clusterSecure) {
      // if the cluster is secure, make sure that
      // the relevant security settings go over
/*
      addConfOptionToCLI(commandLine, config, KEY_SECURITY);
*/
      addConfOptionToCLI(commandLine,
          config,
          DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY);
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
    String suppliedQueue = internalOperations.getGlobalOptions().get(InternalKeys.INTERNAL_QUEUE);
    if(!SliderUtils.isUnset(suppliedQueue)) {
      amQueue = suppliedQueue;
      log.info("Using queue {} for the application instance.", amQueue);
    }

    if (amQueue != null) {
      amLauncher.setQueue(amQueue);
    }

    // submit the application
    LaunchedApplication launchedApplication = amLauncher.submitApplication();
    return launchedApplication;
  }

  private void propagatePythonExecutable(Configuration config,
                                         AggregateConf instanceDefinition) {
    String pythonExec = config.get(
        SliderXmlConfKeys.PYTHON_EXECUTABLE_PATH);
    if (pythonExec != null) {
      instanceDefinition.getAppConfOperations().getGlobalOptions().putIfUnset(
          SliderXmlConfKeys.PYTHON_EXECUTABLE_PATH,
          pythonExec);
    }
  }


  /**
   * Wait for the launched app to be accepted in the time  
   * and, optionally running.
   * <p>
   * If the application
   *
   * @param launchedApplication application
   * @param acceptWaitMillis time in millis to wait for accept
   * @param runWaitMillis time in millis to wait for the app to be running.
   * May be null, in which case no wait takes place
   * @return exit code: success
   * @throws YarnException
   * @throws IOException
   */
  public int waitForAppRunning(LaunchedApplication launchedApplication,
      int acceptWaitMillis, int runWaitMillis) throws YarnException, IOException {
    assert launchedApplication != null;
    int exitCode;
    // wait for the submit state to be reached
    ApplicationReport report = launchedApplication.monitorAppToState(
      YarnApplicationState.ACCEPTED,
      new Duration(acceptWaitMillis));

    // may have failed, so check that
    if (SliderUtils.hasAppFinished(report)) {
      exitCode = buildExitCode(report);
    } else {
      // exit unless there is a wait


      if (runWaitMillis != 0) {
        // waiting for state to change
        Duration duration = new Duration(runWaitMillis * 1000);
        duration.start();
        report = launchedApplication.monitorAppToState(
          YarnApplicationState.RUNNING, duration);
        if (report != null &&
            report.getYarnApplicationState() == YarnApplicationState.RUNNING) {
          exitCode = EXIT_SUCCESS;
        } else {
          exitCode = buildExitCode(report);
        }
      } else {
        exitCode = EXIT_SUCCESS;
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
    String dfsPrincipal = config.get(
        DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY);
    if (dfsPrincipal != null) {
      String siteDfsPrincipal = OptionKeys.SITE_XML_PREFIX +
                                DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
      clusterSpec.getAppConfOperations().getGlobalOptions().putIfUnset(
        siteDfsPrincipal,
        dfsPrincipal);
    }
  }


  private boolean addConfOptionToCLI(CommandLineBuilder cmdLine,
      Configuration conf,
      String key) {
    String val = conf.get(key);
    return defineIfSet(cmdLine, key, val);
  }

  private String addConfOptionToCLI(CommandLineBuilder cmdLine,
      Configuration conf,
      String key,
      String defVal) {
    String val = conf.get(key, defVal);
    define(cmdLine, key, val);
    return val;
  }

  /**
   * Add a <code>-D key=val</code> command to the CLI
   * @param cmdLine command line
   * @param key key
   * @param val value
   */
  private void define(CommandLineBuilder cmdLine, String key, String val) {
    Preconditions.checkArgument(key != null, "null key");
    Preconditions.checkArgument(val != null, "null value");
    cmdLine.add(Arguments.ARG_DEFINE, key + "=" + val);
  }

  /**
   * Add a <code>-D key=val</code> command to the CLI if <code>val</code>
   * is not null
   * @param cmdLine command line
   * @param key key
   * @param val value
   */
  private boolean defineIfSet(CommandLineBuilder cmdLine, String key, String val) {
    Preconditions.checkArgument(key != null, "null key");
    if (val != null) {
      define(cmdLine, key, val);
      return true;
    } else {
      return false;
    }
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
   * @param action
   * @throws SliderException with exit code EXIT_CLUSTER_LIVE
   * if a cluster of that name is either live or starting up.
   */
  public void verifyNoLiveClusters(String clustername, String action) throws
                                                       IOException,
                                                       YarnException {
    List<ApplicationReport> existing = findAllLiveInstances(clustername);

    if (!existing.isEmpty()) {
      throw new SliderException(EXIT_APPLICATION_IN_USE,
          action +" failed for "
                              + clustername
                              + ": "
                              + E_CLUSTER_RUNNING + " :" +
                              existing.get(0));
    }
  }

  public String getUsername() throws IOException {
    return RegistryUtils.currentUser();
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
   * Build an exit code for an application from its report.
   * If the report parameter is null, its interpreted as a timeout
   * @param report report application report
   * @return the exit code
   * @throws IOException
   * @throws YarnException
   */
  private int buildExitCode(ApplicationReport report) throws
                                                      IOException,
                                                      YarnException {
    if (null == report) {
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

  @Override
  public ApplicationReport getApplicationReport() throws
                                                  IOException,
                                                  YarnException {
    return getApplicationReport(applicationId);
  }

  @Override
  public boolean forceKillApplication(String reason)
    throws YarnException, IOException {
    if (applicationId != null) {
      new LaunchedApplication(applicationId, yarnClient).forceKill(reason);
      return true;
    }
    return false;
  }

  /**
   * List Slider instances belonging to a specific user. This will include
   * failed and killed instances; there may be duplicates
   * @param user user: "" means all users, null means "default"
   * @return a possibly empty list of Slider AMs
   */
  @VisibleForTesting
  public List<ApplicationReport> listSliderInstances(String user)
    throws YarnException, IOException {
    return YarnAppListClient.listInstances(user);
  }

  /**
   * A basic list action to list live instances
   * @param clustername cluster name
   * @return success if the listing was considered successful
   * @throws IOException
   * @throws YarnException
   */
  public int actionList(String clustername) throws IOException, YarnException {
    ActionListArgs args = new ActionListArgs();
    args.live = true;
    return actionList(clustername, args);
  }

  /**
   * Implement the list action.
   * @param clustername List out specific instance name
   * @param args Action list arguments
   * @return 0 if one or more entries were listed
   * @throws IOException
   * @throws YarnException
   * @throws UnknownApplicationInstanceException if a specific instance
   * was named but it was not found
   */
  @Override
  @VisibleForTesting
  public int actionList(String clustername, ActionListArgs args)
      throws IOException, YarnException {
    verifyBindingsDefined();

    boolean live = args.live;
    String state = args.state;
    boolean verbose = args.verbose;

    if (live && !state.isEmpty()) {
      throw new BadCommandArgumentsException(
          Arguments.ARG_LIVE + " and " + Arguments.ARG_STATE + " are exclusive");
    }
    // flag to indicate only services in a specific state are to be listed
    boolean listOnlyInState = live || !state.isEmpty();
    
    YarnApplicationState min, max;
    if (live) {
      min = YarnApplicationState.NEW;
      max = YarnApplicationState.RUNNING;
    } else if (!state.isEmpty()) {
      YarnApplicationState stateVal = extractYarnApplicationState(state);
      min = max = stateVal;
    } else {
      min = YarnApplicationState.NEW;
      max = YarnApplicationState.KILLED;
    }
    // get the complete list of persistent instances
    Map<String, Path> persistentInstances = sliderFileSystem.listPersistentInstances();

    if (persistentInstances.isEmpty() && isUnset(clustername)) {
      // an empty listing is a success if no cluster was named
      log.debug("No application instances found");
      return EXIT_SUCCESS;
    }
    
    // and those the RM knows about
    List<ApplicationReport> instances = listSliderInstances(null);
    SliderUtils.sortApplicationsByMostRecent(instances);
    Map<String, ApplicationReport> reportMap =
        SliderUtils.buildApplicationReportMap(instances, min, max);
    log.debug("Persisted {} deployed {} filtered[{}-{}] & de-duped to {}",
        persistentInstances.size(),
        instances.size(),
        min, max,
        reportMap.size() );

    if (isSet(clustername)) {
      // only one instance is expected
      // resolve the persistent value
      Path persistent = persistentInstances.get(clustername);
      if (persistent == null) {
        throw unknownClusterException(clustername);
      }
      // create a new map with only that instance in it.
      // this restricts the output of results to this instance
      persistentInstances = new HashMap<String, Path>();
      persistentInstances.put(clustername, persistent);  
    }
    
    // at this point there is either the entire list or a stripped down instance
    int listed = 0;

    for (String name : persistentInstances.keySet()) {
      ApplicationReport report = reportMap.get(name);
      if (!listOnlyInState || report != null) {
        // list the details if all were requested, or the filtering contained
        // a report
        listed++;
        String details = instanceDetailsToString(name, report, verbose);
        print(details);
      }
    }
    
    return listed > 0 ? EXIT_SUCCESS: EXIT_FALSE;
  }

  /**
   * Convert the instance details of an application to a string
   * @param name instance name
   * @param report the application report
   * @param verbose verbose output
   * @return a string
   */
  String instanceDetailsToString(String name,
      ApplicationReport report,
      boolean verbose) {
    // format strings
    String staticf = "%-30s";
    String reportedf = staticf + "  %10s  %-40s";
    String livef = reportedf + " %s";
    StringBuilder builder = new StringBuilder(200);
    if (report == null) {
      builder.append(String.format(staticf, name));
    } else {
      // there's a report to look at
      String appId = report.getApplicationId().toString();
      String state = report.getYarnApplicationState().toString();
      if (report.getYarnApplicationState() == YarnApplicationState.RUNNING) {
        // running: there's a URL
        builder.append(String.format(livef, name, state, appId ,report.getTrackingUrl()));
      } else {
        builder.append(String.format(reportedf, name, state, appId));
      }
      if (verbose) {
        builder.append('\n');
        builder.append(SliderUtils.appReportToString(report, "\n  "));
      }
    }

    builder.append('\n');
    return builder.toString();
  }

  /**
   * Extract the state of a Yarn application --state argument
   * @param state state argument
   * @return the application state
   * @throws BadCommandArgumentsException if the argument did not match
   * any known state
   */
  private YarnApplicationState extractYarnApplicationState(String state) throws
      BadCommandArgumentsException {
    YarnApplicationState stateVal;
    try {
      stateVal = YarnApplicationState.valueOf(state.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new BadCommandArgumentsException("Unknown state: " + state);

    }
    return stateVal;
  }
  
  /**
   * Is an application active: accepted or running
   * @param report the application report
   * @return true if it is running or scheduled to run.
   */
  public boolean isApplicationActive(ApplicationReport report) {
    return report.getYarnApplicationState() == YarnApplicationState.RUNNING
                || report.getYarnApplicationState() == YarnApplicationState.ACCEPTED;
  }

  /**
   * Implement the islive action: probe for a cluster of the given name existing
   * @return exit code
   */

  @Override
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

  @Override
  public int actionExists(String name, boolean checkLive) throws YarnException, IOException {
    ActionExistsArgs args = new ActionExistsArgs();
    args.live = checkLive;
    return actionExists(name, args);
  }

  public int actionExists(String name, ActionExistsArgs args) throws YarnException, IOException {
    verifyBindingsDefined();
    SliderUtils.validateClusterName(name);
    boolean checkLive = args.live;
    log.debug("actionExists({}, {}, {})", name, checkLive, args.state);

    //initial probe for a cluster in the filesystem
    Path clusterDirectory = sliderFileSystem.buildClusterDirPath(name);
    if (!sliderFileSystem.getFileSystem().exists(clusterDirectory)) {
      throw unknownClusterException(name);
    }
    String state = args.state;
    if (!checkLive && SliderUtils.isUnset(state)) {
      log.info("Application {} exists", name);
      return EXIT_SUCCESS;
    }

    //test for liveness/state
    boolean inDesiredState = false;
    ApplicationReport instance;
    instance = findInstance(name);
    if (instance == null) {
      log.info("Application {} not running", name);
      return EXIT_FALSE;
    }
    if (checkLive) {
      // the app exists, check that it is not in any terminated state
      YarnApplicationState appstate = instance.getYarnApplicationState();
      log.debug(" current app state = {}", appstate);
      inDesiredState =
            appstate.ordinal() < YarnApplicationState.FINISHED.ordinal();
    } else {
      // scan for instance in single --state state
      List<ApplicationReport> userInstances = yarnClient.listInstances("");
      state = state.toUpperCase(Locale.ENGLISH);
      YarnApplicationState desiredState = extractYarnApplicationState(state);
      ApplicationReport foundInstance =
          yarnClient.findAppInInstanceList(userInstances, name, desiredState);
      if (foundInstance != null) {
        // found in selected state: success
        inDesiredState = true;
        // mark this as the instance to report
        instance = foundInstance;
      }
    }

    SliderUtils.OnDemandReportStringifier report =
        new SliderUtils.OnDemandReportStringifier(instance);
    if (!inDesiredState) {
      //cluster in the list of apps but not running
      log.info("Application {} found but is in wrong state {}", name,
          instance.getYarnApplicationState());
      log.debug("State {}", report);
      return EXIT_FALSE;
    } else {
      log.debug("Application instance is in desired state");
      log.info("Application {} is {}\n{}", name,
          instance.getYarnApplicationState(), report);
      return EXIT_SUCCESS;
    }
  }


  @Override
  public int actionKillContainer(String name,
      ActionKillContainerArgs args) throws YarnException, IOException {
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

  @Override
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
  public YarnAppListClient getYarnAppListClient() {
    return YarnAppListClient;
  }

  /**
   * Find an instance of an application belonging to the current user
   * @param appname application name
   * @return the app report or null if none is found
   * @throws YarnException YARN issues
   * @throws IOException IO problems
   */
  private ApplicationReport findInstance(String appname)
      throws YarnException, IOException {
    return YarnAppListClient.findInstance(appname);
  }
  
  private RunningApplication findApplication(String appname)
      throws YarnException, IOException {
    ApplicationReport applicationReport = findInstance(appname);
    return applicationReport != null ?
           new RunningApplication(yarnClient, applicationReport): null; 
  }

  /**
   * find all live instances of a specific app -if there is >1 in the cluster,
   * this returns them all. State should be running or less
   * @param appname application name
   * @return the list of all matching application instances
   */
  private List<ApplicationReport> findAllLiveInstances(String appname)
    throws YarnException, IOException {
    
    return YarnAppListClient.findAllLiveInstances(appname);
  }

  /**
   * Connect to a Slider AM
   * @param app application report providing the details on the application
   * @return an instance
   * @throws YarnException
   * @throws IOException
   */
  private SliderClusterProtocol connect(ApplicationReport app)
      throws YarnException, IOException {

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

  @Override
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

  @Override
  public int actionVersion() {
    SliderVersionInfo.loadAndPrintVersionInfo(log);
    return EXIT_SUCCESS;
  }

  @Override
  public int actionFreeze(String clustername,
      ActionFreezeArgs freezeArgs) throws YarnException, IOException {
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
      // not an error to stop a stopped cluster
      return EXIT_SUCCESS;
    }
    log.debug("App to stop was found: {}:\n{}", clustername,
              new SliderUtils.OnDemandReportStringifier(app));
    if (app.getYarnApplicationState().ordinal() >=
        YarnApplicationState.FINISHED.ordinal()) {
      log.info("Cluster {} is a terminated state {}", clustername,
               app.getYarnApplicationState());
      return EXIT_SUCCESS;
    }

    // IPC request for a managed shutdown is only possible if the app is running.
    // so we need to force kill if the app is accepted or submitted
    if (!forcekill
        && app.getYarnApplicationState().ordinal() < YarnApplicationState.RUNNING.ordinal()) {
      log.info("Cluster {} is in a pre-running state {}. Force killing it", clustername,
          app.getYarnApplicationState());
      forcekill = true;
    }

    LaunchedApplication application = new LaunchedApplication(yarnClient, app);
    applicationId = application.getApplicationId();
    
    if (forcekill) {
      // escalating to forced kill
      application.kill("Forced stop of " + clustername + ": " + text);
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
      log.warn("Exception while waiting for the application {} to shut down: {}",
               clustername, e);
    } catch ( IOException e) {
      log.warn("Exception while waiting for the application {} to shut down: {}",
               clustername, e);
    }

    return EXIT_SUCCESS;
  }

  @Override
  public int actionThaw(String clustername, ActionThawArgs thaw) throws YarnException, IOException {
    SliderUtils.validateClusterName(clustername);
    verifyBindingsDefined();
    // see if it is actually running and bail out;
    verifyNoLiveClusters(clustername, "Start");

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
  public int flex(String clustername, Map<String, Integer> roleInstances)
      throws YarnException, IOException {
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
      resources.getOrAddComponent(role).put(ResourceKeys.COMPONENT_INSTANCES,
                                            Integer.toString(count));

      log.debug("Flexed cluster specification ( {} -> {}) : \n{}",
                role,
                count,
                resources);
    }
    SliderAMClientProvider sliderAM = new SliderAMClientProvider(getConfig());
    AbstractClientProvider provider = createClientProvider(
        instanceDefinition.getInternalOperations().getGlobalOptions().getMandatoryOption(
            InternalKeys.INTERNAL_PROVIDER_NAME));
    // slider provider to validate what there is
    validateInstanceDefinition(sliderAM, instanceDefinition, sliderFileSystem);
    validateInstanceDefinition(provider, instanceDefinition, sliderFileSystem);

    int exitCode = EXIT_FALSE;
    // save the specification
    try {
      InstanceIO.updateInstanceDefinition(sliderFileSystem, clusterDirectory, instanceDefinition);
    } catch (LockAcquireFailedException e) {
      // lock failure
      log.debug("Failed to lock dir {}", clusterDirectory, e);
      log.warn("Failed to save new resource definition to {} : {}", clusterDirectory, e);
    }

    // now see if it is actually running and tell it about the update if it is
    ApplicationReport instance = findInstance(clustername);
    if (instance != null) {
      log.info("Flexing running cluster");
      SliderClusterProtocol appMaster = connect(instance);
      SliderClusterOperations clusterOps = new SliderClusterOperations(appMaster);
      clusterOps.flex(instanceDefinition.getResources());
      log.info("application instance size updated");
      exitCode = EXIT_SUCCESS;
    } else {
      log.info("No running instance to update");
    }
    return exitCode;
  }

  /**
   * Validate an instance definition against a provider.
   * @param provider the provider performing the validation
   * @param instanceDefinition the instance definition
   * @throws SliderException if invalid.
   */
  protected void validateInstanceDefinition(AbstractClientProvider provider,
      AggregateConf instanceDefinition, SliderFileSystem fs) throws SliderException {
    try {
      provider.validateInstanceDefinition(instanceDefinition, fs);
    } catch (SliderException e) {
      //problem, reject it
      log.info("Error {} validating application instance definition ", e.getMessage());
      log.debug("Error validating application instance definition ", e);
      log.info(instanceDefinition.toString());
      throw e;
    }
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


  @Override
  public int actionResolve(ActionResolveArgs args)
      throws YarnException, IOException {
    // as this is an API entry point, validate
    // the arguments
    args.validate();
    RegistryOperations operations = getRegistryOperations();
    String path = SliderRegistryUtils.resolvePath(args.path);
    ServiceRecordMarshal serviceRecordMarshal = new ServiceRecordMarshal();
    try {
      if (args.list) {
        File destDir = args.destdir;
        if (destDir != null) {
          destDir.mkdirs();
        }

        Map<String, ServiceRecord> recordMap;
        Map<String, RegistryPathStatus> znodes;
        try {
          znodes = statChildren(registryOperations, path);
          recordMap = extractServiceRecords(registryOperations,
              path,
              znodes.values());
        } catch (PathNotFoundException e) {
          // treat the root directory as if if is always there
        
          if ("/".equals(path)) {
            znodes = new HashMap<String, RegistryPathStatus>(0);
            recordMap = new HashMap<String, ServiceRecord>(0);
          } else {
            throw e;
          }
        }
        // subtract all records from the znodes map to get pure directories
        log.info("Entries: {}", znodes.size());

        for (String name : znodes.keySet()) {
          println("  " + name);
        }
        println("");

        log.info("Service records: {}", recordMap.size());
        for (Entry<String, ServiceRecord> recordEntry : recordMap.entrySet()) {
          String name = recordEntry.getKey();
          ServiceRecord instance = recordEntry.getValue();
          String json = serviceRecordMarshal.toJson(instance);
          if (destDir == null) {
            println(name);
            println(json);
          } else {
            String filename = RegistryPathUtils.lastPathEntry(name) + ".json";
            File jsonFile = new File(destDir, filename);
            SliderUtils.write(jsonFile,
                serviceRecordMarshal.toBytes(instance),
                true);
          }
        }
      } else  {
        // resolve single entry
        ServiceRecord instance = resolve(path);
        File outFile = args.out;
        if (args.destdir != null) {
          outFile = new File(args.destdir, RegistryPathUtils.lastPathEntry(path));
        }
        if (outFile != null) {
          SliderUtils.write(outFile, serviceRecordMarshal.toBytes(instance), true);
        } else {
          println(serviceRecordMarshal.toJson(instance));
        }
      }
//      TODO JDK7
    } catch (PathNotFoundException e) {
      // no record at this path
      throw new NotFoundException(e, path);
    } catch (NoRecordException e) {
      throw new NotFoundException(e, path);
    }
    return EXIT_SUCCESS;
  }

  @Override
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
        actionRegistryListConfigsYarn(registryArgs);
      } else if (registryArgs.listExports) {
        // list the exports
        actionRegistryListExports(registryArgs);
      } else if (SliderUtils.isSet(registryArgs.getConf)) {
        // get a configuration
        PublishedConfiguration publishedConfiguration =
            actionRegistryGetConfig(registryArgs);
        outputConfig(publishedConfiguration, registryArgs);
      } else if (SliderUtils.isSet(registryArgs.getExport)) {
        // get a export group
        PublishedExports publishedExports =
            actionRegistryGetExport(registryArgs);
        outputExport(publishedExports, registryArgs);
      } else {
        // it's an unknown command
        log.info(CommonArgs.usage(serviceArgs, ACTION_DIAGNOSTICS));
        return EXIT_USAGE;
      }
//      JDK7
    } catch (FileNotFoundException e) {
      log.info("{}", e);
      log.debug("{}", e, e);
      return EXIT_NOT_FOUND;
    } catch (PathNotFoundException e) {
      log.info("{}", e);
      log.debug("{}", e, e);
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
  public Collection<ServiceRecord> actionRegistryList(
      ActionRegistryArgs registryArgs)
      throws YarnException, IOException {
    String serviceType = registryArgs.serviceType;
    String name = registryArgs.name;
    RegistryOperations operations = getRegistryOperations();
    Collection<ServiceRecord> serviceRecords;
    if (StringUtils.isEmpty(name)) {
      String path =
          serviceclassPath(
              currentUser(),
              serviceType);

      try {
        Map<String, ServiceRecord> recordMap =
            listServiceRecords(operations, path);
        if (recordMap.isEmpty()) {
          throw new UnknownApplicationInstanceException(
              "No applications registered under " + path);
        }
        serviceRecords = recordMap.values();
      } catch (PathNotFoundException e) {
        throw new NotFoundException(path, e);
      }
    } else {
      ServiceRecord instance = lookupServiceRecord(registryArgs);
      serviceRecords = new ArrayList<ServiceRecord>(1);
      serviceRecords.add(instance);
    }

    for (ServiceRecord serviceRecord : serviceRecords) {
      logInstance(serviceRecord, registryArgs.verbose);
    }
    return serviceRecords;
  }

  @Override
  public int actionDiagnostic(ActionDiagnosticArgs diagnosticArgs) {
    try {
      if (diagnosticArgs.client) {
        actionDiagnosticClient(diagnosticArgs);
      } else if (diagnosticArgs.application) {
        actionDiagnosticApplication(diagnosticArgs);
      } else if (diagnosticArgs.yarn) {
        actionDiagnosticYarn(diagnosticArgs);
      } else if (diagnosticArgs.credentials) {
        actionDiagnosticCredentials();
      } else if (diagnosticArgs.all) {
        actionDiagnosticAll(diagnosticArgs);
      } else if (diagnosticArgs.level) {
        actionDiagnosticIntelligent(diagnosticArgs);
      } else {
        // it's an unknown option
        log.info(CommonArgs.usage(serviceArgs, ACTION_DIAGNOSTICS));
        return EXIT_USAGE;
      }
    } catch (Exception e) {
      log.error(e.toString());
      return EXIT_FALSE;
    }
    return EXIT_SUCCESS;
  }

  private void actionDiagnosticIntelligent(ActionDiagnosticArgs diagnosticArgs)
      throws YarnException, IOException, URISyntaxException {
    // not using member variable clustername because we want to place
    // application name after --application option and member variable
    // cluster name has to be put behind action
    String clusterName = diagnosticArgs.name;
    if(SliderUtils.isUnset(clusterName)){
      throw new BadCommandArgumentsException("application name must be provided with --name option");
    }
    
    try {
      SliderUtils.validateClientConfigFile();
      log.info("Slider-client.xml is accessible");
    } catch (IOException e) {
      // we are catching exceptions here because those are indication of
      // validation result, and we need to print them here
      log.error(
          "validation of slider-client.xml fails because: " + e.toString(), e);
      return;
    }
    SliderClusterOperations clusterOperations = createClusterOperations(clusterName);
    // cluster not found exceptions will be thrown upstream
    ClusterDescription clusterDescription = clusterOperations
        .getClusterDescription();
    log.info("Slider AppMaster is accessible");

    if (clusterDescription.state == ClusterDescription.STATE_LIVE) {
      AggregateConf instanceDefinition = clusterOperations
          .getInstanceDefinition();
      String imagePath = instanceDefinition.getInternalOperations().get(
          InternalKeys.INTERNAL_APPLICATION_IMAGE_PATH);
      // if null, that means slider uploaded the agent tarball for the user
      // and we need to use where slider has put
      if (imagePath == null) {
        ApplicationReport appReport = findInstance(clusterName);
        Path path1 = sliderFileSystem.getTempPathForCluster(clusterName);
        
        Path subPath = new Path(path1, appReport.getApplicationId().toString()
            + "/agent");
        imagePath = subPath.toString();
      }
      try {
        SliderUtils.validateHDFSFile(sliderFileSystem, imagePath + "/" + AGENT_TAR);
        log.info("Slider agent package is properly installed");
      } catch (FileNotFoundException e) {
        log.error("can not find agent package: " + e.toString());
        return;
      } catch (IOException e) {
        log.error("can not open agent package: " + e.toString());
        return;
      }
      String pkgTarballPath = instanceDefinition.getAppConfOperations()
          .getGlobalOptions().getMandatoryOption(AgentKeys.APP_DEF);
      try {
        SliderUtils.validateHDFSFile(sliderFileSystem, pkgTarballPath);
        log.info("Application package is properly installed");
      } catch (FileNotFoundException e) {
        log.error("can not find application package: {}", e);
        return;
      } catch (IOException e) {
        log.error("can not open application package: {} ", e);
        return;
      }
    }
  }

  private void actionDiagnosticAll(ActionDiagnosticArgs diagnosticArgs)
      throws IOException, YarnException {
    // assign application name from param to each sub diagnostic function
    actionDiagnosticClient(diagnosticArgs);
    actionDiagnosticApplication(diagnosticArgs);
    actionDiagnosticSlider(diagnosticArgs);
    actionDiagnosticYarn(diagnosticArgs);
    actionDiagnosticCredentials();
  }

  private void actionDiagnosticCredentials() throws BadConfigException,
      IOException {
    if (SliderUtils.isHadoopClusterSecure(SliderUtils
        .loadClientConfigurationResource())) {
      String credentialCacheFileDescription = null;
      try {
        credentialCacheFileDescription = SliderUtils.checkCredentialCacheFile();
      } catch (BadConfigException e) {
        log.error("The credential config is not valid: " + e.toString());
        throw e;
      } catch (IOException e) {
        log.error("Unable to read the credential file: " + e.toString());
        throw e;
      }
      log.info("Credential cache file for the current user: "
          + credentialCacheFileDescription);
    } else {
      log.info("the cluster is not in secure mode");
    }
  }

  private void actionDiagnosticYarn(ActionDiagnosticArgs diagnosticArgs)
      throws IOException, YarnException {
    JSONObject converter = null;
    log.info("the node in the YARN cluster has below state: ");
    List<NodeReport> yarnClusterInfo;
    try {
      yarnClusterInfo = yarnClient.getNodeReports(NodeState.RUNNING);
    } catch (YarnException e1) {
      log.error("Exception happened when fetching node report from the YARN cluster: "
          + e1.toString());
      throw e1;
    } catch (IOException e1) {
      log.error("Network problem happened when fetching node report YARN cluster: "
          + e1.toString());
      throw e1;
    }
    for (NodeReport nodeReport : yarnClusterInfo) {
      log.info(nodeReport.toString());
    }

    if (diagnosticArgs.verbose) {
      Writer configWriter = new StringWriter();
      try {
        Configuration.dumpConfiguration(yarnClient.getConfig(), configWriter);
      } catch (IOException e1) {
        log.error("Network problem happened when retrieving YARN config from YARN: "
            + e1.toString());
        throw e1;
      }
      try {
        converter = new JSONObject(configWriter.toString());
        log.info("the configuration of the YARN cluster is: "
            + converter.toString(2));

      } catch (JSONException e) {
        log.error("JSONException happened during parsing response from YARN: "
            + e.toString());
      }
    }
  }

  private void actionDiagnosticSlider(ActionDiagnosticArgs diagnosticArgs)
      throws YarnException, IOException {
    // not using member variable clustername because we want to place
    // application name after --application option and member variable
    // cluster name has to be put behind action
    String clusterName = diagnosticArgs.name;
    if(SliderUtils.isUnset(clusterName)){
      throw new BadCommandArgumentsException("application name must be provided with --name option");
    }
    SliderClusterOperations clusterOperations;
    AggregateConf instanceDefinition = null;
    try {
      clusterOperations = createClusterOperations(clusterName);
      instanceDefinition = clusterOperations.getInstanceDefinition();
    } catch (YarnException e) {
      log.error("Exception happened when retrieving instance definition from YARN: "
          + e.toString());
      throw e;
    } catch (IOException e) {
      log.error("Network problem happened when retrieving instance definition from YARN: "
          + e.toString());
      throw e;
    }
    String imagePath = instanceDefinition.getInternalOperations().get(
        InternalKeys.INTERNAL_APPLICATION_IMAGE_PATH);
    // if null, it will be uploaded by Slider and thus at slider's path
    if (imagePath == null) {
      ApplicationReport appReport = findInstance(clusterName);
      Path path1 = sliderFileSystem.getTempPathForCluster(clusterName);
      Path subPath = new Path(path1, appReport.getApplicationId().toString()
          + "/agent");
      imagePath = subPath.toString();
    }
    log.info("The path of slider agent tarball on HDFS is: " + imagePath);
  }

  private void actionDiagnosticApplication(ActionDiagnosticArgs diagnosticArgs)
      throws YarnException, IOException {
    // not using member variable clustername because we want to place
    // application name after --application option and member variable
    // cluster name has to be put behind action
    String clusterName = diagnosticArgs.name;
    if(SliderUtils.isUnset(clusterName)){
      throw new BadCommandArgumentsException("application name must be provided with --name option");
    }
    SliderClusterOperations clusterOperations;
    AggregateConf instanceDefinition = null;
    try {
      clusterOperations = createClusterOperations(clusterName);
      instanceDefinition = clusterOperations.getInstanceDefinition();
    } catch (YarnException e) {
      log.error("Exception happened when retrieving instance definition from YARN: "
          + e.toString());
      throw e;
    } catch (IOException e) {
      log.error("Network problem happened when retrieving instance definition from YARN: "
          + e.toString());
      throw e;
    }
    String clusterDir = instanceDefinition.getAppConfOperations()
        .getGlobalOptions().get(AgentKeys.APP_ROOT);
    String pkgTarball = instanceDefinition.getAppConfOperations()
        .getGlobalOptions().get(AgentKeys.APP_DEF);
    String runAsUser = instanceDefinition.getAppConfOperations()
        .getGlobalOptions().get(AgentKeys.RUNAS_USER);

    log.info("The location of the cluster instance directory in HDFS is: "
        + clusterDir);
    log.info("The name of the application package tarball on HDFS is: "
        + pkgTarball);
    log.info("The runas user of the application in the cluster is: "
        + runAsUser);

    if (diagnosticArgs.verbose) {
      log.info("App config of the application: "
          + instanceDefinition.getAppConf().toJson());
      log.info("Resource config of the application: "
          + instanceDefinition.getResources().toJson());
    }
  }

  private void actionDiagnosticClient(ActionDiagnosticArgs diagnosticArgs)
      throws SliderException, IOException {
    try {
      String currentCommandPath = SliderUtils.getCurrentCommandPath();
      SliderVersionInfo.loadAndPrintVersionInfo(log);
      String clientConfigPath = SliderUtils.getClientConfigPath();
      String jdkInfo = SliderUtils.getJDKInfo();
      println("The slider command path: %s", currentCommandPath);
      println("The slider-client.xml used by current running command path: %s",
          clientConfigPath);
      println(jdkInfo);

      // security info
      Configuration config = getConfig();
      if (SliderUtils.isHadoopClusterSecure(config)) {
        println("Hadoop Cluster is secure");
        println("Login user is %s", UserGroupInformation.getLoginUser());
        println("Current user is %s", UserGroupInformation.getCurrentUser());

      } else {
        println("Hadoop Cluster is insecure");
      }

      // verbose?
      if (diagnosticArgs.verbose) {
        // do the environment
        Map<String, String> env = System.getenv();
        Set<String> envList = ConfigHelper.sortedConfigKeys(env.entrySet());
        StringBuilder builder = new StringBuilder("Environment variables:\n");
        for (String key : envList) {
          builder.append(key).append("=").append(env.get(key)).append("\n");
        }
        println(builder.toString());

        // Java properties
        builder = new StringBuilder("JVM Properties\n");
        Map<String, String> props =
            SliderUtils.sortedMap(SliderUtils.toMap(System.getProperties()));
        for (Entry<String, String> entry : props.entrySet()) {
          builder.append(entry.getKey()).append("=")
                 .append(entry.getValue()).append("\n");
        }
        
        println(builder.toString());

        // then the config
        println("Slider client configuration:\n"
                + ConfigHelper.dumpConfigToString(config));
        
      }

      SliderUtils.validateSliderClientEnvironment(log);
    } catch (SliderException e) {
      log.error(e.toString());
      throw e;
    } catch (IOException e) {
      log.error(e.toString());
      throw e;
    }

  }

  /**
   * Log a service record instance
   * @param instance record
   * @param verbose verbose logging of all external endpoints
   */
  private void logInstance(ServiceRecord instance,
      boolean verbose) {
    if (!verbose) {
      log.info("{}", instance.get(YarnRegistryAttributes.YARN_ID, ""));
    } else {
      log.info("{}: ", instance.get(YarnRegistryAttributes.YARN_ID, ""));
      logEndpoints(instance);
    }
  }

  /**
   * Log the external endpoints of a service record
   * @param instance service record instance
   */
  private void logEndpoints(ServiceRecord instance) {
    List<Endpoint> endpoints = instance.external;
    for (Endpoint endpoint : endpoints) {
      log.info(endpoint.toString());
    }
  }

 /**
   * list configs available for an instance
   *
   * @param registryArgs registry Arguments
   * @throws YarnException YARN problems
   * @throws IOException Network or other problems
   */
  public void actionRegistryListConfigsYarn(ActionRegistryArgs registryArgs)
      throws YarnException, IOException {

    ServiceRecord instance = lookupServiceRecord(registryArgs);

    RegistryRetriever retriever = new RegistryRetriever(instance);
    PublishedConfigSet configurations =
        retriever.getConfigurations(!registryArgs.internal);
    PrintStream out = null;
    try {
      if (registryArgs.out != null) {
        out = new PrintStream(new FileOutputStream(registryArgs.out));
      } else {
        out = System.out;
      }
      for (String configName : configurations.keys()) {
        if (!registryArgs.verbose) {
          out.println(configName);
        } else {
          PublishedConfiguration published =
              configurations.get(configName);
          out.printf("%s: %s\n",
              configName,
              published.description);
        }
      }
    } finally {
      if (registryArgs.out != null && out != null) {
        out.flush();
        out.close();
      }
    }
  }

  /**
   * list exports available for an instance
   *
   * @param registryArgs registry Arguments
   * @throws YarnException YARN problems
   * @throws IOException Network or other problems
   */
  public void actionRegistryListExports(ActionRegistryArgs registryArgs)
      throws YarnException, IOException {
    ServiceRecord instance = lookupServiceRecord(registryArgs);

    RegistryRetriever retriever = new RegistryRetriever(instance);
    PublishedExportsSet exports =
        retriever.getExports(!registryArgs.internal);
    PrintStream out = null;
    boolean streaming = false;
    try {
      if (registryArgs.out != null) {
        out = new PrintStream(new FileOutputStream(registryArgs.out));
        streaming = true;
        log.debug("Saving output to {}", registryArgs.out);
      } else {
        out = System.out;
      }
      log.debug("Number of exports: {}", exports.keys().size());
      for (String exportName : exports.keys()) {
        if (streaming) {
          log.debug(exportName);
        }
        if (!registryArgs.verbose) {
          out.println(exportName);
        } else {
          PublishedExports published = exports.get(exportName);
          out.printf("%s: %s\n",
              exportName,
              published.description);
        }
      }
    } finally {
      if (streaming) {
        out.flush();
        out.close();
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
    ServiceRecord instance = lookupServiceRecord(registryArgs);

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
   * get a specific export group
   *
   * @param registryArgs registry Arguments
   *
   * @throws YarnException         YARN problems
   * @throws IOException           Network or other problems
   * @throws FileNotFoundException if the config is not found
   */
  @VisibleForTesting
  public PublishedExports actionRegistryGetExport(ActionRegistryArgs registryArgs)
      throws YarnException, IOException {
    ServiceRecord instance = lookupServiceRecord(registryArgs);

    RegistryRetriever retriever = new RegistryRetriever(instance);
    boolean external = !registryArgs.internal;
    PublishedExportsSet exports =
        retriever.getExports(external);

    PublishedExports published = retriever.retrieveExports(exports,
                                                           registryArgs.getExport,
                                                           external);
    return published;
  }

  /**
   * write out the config. If a destination is provided and that dir is a
   * directory, the entry is written to it with the name provided + extension,
   * else it is printed to standard out.
   * @param published published config
   * @param registryArgs registry Arguments
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
    boolean print = registryArgs.out == null;
    if (!print) {
      File outputPath = registryArgs.out;
      if (outputPath.isDirectory()) {
        // creating it under a directory
        outputPath = new File(outputPath, entry + "." + format);
      }
      log.debug("Destination path: {}", outputPath);
      outputter.save(outputPath);
    } else {
      print(outputter.asString());
    }
    
  }

  /**
   * write out the config
   * @param published
   * @param registryArgs
   * @throws BadCommandArgumentsException
   * @throws IOException
   */
  private void outputExport(PublishedExports published,
                            ActionRegistryArgs registryArgs) throws
      BadCommandArgumentsException,
      IOException {
    // decide whether or not to print
    String entry = registryArgs.getExport;
    String format = ConfigFormat.JSON.toString();
    ConfigFormat configFormat = ConfigFormat.resolve(format);
    if (configFormat == null || configFormat != ConfigFormat.JSON) {
      throw new BadCommandArgumentsException(
          "Unknown/Unsupported format %s . Only JSON is supported.", format);
    }

    PublishedExportsOutputter outputter =
        PublishedExportsOutputter.createOutputter(configFormat,
                                                  published);
    boolean print = registryArgs.out == null;
    if (!print) {
      File destFile;
      destFile = registryArgs.out;
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
   * @throws SliderException other failures
   * @throws IOException IO problems or wrapped exceptions
   */
  private ServiceRecord lookupServiceRecord(ActionRegistryArgs registryArgs) throws
      SliderException,
      IOException {
    String user;
    if (StringUtils.isNotEmpty(registryArgs.user)) {
      user = RegistryPathUtils.encodeForRegistry(registryArgs.user);
    } else {
      user = currentUser();
    }

    String path = servicePath(user, registryArgs.serviceType,
        registryArgs.name);
    return resolve(path);
  }

  /**
   * Look up a service record of the current user
   * @param serviceType service type
   * @param id instance ID
   * @return instance data
   * @throws UnknownApplicationInstanceException no path or service record
   * at the end of the path
   * @throws SliderException other failures
   * @throws IOException IO problems or wrapped exceptions
   */
  public ServiceRecord lookupServiceRecord(String serviceType, String id)
      throws IOException, SliderException {
    String path = servicePath(currentUser(), serviceType, id);
    return resolve(path);
  }

  /**
   * 
   * Look up an instance
   * @param path path
   * @return instance data
   * @throws NotFoundException no path/no service record
   * at the end of the path
   * @throws SliderException other failures
   * @throws IOException IO problems or wrapped exceptions
   */
  public ServiceRecord resolve(String path)
      throws IOException, SliderException {
    try {
      return getRegistryOperations().resolve(path);
    } catch (PathNotFoundException e) {
      throw new NotFoundException(e.getPath().toString(), e);
    } catch (NoRecordException e) {
      throw new NotFoundException(e.getPath().toString(), e);
    }
  }

  /**
   * List instances in the registry for the current user
   * @return a list of slider registry instances
   * @throws IOException Any IO problem ... including no path in the registry
   * to slider service classes for this user
   * @throws SliderException other failures
   */

  public Map<String, ServiceRecord> listRegistryInstances()
      throws IOException, SliderException {
    Map<String, ServiceRecord> recordMap = listServiceRecords(
        getRegistryOperations(),
        serviceclassPath(currentUser(), SliderKeys.APP_TYPE));
    return recordMap;
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
      Map<String, ServiceRecord> recordMap = listServiceRecords(
          getRegistryOperations(),
          serviceclassPath(currentUser(), SliderKeys.APP_TYPE));
      return new ArrayList<String>(recordMap.keySet());
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
  private synchronized RegistryOperations maybeStartYarnRegistry()
      throws SliderException, IOException {

    if (registryOperations == null) {
      registryOperations = startRegistryOperationsService();
    }
    return registryOperations;
  }

  @Override
  public RegistryOperations getRegistryOperations()
      throws SliderException, IOException {
    return maybeStartYarnRegistry();
  }

  /**
   * Output to standard out/stderr (implementation specific detail)
   * @param src source
   */
  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  private static void print(CharSequence src) {
    System.out.append(src);
  }

  /**
   * Output to standard out/stderr with a newline after
   * @param message message
   */
  private static void println(String message) {
    print(message);
    print("\n");
  }
  /**
   * Output to standard out/stderr with a newline after, formatted
   * @param message message
   * @param args arguments for string formatting
   */
  private static void println(String message, Object ... args) {
    print(String.format(message, args));
    print("\n");
  }

  /**
   * Implement the lookup action.
   * @param args Action arguments
   * @return 0 if the entry was found
   * @throws IOException
   * @throws YarnException
   * @throws UnknownApplicationInstanceException if a specific instance
   * was named but it was not found
   */
  @VisibleForTesting
  public int actionLookup(ActionLookupArgs args)
      throws IOException, YarnException {
    verifyBindingsDefined();
    try {
      ApplicationId id = ConverterUtils.toApplicationId(args.id);
      ApplicationReport report = yarnClient.getApplicationReport(id);
      SerializedApplicationReport sar = new SerializedApplicationReport(report);
      ApplicationReportSerDeser serDeser = new ApplicationReportSerDeser();
      if (args.outputFile != null) {
        serDeser.save(sar, args.outputFile);
      } else {
        println(serDeser.toJson(sar));
      }
    } catch (IllegalArgumentException e) {
      throw new BadCommandArgumentsException(e, "%s : %s", args, e);
    } catch (ApplicationAttemptNotFoundException notFound) {
      throw new NotFoundException(notFound, notFound.toString());
    } catch (ApplicationNotFoundException notFound) {
      throw new NotFoundException(notFound, notFound.toString());
    }
    return EXIT_SUCCESS;
  }
}


