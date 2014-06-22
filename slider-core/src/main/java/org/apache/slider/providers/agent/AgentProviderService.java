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

package org.apache.slider.providers.agent;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.ClusterDescriptionKeys;
import org.apache.slider.api.ClusterNode;
import org.apache.slider.api.OptionKeys;
import org.apache.slider.api.StatusKeys;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.CommandLineBuilder;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.core.registry.docstore.PublishedConfiguration;
import org.apache.slider.core.registry.info.CustomRegistryConstants;
import org.apache.slider.core.registry.info.RegisteredEndpoint;
import org.apache.slider.core.registry.info.ServiceInstanceData;
import org.apache.slider.providers.AbstractProviderService;
import org.apache.slider.providers.ProviderCompleted;
import org.apache.slider.providers.ProviderCore;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.providers.ProviderUtils;
import org.apache.slider.providers.agent.application.metadata.Component;
import org.apache.slider.providers.agent.application.metadata.Export;
import org.apache.slider.providers.agent.application.metadata.ExportGroup;
import org.apache.slider.providers.agent.application.metadata.Metainfo;
import org.apache.slider.providers.agent.application.metadata.Service;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.appmaster.web.rest.agent.AgentCommandType;
import org.apache.slider.server.appmaster.web.rest.agent.AgentRestOperations;
import org.apache.slider.server.appmaster.web.rest.agent.CommandReport;
import org.apache.slider.server.appmaster.web.rest.agent.ComponentStatus;
import org.apache.slider.server.appmaster.web.rest.agent.ExecutionCommand;
import org.apache.slider.server.appmaster.web.rest.agent.HeartBeat;
import org.apache.slider.server.appmaster.web.rest.agent.HeartBeatResponse;
import org.apache.slider.server.appmaster.web.rest.agent.Register;
import org.apache.slider.server.appmaster.web.rest.agent.RegistrationResponse;
import org.apache.slider.server.appmaster.web.rest.agent.RegistrationStatus;
import org.apache.slider.server.appmaster.web.rest.agent.StatusCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.slider.server.appmaster.web.rest.RestPaths.SLIDER_PATH_AGENTS;

/** This class implements the server-side logic for application deployment
 *  through Slider application package
 **/
public class AgentProviderService extends AbstractProviderService implements
    ProviderCore,
    AgentKeys,
    SliderKeys, AgentRestOperations {


  protected static final Logger log =
      LoggerFactory.getLogger(AgentProviderService.class);
  private static final ProviderUtils providerUtils = new ProviderUtils(log);
  private static final String LABEL_MAKER = "___";
  private static final String CONTAINER_ID = "container_id";
  private static final String GLOBAL_CONFIG_TAG = "global";
  private static final String LOG_FOLDERS_TAG = "LogFolders";
  private static final int MAX_LOG_ENTRIES = 20;
  private static final int DEFAULT_HEARTBEAT_MONITOR_INTERVAL = 60 * 1000;
  private final Object syncLock = new Object();
  private final Map<String, String> allocatedPorts = new ConcurrentHashMap<>();
  private int heartbeatMonitorInterval = 0;
  private AgentClientProvider clientProvider;
  private Map<String, ComponentInstanceState> componentStatuses = new ConcurrentHashMap<>();
  private AtomicInteger taskId = new AtomicInteger(0);
  private volatile Metainfo metainfo = null;
  private ComponentCommandOrder commandOrder = null;
  private HeartbeatMonitor monitor;
  private Map<String, String> workFolders =
      Collections.synchronizedMap(new LinkedHashMap<String, String>(MAX_LOG_ENTRIES, 0.75f, false) {
        protected boolean removeEldestEntry(Map.Entry eldest) {
          return size() > MAX_LOG_ENTRIES;
        }
      });
  private Boolean canAnyMasterPublish = null;
  private AgentLaunchParameter agentLaunchParameter = null;

  /**
   * Create an instance of AgentProviderService
   */
  public AgentProviderService() {
    super("AgentProviderService");
    setAgentRestOperations(this);
    setHeartbeatMonitorInterval(DEFAULT_HEARTBEAT_MONITOR_INTERVAL);
  }

  @Override
  public List<ProviderRole> getRoles() {
    return AgentRoles.getRoles();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    clientProvider = new AgentClientProvider(conf);
  }

  @Override
  public Configuration loadProviderConfigurationInformation(File confDir) throws
      BadCommandArgumentsException,
      IOException {
    return new Configuration(false);
  }

  @Override
  public void validateInstanceDefinition(AggregateConf instanceDefinition)
      throws
      SliderException {
    clientProvider.validateInstanceDefinition(instanceDefinition);
  }

  @Override
  public void buildContainerLaunchContext(ContainerLauncher launcher,
                                          AggregateConf instanceDefinition,
                                          Container container,
                                          String role,
                                          SliderFileSystem fileSystem,
                                          Path generatedConfPath,
                                          MapOperations resourceComponent,
                                          MapOperations appComponent,
                                          Path containerTmpDirPath) throws
      IOException,
      SliderException {

    String appDef = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getMandatoryOption(AgentKeys.APP_DEF);

    if (metainfo == null) {
      synchronized (syncLock) {
        if (metainfo == null) {
          readAndSetHeartbeatMonitoringInterval(instanceDefinition);
          initializeAgentDebugCommands(instanceDefinition);

          metainfo = getApplicationMetainfo(fileSystem, appDef);
          if (metainfo == null || metainfo.getServices() == null || metainfo.getServices().size() == 0) {
            log.error("metainfo.xml is unavailable or malformed at {}.", appDef);
            throw new SliderException("metainfo.xml is required in app package.");
          }

          commandOrder = new ComponentCommandOrder(metainfo.getServices().get(0).getCommandOrder());
          monitor = new HeartbeatMonitor(this, getHeartbeatMonitorInterval());
          monitor.start();
        }
      }
    }

    log.info("Build launch context for Agent");
    log.debug(instanceDefinition.toString());

    // Set the environment
    launcher.putEnv(SliderUtils.buildEnvMap(appComponent));

    String workDir = ApplicationConstants.Environment.PWD.$();
    launcher.setEnv("AGENT_WORK_ROOT", workDir);
    log.info("AGENT_WORK_ROOT set to {}", workDir);
    String logDir = ApplicationConstants.Environment.LOG_DIRS.$();
    launcher.setEnv("AGENT_LOG_ROOT", logDir);
    log.info("AGENT_LOG_ROOT set to {}", logDir);
    launcher.setEnv(HADOOP_USER_NAME, System.getenv(HADOOP_USER_NAME));

    //local resources

    // TODO: Should agent need to support App Home
    String scriptPath = new File(AgentKeys.AGENT_MAIN_SCRIPT_ROOT, AgentKeys.AGENT_MAIN_SCRIPT).getPath();
    String appHome = instanceDefinition.getAppConfOperations().
        getGlobalOptions().get(AgentKeys.PACKAGE_PATH);
    if (SliderUtils.isSet(appHome)) {
      scriptPath = new File(appHome, AgentKeys.AGENT_MAIN_SCRIPT).getPath();
    }

    String agentImage = instanceDefinition.getInternalOperations().
        get(OptionKeys.INTERNAL_APPLICATION_IMAGE_PATH);
    if (agentImage != null) {
      LocalResource agentImageRes = fileSystem.createAmResource(new Path(agentImage), LocalResourceType.ARCHIVE);
      launcher.addLocalResource(AgentKeys.AGENT_INSTALL_DIR, agentImageRes);
    }

    log.info("Using {} for agent.", scriptPath);
    LocalResource appDefRes = fileSystem.createAmResource(
        fileSystem.getFileSystem().resolvePath(new Path(appDef)),
        LocalResourceType.ARCHIVE);
    launcher.addLocalResource(AgentKeys.APP_DEFINITION_DIR, appDefRes);

    String agentConf = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getMandatoryOption(AgentKeys.AGENT_CONF);
    LocalResource agentConfRes = fileSystem.createAmResource(
        fileSystem.getFileSystem().resolvePath(new Path(agentConf)),
        LocalResourceType.FILE);
    launcher.addLocalResource(AgentKeys.AGENT_CONFIG_FILE, agentConfRes);

    String agentVer = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getOption(AgentKeys.AGENT_VERSION, null);
    if (agentVer != null) {
      LocalResource agentVerRes = fileSystem.createAmResource(
          fileSystem.getFileSystem().resolvePath(new Path(agentVer)),
          LocalResourceType.FILE);
      launcher.addLocalResource(AgentKeys.AGENT_VERSION_FILE, agentVerRes);
    }

    String label = getContainerLabel(container, role);
    CommandLineBuilder operation = new CommandLineBuilder();

    operation.add(AgentKeys.PYTHON_EXE);

    operation.add(scriptPath);
    operation.add(ARG_LABEL, label);
    operation.add(ARG_HOST);
    operation.add(getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME));
    operation.add(ARG_PORT);
    operation.add(getClusterInfoPropertyValue(StatusKeys.INFO_AM_WEB_PORT));

    String debugCmd = agentLaunchParameter.getNextLaunchParameter(role);
    if (debugCmd != null && debugCmd.length() != 0) {
      operation.add(ARG_DEBUG);
      operation.add(debugCmd);
    }

    launcher.addCommand(operation.build());

    // initialize the component instance state
    componentStatuses.put(label,
                          new ComponentInstanceState(
                              role,
                              container.getId().toString(),
                              getClusterInfoPropertyValue(OptionKeys.APPLICATION_NAME)));
  }

  /**
   * Reads and sets the heartbeat monitoring interval. If bad value is provided then log it and set to default.
   * @param instanceDefinition
   */
  private void readAndSetHeartbeatMonitoringInterval(AggregateConf instanceDefinition) {
    String hbMonitorInterval = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getOption(AgentKeys.HEARTBEAT_MONITOR_INTERVAL,
                                     Integer.toString(DEFAULT_HEARTBEAT_MONITOR_INTERVAL));
    try {
      setHeartbeatMonitorInterval(Integer.parseInt(hbMonitorInterval));
    }catch (NumberFormatException e) {
      log.warn(
          "Bad value {} for {}. Defaulting to ",
          hbMonitorInterval,
          HEARTBEAT_MONITOR_INTERVAL,
          DEFAULT_HEARTBEAT_MONITOR_INTERVAL);
    }
  }

  /**
   * Reads and sets the heartbeat monitoring interval. If bad value is provided then log it and set to default.
   * @param instanceDefinition
   */
  private void initializeAgentDebugCommands(AggregateConf instanceDefinition) {
    String launchParameterStr = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getOption(AgentKeys.AGENT_INSTANCE_DEBUG_DATA, "");
    agentLaunchParameter = new AgentLaunchParameter(launchParameterStr);
  }

  @VisibleForTesting
  protected Metainfo getMetainfo() {
    return this.metainfo;
  }

  @VisibleForTesting
  protected Map<String, ComponentInstanceState> getComponentStatuses() {
    return componentStatuses;
  }

  @VisibleForTesting
  protected Metainfo getApplicationMetainfo(SliderFileSystem fileSystem,
                                            String appDef) throws IOException {
    return AgentUtils.getApplicationMetainfo(fileSystem, appDef);
  }

  @VisibleForTesting
  protected void setHeartbeatMonitorInterval(int heartbeatMonitorInterval) {
    this.heartbeatMonitorInterval = heartbeatMonitorInterval;
  }

  private int getHeartbeatMonitorInterval() {
    return this.heartbeatMonitorInterval;
  }

  /**
   * Publish a named config bag that may contain name-value pairs for app configurations such as hbase-site
   * @param name
   * @param description
   * @param entries
   */
  protected void publishComponentConfiguration(String name, String description,
                                               Iterable<Map.Entry<String, String>> entries) {
    PublishedConfiguration pubconf = new PublishedConfiguration();
    pubconf.description = description;
    pubconf.putValues(entries);
    log.info("publishing {}", pubconf);
    getAmState().getPublishedSliderConfigurations().put(name, pubconf);
  }

  /**
   * Get a list of all hosts for all role/container per role
   * @return
   */
  protected Map<String, Map<String, ClusterNode>> getRoleClusterNodeMapping() {
    amState.refreshClusterStatus();
    return (Map<String, Map<String, ClusterNode>>)
        amState.getClusterStatus().status.get(
            ClusterDescriptionKeys.KEY_CLUSTER_LIVE);
  }

  private String getContainerLabel(Container container, String role) {
    return container.getId().toString() + LABEL_MAKER + role;
  }

  protected String getClusterInfoPropertyValue(String name) {
    StateAccessForProviders accessor = getAmState();
    assert accessor.isApplicationLive();
    ClusterDescription description = accessor.getClusterStatus();
    return description.getInfo(name);
  }

  /**
   * Lost heartbeat from the container - release it and ask for a replacement
   *
   * @param label
   *
   * @return if release is requested successfully
   */
  protected boolean releaseContainer(String label) {
    componentStatuses.remove(label);
    try {
      getAppMaster().refreshContainer(getContainerId(label), true);
    } catch (SliderException e) {
      log.info("Error while requesting container release for {}. Message: {}", label, e.getMessage());
      return false;
    }

    return true;
  }

  /**
   * Run this service
   *
   * @param instanceDefinition component description
   * @param confDir            local dir with the config
   * @param env                environment variables above those generated by
   * @param execInProgress     callback for the event notification
   *
   * @throws IOException     IO problems
   * @throws SliderException anything internal
   */
  @Override
  public boolean exec(AggregateConf instanceDefinition,
                      File confDir,
                      Map<String, String> env,
                      ProviderCompleted execInProgress) throws
      IOException,
      SliderException {

    return false;
  }

  /**
   * Build the provider status, can be empty
   *
   * @return the provider status - map of entries to add to the info section
   */
  public Map<String, String> buildProviderStatus() {
    Map<String, String> stats = new HashMap<>();
    return stats;
  }

  @Override
  public boolean isSupportedRole(String role) {
    return true;
  }

  /**
   * Handle registration calls from the agents
   * @param registration
   * @return
   */
  @Override
  public RegistrationResponse handleRegistration(Register registration) {
    RegistrationResponse response = new RegistrationResponse();
    String label = registration.getHostname();
    if (componentStatuses.containsKey(label)) {
      response.setResponseStatus(RegistrationStatus.OK);
      componentStatuses.get(label).setLastHeartbeat(System.currentTimeMillis());
    } else {
      response.setResponseStatus(RegistrationStatus.FAILED);
      response.setLog("Label not recognized.");
    }
    return response;
  }

  /**
   * Handle heartbeat response from agents
   * @param heartBeat
   * @return
   */
  @Override
  public HeartBeatResponse handleHeartBeat(HeartBeat heartBeat) {
    HeartBeatResponse response = new HeartBeatResponse();
    long id = heartBeat.getResponseId();
    response.setResponseId(id + 1L);

    String label = heartBeat.getHostname();
    String roleName = getRoleName(label);

    String containerId = getContainerId(label);
    StateAccessForProviders accessor = getAmState();
    String scriptPath = getScriptPathFromMetainfo(roleName);

    if (scriptPath == null) {
      log.error("role.script is unavailable for " + roleName + ". Commands will not be sent.");
      return response;
    }

    if (!componentStatuses.containsKey(label)) {
      return response;
    }

    Boolean isMaster = isMaster(roleName);
    ComponentInstanceState componentStatus = componentStatuses.get(label);
    componentStatus.setLastHeartbeat(System.currentTimeMillis());
    // If no Master can explicitly publish then publish if its a master
    // Otherwise, wait till the master that can publish is ready
    if (isMaster &&
        (canAnyMasterPublishConfig() == false || canPublishConfig(roleName))) {
      processReturnedStatus(heartBeat, componentStatus);
    }

    List<CommandReport> reports = heartBeat.getReports();
    if (reports != null && !reports.isEmpty()) {
      CommandReport report = reports.get(0);
      Map<String, String> ports = report.getAllocatedPorts();
      if (ports != null && !ports.isEmpty()) {
        for (Map.Entry<String, String> port : ports.entrySet()) {
          log.info("Recording allocated port for {} as {}", port.getKey(), port.getValue());
          this.allocatedPorts.put(port.getKey(), port.getValue());
        }
      }
      CommandResult result = CommandResult.getCommandResult(report.getStatus());
      Command command = Command.getCommand(report.getRoleCommand());
      componentStatus.applyCommandResult(result, command);
      log.info("Component operation. Status: {}", result);

      if (command == Command.INSTALL && report.getFolders() != null && report.getFolders().size() > 0) {
        processFolderPaths(report.getFolders(), containerId, heartBeat.getFqdn());
      }
    }

    int waitForCount = accessor.getInstanceDefinitionSnapshot().
        getAppConfOperations().getComponentOptInt(roleName, AgentKeys.WAIT_HEARTBEAT, 0);

    if (id < waitForCount) {
      log.info("Waiting until heartbeat count {}. Current val: {}", waitForCount, id);
      componentStatuses.put(roleName, componentStatus);
      return response;
    }

    Command command = componentStatus.getNextCommand();
    try {
      if (Command.NOP != command) {
        if (command == Command.INSTALL) {
          log.info("Installing {} on {}.", roleName, containerId);
          addInstallCommand(roleName, containerId, response, scriptPath);
          componentStatus.commandIssued(command);
        } else if (command == Command.START) {
          // check against dependencies
          boolean canExecute = commandOrder.canExecute(roleName, command, componentStatuses.values());
          if (canExecute) {
            log.info("Starting {} on {}.", roleName, containerId);
            addStartCommand(roleName, containerId, response, scriptPath);
            componentStatus.commandIssued(command);
          } else {
            log.info("Start of {} on {} delayed as dependencies have not started.", roleName, containerId);
          }
        }
      }
      // if there is no outstanding command then retrieve config
      if (isMaster && componentStatus.getState() == State.STARTED
          && command == Command.NOP) {
        if (!componentStatus.getConfigReported()) {
          addGetConfigCommand(roleName, containerId, response);
        }
      }
    } catch (SliderException e) {
      componentStatus.applyCommandResult(CommandResult.FAILED, command);
      log.warn("Component instance failed operation.", e);
    }

    return response;
  }

  /**
   * Format the folder locations before publishing in the registry service
   * @param folders
   * @param containerId
   * @param hostFqdn
   */
  private void processFolderPaths(Map<String, String> folders, String containerId, String hostFqdn) {
    for (String key : folders.keySet()) {
      workFolders.put(String.format("%s-%s-%s", hostFqdn, containerId, key), folders.get(key));
    }

    publishComponentConfiguration(LOG_FOLDERS_TAG, LOG_FOLDERS_TAG, (new HashMap<>(this.workFolders)).entrySet());
  }

  /**
   * Process return status for component instances
   * @param heartBeat
   * @param componentStatus
   */
  protected void processReturnedStatus(HeartBeat heartBeat, ComponentInstanceState componentStatus) {
    List<ComponentStatus> statuses = heartBeat.getComponentStatus();
    if (statuses != null && !statuses.isEmpty()) {
      log.info("Processing {} status reports.", statuses.size());
      for (ComponentStatus status : statuses) {
        log.info("Status report: " + status.toString());
        if (status.getConfigs() != null) {
          for (String key : status.getConfigs().keySet()) {
            Map<String, String> configs = status.getConfigs().get(key);
            publishComponentConfiguration(key, key, configs.entrySet());
          }

          Service service = getMetainfo().getServices().get(0);
          List<ExportGroup> exportGroups = service.getExportGroups();
          if (exportGroups != null && !exportGroups.isEmpty()) {

            String configKeyFormat = "${site.%s.%s}";
            String hostKeyFormat = "${%s_HOST}";

            // publish export groups if any
            Map<String, String> replaceTokens = new HashMap<>();
            for (Map.Entry<String, Map<String, ClusterNode>> entry : getRoleClusterNodeMapping().entrySet()) {
              String hostName = getHostsList(entry.getValue().values(), true).iterator().next();
              replaceTokens.put(String.format(hostKeyFormat, entry.getKey().toUpperCase(Locale.ENGLISH)), hostName);
            }

            for (String key : status.getConfigs().keySet()) {
              Map<String, String> configs = status.getConfigs().get(key);
              for (String configKey : configs.keySet()) {
                String lookupKey = String.format(configKeyFormat, key, configKey);
                replaceTokens.put(lookupKey, configs.get(configKey));
              }
            }

            for (ExportGroup exportGroup : exportGroups) {
              List<Export> exports = exportGroup.getExports();
              if (exports != null && !exports.isEmpty()) {
                String exportGroupName = exportGroup.getName();
                Map<String, String> map = new HashMap<>();
                for (Export export : exports) {
                  String value = export.getValue();
                  // replace host names
                  for (String token : replaceTokens.keySet()) {
                    if (value.contains(token)) {
                      value = value.replace(token, replaceTokens.get(token));
                    }
                  }
                  map.put(export.getName(), value);
                  log.info("Preparing to publish. Key {} and Value {}", export.getName(), value);
                }
                publishComponentConfiguration(exportGroupName, exportGroupName, map.entrySet());
              }
            }
          }
          componentStatus.setConfigReported(true);
        }
      }
    }
  }

  /**
   * Extract script path from the application metainfo
   * @param roleName
   * @return
   */
  protected String getScriptPathFromMetainfo(String roleName) {
    String scriptPath = null;
    List<Service> services = getMetainfo().getServices();
    if (services.size() != 1) {
      log.error("Malformed app definition: Expect only one service in the metainfo.xml");
    }
    Service service = services.get(0);
    for (Component component : service.getComponents()) {
      if (component.getName().equals(roleName)) {
        scriptPath = component.getCommandScript().getScript();
        break;
      }
    }
    return scriptPath;
  }

  /**
   * Is the role of type MASTER
   * @param roleName
   * @return
   */
  protected boolean isMaster(String roleName) {
    List<Service> services = getMetainfo().getServices();
    if (services.size() != 1) {
      log.error("Malformed app definition: Expect only one service in the metainfo.xml");
    } else {
      Service service = services.get(0);
      for (Component component : service.getComponents()) {
        if (component.getName().equals(roleName)) {
          if (component.getCategory().equals("MASTER")) {
            return true;
          } else {
            return false;
          }
        }
      }
    }
    return false;
  }

  /**
   * Can the role publish configuration
   * @param roleName
   * @return
   */
  protected boolean canPublishConfig(String roleName) {
    List<Service> services = getMetainfo().getServices();
    if (services.size() != 1) {
      log.error("Malformed app definition: Expect only one service in the metainfo.xml");
    } else {
      Service service = services.get(0);
      for (Component component : service.getComponents()) {
        if (component.getName().equals(roleName)) {
          return Boolean.TRUE.toString().equals(component.getPublishConfig());
        }
      }
    }
    return false;
  }

  /**
   * Can any master publish config explicitly, if not a random master is used
   * @return
   */
  protected boolean canAnyMasterPublishConfig() {
    if (canAnyMasterPublish == null) {
      List<Service> services = getMetainfo().getServices();
      if (services.size() != 1) {
        log.error("Malformed app definition: Expect only one service in the metainfo.xml");
      } else {
        Service service = services.get(0);
        for (Component component : service.getComponents()) {
          if (Boolean.TRUE.toString().equals(component.getPublishConfig()) &&
              component.getCategory().equals("MASTER")) {
            canAnyMasterPublish = true;
          }
        }
      }
    }

    if (canAnyMasterPublish == null) {
      canAnyMasterPublish = false;
    }
    return canAnyMasterPublish;
  }

  private String getRoleName(String label) {
    return label.substring(label.indexOf(LABEL_MAKER) + LABEL_MAKER.length());
  }

  private String getContainerId(String label) {
    return label.substring(0, label.indexOf(LABEL_MAKER));
  }

  /**
   * Add install command to the heartbeat response
   * @param roleName
   * @param containerId
   * @param response
   * @param scriptPath
   * @throws SliderException
   */
  @VisibleForTesting
  protected void addInstallCommand(String roleName, String containerId, HeartBeatResponse response, String scriptPath)
      throws SliderException {
    assert getAmState().isApplicationLive();
    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();
    ConfTreeOperations resourcesConf = getAmState().getResourcesSnapshot();
    ConfTreeOperations internalsConf = getAmState().getInternalsSnapshot();

    ExecutionCommand cmd = new ExecutionCommand(AgentCommandType.EXECUTION_COMMAND);
    prepareExecutionCommand(cmd);
    String clusterName = internalsConf.get(OptionKeys.APPLICATION_NAME);
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(Command.INSTALL.toString());
    cmd.setServiceName(clusterName);
    cmd.setComponentName(roleName);
    cmd.setRole(roleName);
    Map<String, String> hostLevelParams = new TreeMap<>();
    hostLevelParams.put(JAVA_HOME, appConf.getGlobalOptions().getMandatoryOption(JAVA_HOME));
    hostLevelParams.put(PACKAGE_LIST, "[{\"type\":\"tarball\",\"name\":\"" +
                                      appConf.getGlobalOptions().getMandatoryOption(
                                          PACKAGE_LIST) + "\"}]");
    hostLevelParams.put(CONTAINER_ID, containerId);
    cmd.setHostLevelParams(hostLevelParams);

    setInstallCommandConfigurations(cmd);

    cmd.setCommandParams(setCommandParameters(scriptPath, false));

    cmd.setHostname(getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME));
    response.addExecutionCommand(cmd);
  }

  private void prepareExecutionCommand(ExecutionCommand cmd) {
    cmd.setTaskId(taskId.incrementAndGet());
    cmd.setCommandId(cmd.getTaskId() + "-1");
  }

  private Map<String, String> setCommandParameters(String scriptPath, boolean recordConfig) {
    Map<String, String> cmdParams = new TreeMap<>();
    cmdParams.put("service_package_folder",
                  "${AGENT_WORK_ROOT}/work/app/definition/package");
    cmdParams.put("script", scriptPath);
    cmdParams.put("schema_version", "2.0");
    cmdParams.put("command_timeout", "300");
    cmdParams.put("script_type", "PYTHON");
    cmdParams.put("record_config", Boolean.toString(recordConfig));
    return cmdParams;
  }

  private void setInstallCommandConfigurations(ExecutionCommand cmd) throws SliderException {
    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();
    Map<String, Map<String, String>> configurations = buildCommandConfigurations(appConf);
    cmd.setConfigurations(configurations);
  }

  @VisibleForTesting
  protected void addStatusCommand(String roleName, String containerId, HeartBeatResponse response, String scriptPath)
      throws SliderException {
    assert getAmState().isApplicationLive();
    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();
    ConfTreeOperations internalsConf = getAmState().getInternalsSnapshot();

    StatusCommand cmd = new StatusCommand();
    String clusterName = internalsConf.get(OptionKeys.APPLICATION_NAME);

    cmd.setCommandType(AgentCommandType.STATUS_COMMAND);
    cmd.setComponentName(roleName);
    cmd.setServiceName(clusterName);
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(StatusCommand.STATUS_COMMAND);

    Map<String, String> hostLevelParams = new TreeMap<>();
    hostLevelParams.put(JAVA_HOME, appConf.getGlobalOptions().getMandatoryOption(JAVA_HOME));
    hostLevelParams.put(CONTAINER_ID, containerId);
    cmd.setHostLevelParams(hostLevelParams);

    cmd.setCommandParams(setCommandParameters(scriptPath, false));

    Map<String, Map<String, String>> configurations = buildCommandConfigurations(appConf);

    cmd.setConfigurations(configurations);

    response.addStatusCommand(cmd);
  }

  @VisibleForTesting
  protected void addGetConfigCommand(String roleName, String containerId, HeartBeatResponse response)
      throws SliderException {
    assert getAmState().isApplicationLive();
    ConfTreeOperations internalsConf = getAmState().getInternalsSnapshot();

    StatusCommand cmd = new StatusCommand();
    String clusterName = internalsConf.get(OptionKeys.APPLICATION_NAME);

    cmd.setCommandType(AgentCommandType.STATUS_COMMAND);
    cmd.setComponentName(roleName);
    cmd.setServiceName(clusterName);
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(StatusCommand.GET_CONFIG_COMMAND);
    Map<String, String> hostLevelParams = new TreeMap<>();
    hostLevelParams.put(CONTAINER_ID, containerId);
    cmd.setHostLevelParams(hostLevelParams);

    hostLevelParams.put(CONTAINER_ID, containerId);

    response.addStatusCommand(cmd);
  }

  @VisibleForTesting
  protected void addStartCommand(String roleName, String containerId, HeartBeatResponse response, String scriptPath)
      throws
      SliderException {
    assert getAmState().isApplicationLive();
    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();
    ConfTreeOperations internalsConf = getAmState().getInternalsSnapshot();

    ExecutionCommand cmd = new ExecutionCommand(AgentCommandType.EXECUTION_COMMAND);
    prepareExecutionCommand(cmd);
    String clusterName = internalsConf.get(OptionKeys.APPLICATION_NAME);
    String hostName = getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME);
    cmd.setHostname(hostName);
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(Command.START.toString());
    cmd.setServiceName(clusterName);
    cmd.setComponentName(roleName);
    cmd.setRole(roleName);
    Map<String, String> hostLevelParams = new TreeMap<>();
    hostLevelParams.put(JAVA_HOME, appConf.getGlobalOptions().getMandatoryOption(JAVA_HOME));
    hostLevelParams.put(CONTAINER_ID, containerId);
    cmd.setHostLevelParams(hostLevelParams);

    cmd.setCommandParams(setCommandParameters(scriptPath, true));

    Map<String, Map<String, String>> configurations = buildCommandConfigurations(appConf);

    cmd.setConfigurations(configurations);
    response.addExecutionCommand(cmd);
  }

  protected Map<String, String> getAllocatedPorts() {
    return this.allocatedPorts;
  }

  private Map<String, Map<String, String>> buildCommandConfigurations(ConfTreeOperations appConf)
      throws SliderException {

    Map<String, Map<String, String>> configurations = new TreeMap<>();
    Map<String, String> tokens = getStandardTokenMap(appConf);

    List<String> configs = getApplicationConfigurationTypes(appConf);

    //Add global
    for (String configType : configs) {
      addNamedConfiguration(configType, appConf.getGlobalOptions().options,
                            configurations, tokens);
    }

    return configurations;
  }

  private Map<String, String> getStandardTokenMap(ConfTreeOperations appConf) throws SliderException {
    Map<String, String> tokens = new HashMap<>();
    String nnuri = appConf.get("site.fs.defaultFS");
    tokens.put("${NN_URI}", nnuri);
    tokens.put("${NN_HOST}", URI.create(nnuri).getHost());
    tokens.put("${ZK_HOST}", appConf.get(OptionKeys.ZOOKEEPER_HOSTS));
    tokens.put("${DEFAULT_DATA_DIR}", getAmState()
        .getInternalsSnapshot()
        .getGlobalOptions()
        .getMandatoryOption(OptionKeys.INTERNAL_DATA_DIR_PATH));
    return tokens;
  }

  private List<String> getApplicationConfigurationTypes(ConfTreeOperations appConf) {
    // for now, reading this from appConf.  In the future, modify this method to
    // process metainfo.xml
    List<String> configList = new ArrayList<>();
    configList.add(GLOBAL_CONFIG_TAG);

    String configTypes = appConf.get("config_types");
    String[] configs = configTypes.split(",");

    configList.addAll(Arrays.asList(configs));

    // remove duplicates.  mostly worried about 'global' being listed
    return new ArrayList<>(new HashSet<>(configList));
  }

  private void addNamedConfiguration(String configName, Map<String, String> sourceConfig,
                                     Map<String, Map<String, String>> configurations,
                                     Map<String, String> tokens) {
    Map<String, String> config = new HashMap<>();
    if (configName.equals(GLOBAL_CONFIG_TAG)) {
      addDefaultGlobalConfig(config);
    }
    // add role hosts to tokens
    addRoleRelatedTokens(tokens);
    providerUtils.propagateSiteOptions(sourceConfig, config, configName, tokens);

    //apply any port updates
    if (!this.getAllocatedPorts().isEmpty()) {
      for (String key : config.keySet()) {
        if (this.getAllocatedPorts().containsKey(key)) {
          config.put(key, getAllocatedPorts().get(key));
        }
      }
    }
    configurations.put(configName, config);
  }

  protected void addRoleRelatedTokens(Map<String, String> tokens) {
    for (Map.Entry<String, Map<String, ClusterNode>> entry : getRoleClusterNodeMapping().entrySet()) {
      String tokenName = entry.getKey().toUpperCase(Locale.ENGLISH) + "_HOST";
      String hosts = StringUtils.join(",", getHostsList(entry.getValue().values(), true));
      tokens.put("${" + tokenName + "}", hosts);
    }
  }

  private Iterable<String> getHostsList(Collection<ClusterNode> values,
                                        boolean hostOnly) {
    List<String> hosts = new ArrayList<>();
    for (ClusterNode cn : values) {
      hosts.add(hostOnly ? cn.host : cn.host + "/" + cn.name);
    }

    return hosts;
  }

  private void addDefaultGlobalConfig(Map<String, String> config) {
    config.put("app_log_dir", "${AGENT_LOG_ROOT}/app/log");
    config.put("app_pid_dir", "${AGENT_WORK_ROOT}/app/run");
    config.put("app_install_dir", "${AGENT_WORK_ROOT}/app/install");
  }

  @Override
  public Map<String, String> buildMonitorDetails(ClusterDescription clusterDesc) {
    Map<String, String> details = super.buildMonitorDetails(clusterDesc);
    buildRoleHostDetails(details);
    return details;
  }

  private void buildRoleHostDetails(Map<String, String> details) {
    for (Map.Entry<String, Map<String, ClusterNode>> entry :
        getRoleClusterNodeMapping().entrySet()) {
      details.put(entry.getKey() + " Host(s)/Container(s): " +
                  getHostsList(entry.getValue().values(), false),
                  "");
    }
  }

  @Override
  public void applyInitialRegistryDefinitions(URL amWebAPI,
                                              ServiceInstanceData instanceData) throws IOException {
    super.applyInitialRegistryDefinitions(amWebAPI, instanceData);

    try {
      instanceData.internalView.endpoints.put(
          CustomRegistryConstants.AGENT_REST_API,
          new RegisteredEndpoint(
              new URL(amWebAPI, SLIDER_PATH_AGENTS),
              "Agent REST API"));
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }

  }
}
