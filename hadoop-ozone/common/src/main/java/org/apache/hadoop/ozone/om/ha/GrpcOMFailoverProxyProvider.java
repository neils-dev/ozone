/**
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
package org.apache.hadoop.ozone.om.ha;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.protocolPB.GrpcOmTransport;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.*;

import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.ozone.om.OMConfigKeys.*;

public class GrpcOMFailoverProxyProvider<T> extends
    OMFailoverProxyProvider<T> {

  private Map<String, String> omAddresses;

  public GrpcOMFailoverProxyProvider(ConfigurationSource configuration,
                                     UserGroupInformation ugi,
                                     String omServiceId,
                                     Class<T> protocol) throws IOException {
    super(configuration, ugi, omServiceId, protocol);
  }

  public static OptionalInt getNumberFromConfigKeys(
      ConfigurationSource conf, String... keys) {
    for (final String key : keys) {
      final String value = conf.getTrimmed(key);
      if (value != null) {
        return OptionalInt.of(Integer.parseInt(value));
      }
    }
    return OptionalInt.empty();
  }

  @Override
  protected void loadOMClientConfigs(ConfigurationSource config, String omSvcId)
    throws IOException {
    Map omProxies = new HashMap<>();
    Map omProxyInfos = new HashMap<>();
    List omNodeIDList = new ArrayList<>();
    omAddresses = new HashMap<>();

    Collection<String> omServiceIds = Collections.singletonList(omSvcId);

    for (String serviceId : OmUtils.emptyAsSingletonNull(omServiceIds)) {
      Collection<String> omNodeIds = OmUtils.getOMNodeIds(config, serviceId);

      for (String nodeId : OmUtils.emptyAsSingletonNull(omNodeIds)) {

        String rpcAddrKey = ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
            serviceId, nodeId);

        Optional<String> hostaddr = getHostNameFromConfigKeys(config,
            rpcAddrKey);

        OptionalInt hostport = getNumberFromConfigKeys(config,
            ConfUtils.addKeySuffixes(OMConfigKeys.OZONE_OM_GRPC_PORT_KEY,
                serviceId, nodeId),
            OMConfigKeys.OZONE_OM_GRPC_PORT_KEY);
        if (nodeId == null) {
          nodeId = OzoneConsts.OM_DEFAULT_NODE_ID;
        }
        if (nodeId == null) {
            nodeId = OzoneConsts.OM_DEFAULT_NODE_ID;
        }
        omProxies.put(nodeId, null);
        if (hostaddr.isPresent()) {
          omAddresses.put(nodeId,
              hostaddr.get() + ":"
                  + hostport.orElse(config
                  .getObject(GrpcOmTransport
                      .GrpcOmTransportConfig.class)
                  .getPort()));
        } else {
          omAddresses.put(nodeId,
              "0.0.0.0:" + getNumberFromConfigKeys(config,
                  OMConfigKeys.OZONE_OM_GRPC_PORT_KEY));
        }
        //omProxyInfos.put(nodeId, null);
        omNodeIDList.add(nodeId);
      }
    }

    if (omProxyInfos.size() == 0) {
      omProxyInfos.put(OzoneConsts.OM_DEFAULT_NODE_ID,
          "0.0.0.0:" + getNumberFromConfigKeys(config,
              OMConfigKeys.OZONE_OM_GRPC_PORT_KEY));
      omNodeIDList.add(OzoneConsts.OM_DEFAULT_NODE_ID);
    }
    if (omProxies.isEmpty()) {
      throw new IllegalArgumentException("Could not find any configured " +
          "addresses for OM. Please configure the system with "
          + OZONE_OM_ADDRESS_KEY);
    }

    setOmProxies(omProxies);
    setOmProxyInfos(omProxyInfos);
    setOmNodeIDList(omNodeIDList);
  }

  @Override
  protected Text computeDelegationTokenService() {
    return new Text();
  }

  // need to throw if nodeID not in omAddresses
  public String getGrpcProxyAddress(String nodeId) {
    return omAddresses.get(nodeId);
  }

  public List<String> getGrpcOmNodeIDList() {
    return getOmNodeIDList();
  }
}
