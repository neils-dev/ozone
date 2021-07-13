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
package org.apache.hadoop.ozone.om.protocolPB;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.lang.reflect.Constructor;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProvider;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;

import static org.apache.hadoop.ozone.om.OMConfigKeys.*;
import static org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OMTokenProto.Type.S3AUTHINFO;
import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.Status;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
/**
 * Grpc transport for grpc between s3g and om.
 */
public class GrpcOmTransport implements OmTransport {
  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcOmTransport.class);

  private final OMFailoverProxyProvider omFailoverProxyProvider;
  private static final String CLIENT_NAME = "GrpcOmTransport";
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  // gRPC specific
  //private ManagedChannel channel;

  private OzoneManagerServiceGrpc.OzoneManagerServiceBlockingStub client;

  private Collection<String> omServiceIds;
  private List<String> omAddresses;
  private int hostIndex = 0;
  private Map<String,
        OzoneManagerServiceGrpc.OzoneManagerServiceBlockingStub> clients;
  private Map<String, ManagedChannel> channels;
  private int lastVisited = -1;
  private String host = "om";
  private int port = 8981;
  private ConfigurationSource conf;


  public GrpcOmTransport(ConfigurationSource conf,
                          UserGroupInformation ugi, String omServiceId)
      throws IOException {
    this.omFailoverProxyProvider = new OMFailoverProxyProvider(conf, ugi,
        omServiceId);
    // keeping mapping of hosts to channels (raw)
    // similarly, hosts to clients (connections)
    // connections Channels kept open throughout
    // the sevice time of the s3g; internally
    // io.grpc recovers from connection failures though
    // connection retries with backoff strategy
    this.channels = new HashMap<>();
    this.clients = new HashMap<>();
    this.conf = conf;
    start();
  }

  /* initializes hosts from configuration and returns single */
  /* single host from set */
  public String initHosts() {
    String localOMServiceId = null;
    localOMServiceId = conf.getTrimmed(OZONE_OM_INTERNAL_SERVICE_ID);
    omServiceIds = localOMServiceId == null ?
        conf.getTrimmedStringCollection(
            OZONE_OM_SERVICE_IDS_KEY) :
        Collections.singletonList(localOMServiceId);

    List<String> oms = new ArrayList<>();
    for (String serviceId : omServiceIds) {
      Collection<String> omNodeIds = OmUtils.getOMNodeIds(conf, serviceId);
      for (String nodeId : omNodeIds) {
        Optional<String> hostaddr = getHostNameFromConfigKeys(conf,
            ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
                serviceId, nodeId));
        if (hostaddr.isPresent()) {
          oms.add(hostaddr.get());
        }
      }
    }

    omAddresses = oms;
    String hostaddr;
    if (oms.size() == 0) {
      Optional<String> omHost = getHostNameFromConfigKeys(conf,
          OZONE_OM_ADDRESS_KEY);
      hostaddr = omHost.orElse("0.0.0.0");
      omAddresses.add(hostaddr);
    } else {
      hostaddr = omAddresses.get(0);
    }
    return hostaddr;
  }

  public void start() {
    if (!isRunning.compareAndSet(false, true)) {
      LOG.info("Ignore. already started.");
      return;
    }

    host = initHosts();
    port = conf.getObject(GrpcOmTransportConfig.class).getPort();

    for (String host : omAddresses) {
      NettyChannelBuilder channelBuilder =
          NettyChannelBuilder.forAddress(host, port)
              .usePlaintext()
              .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE);

      channels.put(host, channelBuilder.build());
      clients.put(host,
                  OzoneManagerServiceGrpc.newBlockingStub(channels.get(host)));
    }

    LOG.info("{}: started", CLIENT_NAME);
  }

  @Override
  @SuppressFBWarnings("DLS_DEAD_LOCAL_STORE")
  public OMResponse submitRequest(OMRequest payload) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    for (TokenIdentifier tid : ugi.
        getTokenIdentifiers()) {
      if (tid instanceof OzoneTokenIdentifier) {
        OzoneTokenIdentifier oti = (OzoneTokenIdentifier) tid;
        LOG.info(oti.toString());
        if (oti.getTokenType().equals(S3AUTHINFO)) {
          payload = OMRequest.newBuilder(payload)
              .setSignature(oti.getSignature())
              .setStringToSign(oti.getStrToSign())
              .setAwsAccessId(oti.getAwsAccessId())
              .setUserInfo(OzoneManagerProtocolProtos
                  .UserInfo.newBuilder()
                  .setUserName(ugi.getUserName()).build())
              .build();
        }
      }
    }
    LOG.debug("OMRequest {}", payload);
    OMResponse resp = null;
    boolean tryOtherHost = true;
    while (tryOtherHost) {
      tryOtherHost = false;
      try {
        resp = clients.get(host).submitRequest(payload);
      } catch (io.grpc.StatusRuntimeException e) {
        ResultCodes resultCode = ResultCodes.INTERNAL_ERROR;
        if (e.getStatus().getCode() == Status.Code.UNAVAILABLE) {
          resultCode = ResultCodes.TIMEOUT;
        }
        throw new OMException(e.getCause(), resultCode);
      }
      if (resp.getStatus() != OK) {
        // check if om leader,
        // if not tryOtherHost
        tryOtherHost = isNotOmLeader(resp);
      }
    }
    return resp;
  }

  @ConfigGroup(prefix = "ozone.om.protocolPB")
  public static final class GrpcOmTransportConfig {
    @Config(key = "port", defaultValue = "8981",
        description = "Port used for"
            + " the GrpcOmTransport OzoneManagerServiceGrpc server",
        tags = {ConfigTag.MANAGEMENT})
    private int port;

    public int getPort() {
      return port;
    }

    public GrpcOmTransportConfig setPort(int portParam) {
      this.port = portParam;
      return this;
    }
  }

  private boolean isNotOmLeader(OMResponse resp) {
    boolean isNotLeader = false;
    try {
      Class<?> realClass = Class.forName(resp.getMessage()
                                         .substring(0, resp.getMessage()
                                                    .indexOf(":")));
      Class<? extends IOException> cls = realClass
          .asSubclass(IOException.class);
      Constructor<? extends IOException> cn = cls.getConstructor(String.class);
      cn.setAccessible(true);
      IOException ex = cn.newInstance(resp.getMessage());
      LOG.info(ex.toString());
      if (ex instanceof OMNotLeaderException) {
        lastVisited = lastVisited == -1 ? hostIndex : lastVisited;
        hostIndex = (hostIndex + 1) % omAddresses.size();
        host = omAddresses.get(hostIndex);
        if (hostIndex != lastVisited) {
          isNotLeader = true;
        } else {
          lastVisited = -1;
        }
      }
    } catch (java.lang.NoSuchMethodException |
      java.lang.InstantiationException |
      java.lang.IllegalAccessException |
      java.lang.reflect.InvocationTargetException |
      java.lang.StringIndexOutOfBoundsException |
      java.lang.ClassNotFoundException e) {}

    return isNotLeader;
  }

  @Override
  public Text getDelegationTokenService() {
    return omFailoverProxyProvider.getCurrentProxyDelegationToken();
  }
  /**
   * Creates a {@link RetryProxy} encapsulating the
   * {@link OMFailoverProxyProvider}. The retry proxy fails over on network
   * exception or if the current proxy is not the leader OM.
   */
  private OzoneManagerProtocolPB createRetryProxy(
      OMFailoverProxyProvider failoverProxyProvider, int maxFailovers) {
    OzoneManagerProtocolPB proxy = (OzoneManagerProtocolPB) RetryProxy.create(
        OzoneManagerProtocolPB.class, failoverProxyProvider,
        failoverProxyProvider.getRetryPolicy(maxFailovers));
    return proxy;
  }

  @VisibleForTesting
  public OMFailoverProxyProvider getOmFailoverProxyProvider() {
    return omFailoverProxyProvider;
  }

  public void shutdown() {
    if (channels == null) {
      return;
    }
    channels.keySet().forEach(k -> {
      ManagedChannel channel = channels.get(k);
      channel.shutdown();
      try {
        channel.awaitTermination(5, TimeUnit.SECONDS);
      } catch (Exception e) {
        LOG.error("failed to shutdown OzoneManagerServiceGrpc channel", e);
      } finally {
        channel.shutdownNow();
      }
    });
    isRunning.set(false);
  }

  @Override
  public void close() throws IOException {
    omFailoverProxyProvider.close();
    shutdown();
  }
}


