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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.net.HostAndPort;
import com.google.protobuf.ServiceException;
import io.grpc.Status;

import io.grpc.StatusRuntimeException;
import org.apache.hadoop.ipc.RemoteException;

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProvider;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;

import com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.om.ha.GrpcOMFailoverProxyProvider;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryInvocationHandler;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.ozone.om.OMConfigKeys.*;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
import static org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OMTokenProto.Type.S3AUTHINFO;

/**
 * Grpc transport for grpc between s3g and om.
 */
public class GrpcOmTransport implements OmTransport {
  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcOmTransport.class);

  private static final String CLIENT_NAME = "GrpcOmTransport";
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  // gRPC specific
  private ManagedChannel channel;

  private OzoneManagerServiceGrpc.OzoneManagerServiceBlockingStub client;
  private Map<String,
      OzoneManagerServiceGrpc.OzoneManagerServiceBlockingStub> clients;
  private Map<String, ManagedChannel> channels;
  private int lastVisited = -1;
  private ConfigurationSource conf;

  private String host = "om";
  private int port = 8981;
  private int maxSize;

  private List<String> oms;
  private RetryPolicy retryPolicy;
  private int failoverCount = 0;
  private Set<String> recoveredProxy;

  private GrpcOMFailoverProxyProvider omFailoverProxyProvider;

  public GrpcOmTransport(ConfigurationSource conf,
                          UserGroupInformation ugi, String omServiceId)
      throws IOException {
    Optional<String> omHost = getHostNameFromConfigKeys(conf,
        OZONE_OM_ADDRESS_KEY);
    this.host = omHost.orElse("0.0.0.0");

    this.channels = new HashMap<>();
    this.clients = new HashMap<>();
    this.conf = conf;
    this.recoveredProxy = new HashSet<>();


    port = conf.getObject(GrpcOmTransportConfig.class).getPort();

    maxSize = conf.getInt(OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH,
        OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT);

    omFailoverProxyProvider = new GrpcOMFailoverProxyProvider(
        conf,
        ugi,
        omServiceId,
        OzoneManagerProtocolPB.class);

    start();
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

  public void start() {
    if (!isRunning.compareAndSet(false, true)) {
      LOG.info("Ignore. already started.");
      return;
    }

    host = omFailoverProxyProvider
        .getGrpcProxyAddress(
            omFailoverProxyProvider.getCurrentProxyOMNodeId());

    List<String> nodes = omFailoverProxyProvider.getGrpcOmNodeIDList();
    for (String nodeId : nodes) {
      String hostaddr = omFailoverProxyProvider.getGrpcProxyAddress(nodeId);
      HostAndPort hp = HostAndPort.fromString(hostaddr);

      NettyChannelBuilder channelBuilder =
          NettyChannelBuilder.forAddress(hp.getHost(), hp.getPort())
              .usePlaintext()
              .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE);
      channels.put(hostaddr, channelBuilder.build());
      clients.put(hostaddr,
          OzoneManagerServiceGrpc
              .newBlockingStub(channels.get(hostaddr)));
    }
    int maxFailovers = conf.getInt(
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);

    retryPolicy = omFailoverProxyProvider.getRetryPolicy(maxFailovers);
    LOG.info("{}: started", CLIENT_NAME);
  }

  @Override
  public OMResponse submitRequest(OMRequest payload) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    final Collection<Token<? extends TokenIdentifier>> tokenS3Requests =
        ugi.getTokens();
    if (tokenS3Requests.size() > 1) {
      throw new OMException(ResultCodes.INVALID_TOKEN);
    }
    Iterator<Token<? extends TokenIdentifier>> tokenit =
        tokenS3Requests.iterator();
    if (tokenit.hasNext()) {
      OzoneTokenIdentifier oti = OzoneTokenIdentifier.newInstance();
      oti.readFields(new DataInputStream(
          new ByteArrayInputStream(tokenit.next().getIdentifier())));
      if (oti.getTokenType().equals(S3AUTHINFO)) {
        payload = OMRequest.newBuilder(payload)
            .setS3Authentication(
                OzoneManagerProtocolProtos
                    .S3Authentication.newBuilder()
                    .setSignature(oti.getSignature())
                    .setStringToSign(oti.getStrToSign())
                    .setAccessId(oti.getAwsAccessId()))
            .setUserInfo(OzoneManagerProtocolProtos
                .UserInfo.newBuilder()
                .setUserName(ugi.getUserName()).build())
            .build();
      }
    }
    LOG.debug("OMRequest {}", payload);
    OMResponse resp = null;
    boolean tryOtherHost = true;
    ResultCodes resultCode = ResultCodes.INTERNAL_ERROR;
    while (tryOtherHost) {
      tryOtherHost = false;
      try {
        resp = clients.get(host).submitRequest(payload);
      } catch (io.grpc.StatusRuntimeException e) {
        if (e.getStatus().getCode() == Status.Code.UNAVAILABLE) {
          resultCode = ResultCodes.TIMEOUT;
        }
        Exception exp = new Exception(e);
        if (exp.getCause() instanceof io.grpc.StatusRuntimeException) {
          io.grpc.StatusRuntimeException srexp = (io.grpc.StatusRuntimeException)exp.getCause();
          io.grpc.Status status = srexp.getStatus();
          LOG.info("** GRPC exception wrapped: "+status.getDescription());
        } else {
          LOG.info("***GRPC exception not StatusRuntimeException");
        }
        if (recoveredProxy.contains(omFailoverProxyProvider.getCurrentProxyOMNodeId())) {
          recoveredProxy.clear();
        }
        if (recoveredProxy.isEmpty()) {
          tryOtherHost = shouldRetry(unwrapException(exp));
          if (tryOtherHost == false) {
            throw new OMException(resultCode);
          }
        }
      }
    }
    recoveredProxy.add(omFailoverProxyProvider.getCurrentProxyOMNodeId());
    return resp;
  }

  private Exception unwrapException(Exception ex) {
    Exception grpcException = null;
    try {
      io.grpc.StatusRuntimeException srexp = (io.grpc.StatusRuntimeException)ex.getCause();
      io.grpc.Status status = srexp.getStatus();
      LOG.info("** GRPC exception wrapped: "+status.getDescription());
      Class<?> realClass = Class.forName(status.getDescription()
          .substring(0, status.getDescription()
              .indexOf(":")));
      Class<? extends Exception> cls = realClass
          .asSubclass(Exception.class);
      Constructor<? extends Exception> cn = cls.getConstructor(String.class);
      cn.setAccessible(true);
      grpcException = cn.newInstance(status.getDescription());
      IOException remote = null;
      try {
        String cause = status.getDescription();
        cause = cause.substring(cause.indexOf(":") + 2);
        remote = new RemoteException(cause.substring(0, cause.indexOf(":")),
            cause.substring(cause.indexOf(":")+1));
        grpcException.initCause(remote);
      } catch (Exception e) {
        LOG.error("cannot get cause for remote exception");
      }
    } catch (Exception e) {
      grpcException = new IOException(e);
      LOG.error("error unwrapping exception from OMResponse");
    }
    return grpcException;
  }

  private boolean shouldRetry(Exception ex) {
    boolean retry = false;
    RetryPolicy.RetryAction action = null;
    try {
      action = retryPolicy.shouldRetry((Exception)ex, 0, failoverCount++, true);
      LOG.info("failover retry action {}", action.action);
      if (action.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
        retry = false;
        LOG.error("Retry request failed. " + action.reason, ex);
      } else {
        if (action.action == RetryPolicy.RetryAction.RetryDecision.RETRY ||
            (action.action == RetryPolicy.RetryAction.RetryDecision.FAILOVER_AND_RETRY)) {
          if (action.delayMillis > 0) {
            try {
              Thread.sleep(action.delayMillis);
            } catch (Exception e) {
            }
          }
          host = omFailoverProxyProvider
              .getGrpcProxyAddress(
                  omFailoverProxyProvider.getCurrentProxyOMNodeId());
          retry = true;
        }
      }
    } catch (Exception e) {
      LOG.error("Failed failover exception {}", e);
    }
    return retry;
  }

  // stub implementation for interface
  @Override
  public Text getDelegationTokenService() {
    return new Text();
  }

  public void shutdown() {
    channel.shutdown();
    try {
      channel.awaitTermination(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.error("failed to shutdown OzoneManagerServiceGrpc channel", e);
    }
  }

  @Override
  public void close() throws IOException {
    shutdown();
  }

  /**
   * GrpcOmTransport configuration in Java style configuration class.
   */
  @ConfigGroup(prefix = "ozone.om.grpc")
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

  @VisibleForTesting
  public void startClient(ManagedChannel testChannel) {
    client = OzoneManagerServiceGrpc.newBlockingStub(testChannel);

    LOG.info("{}: started", CLIENT_NAME);
  }

}
