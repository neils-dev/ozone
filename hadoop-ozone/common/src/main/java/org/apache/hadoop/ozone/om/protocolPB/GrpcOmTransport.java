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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProvider;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;

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
  private ManagedChannel channel;

  private OzoneManagerServiceGrpc.OzoneManagerServiceBlockingStub client;

  private String host = "om";
  private int port = 8981;

  public GrpcOmTransport(ConfigurationSource conf,
                          UserGroupInformation ugi, String omServiceId)
      throws IOException {
    Optional<String> omHost = getHostNameFromConfigKeys(conf,
        OZONE_OM_ADDRESS_KEY);
    this.host = omHost.orElse("0.0.0.0");

    this.omFailoverProxyProvider = new OMFailoverProxyProvider(conf, ugi,
        omServiceId);
    port = conf.getObject(GrpcOmTransportConfig.class).getPort();

  }

  public void start() {
    if (!isRunning.compareAndSet(false, true)) {
      LOG.info("Ignore. already started.");
      return;
    }
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE);

    channel = channelBuilder.build();
    client = OzoneManagerServiceGrpc.newBlockingStub(channel);

    LOG.info("{}: started", CLIENT_NAME);
  }

  @Override
  public OMResponse submitRequest(OMRequest payload) throws IOException {
    return client.submitRequest(payload);
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
    channel.shutdown();
    try {
      channel.awaitTermination(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.error("failed to shutdown OzoneManagerServiceGrpc channel", e);
    }
  }

  @Override
  public void close() throws IOException {
    omFailoverProxyProvider.close();
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
