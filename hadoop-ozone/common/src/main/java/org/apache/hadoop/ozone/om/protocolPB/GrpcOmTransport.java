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
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.grpc.Status;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.netty.GrpcSslContexts;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys
    .OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH;
import static org.apache.hadoop.ozone.om.OMConfigKeys
    .OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT;
import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
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

  private String host = "om";
  private int port = 8981;
  private int maxSize;
  private boolean secureConnection = false;

  public GrpcOmTransport(ConfigurationSource conf,
                          UserGroupInformation ugi, String omServiceId)
      throws IOException {
    Optional<String> omHost = getHostNameFromConfigKeys(conf,
        OZONE_OM_ADDRESS_KEY);
    this.host = omHost.orElse("0.0.0.0");
    this.secureConnection = OzoneSecurityUtil.isSecurityEnabled(conf);
    port = conf.getObject(GrpcOmTransportConfig.class).getPort();

    maxSize = conf.getInt(OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH,
        OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT);

    start();
  }

  public void start() {
    if (!isRunning.compareAndSet(false, true)) {
      LOG.info("Ignore. already started.");
      return;
    }
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .maxInboundMessageSize(maxSize);

    if (secureConnection) {
      try {
        SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE);
        channelBuilder.useTransportSecurity().
            sslContext(sslContextBuilder.build());
        LOG.info("******* Grpc Created TLS client connection *******");
      } catch (Exception ex) {
        LOG.error("cannot establish TLS for grpc om transport client");
      }
    } else {
      channelBuilder.usePlaintext();
    }
    channel = channelBuilder.build();
    client = OzoneManagerServiceGrpc.newBlockingStub(channel);

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
    OMResponse resp = null;
    try {
      resp = client.submitRequest(payload);
    } catch (io.grpc.StatusRuntimeException e) {
      ResultCodes resultCode = ResultCodes.INTERNAL_ERROR;
      if (e.getStatus().getCode() == Status.Code.UNAVAILABLE) {
        resultCode = ResultCodes.TIMEOUT;
      }
      throw new OMException(e.getCause(), resultCode);
    }
    return resp;
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
