/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.security.OzoneDelegationTokenSecretManager;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.netty.GrpcSslContexts;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_PROVIDER;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_PROVIDER_DEFAULT;

/**
 * Separated network server for gRPC transport OzoneManagerService s3g->OM.
 */
public class GrpcOzoneManagerServer {
  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcOzoneManagerServer.class);

  private Server server;
  private int port = 8981;

  public GrpcOzoneManagerServer(OzoneConfiguration config,
                                OzoneManagerProtocolServerSideTranslatorPB
                                    omTranslator,
                                OzoneDelegationTokenSecretManager
                                    delegationTokenMgr,
                                CertificateClient caClient) {
    this.port = config.getObject(
        GrpcOzoneManagerServerConfig.class).
        getPort();
    init(omTranslator,
        delegationTokenMgr,
        config,
        caClient);
  }

  public void init(OzoneManagerProtocolServerSideTranslatorPB omTranslator,
                   OzoneDelegationTokenSecretManager delegationTokenMgr,
                   OzoneConfiguration omServerConfig,
                   CertificateClient caClient) {
    NettyServerBuilder nettyServerBuilder = NettyServerBuilder.forPort(port)
        .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE)
        .addService(new OzoneManagerServiceGrpc(omTranslator,
            delegationTokenMgr,
            omServerConfig));

    if (OzoneSecurityUtil.isSecurityEnabled(omServerConfig)) {
      try {
        SecurityConfig secConf = new SecurityConfig(omServerConfig);
        SslContextBuilder sslClientContextBuilder = SslContextBuilder.forServer(
            caClient.getPrivateKey(), caClient.getCertificate());
        SslContextBuilder sslContextBuilder = GrpcSslContexts.configure(
            sslClientContextBuilder,
            SslProvider.valueOf(omServerConfig.get(HDDS_GRPC_TLS_PROVIDER,
                HDDS_GRPC_TLS_PROVIDER_DEFAULT)));
        nettyServerBuilder.sslContext(sslContextBuilder.build());
        LOG.info("******* Grpc Created TLS server connection *******");
      } catch (Exception ex) {
        LOG.error("Unable to setup TLS for secure Om S3g GRPC channel.", ex);
      }
    }

    server = nettyServerBuilder.build();
  }

  public void start() throws IOException {
    server.start();
    LOG.info("{} is started using port {}", getClass().getSimpleName(),
        server.getPort());
    port = server.getPort();
  }

  public void stop() {
    try {
      server.shutdown().awaitTermination(10L, TimeUnit.SECONDS);
      LOG.info("Server {} is shutdown", getClass().getSimpleName());
    } catch (InterruptedException ex) {
      LOG.warn("{} couldn't be stopped gracefully", getClass().getSimpleName());
    }
  }

  public int getPort() {
    return port;
  }

  /**
   * GrpcOzoneManagerServer configuration in Java style configuration class.
   */
  @ConfigGroup(prefix = "ozone.om.grpc")
  public static final class GrpcOzoneManagerServerConfig {
    @Config(key = "port", defaultValue = "8981",
        description = "Port used for"
            + " the GrpcOmTransport OzoneManagerServiceGrpc server",
        tags = {ConfigTag.MANAGEMENT})
    private int port;

    public int getPort() {
      return port;
    }

    public GrpcOzoneManagerServerConfig setPort(int portParam) {
      this.port = portParam;
      return this;
    }
  }
}
