package org.apache.hadoop.ozone.s3;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import java.io.IOException;

/**
 * Cached ozone client for s3 requests.
 */
@ApplicationScoped
public final class OzoneClientCache {
  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneClientCache.class);
  // single, cached OzoneClient established on first connection
  // for s3g gRPC OmTransport, OmRequest - OmResponse channel
  private static OzoneClientCache instance;
  private OzoneClient client;

  private OzoneClientCache(String omServiceID,
                           OzoneConfiguration ozoneConfiguration)
      throws IOException {
    try {
      if (omServiceID == null) {
        client = OzoneClientFactory.getRpcClient(ozoneConfiguration);
      } else {
        // As in HA case, we need to pass om service ID.
        client = OzoneClientFactory.getRpcClient(omServiceID,
            ozoneConfiguration);
      }
    } catch (IOException e) {
      LOG.warn("cannot create OzoneClient");
      throw e;
    }
  }

  public static OzoneClient getOzoneClientInstance(String omServiceID,
                                                  OzoneConfiguration
                                                      ozoneConfiguration)
      throws IOException {
    if (instance == null) {
      instance = new OzoneClientCache(omServiceID, ozoneConfiguration);
    }
    return instance.client;
  }

  @PreDestroy
  public void destroy() throws IOException {
    client.close();
  }
}
