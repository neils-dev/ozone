/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.scm;

import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.RemoveScmResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmResponseProto;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.ozone.admin.scm.DecommissionScmSubcommand;
import org.apache.ozone.test.GenericTestUtils;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;

import picocli.CommandLine;
import org.mockito.Mockito;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;

/**
 * Unit tests to validate the TestScmDecommissionSubCommand class includes the
 * correct output when executed against a mock client.
 */
public class TestScmDecommissionSubcommand {

  @Test
  public void testScmDecommissionInputParams() throws Exception {
    // requires String <clusterId> and String <nodeId>
    DecommissionScmSubcommand cmd = new DecommissionScmSubcommand();
    ScmClient client = mock(ScmClient.class);
    OzoneAdmin admin = new OzoneAdmin();

    try (GenericTestUtils.SystemErrCapturer capture =
             new GenericTestUtils.SystemErrCapturer()) {
      String[] args = {"scm", "decommissionScm"};
      admin.execute(args);
      assertTrue(capture.getOutput().contains(
          "Usage: ozone admin scm decommissionScm"));
    }

    // now give required String <clusterId> and String <nodeId>
    CommandLine c1 = new CommandLine(cmd);
    String scmId = UUID.randomUUID().toString();
    c1.parseArgs("CID-" + UUID.randomUUID().toString(), scmId);
    RemoveScmResponseProto removeScmResponse =
        RemoveScmResponseProto.newBuilder()
        .setScmId(scmId)
        .setSuccess(true)
        .build();

    DecommissionScmResponseProto response =
        DecommissionScmResponseProto.newBuilder()
            .setRemoveScmResponse(removeScmResponse)
            .build();

    Mockito.when(client.decommissionScm(any()))
        .thenAnswer(invocation -> (
            response));

    try (GenericTestUtils.SystemOutCapturer capture =
             new GenericTestUtils.SystemOutCapturer()) {
      cmd.execute(client);
      assertTrue(capture.getOutput().contains(
          "CID-"));
    }
  }

  @Test
  public void testScmDecommissionScmRemoveErrors() throws Exception {
    // requires String <clusterId> and String <nodeId>
    DecommissionScmSubcommand cmd = new DecommissionScmSubcommand();
    ScmClient client = mock(ScmClient.class);

    CommandLine c1 = new CommandLine(cmd);
    String scmId = UUID.randomUUID().toString();
    c1.parseArgs("CID-" + UUID.randomUUID().toString(), scmId);
    RemoveScmResponseProto removeScmResponse =
        RemoveScmResponseProto.newBuilder()
        .setScmId(scmId)
        .setSuccess(false)
        .build();

    DecommissionScmResponseProto response =
        DecommissionScmResponseProto.newBuilder()
            .setRemoveScmResponse(removeScmResponse)
            .setRemoveScmError("Removal of primordial node is not supported")
            .build();

    Mockito.when(client.decommissionScm(any()))
        .thenAnswer(invocation -> (
            response));

    try (GenericTestUtils.SystemOutCapturer capture =
             new GenericTestUtils.SystemOutCapturer()) {
      cmd.execute(client);
      assertTrue(capture.getOutput().contains(
          "Removal of primordial"));
    }
  }

  // TO DO : test decommission revoke certificate
  @Test
  public void testScmDecommissionScmCertRevokeErrors() throws Exception {
  }

}
