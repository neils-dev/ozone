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
import org.apache.hadoop.ozone.admin.scm.ScmDecommissionSubcommand;
import org.apache.ozone.test.GenericTestUtils;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.mockito.Mockito;
import picocli.CommandLine;

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
    ScmDecommissionSubcommand cmd = new ScmDecommissionSubcommand();
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
    c1.parseArgs("CID-", "4070f47e-");
    RemoveScmResponseProto removeScmResponse = RemoveScmResponseProto.newBuilder()
        .setScmId("4070f47e")
        .setSuccess(true)
        .build();

    DecommissionScmResponseProto response =
        DecommissionScmResponseProto.newBuilder()
            .setRemoveScmResponse(removeScmResponse)
            .build();

    Mockito.when(client.decommissionScm(any(), any(), any()))
        .thenAnswer(invocation -> (
            response));

    try (GenericTestUtils.SystemOutCapturer capture =
             new GenericTestUtils.SystemOutCapturer()) {
      cmd.execute(client);
      assertTrue(capture.getOutput().contains(
          "CID-"));
    }
  }
}
