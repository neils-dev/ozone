/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.scm.ha.io;

import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.bouncycastle.asn1.x509.CRLReason;

import java.math.BigInteger;

public class CRLReasonCodec implements Codec {
  @Override
  public ByteString serialize(Object object)
      throws InvalidProtocolBufferException {
    try {
      BigInteger a = ((CRLReason) object).getValue();
      return ByteString.copyFrom(a.toByteArray());
    } catch (Exception ex) {
      throw new InvalidProtocolBufferException(
          "CRLReason cannot be decoded: " + ex.getMessage());
    }
  }

  @Override
  public Object deserialize(Class<?> type, ByteString value)
      throws InvalidProtocolBufferException {
    try {
      return CRLReason.lookup(new BigInteger(value.toByteArray()).intValue());
    } catch (Exception ex) {
      throw new InvalidProtocolBufferException(
          "CRLReason cannot be decoded: " + ex.getMessage());}
  }
}
