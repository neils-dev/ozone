/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha.io;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.bouncycastle.cert.X509CertificateHolder;

import java.security.PrivateKey;

public class CRLApproverCodec implements Codec {
  @Override
  public ByteString serialize(Object object)
      throws InvalidProtocolBufferException {
    SecurityConfig config;
    PrivateKey caPrivate;
    try {
      byte[] a = ((X509CertificateHolder) object).getEncoded();
      return com.google.protobuf.ByteString.copyFrom(a);

      /*String certString =
          CertificateCodec.getPEMEncodedString((X509Certificate) object);
      return ByteString.copyFrom(certString.getBytes(UTF_8));*/
    } catch (Exception ex) {
      throw new InvalidProtocolBufferException(
          "X509Certificate cannot be decoded: " + ex.getMessage());
    }
  }

  @Override
  public Object deserialize(Class< ? > type, ByteString value)
      throws InvalidProtocolBufferException {
    try {
      return new X509CertificateHolder(value.toByteArray());
      /*String pemEncodedCert = new String(value.toByteArray(), UTF_8);
      return CertificateCodec.getX509Certificate(pemEncodedCert);*/
    } catch (Exception ex) {
      throw new InvalidProtocolBufferException(
          "X509Certificate cannot be decoded: " + ex.getMessage());
    }
  }

}
