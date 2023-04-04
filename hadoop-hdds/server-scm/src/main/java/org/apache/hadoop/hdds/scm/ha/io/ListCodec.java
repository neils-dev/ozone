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
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.ListArgument;
import org.apache.hadoop.hdds.scm.ha.ReflectionUtil;
import org.apache.hadoop.hdds.scm.server.SCMSecurityProtocolServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.ArrayList;

/**
 * {@link Codec} for {@link List} objects.
 */
public class ListCodec implements Codec {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(ListCodec.class);

  @Override
  public ByteString serialize(Object object)
      throws InvalidProtocolBufferException {
    final ListArgument.Builder listArgs = ListArgument.newBuilder();
    final List<?> values = (List<?>) object;
    if (!values.isEmpty()) {
      Class<?> type = values.get(0).getClass();
      listArgs.setType(type.getName());
      for (Object value : values) {
        listArgs.addValue(CodecFactory.getCodec(type).serialize(value));
      }
    } else {
      listArgs.setType(Object.class.getName());
    }
    return listArgs.build().toByteString();
  }

  @Override
  public Object deserialize(Class<?> type, ByteString value)
      throws InvalidProtocolBufferException {
    try {
      List<Object> result = (List<Object>) ArrayList.class.newInstance();
      final ListArgument listArgs = (ListArgument) ReflectionUtil
          .getMethod(ListArgument.class, "parseFrom", byte[].class)
          .invoke(null, (Object) value.toByteArray());
      final Class<?> dataType = ReflectionUtil.getClass(listArgs.getType());
      for (ByteString element : listArgs.getValueList()) {
        result.add(CodecFactory.getCodec(dataType)
            .deserialize(dataType, element));
      }
      return result;
    } catch (InstantiationException | NoSuchMethodException |
        IllegalAccessException | InvocationTargetException |
        ClassNotFoundException ex) {
      LOGGER.info("List cannot be decoded, ex: ", ex);
      throw new InvalidProtocolBufferException(
          "Message cannot be decoded: " + ex.getMessage());
    }
  }
}
