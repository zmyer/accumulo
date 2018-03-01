/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Autogenerated by Thrift Compiler (0.10.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.accumulo.core.master.thrift;



public enum FateOperation implements org.apache.thrift.TEnum {
  TABLE_CREATE(0),
  TABLE_CLONE(1),
  TABLE_DELETE(2),
  TABLE_RENAME(3),
  TABLE_ONLINE(4),
  TABLE_OFFLINE(5),
  TABLE_MERGE(6),
  TABLE_DELETE_RANGE(7),
  TABLE_BULK_IMPORT(8),
  TABLE_COMPACT(9),
  TABLE_IMPORT(10),
  TABLE_EXPORT(11),
  TABLE_CANCEL_COMPACT(12),
  NAMESPACE_CREATE(13),
  NAMESPACE_DELETE(14),
  NAMESPACE_RENAME(15);

  private final int value;

  private FateOperation(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static FateOperation findByValue(int value) { 
    switch (value) {
      case 0:
        return TABLE_CREATE;
      case 1:
        return TABLE_CLONE;
      case 2:
        return TABLE_DELETE;
      case 3:
        return TABLE_RENAME;
      case 4:
        return TABLE_ONLINE;
      case 5:
        return TABLE_OFFLINE;
      case 6:
        return TABLE_MERGE;
      case 7:
        return TABLE_DELETE_RANGE;
      case 8:
        return TABLE_BULK_IMPORT;
      case 9:
        return TABLE_COMPACT;
      case 10:
        return TABLE_IMPORT;
      case 11:
        return TABLE_EXPORT;
      case 12:
        return TABLE_CANCEL_COMPACT;
      case 13:
        return NAMESPACE_CREATE;
      case 14:
        return NAMESPACE_DELETE;
      case 15:
        return NAMESPACE_RENAME;
      default:
        return null;
    }
  }
}
