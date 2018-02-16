/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.util.record;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Utility class to capture key record batch statistics.
 */
public final class RecordBatchStats {
  /** Identifier generator */
  private static final AtomicLong batchStatsIdGenerator = new AtomicLong(0);

  /**
   * @return Generates a new batch statistics identifier (unique within a single Drillbit instance execution)
   */
  public static String generateBatchStatsIdentifier() {
    return Long.toString(batchStatsIdGenerator.incrementAndGet());
  }

  /**
   * Prints batch count and memory statistics for the input record batch
   *
   * @param stats instance identifier
   * @param originator the name of the entity which produced this record batch
   * @param sourceId optional source identifier for scanners
   * @param recordBatch a set of records
   *
   * @return a string containing the record batch statistics
   */
  public static String printRecordBatchStats(String statsId,
    String originator,
    String sourceId,
    Map<String, ValueVector> recordBatch) {

    StringBuilder aggregateMsg = new StringBuilder(256);
    StringBuilder fieldsMsg    = new StringBuilder(256);
    int batchSize              = recordBatch.values().iterator().next().getAccessor().getValueCount();
    long totalAllocSize        = 0;
    long totalBufferSize       = 0;
    long totalPayloadByte      = 0;

    // Print the stats headers
    printValueVectorFieldsStatsHeaders(fieldsMsg);

    for (Map.Entry<String, ValueVector> entry : recordBatch.entrySet()) {
      final String fieldName = entry.getKey();
      final ValueVector v    = entry.getValue();

      printFieldStats(statsId, originator, fieldName, v, fieldsMsg);

      // Aggregate total counter stats
      totalAllocSize   += v.getAllocatedSize();
      totalBufferSize  += v.getBufferSize();
      totalPayloadByte += v.getPayloadByteCount(batchSize);
    }

    // Prints the aggregate record batch statistics
    printAggregateStats(statsId,
      originator,
      sourceId,
      batchSize,
      totalAllocSize,
      totalBufferSize,
      totalPayloadByte,
      aggregateMsg);

    return aggregateMsg.toString() + fieldsMsg.toString();
  }

  private static void printValueVectorFieldsStatsHeaders(StringBuilder msg) {
    msg.append("Originator\tName\tType\tAlloc-Sz\tUsed-Sz\tAvgPay-Sz\n");
  }

  private static void printFieldStats(String statsId,
    String originator,
    String fieldName,
    ValueVector v,
    StringBuilder msg) {

    final MaterializedField field = v.getField();
    final int batchSize           = v.getAccessor().getValueCount();

    msg.append(originator);
    msg.append(':');
    msg.append(statsId);
    msg.append('\t');
    msg.append(fieldName);
    msg.append('\t');
    printType(field, msg);
    msg.append('\t');
    msg.append(v.getAllocatedSize());
    msg.append('\t');
    msg.append(v.getBufferSize());
    msg.append('\t');
    if (batchSize > 0) {
      msg.append(v.getPayloadByteCount(batchSize) / batchSize);
    } else {
      msg.append("NA");
    }
    msg.append('\n');
  }

  private static void printAggregateStats(String statsId,
    String originator,
    String sourceId,
    int batchSize,
    long totalAllocSize,
    long totalBufferSize,
    long totalPayloadByte,
    StringBuilder msg) {

    msg.append("Originator: [");
    msg.append(originator);
    msg.append(':');
    msg.append(statsId);
    msg.append(':');
    msg.append(sourceId);
    msg.append("], Batch Size: [");
    msg.append(batchSize);
    msg.append("], Total Allocated Size: [");
    msg.append(totalAllocSize);
    msg.append("], Total Buffer Size: [");
    msg.append(totalBufferSize);
    msg.append("], Total Paylod Byte: [");
    msg.append(totalPayloadByte);
    msg.append("]\n");

  }

  private static void printType(MaterializedField field, StringBuilder msg) {
    final MajorType type = field.getType();

    msg.append(type.getMinorType().name());
    msg.append(':');
    msg.append(type.getMode().name());
  }

  /**
   * Disabling class object instantiation.
   */
  private RecordBatchStats() {
  }

}
