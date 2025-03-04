/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class TestVectorLoad {
    private static final byte[] STR1 = "AAAAA1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] STR2 = "BBBBBBBBB2".getBytes(StandardCharsets.UTF_8);

    private BufferAllocator allocator;

    @BeforeEach
    public void prepare() {
        allocator = new RootAllocator(Integer.MAX_VALUE);
    }

    @AfterEach
    public void shutdown() {
        allocator.close();
    }

    private void testVectorLoadForNullBuffer(int nullCount) {
        try (final VarCharVector vector1 = new VarCharVector("myvector", allocator)) {
            ValueVectorDataPopulator.setVector(vector1, STR1, STR2);

            Field field = vector1.getField();
            String fieldName = field.getName();

            List<Field> fields = new ArrayList<>();
            List<FieldVector> fieldVectors = new ArrayList<>();

            fields.add(field);
            fieldVectors.add(vector1);

            Schema schema = new Schema(fields);

            VectorSchemaRoot schemaRoot1 =
                    new VectorSchemaRoot(schema, fieldVectors, vector1.getValueCount());
            VectorUnloader vectorUnloader = new VectorUnloader(schemaRoot1);

            try (ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();
                 VectorSchemaRoot schemaRoot2 = VectorSchemaRoot.create(schema, allocator); ) {

                assertEquals(1, recordBatch.getNodes().size());
                assertEquals(0, recordBatch.getNodes().get(0).getNullCount());

                List<ArrowBuf> oldBuffers = recordBatch.getBuffers();
                List<ArrowBuf> newBuffers = new ArrayList<>();
                var i = 0;
                for (ArrowBuf oldBuffer : oldBuffers) {
                    // Skip the null buffer
                    if (i == 0) {
                        i = i + 1;
                        continue;
                    }
                    ArrowBuf newBuffer = allocator.buffer(oldBuffer.capacity());
                    newBuffer.setBytes(0, oldBuffer, 0, oldBuffer.capacity());
                    newBuffer.writerIndex(oldBuffer.writerIndex());
                    newBuffers.add(newBuffer);
                }

                List<ArrowFieldNode> oldNodes = recordBatch.getNodes();
                List<ArrowFieldNode> newNodes = new ArrayList<>();
                for (ArrowFieldNode oldNode : oldNodes) {
                    newNodes.add(new ArrowFieldNode(oldNode.getLength(), nullCount));
                }

                try (ArrowRecordBatch newBatch =
                             new ArrowRecordBatch(recordBatch.getLength(), newNodes, newBuffers); ) {
                    VectorLoader vectorLoader = new VectorLoader(schemaRoot2);

                    if (nullCount == 0) {
                        vectorLoader.load(newBatch);
                        FieldVector loaded = schemaRoot2.getVector(fieldName);

                        assertEquals(0, loaded.getNullCount());
                    } else {
                        try {
                            vectorLoader.load(newBatch);
                        } catch (IllegalArgumentException e) {
                            // Expected
                            assertTrue(e.getMessage().contains("no more buffers for field"));
                        }
                    }

                }
            }
        }
    }

    @Test
    public void testVectorLoadWithoutNullBuffer() {
        testVectorLoadForNullBuffer(0);
    }

    @Test
    public void testVectorLoadWithoutNullBufferError() {
        testVectorLoadForNullBuffer(1);
    }
}
