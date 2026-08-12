/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.XORShiftRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * An abstract record-oriented runtime result writer.
 *
 * <p>The RecordWriter wraps the runtime's {@link ResultPartitionWriter} and takes care of
 * subpartition selection and serializing records into bytes.
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public abstract class RecordWriter<T extends IOReadableWritable> implements AvailabilityProvider {

    /** Default name for the output flush thread, if no name with a task reference is given. */
    @VisibleForTesting
    public static final String DEFAULT_OUTPUT_FLUSH_THREAD_NAME = "OutputFlusher";

    private static final Logger LOG = LoggerFactory.getLogger(RecordWriter.class);

    protected final ResultPartitionWriter targetPartition;

    protected final int numberOfSubpartitions;

    protected final DataOutputSerializer serializer;

    protected final Random rng = new XORShiftRandom();

    protected final boolean flushAlways;

    /** The thread that periodically flushes the output, to give an upper latency bound. */
    @Nullable private final OutputFlusher outputFlusher;

    /**
     * To avoid synchronization overhead on the critical path, best-effort error tracking is enough
     * here.
     */
    private Throwable flusherException;

    private volatile Throwable volatileFlusherException;
    private int volatileFlusherExceptionCheckSkipCount;
    private static final int VOLATILE_FLUSHER_EXCEPTION_MAX_CHECK_SKIP_COUNT = 100;

    RecordWriter(ResultPartitionWriter writer, long timeout, String taskName) {
        this.targetPartition = writer;
        // 跟最大并行度一致
        this.numberOfSubpartitions = writer.getNumberOfSubpartitions();
        //System.out.println("RecordWriter:" + Thread.currentThread().getName() + " -> " + numberOfChannels);
        //将对应的数据结构序列化生成二进制数据写入内存块
        this.serializer = new DataOutputSerializer(128);

        checkArgument(timeout >= ExecutionOptions.DISABLED_NETWORK_BUFFER_TIMEOUT);
        this.flushAlways = (timeout == ExecutionOptions.FLUSH_AFTER_EVERY_RECORD);
        if (timeout == ExecutionOptions.DISABLED_NETWORK_BUFFER_TIMEOUT
                || timeout == ExecutionOptions.FLUSH_AFTER_EVERY_RECORD) {
            outputFlusher = null;
        } else {
            String threadName =
                    taskName == null
                            ? DEFAULT_OUTPUT_FLUSH_THREAD_NAME
                            : DEFAULT_OUTPUT_FLUSH_THREAD_NAME + " for " + taskName;
            //创建OutputFlusher线程 timeout 默认100
            outputFlusher = new OutputFlusher(threadName, timeout);
            //【重点】 启动线程
            outputFlusher.start();
        }
    }

    public void emit(T record, int targetSubpartition) throws IOException {
        //检查错误
        checkErroneous();
        //todo 对数据进行序列化并转换成ByteBuffer
        //serializer = DataOutputSerializer
        //record = SerializationDelegate
        // byteBuffer = HeapByteBuffer
        ByteBuffer byteBuffer = serializeRecord(serializer, record);//
        targetPartition.emitRecord(byteBuffer, targetSubpartition);
        // PipelinedResultPartition#emitRecord  PipelinedResultPartition extends BufferWritingResultPartition
        if (flushAlways) {
            //flushAlways 一般是 false 所以这里一般不运行
            //会通过 OutputFlusher 线程刷新数据  【重点】
            //PipelinedResultPartition#flush
            targetPartition.flush(targetSubpartition);
        }
    }

    protected void emit(ByteBuffer record, int targetSubpartition) throws IOException {
        checkErroneous();

        targetPartition.emitRecord(record, targetSubpartition);

        if (flushAlways) {
            targetPartition.flush(targetSubpartition);
        }
    }

    public void broadcastEvent(AbstractEvent event) throws IOException {
        broadcastEvent(event, false);
    }

    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        targetPartition.broadcastEvent(event, isPriorityEvent);

        if (flushAlways) {
            flushAll();
        }
    }

    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        targetPartition.alignedBarrierTimeout(checkpointId);
    }

    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        targetPartition.abortCheckpoint(checkpointId, cause);
    }

    @VisibleForTesting
    public static ByteBuffer serializeRecord(
            DataOutputSerializer serializer, IOReadableWritable record) throws IOException {
        // the initial capacity should be no less than 4 bytes
        //将position设置为4
        serializer.setPositionUnsafe(4);
        //todo 从4位置开始写入数据，先将数据序列化成二进制再写到serializer
        // write data
        record.write(serializer);

        // write length
        //在0位置 写入数据大小到serializer
        serializer.writeIntUnsafe(serializer.length() - 4, 0);
        //返回 HeapByteBuffer
        return serializer.wrapAsByteBuffer();
    }

    public void flushAll() {
        //todo 分区刷新所有数据
        // PipelinedResultPartition#flushAll
        targetPartition.flushAll();
    }

    /** Sets the metric group for this RecordWriter. */
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        targetPartition.setMetricGroup(metrics);
    }

    public int getNumberOfSubpartitions() {
        return numberOfSubpartitions;
    }

    /**
     * Whether the subpartition where an element comes from can be derived from the existing
     * information. If false, the caller of this writer should attach the subpartition information
     * onto an element before writing it to a subpartition, if the element needs this information
     * afterward.
     */
    public boolean isSubpartitionDerivable() {
        return !(targetPartition instanceof ResultPartition
                && ((ResultPartition) targetPartition).isNumberOfPartitionConsumerUndefined());
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return targetPartition.getAvailableFuture();
    }

    /** This is used to send regular records. */
    public abstract void emit(T record) throws IOException;

    /** This is used to send LatencyMarks to a random target subpartition. */
    public void randomEmit(T record) throws IOException {
        checkErroneous();

        int targetSubpartition = rng.nextInt(numberOfSubpartitions);
        emit(record, targetSubpartition);
    }

    /** This is used to broadcast streaming Watermarks in-band with records. */
    public abstract void broadcastEmit(T record) throws IOException;

    /** Closes the writer. This stops the flushing thread (if there is one). */
    public void close() {
        // make sure we terminate the thread in any case
        if (outputFlusher != null) {
            outputFlusher.terminate();
            try {
                outputFlusher.join();
            } catch (InterruptedException e) {
                // ignore on close
                // restore interrupt flag to fast exit further blocking calls
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Notifies the writer that the output flusher thread encountered an exception.
     *
     * @param t The exception to report.
     */
    private void notifyFlusherException(Throwable t) {
        if (flusherException == null) {
            LOG.error("An exception happened while flushing the outputs", t);
            flusherException = t;
            volatileFlusherException = t;
        }
    }

    protected void checkErroneous() throws IOException {
        // For performance reasons, we are not checking volatile field every single time.
        if (flusherException != null
                || (volatileFlusherExceptionCheckSkipCount
                                >= VOLATILE_FLUSHER_EXCEPTION_MAX_CHECK_SKIP_COUNT
                        && volatileFlusherException != null)) {
            throw new IOException(
                    "An exception happened while flushing the outputs", volatileFlusherException);
        }
        if (++volatileFlusherExceptionCheckSkipCount
                >= VOLATILE_FLUSHER_EXCEPTION_MAX_CHECK_SKIP_COUNT) {
            volatileFlusherExceptionCheckSkipCount = 0;
        }
    }

    /** Sets the max overdraft buffer size of per gate. */
    public void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate) {
        targetPartition.setMaxOverdraftBuffersPerGate(maxOverdraftBuffersPerGate);
    }

    // ------------------------------------------------------------------------

    /**
     * A dedicated thread that periodically flushes the output buffers, to set upper latency bounds.
     *
     * <p>The thread is daemonic, because it is only a utility thread.
     */
    private class OutputFlusher extends Thread {

        private final long timeout;

        private volatile boolean running = true;

        OutputFlusher(String name, long timeout) {
            super(name);
            setDaemon(true);
            this.timeout = timeout;
        }

        public void terminate() {
            running = false;
            interrupt();
        }

        @Override
        public void run() {
            try {
                while (running) {
                    try {
                        // timeout = 100ms
                        Thread.sleep(timeout);
                    } catch (InterruptedException e) {
                        // propagate this if we are still running, because it should not happen
                        // in that case
                        if (running) {
                            throw new Exception(e);
                        }
                    }

                    // any errors here should let the thread come to a halt and be
                    // recognized by the writer
                    //刷新所有记录
                    flushAll();
                }
            } catch (Throwable t) {
                notifyFlusherException(t);
            }
        }
    }

    @VisibleForTesting
    ResultPartitionWriter getTargetPartition() {
        return targetPartition;
    }
}
