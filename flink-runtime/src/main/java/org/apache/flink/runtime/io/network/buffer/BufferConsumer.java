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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder.PositionMarker;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.DATA_BUFFER;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Not thread safe class for producing {@link Buffer}.
 *
 * <p>It reads data written by {@link BufferBuilder}. Although it is not thread safe and can be used
 * only by one single thread, this thread can be different than the thread using/writing to {@link
 * BufferBuilder}. Pattern here is simple: one thread writes data to {@link BufferBuilder} and there
 * can be a different thread reading from it using {@link BufferConsumer}.
 */
@NotThreadSafe
public class BufferConsumer implements Closeable {
    private final Buffer buffer;

    //writerPosition 标记了这块内存中“可读数据的绝对终点（天花板）”
    private final CachedPositionMarker writerPosition;

    //记录了当前网络线程（Netty）“已经读到了哪里”
    private int currentReaderPosition;

    /** Constructs {@link BufferConsumer} instance with static content of a certain size. */
    public BufferConsumer(Buffer buffer, int size) {
        this(buffer, () -> -size, 0);//
        checkState(
                isFinished(),
                "BufferConsumer with static size must be finished after construction!");
    }

    public BufferConsumer(//
            Buffer buffer,
            BufferBuilder.PositionMarker currentWriterPosition,
            int currentReaderPosition) {
        this.buffer = checkNotNull(buffer);
        this.writerPosition = new CachedPositionMarker(checkNotNull(currentWriterPosition));
        checkArgument(
                currentReaderPosition <= writerPosition.getCached(),
                "Reader position larger than writer position");
        this.currentReaderPosition = currentReaderPosition;
    }

    /**
     * Checks whether the {@link BufferBuilder} has already been finished.
     *
     * <p>BEWARE: this method accesses the cached value of the position marker which is only updated
     * after calls to {@link #build()} and {@link #skip(int)}!
     *
     * @return <tt>true</tt> if the buffer was finished, <tt>false</tt> otherwise
     */
    public boolean isFinished() {
        return writerPosition.isFinished();
    }

    /**
     * @return sliced {@link Buffer} containing the not yet consumed data. Returned {@link Buffer}
     *     shares the reference counter with the parent {@link BufferConsumer} - in order to recycle
     *     memory both of them must be recycled/closed.
     */
    public Buffer build() {
        //获取上游 Task 线程最新的写入进度  cachedPosition = position
        writerPosition.update();
        //将这个最新的写位置快照记录在 cachedWriterPosition 变量中。这确立了本次发送数据的右边界
        int cachedWriterPosition = writerPosition.getCached();
        //零拷贝内存切片  currentReaderPosition 左边界（上一次网络线程读到了哪里）
        // cachedWriterPosition - currentReaderPosition：本次要读的长度（上游新写了多少字节）
        //利用 Netty 底层的切片机制，在原有内存块上划定一个只读的区间。它不会发生任何内存拷贝（Zero-Copy），只是创建了一个指向相同物理内存的新指针引用
        //因为是 readOnlySlice（只读），网络发送线程绝对无法篡改数据，保证了线程安全
        Buffer slice = buffer.readOnlySlice(currentReaderPosition, cachedWriterPosition - currentReaderPosition);
        //切片创建成功后，立即将 currentReaderPosition 推进到本次的写位置快照
        currentReaderPosition = cachedWriterPosition;
        //增加引用计数并返回
        //因为底层的物理内存被网络层的 slice 引用了，为了防止上游或者其他线程在发送完成前误释放这块内存，必须调用 retainBuffer() 增加其引用计数（Reference Count）。
        // 当下游 Netty 真正把数据发送到网卡后，网络层会调用 recycleBuffer() 减少引用计数，当计数归零时内存才会真正回收到内存池
        return slice.retainBuffer();
    }

    /**
     * @param bytesToSkip number of bytes to skip from currentReaderPosition
     */
    void skip(int bytesToSkip) {
        writerPosition.update();
        int cachedWriterPosition = writerPosition.getCached();
        int bytesReadable = cachedWriterPosition - currentReaderPosition;
        checkState(bytesToSkip <= bytesReadable, "bytes to skip beyond readable range");
        currentReaderPosition += bytesToSkip;
    }

    /**
     * Returns a retained copy with separate indexes. This allows to read from the same {@link
     * MemorySegment} twice.
     *
     * <p>WARNING: the newly returned {@link BufferConsumer} will have its reader index copied from
     * the original buffer. In other words, data already consumed before copying will not be visible
     * to the returned copies.
     *
     * @return a retained copy of self with separate indexes
     */
    public BufferConsumer copy() {
        return new BufferConsumer(
                buffer.retainBuffer(), writerPosition.positionMarker, currentReaderPosition);
    }

    /**
     * Returns a retained copy with separate indexes and sets the reader position to the given
     * value. This allows to read from the same {@link MemorySegment} twice starting from the
     * supplied position.
     *
     * @param readerPosition the new reader position. Can be less than the {@link
     *     #currentReaderPosition}, but may not exceed the current writer's position.
     * @return a retained copy of self with separate indexes
     */
    public BufferConsumer copyWithReaderPosition(int readerPosition) {
        return new BufferConsumer(
                buffer.retainBuffer(), writerPosition.positionMarker, readerPosition);
    }

    public boolean isBuffer() {
        //NetworkBuffer#isBuffer
        return buffer.isBuffer();
    }

    public Buffer.DataType getDataType() {
        return buffer.getDataType();
    }

    @Override
    public void close() {
        if (!buffer.isRecycled()) {
            buffer.recycleBuffer();
        }
    }

    public boolean isRecycled() {
        return buffer.isRecycled();
    }

    public int getWrittenBytes() {
        return writerPosition.getCached();
    }

    int getCurrentReaderPosition() {
        return currentReaderPosition;
    }

    boolean isStartOfDataBuffer() {
        return buffer.getDataType() == DATA_BUFFER && currentReaderPosition == 0;
    }

    int getBufferSize() {
        return buffer.getMaxCapacity();
    }

    /** Returns true if there is new data available for reading. */
    public boolean isDataAvailable() {
        return currentReaderPosition < writerPosition.getLatest();
    }

    public String toDebugString(boolean includeHash) {
        Buffer buffer = null;
        try (BufferConsumer copiedBufferConsumer = copy()) {
            buffer = copiedBufferConsumer.build();
            checkState(copiedBufferConsumer.isFinished());
            return buffer.toDebugString(includeHash);
        } finally {
            if (buffer != null) {
                buffer.recycleBuffer();
            }
        }
    }

    /**
     * Cached reading wrapper around {@link PositionMarker}.
     *
     * <p>Writer ({@link BufferBuilder}) and reader ({@link BufferConsumer}) caches must be
     * implemented independently of one another - so that the cached values can not accidentally
     * leak from one to another.
     */
    private static class CachedPositionMarker {
        private final PositionMarker positionMarker;

        /**
         * Locally cached value of {@link PositionMarker} to avoid unnecessary volatile accesses.
         */
        private int cachedPosition;

        CachedPositionMarker(PositionMarker positionMarker) {
            this.positionMarker = checkNotNull(positionMarker);
            update();
        }

        public boolean isFinished() {
            return PositionMarker.isFinished(cachedPosition);
        }

        public int getCached() {
            return PositionMarker.getAbsolute(cachedPosition);
        }

        private int getLatest() {
            return PositionMarker.getAbsolute(positionMarker.get());
        }

        private void update() {
            //SettablePositionMarker#get() 获取 position 的值
            this.cachedPosition = positionMarker.get();
        }
    }
}
