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

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Not thread safe class for filling in the content of the {@link MemorySegment}. To access written
 * data please use {@link BufferConsumer} which allows to build {@link Buffer} instances from the
 * written data.
 */
@NotThreadSafe
public class BufferBuilder implements AutoCloseable {
    private final Buffer buffer;
    private final MemorySegment memorySegment;
    private int maxCapacity;

    private final SettablePositionMarker positionMarker = new SettablePositionMarker();//

    private boolean bufferConsumerCreated = false;

    public BufferBuilder(MemorySegment memorySegment, BufferRecycler recycler) {
        this.memorySegment = checkNotNull(memorySegment);
        this.buffer = new NetworkBuffer(memorySegment, recycler);
        this.maxCapacity = buffer.getMaxCapacity();
    }

    /**
     * This method always creates a {@link BufferConsumer} starting from the current writer offset.
     * Data written to {@link BufferBuilder} before creation of {@link BufferConsumer} won't be
     * visible for that {@link BufferConsumer}.
     *
     * @return created matching instance of {@link BufferConsumer} to this {@link BufferBuilder}.
     */
    public BufferConsumer createBufferConsumer() {
        return createBufferConsumer(positionMarker.cachedPosition);
    }

    /**
     * This method always creates a {@link BufferConsumer} starting from position 0 of {@link
     * MemorySegment}.
     *
     * @return created matching instance of {@link BufferConsumer} to this {@link BufferBuilder}.
     */
    public BufferConsumer createBufferConsumerFromBeginning() {
        return createBufferConsumer(0);//
    }

    private BufferConsumer createBufferConsumer(int currentReaderPosition) {
        //一个 BufferBuilder 在其生命周期内，有且仅能调用一次这个方法
        //因为一个内存片段（MemorySegment）在网络发送端被切分时，一条写通道（Builder）只能对应一条读通道（Consumer）。
        // 如果允许为一个写缓冲区创建多个消费者代理，多个 Netty 线程并发去读，底层的读写指针和位置标记就会彻底乱套
        checkState(!bufferConsumerCreated, "Two BufferConsumer shouldn't exist for one BufferBuilder");
        bufferConsumerCreated = true;
        //NetworkBuffer#retainBuffer  它不会去复制内存中的实际数据（那是极度高昂的 CPU 开销），它仅仅是把当前底层的物理 Buffer 的引用计数（Reference Count）加 1
        //positionMarker 状态共享指针
        //当上游往 Builder 里写了 10 个字节，Builder 会动态更新这个 positionMarker。
        // 下游的 Consumer 通过这个共享标记，能实时、动态地感知到上游写到了哪里。Netty 线程不需要等待上游把一整块 Buffer 全部写满，
        // 只要看到 positionMarker 往前挪了，就能立刻把刚写进去的几个字节热乎地发出去（这也是 Flink 能够做到超低端到端延迟的终极黑科技 —— Flush 机制 的底功）
        //currentReaderPosition 指定这个消费者开始读取的初始位置。通常情况下是 0（从头开始读）
        return new BufferConsumer(buffer.retainBuffer(), positionMarker, currentReaderPosition);//
    }

    /** Gets the data type of the internal buffer. */
    public Buffer.DataType getDataType() {
        return buffer.getDataType();
    }

    /** Sets the data type of the internal buffer. */
    public void setDataType(Buffer.DataType dataType) {
        buffer.setDataType(dataType);
    }

    /** Same as {@link #append(ByteBuffer)} but additionally {@link #commit()} the appending. */
    public int appendAndCommit(ByteBuffer source) {
        //将传入的 ByteBuffer source 里的二进制数据，拷贝到当前 BufferBuilder 所持有的堆外内存片段（MemorySegment）中
        //返回这次实际上成功写进去了多少个字节
        int writtenBytes = append(source);
        //它负责把当前写线程刚刚推高的最新写指针位置，同步、固化 到我们之前长篇分析过的共享指针标记（positionMarker）中
        commit();//
        return writtenBytes;
    }

    /**
     * Append as many data as possible from {@code source}. Not everything might be copied if there
     * is not enough space in the underlying {@link MemorySegment}
     *
     * @return number of copied bytes
     */
    //将数据写入memorySegment
    public int append(ByteBuffer source) {
        checkState(!isFinished());

        int needed = source.remaining();//
        int available = getMaxCapacity() - positionMarker.getCached();
        int toCopy = Math.min(needed, available);

        memorySegment.put(positionMarker.getCached(), source, toCopy);
        //移动 数字字节大小
        positionMarker.move(toCopy);//
        return toCopy;
    }

    /**
     * Make the change visible to the readers. This is costly operation (volatile access) thus in
     * case of bulk writes it's better to commit them all together instead one by one.
     */
    public void commit() {
        positionMarker.commit();
    }

    /**
     * Mark this {@link BufferBuilder} and associated {@link BufferConsumer} as finished - no new
     * data writes will be allowed.
     *
     * <p>This method should be idempotent to handle failures and task interruptions. Check
     * FLINK-8948 for more details.
     *
     * @return number of written bytes.
     */
    public int finish() {
        int writtenBytes = positionMarker.markFinished();
        commit();
        return writtenBytes;
    }

    public boolean isFinished() {
        return positionMarker.isFinished();
    }

    public boolean isFull() {
        checkState(positionMarker.getCached() <= getMaxCapacity());
        return positionMarker.getCached() == getMaxCapacity();
    }

    public int getWritableBytes() {
        checkState(positionMarker.getCached() <= getMaxCapacity());
        return getMaxCapacity() - positionMarker.getCached();
    }

    public int getCommittedBytes() {
        return positionMarker.getCached();
    }

    public int getMaxCapacity() {
        return maxCapacity;
    }

    /**
     * The result capacity can not be greater than allocated memorySegment. It also can not be less
     * than already written data.
     */
    public void trim(int newSize) {
        maxCapacity =
                Math.min(Math.max(newSize, positionMarker.getCached()), buffer.getMaxCapacity());
    }

    @Override
    public void close() {
        buffer.recycleBuffer();
    }

    /**
     * Holds a reference to the current writer position. Negative values indicate that writer
     * ({@link BufferBuilder} has finished. Value {@code Integer.MIN_VALUE} represents finished
     * empty buffer.
     */
    @ThreadSafe
    interface PositionMarker {
        int FINISHED_EMPTY = Integer.MIN_VALUE;

        int get();

        static boolean isFinished(int position) {
            return position < 0;
        }

        static int getAbsolute(int position) {
            if (position == FINISHED_EMPTY) {
                return 0;
            }
            return Math.abs(position);
        }
    }

    /**
     * Cached writing implementation of {@link PositionMarker}.
     *
     * <p>Writer ({@link BufferBuilder}) and reader ({@link BufferConsumer}) caches must be
     * implemented independently of one another - so that the cached values can not accidentally
     * leak from one to another.
     *
     * <p>Remember to commit the {@link SettablePositionMarker} to make the changes visible.
     */
    //是连接 上游 Task 线程（写数据） 与 下游 Netty 线程（读数据） 的关键桥梁
    static class SettablePositionMarker implements PositionMarker {

        //由写线程实时更新的易变状态  上游写线程的真实写入位置 每往下游 Buffer 写入一条 Record，它就前进一次
        private volatile int position = 0;

        /**
         * Locally cached value of volatile {@code position} to avoid unnecessary volatile accesses.
         */
        //读线程在特定时机同步过来的只读快照  下游读线程看到的写入位置快照。 由 Netty 线程（消费者） 维护和更新
        private int cachedPosition = 0;

        @Override
        public int get() {
            return position;
        }

        public boolean isFinished() {
            return PositionMarker.isFinished(cachedPosition);
        }

        public int getCached() {
            return PositionMarker.getAbsolute(cachedPosition);
        }

        /**
         * Marks this position as finished and returns the current position.
         *
         * @return current position as of {@link #getCached()}
         */
        public int markFinished() {
            int currentPosition = getCached();
            int newValue = -currentPosition;
            if (newValue == 0) {
                newValue = FINISHED_EMPTY;
            }
            set(newValue);
            return currentPosition;
        }

        public void move(int offset) {
            set(cachedPosition + offset);
        }

        public void set(int value) {
            cachedPosition = value;
        }

        public void commit() {
            position = cachedPosition;
        }
    }
}
