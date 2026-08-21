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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;

import java.io.IOException;

/**
 * We either timed out before seeing any barriers or started unaligned. We might've seen some
 * announcements if we started aligned.
 */
final class AlternatingWaitingForFirstBarrierUnaligned implements BarrierHandlerState {

    private final boolean alternating;
    private final ChannelState channelState;

    AlternatingWaitingForFirstBarrierUnaligned(boolean alternating, ChannelState channelState) {
        this.alternating = alternating;
        this.channelState = channelState;
    }

    @Override
    public BarrierHandlerState alignedCheckpointTimeout(
            Controller controller, CheckpointBarrier checkpointBarrier) {
        // ignore already processing unaligned checkpoints
        return this;
    }

    @Override
    public BarrierHandlerState announcementReceived(
            Controller controller, InputChannelInfo channelInfo, int sequenceNumber)
            throws IOException {
        channelState.getInputs()[channelInfo.getGateIdx()].convertToPriorityEvent(
                channelInfo.getInputChannelIdx(), sequenceNumber);
        return this;
    }

    @Override
    public BarrierHandlerState barrierReceived(
            Controller controller,
            InputChannelInfo channelInfo,
            CheckpointBarrier checkpointBarrier,
            boolean markChannelBlocked)
            throws CheckpointException, IOException {

        // we received an out of order aligned barrier, we should book keep this channel as blocked,
        // as it is being blocked by the credit-based network
        if (markChannelBlocked && !checkpointBarrier.getCheckpointOptions().isUnalignedCheckpoint()) {
            //对齐 Barrier
            channelState.blockChannel(channelInfo);
        }
        //强行将这个刚进来的 Barrier 克隆并魔改为“非对齐属性”
        CheckpointBarrier unalignedBarrier = checkpointBarrier.asUnaligned();
        //通知输入网关（InputGate）初始化非对齐环境。
        controller.initInputsCheckpoint(unalignedBarrier);
        for (CheckpointableInput input : channelState.getInputs()) {
            //遍历当前 Task 所有的输入源（Inputs），告诉它们：“非对齐检查点正式启动了，各单位注意拦截并准备抓取在途数据（In-flight Data）！”
            input.checkpointStarted(unalignedBarrier);
        }
        //【重点】调用控制器，向当前 Task 的所有输出端（PipelinedSubpartition）下发刚刚升级好的 unalignedBarrier
        controller.triggerGlobalCheckpoint(unalignedBarrier);
        if (controller.allBarriersReceived()) {
            //单通道直接通关
            for (CheckpointableInput input : channelState.getInputs()) {
                input.checkpointStopped(unalignedBarrier.getId());
            }
            //宣告当前快照采集结束
            return stopCheckpoint();
        }
        //多通道开启漫长的“非对齐收集”
        return new AlternatingCollectingBarriersUnaligned(alternating, channelState);
    }

    @Override
    public BarrierHandlerState abort(long cancelledId) throws IOException {
        return stopCheckpoint();
    }

    @Override
    public BarrierHandlerState endOfPartitionReceived(
            Controller controller, InputChannelInfo channelInfo)
            throws IOException, CheckpointException {
        channelState.channelFinished(channelInfo);

        // Do nothing since we have no pending checkpoint.
        return this;
    }

    private BarrierHandlerState stopCheckpoint() throws IOException {
        channelState.unblockAllChannels();
        if (alternating) {
            return new AlternatingWaitingForFirstBarrier(channelState.emptyState());
        } else {
            return new AlternatingWaitingForFirstBarrierUnaligned(false, channelState.emptyState());
        }
    }
}
