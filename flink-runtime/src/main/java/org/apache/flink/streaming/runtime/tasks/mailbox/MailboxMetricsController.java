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

package org.apache.flink.streaming.runtime.tasks.mailbox;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.runtime.tasks.TimerService;
import org.apache.flink.util.clock.SystemClock;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Mailbox metrics controller class. The use of mailbox metrics, in particular scheduling latency
 * measurements that require a {@link TimerService}, induce (cyclic) dependencies between {@link
 * MailboxProcessor} and {@link org.apache.flink.streaming.runtime.tasks.StreamTask}. An instance of
 * this class contains and gives control over these dependencies.
 */
@Internal
public class MailboxMetricsController {

    /** Default timer interval in milliseconds for triggering mailbox latency measurement. */
    //默认的延迟测量时间间隔
    public final int defaultLatencyMeasurementInterval = 1000;

    //Mailbox 邮件处理延迟直方图
    //机制：这是排查 Flink 卡顿最关键的指标之一。它负责统计 Mailbox 里的邮件从投递到真正被执行完所消耗的时间。
    //通过直方图，你可以看到延迟的分布情况（如 p50、p99 延迟）。如果 p99 延迟极高，说明某些控制事件在队列里憋了很久才被执行，通常是因为默认行为（数据处理）占用了太多时间
    private final Histogram latencyHistogram;
    //作用：邮件处理计数器。机制：一个简单高效的累加器。每当 Mailbox 线程成功处理完一封非默认行为的控制邮件（Mail），这个计数器就会加 1。它可以直观反映出当前算子处理控制事件的频繁程度
    private final Counter mailCounter;

    //作用：定时器服务引用。机制：用于注册定时任务。因为指标控制器需要每隔 measurementInterval（1秒）去测一次延迟，
    //它必须依赖系统的 TimerService 来触发定时回调。当定时器到期时，会触发向 Mailbox 投递“测量信件”的动作，并注册下一个周期的定时器
    @Nullable private TimerService timerService;
    //作用：面向指标控制器的邮箱执行器。机制（核心测量原理）：它是 Flink 测量 Mailbox 延迟的精妙工具。如何利用它测延迟？：当 timerService 每隔 1 秒触发时，它会通过这个 mailboxExecutor 向 Mailbox 队列里投递一封特殊的“探针邮件（Latency Measurement Mail）”。这封邮件在创建时会记录一个当前时间戳 \(T_{1}\)。当 Mailbox 线程排队排到这封信并执行它时，
    //会获取当前时间戳T2。此时，T2 - T1 的差值就是这封信在队列里排队+等待执行的真实延迟，这个差值会被立刻记录到上面的 latencyHistogram 中
    @Nullable private MailboxExecutor mailboxExecutor;

    //作用：当前的延迟测量时间间隔。设计：它通常初始化为默认值 1000。
    //单独拆出这个变量是为了提供灵活性，允许 Flink 在某些特定场景下通过配置文件或动态调优来修改测量的频率（例如降低到 500ms 提高监控精度，或调大到 5000ms 追求极致性能）
    private int measurementInterval = defaultLatencyMeasurementInterval;

    //作用：控制器启动状态旗标。
    //机制：用来标记当前度量控制器是否已经开始工作。它能防止在 StreamTask 还没完全初始化好、或者处于取消（Cancel）/退出流程中时，重复启动定时采样任务，确保监控指标生命周期的安全
    private boolean started = false;

    /**
     * Creates instance of {@link MailboxMetricsController} with references to metrics provided as
     * parameters.
     *
     * @param latencyHistogram Histogram of mailbox latency measurements.
     * @param mailCounter Counter for number of mails processed.
     */
    public MailboxMetricsController(Histogram latencyHistogram, Counter mailCounter) {
        this.timerService = null;
        this.mailboxExecutor = null;
        this.latencyHistogram = latencyHistogram;
        this.mailCounter = mailCounter;
    }

    /**
     * Sets up latency measurement with required {@link TimerService} and {@link MailboxExecutor}.
     *
     * <p>Note: For each instance, latency measurement can be set up only once.
     *
     * @param timerService {@link TimerService} used for latency measurement.
     * @param mailboxExecutor {@link MailboxExecutor} used for latency measurement.
     */
    public void setupLatencyMeasurement(
            TimerService timerService, MailboxExecutor mailboxExecutor) {
        checkState(
                !isLatencyMeasurementSetup(),
                "latency measurement has already been setup and cannot be setup twice");
        this.timerService = timerService;
        this.mailboxExecutor = mailboxExecutor;
    }

    /**
     * Starts mailbox latency measurement. This requires setup of latency measurement via {@link
     * MailboxMetricsController#setupLatencyMeasurement(TimerService, MailboxExecutor)}. Latency is
     * measured through execution of a mail that is triggered by default in the interval defined by
     * {@link MailboxMetricsController#defaultLatencyMeasurementInterval}.
     *
     * <p>Note: For each instance, latency measurement can be started only once.
     */
    public void startLatencyMeasurement() {
        checkState(!isLatencyMeasurementStarted(), "latency measurement has already been started");
        checkState(
                isLatencyMeasurementSetup(),
                "timer service and mailbox executor must be setup for latency measurement");
        //定时投递 Measure邮件
        scheduleLatencyMeasurement();
        started = true;
    }

    /**
     * Indicates if latency mesurement has been started.
     *
     * @return True if latency measurement has been started.
     */
    public boolean isLatencyMeasurementStarted() {
        return started;
    }

    /**
     * Indicates if latency measurement has been setup.
     *
     * @return True if latency measurement has been setup.
     */
    public boolean isLatencyMeasurementSetup() {
        return this.timerService != null && this.mailboxExecutor != null;
    }

    /**
     * Gets {@link Counter} for number of mails processed.
     *
     * @return {@link Counter} for number of mails processed.
     */
    public Counter getMailCounter() {
        return this.mailCounter;
    }

    @VisibleForTesting
    public void setLatencyMeasurementInterval(int measurementInterval) {
        this.measurementInterval = measurementInterval;
    }

    @VisibleForTesting
    public void measureMailboxLatency() {
        assert mailboxExecutor != null;
        long startTime = SystemClock.getInstance().relativeTimeMillis();
        //投递邮件
        mailboxExecutor.execute(
                () -> {
                    long endTime = SystemClock.getInstance().relativeTimeMillis();
                    long latency = endTime - startTime;
                    //计算延迟 (T2 - T1)，更新到 [latencyHistogram] 直方图,同时 [mailCounter] 自增，反映控制邮件的消费吞吐量
                    //DescriptiveStatisticsHistogram#update
                    latencyHistogram.update(latency);
                    scheduleLatencyMeasurement();
                },
                "Measure mailbox latency metric");
    }

    private void scheduleLatencyMeasurement() {
        assert timerService != null;
        // measurementInterval = 1000
        // registerTimer是 基于时间触发的定时任务  当系统时间（Processing Time）或水印时间（Event Time）达到该时间点时，Flink 会自动触发对应算子的定时器回调方法
        // 时间到了出触发 measureMailboxLatency 方法的执行
        // measureMailboxLatency 又会调用scheduleLatencyMeasurement  相当于递归调用
        timerService.registerTimer(
                timerService.getCurrentProcessingTime() + measurementInterval,
                //投递 Measure邮件
                timestamp -> measureMailboxLatency());
    }
}
