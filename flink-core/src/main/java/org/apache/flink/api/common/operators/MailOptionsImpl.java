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

package org.apache.flink.api.common.operators;

import org.apache.flink.annotation.Internal;

/** Options to configure behaviour of executing mailbox mails. */
@Internal
public class MailOptionsImpl implements MailboxExecutor.MailOptions {

    //默认 邮件会被放入 queue 队列的尾部（Tail），严格遵循先进先出（FIFO）的原则
    static final MailboxExecutor.MailOptions DEFAULT = new MailOptionsImpl(false, false);
    //可延迟 如果当前主线程非常忙（比如正在疯狂处理上游积压的数据，或者队列里还有很多普通邮件），这封邮件会被暂时“挂起”或放到最后执行
    static final MailboxExecutor.MailOptions DEFERRABLE = new MailOptionsImpl(false, true);
    //紧急。底层 TaskMailboxImpl 会绕过普通的 FIFO 排队规则，直接把这封邮件插入到 queue 队列的头部（Head）
    static final MailboxExecutor.MailOptions URGENT = new MailOptionsImpl(true, false);

    private final boolean isUrgent;
    private final boolean deferrable;

    private MailOptionsImpl(boolean isUrgent, boolean deferrable) {
        this.isUrgent = isUrgent;
        this.deferrable = deferrable;
    }

    @Override
    public boolean isDeferrable() {
        return deferrable;
    }

    @Override
    public boolean isUrgent() {
        return isUrgent;
    }
}
