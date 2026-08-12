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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Defines the chaining scheme for the operator. When an operator is chained to the predecessor, it
 * means that they run in the same thread. They become one operator consisting of multiple steps.
 *
 * <p>The default value used by the StreamOperator is {@link #ALWAYS}, which means that the operator
 * will be chained to predecessors whenever possible.
 */
//算子链链化策略 是决定当前算子（Operator）能否与上游或下游算子合并到同一个 Operator Chain（算子链）中的核心机制
@PublicEvolving
public enum ChainingStrategy {

    /**
     * Operators will be eagerly chained whenever possible.
     *
     * <p>To optimize performance, it is generally a good practice to allow maximal chaining and
     * increase operator parallelism.
     */
    //当前算子总是倾向于与上游和下游算子链接在一起
    ALWAYS,

    /** The operator will not be chained to the preceding or succeeding operators. */
    //禁止链化
    NEVER,

    /**
     * The operator will not be chained to the predecessor, but successors may chain to this
     * operator.
     */
    //当前算子不能与上游算子链化，但允许与下游算子链化
    HEAD,

    /**
     * This operator will run at the head of a chain (similar as in {@link #HEAD}, but it will
     * additionally try to chain source inputs if possible. This allows multi-input operators to be
     * chained with multiple sources into one task.
     */
    //其基本逻辑与 HEAD 类似（开启一个新链），但在特定条件下（例如多个 Source 连接到同一个物理节点或批处理优化时），它允许与特定的 Source 算子进行链化合并
    HEAD_WITH_SOURCES;

    //默认策略
    public static final ChainingStrategy DEFAULT_CHAINING_STRATEGY = ALWAYS;
}
