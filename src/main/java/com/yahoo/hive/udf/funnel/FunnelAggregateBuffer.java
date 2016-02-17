/*
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yahoo.hive.udf.funnel;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;

/**
 * Used to build funnel.
 */
class FunnelAggregateBuffer implements AggregationBuffer {
    /** List of actions. */
    List<Object> actions = new ArrayList<>();

    /** List of timestamps associated with actions. */
    List<Object> timestamps = new ArrayList<>();

    /** List of funnel steps. Funnel steps can have multiple funnels. */
    List<Set<Object>> funnelSteps = new ArrayList<Set<Object>>();

    /** Set of all funnels we are looking for. */
    Set<Object> funnelSet = new HashSet<>();

    /**
     * Serialize actions, timestamps, and funnel steps. Have to split funnel
     * steps with null. This is more efficient than passing around structs.
     *
     * @return List of objects
     */
    public List<Object> serialize() {
        List<Object> serialized = new ArrayList<>();
        serialized.add(actions);
        serialized.add(timestamps);
        // Need to do special logic for funnel steps
        List<Object> serializedFunnelSteps = new ArrayList<>();
        for (Set e : funnelSteps) {
            // Separate funnel steps with null
            serializedFunnelSteps.addAll(e);
            serializedFunnelSteps.add(null);
        }
        // Add the funnel steps
        serialized.add(serializedFunnelSteps);
        return serialized;
    }

    /**
     * Deserialize funnel steps. Have to deserialize the null separated list.
     */
    public void deserializeFunnel(List<Object> serializedFunnel) {
        // Have to "deserialize" from the null separated list
        Set<Object> funnelStepAccumulator = new HashSet<>();
        for (Object e : serializedFunnel) {
            // If not null
            if (e != null) {
                // Add to the step accumulator
                funnelStepAccumulator.add(e);
            } else {
                // Found a null, add the funnel step
                // Need to do a deep copy
                funnelSteps.add(new HashSet<>(funnelStepAccumulator));
                // Clear the set
                funnelStepAccumulator.clear();
            }
        }
    }

    /**
     * Clear the aggregate.
     */
    public void clear() {
        actions.clear();
        timestamps.clear();
        // TODO Might be able to remove these two and reuse, possible optimization
        funnelSteps.clear();
        funnelSet.clear();
    }
}
