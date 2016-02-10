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

class FunnelAggregateBuffer implements AggregationBuffer {
    List<Object> actions = new ArrayList<>();
    List<Object> timestamps = new ArrayList<>();
    List<Set<Object>> funnelSteps = new ArrayList<Set<Object>>();
    Set<Object> funnelSet = new HashSet<>();

    // Serialize actions, timestamps, and funnel steps
    public List<Object> serialize() {
        List<Object> serialized = new ArrayList<>();
        serialized.add(actions);
        serialized.add(timestamps);
        // Need to do special logic for funnelSteps
        List<Object> serializedFunnelSteps = new ArrayList<>();
        for (Set e : funnelSteps) {
            // Seperate funnel steps with null
            serializedFunnelSteps.addAll(e);
            serializedFunnelSteps.add(null);
        }
        // Add the funnel steps
        serialized.add(serializedFunnelSteps);
        return serialized;
    }
}
