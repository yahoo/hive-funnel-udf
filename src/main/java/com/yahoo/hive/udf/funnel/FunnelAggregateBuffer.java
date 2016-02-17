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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
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
        //funnelSteps.clear();
        //funnelSet.clear();
    }

    /**
     * Compute the funnel. Sort the actions by timestamp/action, then build the
     * funnel.
     * 
     * @return list of longs representing the funnel
     */
    public List<Long> computeFunnel() {
        // Create index, sort on timestamp/action
        Integer[] sortedIndex = IntStream.rangeClosed(0, actions.size() - 1)
                                         .boxed()
                                         .sorted(this::funnelAggregateComparator)
                                         .toArray(Integer[]::new);

        // Input size
        int inputSize = actions.size();

        // Stores the current index we are at for the funnel
        int currentFunnelStep = 0;

        // The last funnel index
        int funnelStepSize = funnelSteps.size();

        // Result funnel, all 0's at the start
        List<Long> results = new ArrayList<>(Collections.nCopies(funnelStepSize, 0L));

        // Check every sorted action until we reach the end of the funnel
        for (int i = 0; i < inputSize && currentFunnelStep < funnelStepSize; i++) {
            // Check if the current action is in the current funnel step
            if (funnelSteps.get(currentFunnelStep).contains(actions.get(sortedIndex[i]))) {
                // We have a match, output 1 for this funnel step
                results.set(currentFunnelStep, 1L);
                // Move to the next funnel step
                currentFunnelStep++;
            }
        }

        return results;
    }

    /**
     * Used for sorting array of integers according to funnel aggregate
     * timestamp/action columns. If timestamps match, uses action column.
     *
     * @param i1
     * @param i2
     * @param funnelAggregate
     * @return ordering of i1 and i2
     */
    private int funnelAggregateComparator(Integer i1, Integer i2) {
        int result = ((Comparable) timestamps.get(i1)).compareTo(timestamps.get(i2));
        // Match in timestamp, sort on action
        if (result == 0) {
            return ((Comparable) actions.get(i1)).compareTo(actions.get(i2));
        }
        return result;
    }
}
