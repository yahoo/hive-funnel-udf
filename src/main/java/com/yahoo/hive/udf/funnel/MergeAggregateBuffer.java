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
import java.util.List;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;

/**
 * Merges funnels into an aggregate.
 */
class MergeAggregateBuffer implements AggregationBuffer {
    /** Stores funnel aggregate. */
    List<Long> elements = new ArrayList<>();

    /**
     * Add a funnel to the aggregate.
     *
     * @param funnel Funnel in the form of a list of longs.
     */
    public void addFunnel(List<Long> funnel) throws HiveException {
        // If empty, just copy elements
        if (elements.isEmpty()) {
            elements.addAll(funnel);
        } else {
            // If the sizes don't match, throw an exception
            if (elements.size() != funnel.size()) {
                throw new UDFArgumentTypeException(0, "Funnels must be of the same size to merge!");
            }
            // Not empty, merge with existing list
            for (int i = 0; i < funnel.size(); i++) {
                elements.set(i, (funnel.get(i) + elements.get(i)));
            }
        }
    }

    /**
     * Clear the aggregate.
     */
    public void clear() {
        elements.clear();
    }

    /**
     * Output aggregate. Must do a deep copy, and return a new list.
     *
     * @return Funnel aggregate counts.
     */
    public List<Long> output() {
        List<Long> values = new ArrayList<Long>(elements.size());
        values.addAll(elements);
        return values;
    }
}
