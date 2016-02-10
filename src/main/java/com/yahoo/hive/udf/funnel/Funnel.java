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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

@UDFType(deterministic = true)
@Description(name = "funnel",
             value = "_FUNC_(action_column, timestamp_column, funnel_1, funnel_2, ...) - Builds a funnel report applied to the action_column. Funnels are lists. Should be used with merge_funnel UDF.",
             extended = "Example: SELECT funnel(action, timestamp, array('signup_page', 'email_signup'), \n" +
                        "                                          array('confirm_button'),\n" +
                        "                                          array('submit_button')) AS funnel\n" +
                        "         FROM table\n" +
                        "         GROUP BY user_id;")
public class Funnel extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(Funnel.class.getName());

    @Override
    public FunnelEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        // Get the parameters
        TypeInfo [] parameters = info.getParameters();

        // Check number of arguments
        if (parameters.length < 3) {
            throw new UDFArgumentLengthException("Please specify the action column, the timestamp column, and at least one funnel.");
        }

        // Check the action_column type and enforce that all funnel steps are the same type
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but " + parameters[0].getTypeName() + " was passed.");
        }
        PrimitiveCategory actionColumnCategory = ((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory();

        // Check the timestamp_column type
        if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(1, "Only primitive type arguments are accepted but " + parameters[0].getTypeName() + " was passed.");
        }

        // Check that all funnel steps are the same type as the action_column
        for (int i = 2; i < parameters.length; i++) {
            // Check that the parameter is a list
            if (parameters[i].getCategory() != ObjectInspector.Category.LIST) {
                throw new UDFArgumentTypeException(i, "Funnel parameter " + Integer.toString(i) + " of type " + parameters[i].getTypeName() + ", it should be a list.");
            }
            // Get list type info
            TypeInfo typeInfo = ((ListTypeInfo) parameters[i]).getListElementTypeInfo();
            if (typeInfo.getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i, "Funnel list parameter " + Integer.toString(i) + " of type " + parameters[i].getTypeName() + " does not match expected type " + parameters[0].getTypeName() + ".");
            }
            // Check that the list type matches the action column
            if (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory() != actionColumnCategory) {
                throw new UDFArgumentTypeException(i, "Funnel list parameter " + Integer.toString(i) + " of type " + parameters[i].getTypeName() + " does not match expected type " + parameters[0].getTypeName() + ".");
            }
        }

        return new FunnelEvaluator();
    }

    public static class FunnelEvaluator extends GenericUDAFEvaluator {
        //// For PARTIAL1 and COMPLETE
        private ObjectInspector actionObjectInspector;
        private ObjectInspector timestampObjectInspector;
        private ListObjectInspector funnelObjectInspector;

        //// For PARTIAL2 and FINAL
        private StandardStructObjectInspector internalMergeObjectInspector;

        // Constants for internal struct
        private static final String ACTION = "action";
        private static final String TIMESTAMP = "timestamp";
        private static final String FUNNEL = "funnel";

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            // Setup the object inspectors and return type
            switch (m) {
                case PARTIAL1:
                    actionObjectInspector = parameters[0];
                    timestampObjectInspector = parameters[1];
                    funnelObjectInspector = (ListObjectInspector) parameters[2];
                    List<String> fieldNames = new ArrayList<>();
                    fieldNames.add(ACTION);
                    fieldNames.add(TIMESTAMP);
                    fieldNames.add(FUNNEL);
                    List<ObjectInspector> fieldInspectors = new ArrayList<>();
                    fieldInspectors.add(ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(actionObjectInspector)));
                    fieldInspectors.add(ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(timestampObjectInspector)));
                    fieldInspectors.add(ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(actionObjectInspector))); // Want lazystring, not text
                    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldInspectors);
                case PARTIAL2:
                    internalMergeObjectInspector = (StandardStructObjectInspector) parameters[0];
                    return internalMergeObjectInspector;
                case FINAL:
                    internalMergeObjectInspector = (StandardStructObjectInspector) parameters[0];
                    return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
                case COMPLETE:
                    actionObjectInspector = parameters[0];
                    timestampObjectInspector = parameters[1];
                    funnelObjectInspector = (ListObjectInspector) parameters[2];
                    return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
                default:
                    throw new HiveException("Unknown Mode: " + m.toString());
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new FunnelAggregateBuffer();
        }

        @Override
        public void iterate(AggregationBuffer aggregate, Object[] parameters) throws HiveException {
            FunnelAggregateBuffer funnelAggregate = (FunnelAggregateBuffer) aggregate;

            // Add the funnel steps if not alread in funnelAggregate
            if (funnelAggregate.funnelSteps.isEmpty()) {
                for (int i = 2; i < parameters.length; i++) {
                    // Get the funnel list for this step
                    List<Object> funnelStep = (List<Object>) funnelObjectInspector.getList(parameters[i]);
                    // Check if the funnel step is null
                    if (funnelStep != null) {
                        // Remove all nulls from list
                        funnelStep.removeAll(Collections.singleton(null));
                        // If there are values in the funnel
                        if (!funnelStep.isEmpty()) {
                            // Add the funnel steps to the funnel list
                            funnelAggregate.funnelSteps.add(new HashSet<Object>(funnelStep));
                            // Also add all actions in the funnel step to the total funnel set
                            funnelAggregate.funnelSet.addAll(funnelStep);
                        }
                    }
                }
            }

            // Get the action_column value and add it (if it matches a funnel)
            Object action = parameters[0];
            Object timestamp = parameters[1];
            if (action != null && timestamp != null) {
                // Get the action value
                Object actionValue = ObjectInspectorUtils.copyToStandardObject(action, actionObjectInspector);
                // Get the timestamp value
                Object timestampValue = ObjectInspectorUtils.copyToStandardObject(timestamp, timestampObjectInspector);

                // If the action is not null and it is one of the funnels we are looking for, keep it
                if (actionValue != null && timestampValue != null && funnelAggregate.funnelSet.contains(actionValue)) {
                    funnelAggregate.actions.add(actionValue);
                    funnelAggregate.timestamps.add(timestampValue);
                }
            }
        }

        @Override
        public void merge(AggregationBuffer aggregate, Object partial) throws HiveException {
            FunnelAggregateBuffer funnelAggregate = (FunnelAggregateBuffer) aggregate;

            // Get the partial data
            Object partialAction = internalMergeObjectInspector.getStructFieldData(partial, internalMergeObjectInspector.getStructFieldRef(ACTION));
            Object partialTimestamp = internalMergeObjectInspector.getStructFieldData(partial, internalMergeObjectInspector.getStructFieldRef(TIMESTAMP));
            Object partialFunnel = internalMergeObjectInspector.getStructFieldData(partial, internalMergeObjectInspector.getStructFieldRef(FUNNEL));

            // Lists for partial data
            List<Object> partialActionList;
            List<Object> partialTimestampList;

            // Get the partial action list
            if (partialAction instanceof LazyBinaryArray) {
                partialActionList = ((LazyBinaryArray) partialAction).getList();
            } else {
                partialActionList = (List<Object>) partialAction;
            }

            // Get the partial timestamp list
            if (partialTimestamp instanceof LazyBinaryArray) {
                partialTimestampList = ((LazyBinaryArray) partialTimestamp).getList();
            } else {
                partialTimestampList = (List<Object>) partialTimestamp;
            }

            // If we don't have any funnel steps stored, then we should copy the funnel steps from the partial list
            if (funnelAggregate.funnelSteps.isEmpty()) {
                List<Object> partialFunnelList;

                // Get the partial funnel list
                if (partialFunnel instanceof LazyBinaryArray) {
                    partialFunnelList = ((LazyBinaryArray) partialFunnel).getList();
                } else {
                    partialFunnelList = (List<Object>) partialFunnel;
                }

                // Have to "deserialize" from the null separated list
                Set<Object> funnelStepAccumulator = new HashSet<>();
                for (Object e : partialFunnelList) {
                    if (e == null) {
                        // Add the funnel step, need to do a deep copy
                        funnelAggregate.funnelSteps.add(new HashSet<>(funnelStepAccumulator));
                        // Clear the set
                        funnelStepAccumulator.clear();
                    } else {
                        // Add to the step accumulator
                        funnelStepAccumulator.add(e);
                    }
                }
            }

            // Add all the actions in partial to the end of the actions list
            funnelAggregate.actions.addAll(partialActionList);
            // Add all the timestamps in partial to the end of the timestamps list
            funnelAggregate.timestamps.addAll(partialTimestampList);
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            ((FunnelAggregateBuffer) buff).actions.clear();
            ((FunnelAggregateBuffer) buff).timestamps.clear();
            ((FunnelAggregateBuffer) buff).funnelSteps.clear();
            ((FunnelAggregateBuffer) buff).funnelSet.clear();
        }

        @Override
        public Object terminate(AggregationBuffer aggregate) throws HiveException {
            final FunnelAggregateBuffer funnelAggregate = (FunnelAggregateBuffer) aggregate;

            // Create index for sorting on timestamp/action
            Integer[] sortedIndex = new Integer[funnelAggregate.actions.size()];
            for (int i = 0 ; i < sortedIndex.length; i++) {
                sortedIndex[i] = i;
            }

            // Sort index
            Arrays.sort(sortedIndex, new Comparator<Integer>() {
                public int compare(Integer i1, Integer i2) {
                    int result = ((Comparable) funnelAggregate.timestamps.get(i1)).compareTo(funnelAggregate.timestamps.get(i2));
                    // Match in timestamp, sort on action
                    if (result == 0) {
                        return ((Comparable) funnelAggregate.actions.get(i1)).compareTo(funnelAggregate.actions.get(i2));
                    }
                    return result;
                }
            });

            // Input size
            int inputSize = funnelAggregate.actions.size();

            // Stores the current index we are at for the funnel
            int currentFunnelStepIndex = 0;

            // The last funnel index
            int funnelStepSize = funnelAggregate.funnelSteps.size();

            // Result funnel, all 0's at the start
            List<Long> results = new ArrayList<>(Collections.nCopies(funnelStepSize, 0L));

            // Check every sorted action until we reach the end of the funnel
            for (int i = 0; i < inputSize && currentFunnelStepIndex < funnelStepSize; i++) {
                // Check if the current action is in the current funnel step
                if (funnelAggregate.funnelSteps.get(currentFunnelStepIndex).contains(funnelAggregate.actions.get(sortedIndex[i]))) {
                    // We have a match, output 1 for this funnel step
                    results.set(currentFunnelStepIndex, 1L);
                    // Move to the next funnel step
                    currentFunnelStepIndex++;
                }
            }

            return results;
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregate) throws HiveException {
            FunnelAggregateBuffer funnelAggregate = (FunnelAggregateBuffer) aggregate;
            return funnelAggregate.serialize();
        }
    }
}
