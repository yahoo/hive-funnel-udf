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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
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
             value = "_FUNC_(action_column, timestamp_column, funnel_1, funnel_2, ...) - Builds a funnel report applied to the action_column. Funnels can be lists or scalars. Should be used with merge_funnel UDF.",
             extended = "Example: SELECT funnel(action, timestamp, array('signup_page', 'email_signup'), \n" +
                        "                                          'confirm_button',\n" +
                        "                                          'submit_button') AS funnel\n" +
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
            switch (parameters[i].getCategory()) {
                case LIST:
                    // Check that the list is of primitives of the same type as the action column
                    TypeInfo typeInfo = ((ListTypeInfo) parameters[i]).getListElementTypeInfo();
                    if (typeInfo.getCategory() != ObjectInspector.Category.PRIMITIVE || ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory() != actionColumnCategory) {
                        throw new UDFArgumentTypeException(i, "Funnel list parameter " + Integer.toString(i) + " of type " + parameters[i].getTypeName() + " does not match expected type " + parameters[0].getTypeName() + ".");
                    }
                    break;
                case PRIMITIVE:
                    if (((PrimitiveTypeInfo) parameters[i]).getPrimitiveCategory() != actionColumnCategory) {
                        throw new UDFArgumentTypeException(i, "Funnel list parameter " + Integer.toString(i) + " of type " + parameters[i].getTypeName() + " does not match expected type " + parameters[0].getTypeName() + ".");
                    }
                    break;
                default:
                    throw new UDFArgumentTypeException(i, "Funnel list parameter " + Integer.toString(i) + " of type " + parameters[i].getTypeName() + " should be a list or a scalar.");
            }
        }

        return new FunnelEvaluator();
    }

    public static class FunnelEvaluator extends GenericUDAFEvaluator {
        /** For PARTIAL1 and COMPLETE. */
        private ObjectInspector actionObjectInspector;

        /** For PARTIAL1 and COMPLETE. */
        private ObjectInspector timestampObjectInspector;

        /** For PARTIAL1 and COMPLETE. */
        private ListObjectInspector funnelObjectInspector;

        /** For PARTIAL2 and FINAL. */
        private StandardStructObjectInspector internalMergeObjectInspector;

        /** Action key constant. */
        private static final String ACTION = "action";

        /** Timestamp key constant. */
        private static final String TIMESTAMP = "timestamp";

        /** Funnel key constant. */
        private static final String FUNNEL = "funnel";

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            // Setup the object inspectors and return type
            switch (m) {
                case PARTIAL1:
                    // Get the object inspectors
                    actionObjectInspector = parameters[0];
                    timestampObjectInspector = parameters[1];
                    funnelObjectInspector = (ListObjectInspector) parameters[2];

                    // The field names for the struct, order matters
                    List<String> fieldNames = Arrays.asList(ACTION, TIMESTAMP, FUNNEL);

                    // The field inspectors for the struct, order matters
                    List<ObjectInspector> fieldInspectors = Arrays.asList(actionObjectInspector, timestampObjectInspector, actionObjectInspector)
                                                                  .stream()
                                                                  .map(ObjectInspectorUtils::getStandardObjectInspector)
                                                                  .map(ObjectInspectorFactory::getStandardListObjectInspector)
                                                                  .collect(Collectors.toList());

                    // Will output structs
                    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldInspectors);
                case PARTIAL2:
                    // Get the struct object inspector
                    internalMergeObjectInspector = (StandardStructObjectInspector) parameters[0];

                    // Will output structs
                    return internalMergeObjectInspector;
                case FINAL:
                    // Get the struct object inspector
                    internalMergeObjectInspector = (StandardStructObjectInspector) parameters[0];

                    // Will output list of longs
                    return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
                case COMPLETE:
                    // Get the object inspectors
                    actionObjectInspector = parameters[0];
                    timestampObjectInspector = parameters[1];
                    funnelObjectInspector = (ListObjectInspector) parameters[2];

                    // Will output list of longs
                    return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
                default:
                    throw new HiveException("Unknown Mode: " + m.toString());
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new FunnelAggregateBuffer();
        }


        /**
         * Adds funnel steps to the aggregate. Funnel steps can be lists or
         * scalars.
         *
         * @param funnelAggregate
         * @param parameters
         */
        private void addFunnelSteps(FunnelAggregateBuffer funnelAggregate, Object[] parameters) {
            Arrays.stream(parameters)
                  .map(this::convertFunnelStepObjectToList)
                  .map(this::removeNullFromList)
                  .filter(this::isNotEmpty)
                  .forEach(funnelStep -> {
                          funnelAggregate.funnelSteps.add(new HashSet<Object>(funnelStep));
                          funnelAggregate.funnelSet.addAll(funnelStep);
                      });
        }

        @Override
        public void iterate(AggregationBuffer aggregate, Object[] parameters) throws HiveException {
            FunnelAggregateBuffer funnelAggregate = (FunnelAggregateBuffer) aggregate;

            // Add the funnel steps if not already stored
            if (funnelAggregate.funnelSteps.isEmpty()) {
                // Funnel steps start at index 2
                addFunnelSteps(funnelAggregate, Arrays.copyOfRange(parameters, 2, parameters.length));
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

        /**
         * Given a struct and a key, look the key up in the struct with the
         * merge object inspector.
         *
         * @param object Struct object
         * @param key Key to look up
         */
        private Object structLookup(Object object, String key) {
            return internalMergeObjectInspector.getStructFieldData(object, internalMergeObjectInspector.getStructFieldRef(key));
        }

        @Override
        public void merge(AggregationBuffer aggregate, Object partial) throws HiveException {
            FunnelAggregateBuffer funnelAggregate = (FunnelAggregateBuffer) aggregate;

            // Lists for partial data
            List<Object> partialActionList = toList(structLookup(partial, ACTION));
            List<Object> partialTimestampList = toList(structLookup(partial, TIMESTAMP));

            // If we don't have any funnel steps stored, then we should copy the funnel steps from the partial list
            if (funnelAggregate.funnelSteps.isEmpty()) {
                List<Object> partialFunnelList = toList(structLookup(partial, FUNNEL));
                funnelAggregate.deserializeFunnel(partialFunnelList);
            }

            // Add all the partial actions and timestamps to the end of the lists
            funnelAggregate.actions.addAll(partialActionList);
            funnelAggregate.timestamps.addAll(partialTimestampList);
        }

        @Override
        public void reset(AggregationBuffer aggregate) throws HiveException {
            FunnelAggregateBuffer funnelAggregate = (FunnelAggregateBuffer) aggregate;
            funnelAggregate.clear();
        }


        @Override
        public Object terminate(AggregationBuffer aggregate) throws HiveException {
            FunnelAggregateBuffer funnelAggregate = (FunnelAggregateBuffer) aggregate;
            return funnelAggregate.computeFunnel();
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregate) throws HiveException {
            FunnelAggregateBuffer funnelAggregate = (FunnelAggregateBuffer) aggregate;
            return funnelAggregate.serialize();
        }

        /**
         * Convert object to list of funnels for a funnel step.
         * 
         * @parameter
         * @return List of funnels in funnel step
         */
        private List<Object> convertFunnelStepObjectToList(Object parameter) {
            if (parameter instanceof List) {
                return (List<Object>) funnelObjectInspector.getList(parameter);
            } else {
                return Arrays.asList(ObjectInspectorUtils.copyToStandardObject(parameter, funnelObjectInspector.getListElementObjectInspector()));
            }
        }

        /**
         * Returns true if list if not empty.
         *
         * @param list
         * @return True if list not empty
         */
        private boolean isNotEmpty(List<Object> list) {
            return !list.isEmpty();
        }

        /**
         * Removes null values from list.
         *
         * @param list
         * @return List without null values
         */
        private List<Object> removeNullFromList(List<Object> list) {
            return list.stream()
                       .filter(Objects::nonNull)
                       .collect(Collectors.toList());
        }

        /**
         * Cast an object to a list. Checks if it is a LazyBinaryArray or a
         * regular list.
         *
         * @param object Input object to try and cast to a list.
         * @return List of objects.
         */
        private List<Object> toList(Object object) {
            List<Object> result;
            if (object instanceof LazyBinaryArray) {
                result = ((LazyBinaryArray) object).getList();
            } else {
                result = (List<Object>) object;
            }
            return result;
        }
    }
}
