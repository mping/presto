/*
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
package com.facebook.presto.hll;

import com.facebook.presto.hll.impl.HLL;
import com.facebook.presto.hll.impl.RegisterSet;
import com.facebook.presto.hll.state.HLLState;
import com.facebook.presto.operator.aggregation.AggregationCompiler;
import com.facebook.presto.operator.aggregation.InputFunction;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.aggregation.CombineFunction;
import com.facebook.presto.operator.aggregation.OutputFunction;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.SqlType;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.io.IOException;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_INT;

/**
 * TODO use ESTIMATOR and Murmur3 if the hashes are compatible
 */
@AggregationFunction("hllagg")
public final class HLLAggregation
{
    public static final InternalAggregationFunction VARBINARY_HLL_AGGREGATIONS = new AggregationCompiler().generateAggregationFunction(HLLAggregation.class, BIGINT, ImmutableList.<Type>of(VARCHAR));
    public static final InternalAggregationFunction ARRAY_BIGINT_HLL_AGGREGATIONS = new AggregationCompiler().generateAggregationFunction(HLLAggregation.class, BIGINT, ImmutableList.<Type>of(new ArrayType(BIGINT)));

    private static final JsonFactory JSON_FACTORY = new JsonFactory().disable(CANONICALIZE_FIELD_NAMES);

    private static final int FACTOR = 10;
    private static final int COUNT = (int) Math.pow(2, FACTOR);

    private HLLAggregation()
    {
    }

    //https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types

    @InputFunction
    public static void inputVarchar(HLLState state, @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        // presto seems to be "reusing" the same state for a different slice
        // for queries without 'group by' clause > (select hllagg(col) from table);
        mergeHLLs(state, fromVarcharSlice(value));

    }

    @InputFunction
    public static void inputArrayBigint(HLLState state, @SqlType("array<bigint>") Slice value)
    {
        mergeHLLs(state, fromArraySlice(value));
    }

    private static void mergeHLLs(HLLState state, HLL sliceHLL)
    {
        HLL stateHLL = state.getHLL();
        HLL merged = (stateHLL == null) ? sliceHLL : (HLL) stateHLL.merge(sliceHLL);
        state.setHLL(merged);
    }

    @VisibleForTesting
    static int parseIntNoCheck(final String s)
    {
        // Check for a sign.
        int num = 0;
        int sign = -1;
        final int len = s.length();
        final char ch = s.charAt(0);
        //always positive
        //if ( ch == '-' )
        //    sign = 1;
        //else
        num = '0' - ch;

        int i = 1;
        //dunno why but scalding maps numbers to floats with decimal
        //should fix that
        while (i < len && s.charAt(i) != '.') {
            num = num * 10 + '0' - s.charAt(i++);
        }

        return sign * num;
    }

    @VisibleForTesting
    static HLL fromVarcharSlice(Slice value)
    {
        //@SqlType("array<bigint>")
        String[] values = new String(value.getBytes()).split(",");
        int[] ints = new int[values.length];
        for (int i = 0; i < values.length; i++) {
            ints[i] = parseIntNoCheck(values[i]); //Integer.parseInt(values[i]);
        }
        RegisterSet rs = new RegisterSet(COUNT, ints);
        HLL hll = new HLL(FACTOR, rs);
        return hll;
    }

    @VisibleForTesting
    static HLL fromArraySlice(Slice value)
    {
        // copied from JsonFunctions, should probably optimizegst
        //TODO: untested
        int[] nums = new int[171]; //TODO refactor to allow variable size
        try (JsonParser parser = JSON_FACTORY.createJsonParser(value.getInput())) {
            if (parser.nextToken() != START_ARRAY) {
                return null;   //error
            }

            int pos = 0;
            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    break;
                    //return false;
                }
                parser.skipChildren();

                // we allow only ints and longs, longs shouldn't really be necessary
                if ((token == VALUE_NUMBER_INT) &&
                        ((parser.getNumberType() == JsonParser.NumberType.INT) || (parser.getNumberType() == JsonParser.NumberType.LONG))) {
                    nums[pos++] = parser.getIntValue(); //TODO overflow
                }
            }
        }
        catch (IOException e) {
            return null;
        }

        RegisterSet rs = new RegisterSet(COUNT, nums);
        HLL hll = new HLL(FACTOR, rs);
        return hll;
    }

    @CombineFunction
    public static void combine(HLLState state, HLLState otherState)
    {
        HLL input = otherState.getHLL();
        HLL previous = state.getHLL();

        if (previous == null) {
            state.setHLL(input);
            state.addMemoryUsage(input.sizeof());
        }
        else {
            state.addMemoryUsage(-previous.sizeof());
            previous = (HLL) previous.merge(input);
            state.setHLL(previous);
            state.addMemoryUsage(previous.sizeof());
        }
    }

    @OutputFunction(value = StandardTypes.BIGINT)
    public static void output(HLLState state, BlockBuilder out)
    {
        if (state.getHLL() != null) {
            BIGINT.writeLong(out, state.getHLL().cardinality());
        }
        else {
            BIGINT.writeLong(out, 0);
        }
    }
}
