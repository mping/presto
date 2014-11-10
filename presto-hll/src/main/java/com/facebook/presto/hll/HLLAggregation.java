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
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.aggregation.CombineFunction;
import com.facebook.presto.operator.aggregation.InputFunction;
import com.facebook.presto.operator.aggregation.OutputFunction;
import com.facebook.presto.operator.aggregation.AggregationCompiler;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

/**
 * TODO use ESTIMATOR and Murmur3 if the hashes are compatible
 */
@AggregationFunction("hllagg")
public final class HLLAggregation
{
    //    public static final InternalAggregationFunction LONG_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS = new AggregationCompiler().generateAggregationFunction(ApproximateCountDistinctAggregations.class, BIGINT, ImmutableList.<Type>of(BIGINT));
    //    public static final InternalAggregationFunction DOUBLE_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS = new AggregationCompiler().generateAggregationFunction(ApproximateCountDistinctAggregations.class, BIGINT, ImmutableList.<Type>of(DOUBLE));
    public static final InternalAggregationFunction VARBINARY_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS = new AggregationCompiler().generateAggregationFunction(HLLAggregation.class, BIGINT, ImmutableList.<Type>of(VARCHAR));
    //@SqlType("array<bigint>")

    private static final int FACTOR = 10;
    private static final int COUNT = (int) Math.pow(2, FACTOR);

    private HLLAggregation()
    {
    }

    @InputFunction
    public static void input(HLLState state, @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        state.setHLL(fromVarcharSlice(value));
    }

    static HLL fromVarcharSlice(Slice value)
    {
        String[] values = new String(value.getBytes()).split(",");
        int[] ints = new int[values.length];
        for (int i = 0; i < values.length; i++) {
            ints[i] = Integer.valueOf(values[i]);
        }
        RegisterSet rs = new RegisterSet(COUNT, ints);
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
