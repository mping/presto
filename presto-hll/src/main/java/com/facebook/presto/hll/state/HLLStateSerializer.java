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

package com.facebook.presto.hll.state;

import com.facebook.presto.hll.impl.HLL;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateSerializer;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

public class HLLStateSerializer
        implements AccumulatorStateSerializer<HLLState>
{
    @Override
    public Type getSerializedType()
    {
        return HLLType.HLL;
    }

    @Override
    public void serialize(HLLState state, BlockBuilder out)
    {
        if (state.getHLL() == null) {
            out.appendNull();
        }
        else {
            HLLType.HLL.writeSlice(out, state.getHLL().serialize());
        }
    }

    @Override
    public void deserialize(Block block, int index, HLLState state)
    {
        if (!block.isNull(index)) {
            Slice slice = block.getSlice(index, 0, block.getLength(index));
            state.setHLL(HLL.fromBytes(slice.getBytes()));
        }
    }
}
