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
package com.facebook.presto.hll.impl;

import com.google.common.base.Joiner;

import java.io.Serializable;
import java.util.Arrays;

public class RegisterSet implements Serializable
{
    public static final int REGISTER_SIZE = 5; /* Each register has a size of log2(32) = 5 bits */
    public static final int LOG2_BITS_PER_WORD = 6; /* 32bits / 5 ?? */

    public final int count; /* Number of bits for register selection */
    public final int size; /* Number of registers */

    final int[] joined; /* Registers (joined) */

    public RegisterSet(int count)
    {
        this(count, null);
    }

    public RegisterSet(int count, int[] initialValues)
    {
        this.count = count;
        int bits = getBits(count);

        if (initialValues == null) {
            if (bits == 0) {
                // Only one register is required
                this.joined = new int[1];
            }
            else if (bits % Integer.SIZE == 0) {
                // All 32 registers are required
                this.joined = new int[bits];
            }
            else {
                this.joined = new int[bits + 1];
            }
        }
        else {
            this.joined = initialValues; // Initiates registers with specific values
        }
        this.size = this.joined.length;
    }

    public static int getBits(int count)
    {
        return (int) Math.floor(count / LOG2_BITS_PER_WORD);
    }

    public void set(int position, int value) // Sets register[position] to value
    {
        int bucketPos = (int) Math.floor(position / LOG2_BITS_PER_WORD);
        int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
        this.joined[bucketPos] = (this.joined[bucketPos] & ~(0x1f << shift)) | (value << shift);
    }

    public int get(int position) // Gets register[position]
    {
        int bucketPos = (int) Math.floor(position / LOG2_BITS_PER_WORD);
        int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
        return (this.joined[bucketPos] & (0x1f << shift)) >>> shift;
    }

    public int[] bits()
    {
        int[] copy = new int[size];
        System.arraycopy(joined, 0, copy, 0, joined.length);
        return copy;
    }

    @Override
    public String toString()
    {
        return Joiner.on(",").join(Arrays.asList(joined));
    }

    public static void main(String[] args)
    {
        RegisterSet rs = new RegisterSet(10, new int[]{0, 0, 0, 0, 5120, 0, 160, 2049, 1024, 0, 67108864, 33554432, 0, 1024, 0, 0, 2097152, 0, 2, 0, 2, 96, 0, 1, 0, 4194304, 0, 33554432, 0, 0, 0, 0, 6292512, 33557504, 0, 0, 98304, 32768, 2, 0, 0, 1026, 1024, 0, 33554432, 32768, 0, 1048576, 1048576, 0, 4, 98304, 0, 0, 0, 3145728, 33554432, 0, 0, 65665, 0, 96, 0, 0, 100663296, 32, 65632, 0, 33554433, 0, 0, 0, 1024, 34603008, 0, 5, 33685504, 0, 67109888, 0, 1, 3, 67108896, 0, 65536, 33792, 0, 0, 0, 0, 2097152, 0, 0, 168822784, 0, 0, 4, 3145728, 32, 33554432, 0, 1048576, 0, 11264, 100663296, 3145728, 0, 3145728, 1024, 0, 33554432, 0, 35840, 98336, 131074, 0, 0, 0, 2, 33554432, 3072, 1048576, 1048640, 0, 0, 1024, 3072, 1048578, 1, 1024, 0, 4096, 67108864, 0, 1024, 0, 0, 0, 67108864, 32768, 33554432, 0, 33792, 69206016, 1048576, 1048576, 0, 0, 0, 67108864, 134218752, 1048576, 0, 0, 32, 32768, 3145728, 0, 0, 0, 0, 0, 0, 0, 0, 131072, 1, 4096, 0, 0, 0});
        for (int i = 0; i < rs.size; i++) {
            System.out.print(rs.get(i) + ",");
        }
    }
}
