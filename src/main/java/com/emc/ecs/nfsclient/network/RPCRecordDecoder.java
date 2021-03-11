/**
 * Copyright 2016-2018 Dell Inc. or its subsidiaries. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.emc.ecs.nfsclient.network;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * To receive the entire response. We do not actually decode the rpc packet here.
 * Just get the size from the packet and then put them in internal buffer until all data arrive.
 * 
 * @author seibed
 */
public class RPCRecordDecoder extends ByteToMessageDecoder {

    /**
     * Holds the calculated record length for each channel until the Channel is ready for buffering.
     * Reset to 0 after that for the next channel.
     */
    private int _recordLength = 0;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // Wait until the length prefix is available.
        if (in.readableBytes() < 4) {
            // If null is returned, it means there is not enough data yet.
            // FrameDecoder will call again when there is a sufficient amount of data available.
            return;
        }

        //marking the current reading position
        in.markReaderIndex();

        //get the fragment size and wait until the entire fragment is available.
        long fragSize = in.readUnsignedInt();
        boolean lastFragment = RecordMarkingUtil.isLastFragment(fragSize);
        fragSize = RecordMarkingUtil.maskFragmentSize(fragSize);
        if (in.readableBytes() < fragSize) {
            in.resetReaderIndex();
            return;
        }

        //seek to the beginning of the next fragment
        in.skipBytes((int) fragSize);

        _recordLength += 4 + (int) fragSize;

        //check the last fragment
        if (!lastFragment) {
            //not the last fragment, the data is put in an internally maintained cumulative buffer
            return;
        }

        byte[] rpcResponse = new byte[_recordLength];
        in.readerIndex(in.readerIndex() - _recordLength);
        in.readBytes(rpcResponse, 0, _recordLength);

        _recordLength = 0;
        out.add(rpcResponse);
    }
}
