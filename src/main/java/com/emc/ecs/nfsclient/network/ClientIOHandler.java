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

import com.emc.ecs.nfsclient.rpc.Xdr;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.NotYetConnectedException;

/**
 * Each TCP connection has a corresponding ClientIOHandler instance.
 * ClientIOHandler includes the handler to receiving data and connection
 * establishment
 * 
 * @author seibed
 */
public class ClientIOHandler extends ChannelInboundHandlerAdapter {

    /**
     * The usual logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ClientIOHandler.class);

    /**
     * The connection instance
     */
    private final Connection _connection;

    /**
     * The only constructor.
     * 
     * @param bootstrap
     *            A Netty helper instance.
     */
    public ClientIOHandler(Connection connection) {
        _connection = connection;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Connected to: {}", ctx.channel().remoteAddress());
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        LOG.warn("Channel inactive: {}", ctx.channel().remoteAddress());
        _connection.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        byte[] rpcResponse = (byte[]) msg;
        // remove marking
        Xdr x = RecordMarkingUtil.removeRecordMarking(rpcResponse);
        // remove the request from timeout manager map
        _connection.notifySender(x.getXid(), x);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // do not print exception if it is BindException.
        // we are trying to search available port below 1024. It is not good to
        // print a flood
        // of error logs during the searching.
        if (cause instanceof java.net.BindException) {
            return;
        }

        LOG.error("Exception on connection to " + ctx.channel().remoteAddress(), cause.getCause());
        ctx.channel().close();
    }
}
