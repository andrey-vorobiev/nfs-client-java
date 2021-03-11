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

import com.emc.ecs.nfsclient.rpc.RpcException;
import com.emc.ecs.nfsclient.rpc.RpcStatus;
import com.emc.ecs.nfsclient.rpc.Xdr;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Each Connection instance manages a tcp connection. The class is used to send
 * data and track the status of a connection.
 * 
 * @author seibed
 */
public class Connection {
    /**
     * The usual logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(Connection.class);

    /**
     * The value in milliseconds.
     */
    private static final int CONNECT_TIMEOUT = 10000; // 10 seconds

    /**
     * internal sending queue (not tcp sending buffer) the rename/lookup/readdir
     * has size between 256-516bytes, 5M can hold 10000 pending requests for
     * data writing, each request is 512K and maximum 40 pending requests are
     * allowed. (512k is the preferred size in a single request. For object >
     * 512K, it should be split to 512K-size chunks.) the queue size need be
     * 20M.
     */
    private static final int MAX_SENDING_QUEUE_SIZE = 1 * 1024 * 1024 * 1024;// bytes - total
                                                                // 1G

    /**
     * Netty helper instance.
     */
    private final Bootstrap _bootstrap;

    /**
     * Netty channel representing a tcp connection.
     */
    private Channel _channel;

    /**
     * The remote server address, in any form.
     */
    private final String _remoteHost;

    /**
     * The remote server port being used.
     */
    private final int _remotePort;

    /**
     * <ul><li>
     * If <code>true</code>, use a privileged local port (below 1024) for RPC communication.
     * </li><li>
     * If <code>false</code>, use any non-privileged local port for RPC communication.
     * </li></ul>
     */
    private final boolean _usePrivilegedPort;

    /**
     * Store the ChannelFuture instances while they are in progress. The map is final, but the content will change.
     */
    private final ConcurrentHashMap<Integer, ChannelPromise> _futureMap = new ConcurrentHashMap<>();

    /**
     * Store the Xdr response instances while they are in progress. The map is final, but the content will change.
     */
    private final ConcurrentHashMap<Integer, Xdr> _responseMap = new ConcurrentHashMap<>();

    /**
     * @param remoteHost A unique name for the host to which the connection is being made.
     * @param rmotePort The remote host port being used for the connection.
     * @param usePrivilegedPort
     *            <ul>
     *            <li>If <code>true</code>, use a privileged port (below 1024)
     *            for RPC communication.</li>
     *            <li>If <code>false</code>, use any non-privileged port for RPC
     *            communication.</li>
     *            </ul>
     */
    public Connection(String remoteHost, int rmotePort, boolean usePrivilegedPort, EventLoopGroup workerGroup) {
        _remoteHost = remoteHost;
        _remotePort = rmotePort;
        _usePrivilegedPort = usePrivilegedPort;
        _bootstrap = new Bootstrap();
        _bootstrap.group(workerGroup);
        _bootstrap.option(ChannelOption.TCP_NODELAY, true);
        _bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        _bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT);
        _bootstrap.channel(NioSocketChannel.class);
        _bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ch.pipeline().addLast(new RPCRecordDecoder(), new ClientIOHandler(Connection.this));
            }
        });
    }

    /**
     * Send a RPC request and wait until a response is received or timeout. The
     * function will not retry. It is the responsibility of the application to
     * do retry. The behaviours: a. If the tcp connection is not established
     * yet: (1). sendAndWait will wait until the connection is established or
     * timeout or network error occurs. (2). Once the connection is established,
     * sendAndWait can send data. b. If the tcp connection is established: (1).
     * the request is put in the internal queue of netty. Netty will send it
     * asap. If there are already lots of pending requests in the queue,
     * sendAndWait return error. (2). sendAndWait will wait until it gets a
     * response from NFS server or timeout. (3). If the tcp connection is
     * broken, the function return error with network error. c. If the tcp
     * connection is broken suddenly: (1) the old sendAndWait will get the
     * network error or timeout (2) The new sendAndWait will follow (a).
     * 
     * @param timeout
     *            The timeout in seconds.
     * @param xdrRequest
     *            The generic RPC data and protocol-specific data.
     * @return The Xdr data for the response.
     * @throws RpcException if something went wrong
     */
    public Xdr sendAndWait(int timeout, Xdr xdrRequest) throws RpcException {
        if (_channel == null) {
            throw new IllegalStateException("Connection is not opened");
        }

        // check whether the internal queue of netty has enough spaces to hold
        // the request
        // False means that the too many pending requests are in the queue or
        // the connection is closed.
        if (!_channel.isWritable()) {
            String msg;
            if (_channel.isActive()) {
                msg = String.format("too many pending requests for the connection: %s", getRemoteAddress());
            } else {
                msg = String.format("the connection is broken: %s", getRemoteAddress());
            }
            // too many pending request are in the queue, return error
            throw new RpcException(RpcStatus.NETWORK_ERROR, msg);
        }

        // put the request into a map for timeout management
        ChannelPromise timeoutFuture = _channel.newPromise();
        Integer xid = xdrRequest.getXid();
        _futureMap.put(xid, timeoutFuture);

        // put the request into the queue of the netty, netty will send data
        // asynchronously
        RecordMarkingUtil.putRecordMarkingAndSend(_channel, xdrRequest);

        timeoutFuture.awaitUninterruptibly(timeout, TimeUnit.SECONDS);

        // remove the response from timeout maps
        Xdr response = _responseMap.remove(xid);
        _futureMap.remove(xid);

        if (!timeoutFuture.isSuccess()) {
            LOG.warn("cause:", timeoutFuture.cause());

            if (timeoutFuture.isDone()) {
                String msg = String.format("tcp IO error on the connection: %s", getRemoteAddress());
                throw new RpcException(RpcStatus.NETWORK_ERROR, msg);
            } else {
                String msg = String.format("rpc request timeout on the connection: %s", getRemoteAddress());
                throw new RpcException(RpcStatus.NETWORK_ERROR, msg);
            }
        }
        return response;
    }

    /**
     * If there is no current connection, start a new tcp connection asynchronously.
     * 
     * @throws RpcException if connection fails
     */
    protected void connect() throws RpcException {
        if (_channel != null) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Connecting to {} using privileged port: {}", getRemoteAddress(), _usePrivilegedPort);
        }
        if (_usePrivilegedPort) {
            bindToPrivilegedPort();
        }
        ChannelFuture connectFuture = _bootstrap.connect(_remoteHost, _remotePort).syncUninterruptibly();
        _channel = connectFuture.channel();
        _channel.config().setWriteBufferHighWaterMark(MAX_SENDING_QUEUE_SIZE);
    }

    /**
     * This is called when the application is shutdown or the channel is closed.
     */
    protected void shutdown() {
        if (_channel != null) {
            _channel.close();
        }
    }

    /**
     * This is called when the connection should be closed.
     */
    protected void close() {
        shutdown();

        // remove the connection from map
        NetMgr.getInstance().dropConnection(InetSocketAddress.createUnresolved(_remoteHost, _remotePort));

        // notify all the pending requests in the timeout map
        notifyAllPendingSenders("Channel closed, connection closing.");
    }

    /**
     * Update the response map with the response and notify the thread waiting for the
     * response. Do nothing if the future has been removed.
     * 
     * @param xid
     * @param response
     */
    protected void notifySender(Integer xid, Xdr response) {
        ChannelPromise future = _futureMap.get(xid);
        if (future != null) {
            _responseMap.put(xid, response);
            future.setSuccess();
        }
    }

    /**
     * Notify all the senders of all pending requests
     */
    protected void notifyAllPendingSenders(String message) {
        for (ChannelPromise future : _futureMap.values()) {
            future.setFailure(new Error(message));
        }
    }

    private String getRemoteAddress() {
        return _remoteHost + ':' + _remotePort;
    }

    /**
     * This attempts to bind to privileged ports, starting with 1023 and working
     * downwards, and returns when the first binding succeeds.
     *
     * <p>
     * Some NFS servers apparently may require that some requests originate on
     * an Internet port below IPPORT_RESERVED (1024). This is generally not
     * used, though, as the client then has to run as a user authorized for
     * privileged, which is dangerous. It is also not generally needed.
     * </p>
     *
     * @throws RpcException If an exception occurs, or if no binding succeeds.
     */
    private void bindToPrivilegedPort() throws RpcException {
        for (int port = 1023; port > 0; --port) {
            try {
                ChannelFuture bindFuture = _bootstrap.bind(port).sync();
                if (bindFuture.isSuccess()) {
                    return;
                }
            } catch (Exception e) {
                String msg = String.format("rpc request bind error for address: %s", port);
                throw new RpcException(RpcStatus.NETWORK_ERROR, msg, e);
            }
        }
        throw new RpcException(RpcStatus.LOCAL_BINDING_ERROR, "Cannot bind a port < 1024");
    }
}
