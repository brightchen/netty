package io.netty.example;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Iterator;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.example.echo.EchoServerHandler;
import io.netty.example.telnet.TelnetClientHandler;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

/**
 * Test how Netty can handle socket which created outside
 *
 */

public class InteractTest
{
  private static final int PORT_NUMBER = 12345;

  private static final int BUFFER_SIZE = 256;

  @Test
  public void test() throws IOException, InterruptedException
  {
    ServerSocketChannel server = ServerSocketChannel.open();
    server.socket().bind(new InetSocketAddress(PORT_NUMBER));
    server.socket().setReuseAddress(true);
    server.configureBlocking(false);

    final Selector selector = Selector.open();
    server.register(selector, SelectionKey.OP_ACCEPT);
    
    System.out.println("Listen on port: " + PORT_NUMBER);
    
    ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
    java.nio.channels.SocketChannel javaWorkChannel = null;
    boolean accepted = false;
    while (!accepted) {
      int channelCount = selector.select();
      if (channelCount > 0) {
        Set<SelectionKey> keys = selector.selectedKeys();
        Iterator<SelectionKey> iterator = keys.iterator();
        while (iterator.hasNext()) {
          SelectionKey key = iterator.next();
          iterator.remove();

          if (key.isAcceptable()) {
            javaWorkChannel = server.accept();
            javaWorkChannel.configureBlocking(false);
            javaWorkChannel.register(selector, SelectionKey.OP_READ, javaWorkChannel.socket().getPort());
            accepted = true;
            break;
          }
        }
      }
    }
   
    NioSocketChannel nettyChannel = new NioSocketChannel(javaWorkChannel);
    handleWorkSocket(nettyChannel);
  }
  
  
  private static class MyEchoHandler extends EchoServerHandler
  {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      System.err.println(msg);
      ctx.write(msg);
    }

  }
  
  public void handleWorkSocket(final NioSocketChannel ch) throws InterruptedException
  {
    EventLoopGroup group = new NioEventLoopGroup();
    try {
      Bootstrap b = new Bootstrap();
      b.group(group);
      
      ch.pipeline().addFirst(new MyEchoHandler()).addFirst(new StringDecoder()).addFirst(new StringEncoder());
      EventLoop eventLoop = group.next();
      ch.unsafe().register(eventLoop, new DefaultChannelPromise(ch, eventLoop));

      if(!ch.isRegistered()) {
        System.err.println("Channel not registered yet.");
        Assert.assertFalse(true);
      }
      
      ch.closeFuture().sync();
      
    } finally {
      group.shutdownGracefully();
    }
  }

}
