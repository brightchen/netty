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

import com.google.common.collect.Sets;

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

  /**
   * test use netty to handle the work socket which create by java server socket.
   * The netty will take over after accept
   */
  @Test
  public void test1() throws IOException, InterruptedException
  {
    ServerSocketChannel server = ServerSocketChannel.open();
    server.socket().bind(new InetSocketAddress(PORT_NUMBER));
    server.socket().setReuseAddress(true);
    server.configureBlocking(false);

    final Selector selector = Selector.open();
    server.register(selector, SelectionKey.OP_ACCEPT);
    
    System.out.println("Listen on port: " + PORT_NUMBER);
    
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
  
  
  /**
   * test use netty to handle the work socket which create by java server socket.
   * - java server socket waiting for connection
   * - client connected and a java work socket created
   * - java work socket handle messages from client
   * - netty take over. the java work socket should be suspended.
   * 
   */
  @Test
  public void test2() throws IOException, InterruptedException
  {
    ServerSocketChannel server = ServerSocketChannel.open();
    server.socket().bind(new InetSocketAddress(PORT_NUMBER));
    server.socket().setReuseAddress(true);
    server.configureBlocking(false);

    final Selector selector = Selector.open();
    server.register(selector, SelectionKey.OP_ACCEPT);
    
    System.out.println("Listen on port: " + PORT_NUMBER);
    
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
   
    //java work socket handle message
    handleWorkSocketJavaChannel(selector, javaWorkChannel);
    
    NioSocketChannel nettyChannel = new NioSocketChannel(javaWorkChannel);
    handleWorkSocket(nettyChannel);
  }
  
  private void handleWorkSocketJavaChannel(Selector selector, java.nio.channels.SocketChannel javaWorkChannel) throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(2048);
    boolean done = false;
    while (!done) {
      int readyChannels = selector.select();
      if (readyChannels == 0)
        continue;

      Set<SelectionKey> selectedKeys = selector.selectedKeys();
      Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

      while (keyIterator.hasNext()) {

        SelectionKey key = keyIterator.next();
        if (key.isReadable()) {
          javaWorkChannel.read(buffer);
          System.out.println(buffer);
          done = true;
          break;
        }
        keyIterator.remove();
      }
    }
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
