package io.netty.example;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
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
  
  private ChannelPipeline pipeline;
  public void handleWorkSocket(final NioSocketChannel ch) throws InterruptedException
  {
    EventLoopGroup group = new NioEventLoopGroup();
    try {
      Bootstrap b = new Bootstrap();
      b.group(group);
      
      pipeline = ch.pipeline().addFirst(new MyEchoHandler()).addFirst(new StringDecoder()).addFirst("encode",new StringEncoder());
      EventLoop eventLoop = group.next();
      ch.unsafe().register(eventLoop, new DefaultChannelPromise(ch, eventLoop));
      //sendContext = ch.pipeline().context("encode");
      if(!ch.isRegistered()) {
        System.err.println("Channel not registered yet.");
        Assert.assertFalse(true);
      }
      
      ch.closeFuture().sync();
      
    } finally {
      group.shutdownGracefully();
    }
  }


  /**
   * This test mimic the pub-sub and sub messages will be switch to netty
   * - java server socket waiting for connection
   * - clients(pub and sub) connected and java work sockets created
   * - sub send message to identify it's the sub. The handler switch to netty.
   * - the java work channel handles the message from pub and netty handle message from sub
   * - publisher client send a message, the application handle this message and redirect to subscriber client
   * 
   */
  @Test
  public void testMimicPubSub() throws IOException, InterruptedException
  {
    ServerSocketChannel server = ServerSocketChannel.open();
    server.socket().bind(new InetSocketAddress(PORT_NUMBER));
    server.socket().setReuseAddress(true);
    server.configureBlocking(false);

    final Selector selector = Selector.open();
    server.register(selector, SelectionKey.OP_ACCEPT);
    
    System.out.println("Listen on port: " + PORT_NUMBER);
    
    
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.execute(new Task1(server, selector, this));
   
    //wait until subscriber connected and identified
    suscriberCountDown.await();
    logger.info("creating netty...");
    
    NioSocketChannel nettyChannel = new NioSocketChannel(javaSuscriberChannel);
    handleWorkSocket(nettyChannel);
  }
  
  private java.nio.channels.SocketChannel javaSuscriberChannel = null;
  private java.nio.channels.SocketChannel javaPublisherChannel = null;
  private CountDownLatch suscriberCountDown = new CountDownLatch(1);
  
  /**
   * handle server socket and client socket for publisher
   *
   */
  private static class Task1 implements Runnable
  {
    InteractTest owner;
    ServerSocketChannel server;
    final Selector selector;
    java.nio.channels.SocketChannel javaWorkChannel;
    AtomicBoolean closing = new AtomicBoolean(false);
    public Task1(ServerSocketChannel server, Selector selector, InteractTest owner)
    {
      this.server = server;
      this.selector = selector;
      this.owner = owner;
    }

    @Override
    public void run()
    {
      while (!closing.get()) {
        try {
          int channelCount = selector.select();
          if (channelCount > 0) {
            Set<SelectionKey> keys = selector.selectedKeys();
            Iterator<SelectionKey> iterator = keys.iterator();
            while (iterator.hasNext()) {
              SelectionKey key = iterator.next();

              if (key.isAcceptable()) {
                iterator.remove();
                
                javaWorkChannel = server.accept();
                javaWorkChannel.configureBlocking(false);
                MessageListener listener = new MessageListener(owner);
                listener.register(javaWorkChannel.register(selector, SelectionKey.OP_READ, listener));
              } else if (key.isReadable()) {
                SocketChannel channel = (SocketChannel)key.channel();
                if(channel == owner.javaSuscriberChannel) {
                  //NOTE: need remove it even netty handle it?
                  iterator.remove();
                  logger.warn("Subscriber channel. suppose netty handle it.");
                } else if(channel == owner.javaPublisherChannel) {
                  logger.info("Publisher channel. handle it.");
                  iterator.remove();
                  ((MessageListener)key.attachment()).handle();
                } else {
                  logger.info("register message. handle it");
                  iterator.remove();
                  ((MessageListener)key.attachment()).handle();
                }
              }
            }
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }

      }
    }
  }
  
  private static class MessageListener
  {
    private InteractTest owner;
    private SelectionKey key;
    
    private static final byte[] subscriberIndicator = "subscriber\r\n".getBytes();
    private static final byte[] publisherIndicator = "publisher\r\n".getBytes();
    
    public MessageListener(InteractTest owner)
    {
      this.owner = owner;
    }
    
    public void register(SelectionKey key)
    {
      this.key = key;
    }
    public final void handle() throws IOException
    {
      SocketChannel channel = (SocketChannel)key.channel();
      int readLen;
      ByteBuffer buffer = ByteBuffer.allocate(2048);
      if ((readLen = channel.read(buffer)) > 0) {
        byte[] data = new byte[readLen];
        System.arraycopy(buffer.array(), 0, data, 0, readLen);
        if (readLen == subscriberIndicator.length && Arrays.equals(data, subscriberIndicator)) {
          logger.info("subscriber identified.");
          owner.javaSuscriberChannel = channel;
          owner.suscriberCountDown.countDown();
        } else if (readLen == publisherIndicator.length && Arrays.equals(data, publisherIndicator)) {
          logger.info("publisher identified.");
          owner.javaPublisherChannel = channel;
        } else if (owner.javaPublisherChannel == channel) {
          //this is the message from publisher
          //redirect this message to subscriber
          owner.redirectPublishData(data);
        } else {
          logger.info("unexpected message. bytes: {}", readLen);
        }
        buffer.clear();
      } else if (readLen == -1) {
        try {
          channel.close();
        } finally {
          logger.warn("socket closed.");
        }
      } else {
        logger.warn("{} read 0 bytes", this);
      }
    }
  }
  

  /**
   * send message to subscriber client
   * instead directly call Channel#write(ByteBuffer)
   */
  private void redirectPublishData(byte[] data)
  {
    String message = new String(data);
    pipeline.writeAndFlush(message);
    
//    try {
//      this.javaSuscriberChannel.write(ByteBuffer.wrap(data));
//    } catch (IOException e) {
//      e.printStackTrace();
//      throw new RuntimeException("e");
//    }
  }
  
  private static final Logger logger = LoggerFactory.getLogger(InteractTest.class);
}
