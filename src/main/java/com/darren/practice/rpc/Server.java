package com.darren.parctice.rpc;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.ByteBuffer;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.DataInputStream;

import java.net.InetSocketAddress;

import java.util.Iterator;
import java.util.Queue;
import java.util.ArrayDeque;
import java.util.Set;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Arrays;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A NIO Server Service
 *
 */
public class Server {
  private static final Logger logger = LogManager.getLogger(Server.class);
  private int port; 
  private boolean running = true;
  private ConnectionManager connectionManager = new ConnectionManager();
  private Listener listener = null;
  private Reader reader = null;
  private Handler handler = null;
  private Responder responder = null;
  private Object instance = null;

  //how to put all the components to work together ->  should be simple and elegant!
  //central message, all the component operate on this sequentially
  //all the component is running in there own thread loop. ready to process a message (call).
  //echo components don't call each other's method or tell them what to do, just simply provide message to them.

  //io part went wrong(only reader, responder part), it will be closed. (connection, format) 
  //app logic part went wrong, an error message will be returned as rpc exception.
  //framewokr went wrong, service is going down.. 
  private BlockingQueue<Call>  calls = new LinkedBlockingQueue<>();

  public Server(int port, Object instance) throws IOException {
    this.port = port;
    this.listener = new Listener();
    this.responder = new Responder();
    this.reader = new Reader();
    this.handler = new Handler();
    this.instance = instance;
  }

  public void start() {
    reader.start();
    handler.start();
    responder.start();
    listener.start();
    logger.info("Server on port" + port +  " started!");
  }

  public synchronized void stop() {
    running = false;
    listener.doStop();
    reader.doStop();
    responder.doStop();
    //call stop on each components ? 
    //each components should do their clean up work?
  }


  /**
   * Listener simply listen on server port and wait for incoming connections
   *
   * get connection and register with reader.  once connection is dilivered, its job is done.
   */
  private class Listener extends Thread {
    private ServerSocketChannel serverSocketChannel = null; 
    private Selector selector = null; 

    //User of Listener should handle start exception. 
    public Listener() throws IOException {
      serverSocketChannel = ServerSocketChannel.open();
      serverSocketChannel.bind(new InetSocketAddress(port));
      serverSocketChannel.configureBlocking(false);

      selector = Selector.open();
      serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
      setName("IPC Listener on port : " + port);
    }

    @Override
      public void run() { 
        logger.info(getName() + " started!");
        while (running) {
          try {
            logger.debug(getName() + " selector waiting to accept");
            selector.select();
            Iterator<SelectionKey> keyIter = selector.selectedKeys().iterator();
            while (keyIter.hasNext()) {

              SelectionKey key = keyIter.next();
              keyIter.remove();
              try {
                if (key.isValid()) {
                  if (key.isAcceptable()) {
                    doAccept(key);
                  }
                }
              } catch (IOException e) { 
                //no need to handle key, it's removed already.
                //key.cancel(); //if this broken it's done!
                //error stop..all other threads are daemon. unusual exit!
                logger.error("io error happends on acceptor, shutdonw service", e);
                break;
              }
            }
          } catch (OutOfMemoryError e) {
            logger.warn(e);
          } catch (Exception e) {
            logger.error("unknown error happends on acceptor, shutdonw service", e);
          } finally {
            //should close connection here? well service is done, not really matters 
          }
        }
        logger.info(getName() + " stopped accepting");

        try {
          serverSocketChannel.close();
          selector.close();
        } catch (IOException e) {
          //
        } 
      }

    public synchronized void doStop() {
      selector.wakeup();
    }
  }


  //must be Server's method. coz need access to reader, get a new socketchannel then rigister to reader;
  void doAccept(SelectionKey key) throws IOException, OutOfMemoryError {
    ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();

    //can be multile connection read..
    SocketChannel socketchannel = null;
    while ((socketchannel = serverSocketChannel.accept()) != null) { 
      logger.debug(" got " + socketchannel);
      try {
        socketchannel.configureBlocking(false);
        //now reader is the consumer. (selector consumes socketchannel)
        Connection connection = new Connection(socketchannel); 
        connectionManager.add(connection);
        reader.addConnection(connection);
      } catch (Exception e) { //for a single connection error, shouldn't broken service
        logger.warn(e);
      }
    }
  }

  private class Reader extends Thread {

    private Selector selector = null;
    private Queue<Connection>  pendingConnections = new ArrayDeque<>();

    public Reader() throws IOException {
      selector = Selector.open();
      setName("IPC Reader");
      setDaemon(true);
    }

    public void addConnection(Connection connection) {
      synchronized(pendingConnections) {
        pendingConnections.offer(connection);
      }
      selector.wakeup(); //doesn't matter. out or in..
    }

    @Override
      public void run() {
        logger.info(getName() + " started");
        while (running) { 
          try {
            synchronized(pendingConnections) {
              Iterator<Connection> connIter = pendingConnections.iterator();
              while (connIter.hasNext()) {
                Connection connection = connIter.next();
                connIter.remove();
                try {
                  SelectionKey key = connection.socketchannel.register(selector, SelectionKey.OP_READ);
                  key.attach(connection);
                } catch (ClosedChannelException e) {
                  logger.warn("one connection is closed" + connection, e); //should not happend.
                  connectionManager.closeConnection(connection); 
                }
              }
            }
            logger.debug(getName() + " selector wait...");
            selector.select();
            Iterator<SelectionKey> keyIter = selector.selectedKeys().iterator();
            while (keyIter.hasNext()) {
              SelectionKey key = keyIter.next();
              keyIter.remove();
              try {
                if (key.isValid()) {
                  if (key.isReadable()) {
                    doRead(key); //wrong RPC info. (send back some info, close),  io error just close -- always, can  
                  }
                }
              } catch (RpcException e) {
                logger.warn("Reader get invalid Rpc Call", e);
                //send back some info, closed here : acceptable for client, still io error, will not blocking at least
                connectionManager.closeConnection((Connection)key.attachment());
              } catch (IOException e) {
                logger.warn("Connection broke while reading: " + (Connection)key.attachment(), e); 
                connectionManager.closeConnection((Connection)key.attachment());
              } catch (CancelledKeyException e) {
                logger.warn("Connection closed already: " + (Connection)key.attachment(), e); 
              } catch (Exception e) {
                logger.warn("unknow exception on: " + (Connection)key.attachment(), e); 
                connectionManager.closeConnection((Connection)key.attachment());
              }
            }
          } catch (Exception e) {
            //here should close all connection.?
            logger.error("reader borken,", e);
          }
        }
        logger.info(getName() + " exited!");
      }

    //more thread should on IO part. more blocking. mightbe. better response. 
    private void doRead(SelectionKey key) throws IOException, RpcException { //read at most one RPC call.  //first length, then data.
      Connection connection = (Connection)key.attachment();
      int count = connection.readRpcCall();
      if (count < 0) throw new IOException("end of stream");
      //here can set last contact time. 
    }


    public synchronized void doStop() {
      selector.wakeup();
    }
  }

  private void processOne(byte[] data, Connection connection) throws IOException {
    logger.debug("processing one call .."); 

    try {
      calls.put(new Call(data, connection)); 
    } catch (InterruptedException e) {
      //TO DO
      logger.warn("drop one call! should response some meanfully message to client", e);
    }
  }

  //Only consume calls queue, and set value for call, then register call 'connection to responder;
  //handler pulling data, then publish. actually, calls is could be handler's queue if there is only one handler. //LOL!
  private class Handler extends Thread {

    public Handler() {
      setName("RPC Call Handler");
      setDaemon(true);
    }

    @Override
      public void run() {
        logger.info(getName() + " started!");
        Connection connection = null;
        while (running) {
          try {
            Call call = calls.take();
            connection =  call.connection;
            handleOneCall(call);
            logger.debug("handled one call, deliver to responder ");
            responder.doRespond(call);
          } catch (IOException e) { //when sending out IO error
            logger.warn("one connection write went wrong : " + connection, e);
            connectionManager.closeConnection(connection);
          } catch (NoSuchMethodException e) {
            logger.warn("no such method", e); //this should no happens, should controled at hand sake level to ensure
            connectionManager.closeConnection(connection);
          } catch (IllegalAccessException e) {
            logger.warn("illegal access", e); //all public methods. should no happend
            connectionManager.closeConnection(connection);
          } catch (InterruptedException e) {
            //ignore this.
          } catch (Exception e) {
            logger.warn("unknown exceptions on :" + connection);
            connectionManager.closeConnection(connection);
          }
        }
      }


    //TO DO:
    //should be able to return method exceptions and no string return A. 
    //also connection monitor B.
    private void handleOneCall(Call call) throws IOException, NoSuchMethodException, IllegalAccessException {
      logger.debug(getName() + " processing call " + call.id);
      RPC.Invocation invocation = call.invocation;
      Method method = instance.getClass().getMethod(invocation.getMethodName(), invocation.getParameterClasses());
      try {
        Object ret =  method.invoke(instance, invocation.getParameters()); //method itself exception..
        call.setResponse(ret);
      } catch (InvocationTargetException e) {
        call.setResponse(e.getTargetException()); 
      }
    }
  }


  private class Responder extends Thread {
    //the point is.. reponseder will loop to check write channel. then write
    //if we can write it once. then write, if not then write it multiple times.
    //never blocking for async io
    private Selector selector = null;
    private int pending = 0 ;

    public Responder() throws IOException {
      selector = Selector.open();
      setName("IPC Responder");
      setDaemon(true);
    }

    synchronized void incPending() {
      pending++;
    }

    synchronized void decPending() {
      pending--;
      notify();
    }

    synchronized void waitPending() throws InterruptedException {
      while (pending > 0) {
        wait();
      }
    }

    public void doRespond(Call call) throws IOException {
      synchronized (call.connection.responseQueue) {
        call.connection.responseQueue.addLast(call);
        if (call.connection.responseQueue.size() == 1) {
          processResponse(call.connection.responseQueue, true);
        }
      }
    }

    private boolean processResponse(LinkedList<Call> responseQueue, boolean inHandler) throws IOException {
      //send a call from this queue. at most one at a time. 
      logger.debug("send one reponse started");
      synchronized(responseQueue) {
        if (responseQueue.size() == 0) return true;
        Call call = responseQueue.removeFirst();
        SocketChannel socketchannel = call.connection.socketchannel;

        int count = socketchannel.write(call.response);
        if (!call.response.hasRemaining()) { //current one sent out!. 
          logger.debug("send one reponse done!");
          return responseQueue.size() == 0;
        } else {
          //now must add it back
          responseQueue.addFirst(call);

          if (inHandler) { //size must 1 of course. register.
            incPending();
            try {
              selector.wakeup();
              socketchannel.register(selector, SelectionKey.OP_WRITE, call.connection);
            } catch (ClosedChannelException e) {
              return true; // closed channel by others. 
            } finally {
              decPending();
            }
          }
          return false;
        }
      }

    }

    @Override
      public void run() {
        logger.info(getName() +  " Responder starting... ");
        while (running) {
          try {
            logger.debug(getName() + " Responder selector waiting ..");
            try {
              waitPending();
            } catch (InterruptedException e) {
              //ignore this.
            }
            selector.select();
            Iterator<SelectionKey> keyIter = selector.selectedKeys().iterator();
            while (keyIter.hasNext()) {
              SelectionKey key = keyIter.next();
              keyIter.remove();
              Connection connection = (Connection) key.attachment();
              try {
                if (key.isValid()) {
                  if (key.isWritable()) {
                    doWrite(key);
                  }
                }
              } catch (IOException e) {
                logger.warn("one connection broken", e);
                connectionManager.closeConnection(connection);
              } catch (CancelledKeyException e) {
                logger.warn(e);
              }
            }
          } catch (IOException e) {
            logger.warn(e);
            //close all connection? 
          }
        }
      }

    void doWrite(SelectionKey key) throws IOException {
      Connection connection = (Connection) key.attachment();
      if (processResponse(connection.responseQueue, false)) {
        try {
          key.interestOps(0);
        } catch (CancelledKeyException e) {
          logger.debug(e);
        }
      }
    }

    public synchronized void doStop() {
      selector.wakeup();
    }

  }

  private static class ConnectionManager {
    Set<Connection> connections = new HashSet<Connection> ();

    synchronized void add(Connection connection) {
      connections.add(connection);
    }

    synchronized void remove(Connection connection) {
      connections.remove(connection);
    }

    synchronized void closeConnection(Connection connection) {
      connection.close();
      remove(connection);
    }
  }

  private class Connection {
    SocketChannel socketchannel = null;
    LinkedList<Call> responseQueue = new LinkedList<>();
    ByteBuffer dataBuffer = null;
    ByteBuffer dataLengthBuffer = ByteBuffer.allocate(4); 
    int dataLength;
    boolean dataLengthReaded = false;

    @Override
      public String toString() {
        if (socketchannel == null) {
          return "invalid connection";
        } else {
          return socketchannel.toString();
        }
      }

    public int readRpcCall() throws RpcException, IOException {
      logger.debug(" reading one RPC call...");
      while (true) {
        int count = 0;
        if (dataLengthBuffer.hasRemaining()) {
          count = socketchannel.read(dataLengthBuffer);
          if (count < 0 || dataLengthBuffer.hasRemaining()) { //either read to the end, or blocked 
            return count;
          }
          dataLengthBuffer.flip();
          dataLength = dataLengthBuffer.getInt(); 
          if (dataLength <= 0) throw new RpcException("data length can't be less than 0");
        } else {
          if (dataBuffer == null) {
            dataBuffer = ByteBuffer.allocate(dataLength);
          }

          count = socketchannel.read(dataBuffer);
          if (count < 0 || dataBuffer.hasRemaining()) { 
            return count;
          }

          logger.debug(" finish reading one RPC call!");
          processOne(dataBuffer.array(), this);
          dataLengthBuffer.clear();
          dataBuffer = null;

          //process one at most at a time
          return count;
        }
      }
    }

    public Connection(SocketChannel socketchannel) {
      this.socketchannel = socketchannel;
    }

    public synchronized void close() { //error could happend in multiple place. one close is enough.
      if (socketchannel != null && socketchannel.isOpen()) {
        try {
          socketchannel.close();
        } catch (Exception e) {

        }
      }
    }
  }

  private static class Call{
    private int id;
    RPC.Invocation invocation = null; 
    ByteBuffer response = null; 
    Connection connection = null;

    public Call(byte data[], Connection connection) throws IOException {
      this.connection = connection;

      ByteArrayInputStream inputStream = new ByteArrayInputStream(data, 0, 4);
      DataInputStream in = new DataInputStream(inputStream);

      //should check id, could be something crappy
      //also, could use -1 to act as heartbeat..
      this.id = in.readInt();

      ByteArrayInputStream dataInputStream = new ByteArrayInputStream(data, 4, data.length - 4);
      ObjectInputStream dataIn = new ObjectInputStream(dataInputStream);
      try {
        invocation = (RPC.Invocation) dataIn.readObject();
      } catch (ClassNotFoundException e) {
        throw new IOException("class not found");
      }
    }

    public int getId() {
      return id;
    }

    public synchronized void setResponse(Object ret) throws IOException {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      ObjectOutputStream dataOut = new ObjectOutputStream(outputStream);
      dataOut.writeObject(ret);
      byte[] responseBytes = outputStream.toByteArray();
      int length = responseBytes.length;
      response = ByteBuffer.allocate(length + 4 + 4); //id + length; 
      response.putInt(length + 4);
      response.putInt(id);
      response.put(responseBytes);
      response.flip();
    }
  }

  private static class RpcException extends IOException {
    RpcException(String message) {
      super(message);
    }

    RpcException() {}
  }

  public static void main(String[] args) throws Exception {
    Server server = new Server(8880, new NamenodeMock());
    server.start();
    Server server2 = new Server(8882, new NamenodeMock());
    server2.start();
  }
}
