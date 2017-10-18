package com.darren.parctice.rpc;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.net.Socket;
import java.net.InetSocketAddress;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class Client {
  Map<ConnectionId, Connection> connections = new HashMap<>();
  private int guid = 0;
  static private final Logger logger = LogManager.getLogger();

  public static void main(String[] args) throws Exception {
    Client client = new Client();
    String str = "hello world!";
    InetSocketAddress address = new InetSocketAddress("localhost", 8880);
    InetSocketAddress address2 = new InetSocketAddress("localhost", 8882);
    ConnectionId connectionId = new ConnectionId(address);
    byte[] response = client.call(str.getBytes(), connectionId);
    printBytes(response);

    str = "antoher testing message";
    response = client.call(str.getBytes(), connectionId);
    printBytes(response);

    String[] strs = new String[] {
      "this is a testing string",
        "so fake testing is ok  ?",
        "to do something meaningful",
        "how to make a sense",
        "wow this is getting low",
        "---this is a testing string",
        "---so fake testing is ok  ?",
        "---to do something meaningful",
        "---how to make a sense",
        "---wow this is getting low",
    };

    Caller[] callers = new Caller[10];
    for (int i = 0; i < 5; ++i) {
      callers[i] = new Caller(client, i, new ConnectionId(address), strs[i]);
      callers[i].start();
    }
    for (int i = 5; i < 10; ++i) {
      callers[i] = new Caller(client, i, new ConnectionId(address2), strs[i]);
      callers[i].start();
    }
  }

  static class Caller extends Thread {
    Client client;
    String str;
    ConnectionId connectionId;
    Caller(Client client, int id, ConnectionId connectionId, String str) {
      this.client = client;
      setName("thread " + id);
      this.connectionId = connectionId;
      this.str = str;
    }

    @Override
      public void run() {
        try {
          while (true) {
            System.out.println(getName() + "  send : " + str);
            byte[] response = client.call(str.getBytes(), connectionId);
            System.out.println(getName() + "  got: ");
            printBytes(response);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
  }

  static void printBytes(byte[] bytes) {
    for (byte b : bytes) {
      System.out.print((char) b);
    }
    System.out.println();
  }

  private class Call {
    private int id;
    byte[] data;
    byte[] response;
    IOException exception;
    boolean done;
    boolean error;

    Call(byte[] data) {
      this.data = data;
      synchronized(Client.this) {
        id = guid++;
      }
    }

    synchronized void callComplete() {
      done = true;
      notify();
    }

    synchronized void setValue(byte[] response) {
      this.response = response;
      callComplete();
    }

    synchronized void setException(IOException exception) {
      this.exception = exception;
      callComplete();
    }
  }

  private class Connection extends Thread { //need acess to connections.
    Socket socket = null;
    DataOutputStream out = null;
    DataInputStream in = null;
    AtomicBoolean shouldCloseConnection = new AtomicBoolean();

    //current active calls!
    Map<Integer, Call> calls = new HashMap<>();

    Connection(ConnectionId connectionId) throws IOException {
      socket = new Socket(connectionId.address.getAddress(), connectionId.address.getPort());
      out = new DataOutputStream(socket.getOutputStream());
      in = new DataInputStream(socket.getInputStream());
      setName("Connection on port : " + socket.getLocalPort());
    }

    //will bloking current thread, but thread safe can be used by multiple thread
    void sendCall(Call call) throws IOException {
      //one of the two place IO error can happen(or can be detected or matters) sending & receiving..
      //shoulde clean up itself, should notify others, and close self
      //denoted as close. use receiving thread to do actually close. so call can return immediately   :) 
      //or does it matter? if other calls blocking. others still waiting then? 

      try {
        synchronized(out) {
          logger.debug("send out call on " + out + " with id " + call.id); 
          out.writeInt(call.data.length + 4);
          out.writeInt(call.id);
          out.write(call.data, 0, call.data.length);
          out.flush();
        }

        synchronized(calls) {
          calls.put(call.id, call);
        }
      } catch (IOException ex) { //connection pretty close. and notify user, exception happens.
        logger.warn(getName() + " broken while sending out call"); 
        close();
        throw new IOException("Error while send out call" , ex);
      }
    }

    @Override
    public void run() {//connection has a receiving thread for handling incoming response..
      logger.info(getName() + " starting receiving...");
      while (!shouldCloseConnection.get()) {
        try {
          receiveResponse(); //blocking read inside? can this a be trouble if server not responsive or go away ? //TO DO
        } catch (IOException ex) {
          logger.warn(getName() + " broken while reading"); 
          break;
        }
      }

      close();
    }

    void receiveResponse() throws IOException {
      //read reponse of a call.
      int dataLength = in.readInt();
      if (dataLength < 4) throw new IOException();

      int id = in.readInt(); 

      byte[] data = new byte[dataLength - 4]; 
      in.readFully(data);

      Call call = null;
      synchronized(calls) {
        call = calls.remove(id);
      }

      if (call == null) return;
      call.setValue(data);
    }

    synchronized void close() { 
      //close io
      //notify all the callers, set reponse for calls.
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
        }
      }
      logger.info(getName() + " closed");
      shouldCloseConnection.set(true);
      synchronized(calls) {
        for (Call call : calls.values()) {
          //notify? yes, but call has provided setException for you : ) nice and pretty
          //object oriented.
          //object own data, methods to maintain and modity data and status. exception if broke
          //frame work to put all object together and handler exception
          call.setException(new IOException("connection error"));
        }
      }

      synchronized(connections) {
        connections.remove(this);
      }
    }

  }

  Connection getConnection(ConnectionId connectionId) throws IOException {
    Connection connection = null; 
    synchronized(connections) {
      connection = connections.get(connectionId); 
      if (connection == null) {
        connection = new Connection(connectionId);
        connections.put(connectionId, connection);
        connection.start();
      }
    }
    return connection;
  }

  public byte[] call(byte[] data, ConnectionId connectionId) throws InterruptedException, IOException { //blocking fasion, need to handle interrupte
    Call call = new Call(data);
    Connection connection = getConnection(connectionId); //if here IOException, OK, no more clean up needed.  
    connection.sendCall(call); //IO IOException happends, connection clean its self and others, meanwhile pass IOException to user. 
    //exception handling. one happens, two lawyers do their corresponding job for handling this.

    synchronized(call) {
      while (!call.done) {
        try {
          call.wait();
        } catch (InterruptedException e) {
          //interrupted happens, this call's return will be dropped when it back..
          logger.warn("InterruptedException happens while waiting for call " + call.id + " response");
          throw new InterruptedException();
        }
      }
    }

    if (call.exception != null) {
      logger.warn("call " + call.id + " encountered exception"); 
      throw new IOException(call.exception);
    } 

    return call.response;
  }

  public static class ConnectionId {
    InetSocketAddress address = null;

    ConnectionId(InetSocketAddress address) {
      this.address = address;
    }

    @Override 
      public boolean equals (Object object) {
        if (object == this) return true;

        if (object instanceof ConnectionId) {
          ConnectionId that = (ConnectionId) object;
          return address == null ? that.address == null : address.equals(that.address);
        }
        return false;
      }

    @Override 
      public int hashCode() {
        return address == null ? 0 : address.hashCode();
      }
  }

}
