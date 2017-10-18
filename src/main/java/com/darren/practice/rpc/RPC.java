package com.darren.parctice.rpc;


import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.lang.reflect.Proxy;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;


import java.net.InetSocketAddress;
import java.io.IOException;
import java.io.Serializable;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;


public class RPC {
  private static final Logger logger = LogManager.getLogger(RPC.class);

  public static class Invocation implements Serializable {
    private String methodName;
    private Class[] parameterClasses;
    private Object[] parameters;

    public Invocation(Method method, Object[] parameters) {
      this.methodName = method.getName();
      this.parameterClasses = method.getParameterTypes();
      this.parameters = parameters;
    }

    public String getMethodName() { return methodName; }

    public Class[] getParameterClasses() { return parameterClasses; }

    public Object[] getParameters() { return parameters; }
  }


  private static class RPCInvocationHandler implements InvocationHandler {
    InetSocketAddress address;
    Client client = new Client();
    
    RPCInvocationHandler(InetSocketAddress address) {
      this.address = address;
    }

    @Override 
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      System.out.println(method);
      Invocation invocation = new Invocation(method, args);
      ByteArrayOutputStream bytesOutStream = new ByteArrayOutputStream();
      ObjectOutputStream objectOutStream = new ObjectOutputStream(bytesOutStream);
      objectOutStream.writeObject(invocation);

      //here could throw io / local exception!
      byte[] responseBytes = client.call(bytesOutStream.toByteArray(), new Client.ConnectionId(address));

      ByteArrayInputStream inputStream = new ByteArrayInputStream(responseBytes);
      ObjectInputStream objectIn = new ObjectInputStream(inputStream);
      Object ret = objectIn.readObject();
      if (ret instanceof Exception) {
        throw new Exception("call end up with logic exception", (Exception)ret);
      }
      return ret;
    }
  }
   
  public static Protocol getProxy(Class<? extends Protocol> protocol, InetSocketAddress address) {
    return (Protocol) Proxy.newProxyInstance(protocol.getClassLoader(), new Class[] {protocol}, new RPCInvocationHandler(address));
  }


  public static Server getProtocolServer(int port, Protocol protocol) throws IOException {
    return new Server(port, protocol);
  }

  private static class Caller extends Thread {
    NamenodeProtocol pro = null;
    Caller(NamenodeProtocol pro) {
      this.pro = pro;
    }

    @Override
    public void run () {
      int index = 1;
      while (true) {
        try {
          String ret = pro.getFileStatus("/tmp/omn/output " + index++);
          System.out.println(ret);
        } catch (IOException e) {
          e.printStackTrace();
          break; //for testing only
        }
      }
    }
  }

  public static void main(String[] args) throws IOException {
    try {
      Server server = getProtocolServer(8880, new NamenodeMock());
      server.start();
    } catch (IOException ex) {
      logger.warn("one server instance is running on port " + 8880);
    }

    InetSocketAddress address = new InetSocketAddress("localhost", 8880);
    NamenodeProtocol pro = (NamenodeProtocol) getProxy(NamenodeProtocol.class, address);
    Caller caller1 = new Caller(pro);
    Caller caller2 = new Caller(pro);
    Caller caller3 = new Caller(pro);
    caller1.start();
    caller2.start();
    caller3.start();
    try {
        String ret = pro.getFileStatus("/tmp/omn/output");
        System.out.println(ret);
        System.out.println("done call!");

        double retD = pro.getDouble(1);
        System.out.println(retD);


        ret = pro.getFileStatus("/tmp/omn/output2");
        System.out.println(ret);


        retD = pro.getDouble(1);
        System.out.println(retD);

        int size = pro.getFileSize("good");
        System.out.println(size);

        size = pro.getFileSize("bad");
        System.out.println(size);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
