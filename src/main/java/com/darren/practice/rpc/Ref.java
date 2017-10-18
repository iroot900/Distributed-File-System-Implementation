package com.darren.parctice.rpc;

import java.lang.reflect.Proxy;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.io.IOException;


public class Ref {
  public static void main(String[] args) throws Exception {
    System.out.println("out");

    ProtocolTest pro = (ProtocolTest) Proxy.newProxyInstance(ProtocolTest.class.getClassLoader(), new Class[] {ProtocolTest.class}, new myInvocationHandler());
    System.out.println(pro.getLocaltion("/root/dev/", "docs"));

  }
}

interface ProtocolTest {
  String getLocaltion(String path, String name) throws IOException;
}

class myInvocationHandler implements InvocationHandler {

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

    System.out.println(method);
    for (Object arg : args) {
      System.out.println((String) arg);
    }
    return "hello world!";
  }
}
