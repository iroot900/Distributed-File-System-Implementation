package com.darren.parctice.hdfs;
import java.util.Map;
import java.util.HashMap;

//least recently modified map
public class LRM <K, V extends TimedItem> {

  public int size() {
    return map.size();
  }

  public V get(K key) { //don't move it
    Node<K, V> node = map.get(key);
    if (node == null) return null;
    return map.get(key).val;
  }

  public K getFirstKey() {
    if (map.size() == 0) return null;
    return linkedlist.head.key;
  }

  public V put(K key, V val) { //not exist, could be update or insert -- update, then newest timestamp; 
    Node<K, V> node = map.get(key);
    V oldVal = null;
    if (node == null) {
      node = new Node<K, V> (key, val);
      linkedlist.addToTail(node);
      map.put(key, node);
    } else {
      oldVal = node.val;
      node.key = key; node.val = val;
      linkedlist.moveToTail(node);
    }
    val.updateTimeStamp();
    return oldVal;
  }

  public boolean remove(K key) { //remove --- might remove a datanode deliberately -- 
    if (!map.containsKey(key)) return false;
    Node<K, V> node = map.get(key);
    map.remove(key);
    linkedlist.remove(node);
    return true;
  }

  private Map<K, Node<K, V>> map = new HashMap<>();
  private DoubleLinkedList<K, V> linkedlist = new DoubleLinkedList<K, V>(); 

  //   node-node -node -  
  static private class Node <K, V extends TimedItem> {
    Node prev;
    Node next;

    public Node(K key, V val) {
      this.key = key; this.val = val;
      this.prev = null; this.next = null;
    }

    K key;
    V val;
  }

  static private class DoubleLinkedList <K, V extends TimedItem> {
    private Node<K, V> head = null;
    private Node<K, V> tail = null;
    
    void addToTail(Node node) {
      if (tail == null) {
        head = tail = node; node.prev = node.next = null; 
      } else {
        //already exist something.
        tail.next = node;
        tail.next.prev = tail;
        tail = tail.next;
        node.next = null;
      }
    }

    void moveToTail(Node node) {
      if (node == tail) return;
      remove(node);
      addToTail(node);
    }

    void remove(Node node) {
      if (node == head && node == tail) {
        head = tail = null;
      } else if (node == head) {
        head = head.next; head.prev = null;
      } else if (node == tail) {
        tail = tail.prev; tail.next = null;
      } else {
        node.prev.next = node.next;
        node.next.prev = node.prev;
      }
    }

  }
}

class TimedItem {
  long timeStamp; 

  public TimedItem() {
    this.timeStamp = System.currentTimeMillis();
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  //this should not be call by anything other than in LRM 
  void updateTimeStamp() {
    this.timeStamp = System.currentTimeMillis();
  }
}
