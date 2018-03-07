package com.gaogf.concurrent;

/**
 * Created by pactera on 2018/3/6.
 */
public class MyThread extends Thread {
    private int  count = 5;
    @Override
    public synchronized void run() {
        count --;
        System.out.println(this.getThreadGroup().getName() + "count = " + count);
    }
    public static void main(String args[]){
        MyThread thread = new MyThread();
        Thread t1 = new Thread(thread,"t1");
        Thread t2 = new Thread(thread,"t2");
        Thread t3 = new Thread(thread,"t3");
        Thread t4 = new Thread(thread,"t4");
        Thread t5 = new Thread(thread,"t5");
        t1.start();
        t2.start();
        t3.start();
        t4.start();
        t5.start();
    }
}
