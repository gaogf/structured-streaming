package com.gaogf.concurrent;

/**
 * Created by pactera on 2018/3/6.
 */
public class MuliThread {
    private  int num = 0;
    public synchronized void changeNum(String tag) throws InterruptedException {
        if(tag.equals("a")){
            num = 100;
            System.out.println(num);
            Thread.sleep(1000L);
        }
        if(tag.equals("b")){
            num = 200;
            System.out.println(num);
        }
    }
    public static void main(String agrs[]){
        MuliThread t1 = new MuliThread();
        MuliThread t2 = new MuliThread();
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    t1.changeNum("a");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    t2.changeNum("b");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
        thread1.start();

    }
}
