package com.lannister.maven.demo.zookeeper;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class DistributeServer {
	private static String connectString = "192.168.2.101:2181,192.168.2.102:2181,192.168.2.103:2181";
	private static int sessionTimeout = 2000;
	private static String parentNode = "/servers";

	private ZooKeeper zk = null;

	public void connect() throws IOException {
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub

			}});
		System.out.println("连接成功！");
	}

	public void registe(String hostname) throws KeeperException, InterruptedException {

		String create = zk.create(parentNode + "/server", hostname.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

		System.out.println(hostname + " is online " + create);
	}

	public void business(String hostname , int time) {
		System.out.println(hostname + " is working");

		try {TimeUnit.SECONDS.sleep(time);} catch (InterruptedException e) {e.printStackTrace();}
	}

	public void close() throws InterruptedException {
		zk.close();
		System.out.println("服务器关闭！");
	}

	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

		Thread t1 = new Thread(()->{
			String hostname = "192.168.2.101";
			DistributeServer server = new DistributeServer();
			try {
				server.connect();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				server.registe(hostname);
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			server.business(hostname, 10);
			try {
				server.close();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		});


		Thread t2 = new Thread(()->{
			String hostname = "192.168.2.102";
			DistributeServer server = new DistributeServer();
			try {
				server.connect();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				server.registe(hostname);
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			server.business(hostname, 30);
			try {
				server.close();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});


		Thread t3 = new Thread(()->{
			String hostname = "192.168.2.103";
			DistributeServer server = new DistributeServer();
			try {
				server.connect();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				server.registe(hostname);
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			server.business(hostname, 20);
			try {
				server.close();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});

		t1.start();
		t2.start();
		t3.start();

		t1.join();
		t2.join();
		t3.join();
	}
}
