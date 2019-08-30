package com.lannister.maven.demo.zookeeper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;

public class DistributeClient {
	private static String connectString = "192.168.2.101:2181,192.168.2.102:2181,192.168.2.103:2181";
	private static int sessionTimeout = 2000;
	private static String parentNode = "/servers";
	private static List<String> servers = new LinkedList<String>();
	
	private ZooKeeper zk = null;
	
	public void connect() throws IOException {
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

			public void process(WatchedEvent event) {
				
				
			}});
		System.out.println("���ӳɹ���");
	}
	
	public void getServerList() throws KeeperException, InterruptedException {
		servers.clear();
		List<String> children = zk.getChildren(parentNode, true);
		for (String child : children) {
			servers.add(parentNode + "/" + child);
		}
	}
	
	public void work(String serverNode) throws KeeperException, InterruptedException {
		byte[] hostname = zk.getData(serverNode, new Watcher() {

			public void process(WatchedEvent event) {
				if(event.getType().equals(EventType.NodeDeleted)) {
					try {
						getServerList();
						if (servers.size() > 0) {
							work(servers.get(0));
						}else {
							System.out.println("û�з�������");
							close();
						}
					} catch (KeeperException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
			}}, null);
		
		System.out.println("Client work with " + new String(hostname));
	}
	
	public void close() throws InterruptedException {
		zk.close();
		System.out.println("�ͻ��˹رգ�");
	}
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		DistributeClient client = new DistributeClient();
		client.connect();
		client.getServerList();
		if (servers.size() > 0) {
			client.work(servers.get(0));
		}else {
			System.out.println("û�з�������");
			client.close();
		}
		try {TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);} catch (InterruptedException e) {e.printStackTrace();}
	}
	
}
