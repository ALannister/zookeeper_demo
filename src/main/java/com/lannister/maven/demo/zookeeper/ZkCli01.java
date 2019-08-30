package com.lannister.maven.demo.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.core.config.Node;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

public class ZkCli01 {
	private static String connectString = "192.168.2.101:2181,192.168.2.102:2181,192.168.2.103:2181";
	private static int sessionTimeout = 2000;
	private ZooKeeper zkClient = null;

	//KeeperException 异常:一般是创建节点时节点已存在，或者获取节点时节点不存在导致的
	//InterruptedException 异常: 一般是线程阻塞或等待时被打断导致的
	//创建zookeeper客户端，并添加监听器
	@Before
	public void init() throws IOException{

		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

			public void process(WatchedEvent event) {
				System.out.println("****Watcher****** " + event.getType() + " -- " + event.getPath());

				try {
					zkClient.getChildren("/", true);
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		});

	}

	// 创建子节点
	@Test
	public void create() throws KeeperException, InterruptedException {

		//参数1 ： 要创建的节点的路径
		//参数2 ： 节点数据
		//参数3 ： 节点权限
		//参数4 : 节点类型
		String nodeCreated = zkClient.create("/edg", "iboy".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println("creat node: " + nodeCreated);
	}

	//修改节点数据
	@Test
	public void set() throws KeeperException, InterruptedException {
		String node = "/rng";
		zkClient.setData(node, "windows".getBytes(), -1);
		System.out.println("修改成功！");
	}

	//获取子节点并监听子节点变化
	@Test
	public void getChildren() throws KeeperException, InterruptedException {

		//获取子节点
		List<String> children = null;
		String node = "/";
		children = zkClient.getChildren(node, true);

		for(String child : children) {
			System.out.println(child + ":");
			Stat stat = new Stat();
			byte[] data = zkClient.getData(node + child, false, stat);
			System.out.println("data: " + new String(data));
			System.out.println("stat: " + stat);
			System.out.println();
		}

		//线程不退出，可以监听子节点变化
		try {TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);} catch (InterruptedException e) {e.printStackTrace();}
	}

	//判断节点是否存在
	@Test
	public void exist() throws KeeperException, InterruptedException {
		Stat stat = null;
		String node = "/rng";
		stat = zkClient.exists(node, false);
		System.out.println(node + (stat == null ? " not exist" : " exist"));
	}

	//删除节点
	@Test
	public void delete() throws InterruptedException, KeeperException {
		String node = "/rng4";
		zkClient.delete(node, -1);
		System.out.println("delete node: " + node);
	}
}