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
	
	//KeeperException �쳣:һ���Ǵ����ڵ�ʱ�ڵ��Ѵ��ڣ����߻�ȡ�ڵ�ʱ�ڵ㲻���ڵ��µ� 
	//InterruptedException �쳣: һ�����߳�������ȴ�ʱ����ϵ��µ�
	//����zookeeper�ͻ��ˣ�����Ӽ�����
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
	
	// �����ӽڵ�
	@Test
	public void create() throws KeeperException, InterruptedException {
		
		//����1 �� Ҫ�����Ľڵ��·��
		//����2 �� �ڵ�����
		//����3 �� �ڵ�Ȩ��
		//����4 : �ڵ�����
		String nodeCreated = zkClient.create("/edg", "iboy".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println("creat node: " + nodeCreated);
	}
	
	//�޸Ľڵ�����
	@Test
	public void set() throws KeeperException, InterruptedException {
		String node = "/rng";
		zkClient.setData(node, "windows".getBytes(), -1);
		System.out.println("�޸ĳɹ���");
	}
	
	//��ȡ�ӽڵ㲢�����ӽڵ�仯
	@Test
	public void getChildren() throws KeeperException, InterruptedException {
		
		//��ȡ�ӽڵ�
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
		
		//�̲߳��˳������Լ����ӽڵ�仯
		try {TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);} catch (InterruptedException e) {e.printStackTrace();}
	}
	
	//�жϽڵ��Ƿ����
	@Test
	public void exist() throws KeeperException, InterruptedException {
		Stat stat = null;
		String node = "/rng";
		stat = zkClient.exists(node, false);
		System.out.println(node + (stat == null ? " not exist" : " exist"));
	}
	
	//ɾ���ڵ�
	@Test
	public void delete() throws InterruptedException, KeeperException {
		String node = "/rng4";
		zkClient.delete(node, -1);
		System.out.println("delete node: " + node);
	}
}