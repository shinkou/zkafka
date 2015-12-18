/*
 * Copyright (C) 2015  Chun-Kwong Wong
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.github.shinkou.zkafka.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import scala.collection.JavaConversions;

import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.Json;

import org.apache.kafka.common.KafkaException;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkException;

/*
 * This is a class dedicated to consuming kafka messages with zookeeper for
 * broker and partition info retrieval
 */
public abstract class ZkConsumer extends AbstractConsumer
{
	// C L A S S   C O N S T A N T S ---------------------------------------
	final public static int ZK_TIMEOUT = 10000;
	final public static int CNX_TIMEOUT = 10000;

	// C L A S S   M E M B E R S -------------------------------------------
	protected String m_zookeeperEndpoint;
	protected int m_sessionTimeout;
	protected int m_connectionTimeout;

	protected ZkClient m_zkClient;

	// C O N S T R U C T O R S ---------------------------------------------
	/**
	 * constructor
	 * @param groupId group ID
	 * @param topic kafka topic name
	 * @param endpoint a string representing zookeeper endpoint(s)
	 */
	public ZkConsumer(String groupId, String topic, String endpoint)
	{
		super(groupId, topic);
		init(endpoint);
	}

	// P R I V A T E   M E T H O D S ---------------------------------------
	/**
	 * initialize class members
	 * @param endpoint a string representing zookeeper endpoint(s)
	 */
	private void init(String endpoint)
	{
		m_zookeeperEndpoint = endpoint;

		setSessionTimeout(ZK_TIMEOUT);
		setConnectionTimeout(CNX_TIMEOUT);
	}

	// P R O T E C T E D   M E T H O D S -----------------------------------
	final protected String getZkBrokerPath()
	{
		return "/brokers/topics/" + m_topic;
	}

	final protected String getZkBrokerPath(int brokerId)
	{
		return "/brokers/ids/" + brokerId;
	}

	final protected String getZkBrokerPartitionPath(int partitionId)
	{
		return getZkBrokerPath() + "/partitions/" + partitionId;
	}

	final protected String getZkConsumerPath()
	{
		return "/consumers/" + m_groupId;
	}

	final protected String getZkConsumerOwnerPath()
	{
		return getZkConsumerPath() + "/owners/" + m_topic;
	}

	final protected String getZkConsumerOffsetPath(int partitionId)
	{
		return getZkConsumerPath() + "/offsets/" + m_topic + "/"
			+ partitionId;
	}

	/**
	 * connect to zookeeper cluster
	 */
	final protected void connectZk()
	{
		m_zkClient = new ZkClient
		(
			m_zookeeperEndpoint
			, m_sessionTimeout
			, m_connectionTimeout
			, kafka.utils.ZKStringSerializer$.MODULE$
		);
	}

	/**
	 * make and cache a SimpleConsumer connected to kafka for a given
	 * partition ID
	 * @param partitionId partition ID
	 * @return SimpleConsumer
	 */
	final protected SimpleConsumer connectKafka(int partitionId)
	{

		Map<String, Object> brokerinfo = loadLeaderAddresses(partitionId);

		return connectKafka
		(
			(String) brokerinfo.get("host")
			, (Integer) brokerinfo.get("port")
			, partitionId
		);
	}

	/**
	 * load partition IDs from zookeeper cluster
	 * @return partition IDs
	 */
	final protected List<Integer> loadPartitions()
	{
		String sPath = getZkBrokerPath() + "/partitions";

		if (! m_zkClient.exists(sPath))
			throw new ZkException("Topic " + m_topic + " does not exist");

		List<Integer> partitions = new ArrayList<Integer>();

		for(String s: m_zkClient.getChildren(sPath))
			partitions.add(Integer.parseInt(s));

		return partitions;
	}

	/**
	 * load broker address specified by the given ID from zookeeper cluster
	 * @param brokerId broker ID
	 * @return mapped broker address info
	 */
	@SuppressWarnings("unchecked")
	final protected Map<String, Object> loadBrokerAddress(int brokerId)
	{
		String sPath = getZkBrokerPath(brokerId);

		if (! m_zkClient.exists(sPath))
		{
			throw new ZkException
			(
				"Broker ID " + brokerId + " does not exist"
			);
		}

		String sJson = m_zkClient.<String>readData(sPath);
		Map<String, Object> brokerinfo = null;

		try
		{
			brokerinfo = JavaConversions.<String, Object>mapAsJavaMap
			(
				(scala.collection.Map<String, Object>)
					Json.parseFull(sJson).get()
			);
		}
		catch(KafkaException e)
		{
			e.printStackTrace();
		}

		return brokerinfo;
	}

	/**
	 * load leader address specified by the given partition ID from
	 * zookeeper cluster
	 * @param partitionId partition ID
	 * @return mapped leader address info
	 */
	@SuppressWarnings("unchecked")
	final protected Map<String, Object> loadLeaderAddresses(int partitionId)
	{
		String sPath = getZkBrokerPartitionPath(partitionId) + "/state";

		if (! m_zkClient.exists(sPath))
		{
			throw new ZkException
			(
				"Topic " + m_topic + " or partition ID " + partitionId
					+ " does not exist"
			);
		}

		String sLeader = m_zkClient.<String>readData(sPath);
		Map<String, Object> leaderinfo = null;
		Map<String, Object> brokerinfo = null;

		try
		{
			leaderinfo = JavaConversions.<String, Object>mapAsJavaMap
			(
				(scala.collection.Map<String, Object>)
					Json.parseFull(sLeader).get()
			);

			brokerinfo = loadBrokerAddress((Integer) leaderinfo.get("leader"));
		}
		catch(KafkaException e)
		{
			e.printStackTrace();
		}

		return brokerinfo;
	}

	// P U B L I C   M E T H O D S -----------------------------------------
	/**
	 * get zookeeper endpoint
	 * @return zookeeper endpoint
	 */
	final public String getZkEndpoint()
	{
		return m_zookeeperEndpoint;
	}

	/**
	 * get session timeout
	 * @return session timeout
	 */
	final public int getSessionTimeout()
	{
		return m_sessionTimeout;
	}

	/**
	 * get connection timeout
	 * @return connection timeout
	 */
	final public int getConnectionTimeout()
	{
		return m_connectionTimeout;
	}

	/**
	 * set session timeout
	 * @param timeout session timeout
	 */
	final public void setSessionTimeout(int timeout)
	{
		m_sessionTimeout = timeout;
	}

	/**
	 * set connection timeout
	 * @param timeout connection timeout
	 */
	final public void setConnectionTimeout(int timeout)
	{
		m_connectionTimeout = timeout;
	}

	/**
	 * stop consuming and processing kafka messages
	 */
	@Override
	public void stop()
	{
		for(final SimpleConsumer consumer: m_consumers.values())
			consumer.close();

		m_consumers.clear();
		m_executor.shutdown();
	}

	/**
	 * interrupt current kafka message consumption and processing
	 */
	@Override
	public void interrupt()
	{
		m_executor.shutdownNow();
	}
}
