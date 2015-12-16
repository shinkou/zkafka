package com.github.shinkou.zkafka.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;

/*
 * This is a class dedicated to consuming kafka messages
 */
public abstract class AbstractConsumer
{
	// C L A S S   C O N S T A N T S ---------------------------------------
	final public static int FETCH_SIZE = 1024;
	final public static long MAX_READ = 0L;
	final public static int SO_TIMEOUT = 30000;

	// C L A S S   M E M B E R S -------------------------------------------
	protected String m_groupId;
	protected String m_topic;
	protected int m_fetchSize;
	protected long m_maxRead;
	protected int m_soTimeout;
	protected ExecutorService m_executor;

	// partition to consumer map
	protected Map<Integer, SimpleConsumer> m_consumers;
	// partition to client name
	protected Map<Integer, String> m_clientnames;

	// C O N S T R U C T O R S ---------------------------------------------
	/**
	 * constructor
	 * @param groupId group ID
	 * @param topic kafka topic name
	 */
	public AbstractConsumer(String groupId, String topic)
	{
		init(groupId, topic);
	}

	// P R I V A T E   M E T H O D S ---------------------------------------
	/**
	 * initialize class members
	 * @param groupId group ID
	 * @param topic kafka topic name
	 */
	private void init(String groupId, String topic)
	{
		m_groupId = groupId;
		m_topic = topic;

		setFetchSize(FETCH_SIZE);
		setMaxRead(MAX_READ);
		setSoTimeout(SO_TIMEOUT);

		m_consumers = new HashMap<Integer, SimpleConsumer>();
		m_clientnames = new HashMap<Integer, String>();
	}

	// P R O T E C T E D   M E T H O D S -----------------------------------
	/**
	 * get or generate and store client name for a given partition ID
	 * @param partitionId partition ID
	 * @return client name
	 */
	final protected String getClientname(int partitionId)
	{
		String clientname = m_clientnames.get(partitionId);

		if (null == clientname)
		{
			clientname = "Client_" + m_topic + "_" + partitionId + "_"
				+ System.currentTimeMillis();

			m_clientnames.put(partitionId, clientname);
		}

		return clientname;
	}

	/**
	 * make and cache a SimpleConsumer connected to kafka for a given
	 * partition ID
	 * @param host host
	 * @param port port
	 * @param partitionId partition ID
	 * @return SimpleConsumer
	 */
	final protected SimpleConsumer connectKafka
	(
		String host
		, int port
		, int partitionId
	)
	{
		SimpleConsumer consumer = new SimpleConsumer
		(
			host
			, port
			, m_soTimeout
			, m_fetchSize
			, getClientname(partitionId)
		);

		return consumer;
	}

	/**
	 * get offset from partition broker before the given timestamp
	 * @param partition partition ID
	 * @param timestamp timestamp
	 * @return offset
	 */
	final protected long getOffsetBefore(int partition, long timestamp)
	{
		SimpleConsumer consumer = m_consumers.get(partition);
		if (null == consumer) return 0L;

		String clientname = getClientname(partition);

		TopicAndPartition tap = new TopicAndPartition(m_topic, partition);

		Map<TopicAndPartition, PartitionOffsetRequestInfo> reqInfo
			= new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		reqInfo.put(tap, new PartitionOffsetRequestInfo(timestamp, 1));

		OffsetRequest req = new OffsetRequest
		(
			reqInfo
			, kafka.api.OffsetRequest.CurrentVersion()
			, clientname
		);

		OffsetResponse res = consumer.getOffsetsBefore(req);

		if (res.hasError())
		{
			System.err.println
			(
				"Error fetching offset from broker. Reason: "
					+ res.errorCode(m_topic, partition)
			);
			return 0L;
		}

		long[] offsets = res.offsets(m_topic, partition);
		return offsets[0];
	}

	// P U B L I C   M E T H O D S -----------------------------------------
	/**
	 * get group ID
	 * @return group ID
	 */
	final public String getGroupId()
	{
		return m_groupId;
	}

	/**
	 * get kafka topic name
	 * @return kafka topic name
	 */
	final public String getTopic()
	{
		return m_topic;
	}

	/**
	 * get fetch size
	 * @return fetch number of kafka messages per fetch
	 */
	final public int getFetchSize()
	{
		return m_fetchSize;
	}

	/**
	 * get maximum number of reads
	 * @return maxmium number of reads
	 */
	final public long getMaxRead()
	{
		return m_maxRead;
	}

	/**
	 * get socket timeout
	 * @return socket timeout
	 */
	final public int getSoTimeout()
	{
		return m_soTimeout;
	}

	/**
	 * set fetch size
	 * @param fetchSize number of kafka messages per fetch
	 */
	final public void setFetchSize(int fetchSize)
	{
		m_fetchSize = fetchSize;
	}

	/**
	 * set maximum number of reads
	 * @param maxRead maximum number of reads
	 */
	final public void setMaxRead(long maxRead)
	{
		m_maxRead = maxRead;
	}

	/**
	 * set socket timeout
	 * @param soTimeout socket timeout
	 */
	final public void setSoTimeout(int soTimeout)
	{
		m_soTimeout = soTimeout;
	}

	/**
	 * start consuming and processing kafka messages
	 */
	abstract public void start();

	/**
	 * stop consuming and processing kafka messages
	 */
	abstract public void stop();

	/**
	 * interrupt current kafka message consumption and processing
	 */
	abstract public void interrupt();

	/**
	 * process kafka messages
	 * @param bytes kafka message in the form of bytes
	 */
	abstract public void process(byte[] bytes);
}
