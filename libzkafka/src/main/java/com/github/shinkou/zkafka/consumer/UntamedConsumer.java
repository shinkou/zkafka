package com.github.shinkou.zkafka.consumer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

/*
 * This is a class dedicated to consuming kafka messages with zookeeper for
 * broker and partition info retrieval
 */
public abstract class UntamedConsumer extends ZkConsumer
{
	// C L A S S   C O N S T A N T S ---------------------------------------
	final public static long LATESTTIME = -1L;
	final public static long EARLIESTTIME = -2L;

	// C L A S S   M E M B E R S -------------------------------------------
	protected long m_timestamp;

	// C O N S T R U C T O R S ---------------------------------------------
	/**
	 * constructor
	 * @param groupId group ID
	 * @param topic kafka topic name
	 * @param endpoint a string representing zookeeper endpoint(s)
	 */
	public UntamedConsumer(String groupId, String topic, String endpoint)
	{
		super(groupId, topic, endpoint);
		init();
	}

	// P R I V A T E   M E T H O D S ---------------------------------------
	/**
	 * initialize class members
	 */
	private void init()
	{
		setLatest();
	}

	// P U B L I C   M E T H O D S -----------------------------------------
	/**
	 * get whether latest offsets will be used
	 * @return true or false
	 */
	final public boolean getLatest()
	{
		return (m_timestamp == LATESTTIME);
	}

	/**
	 * get whether earliest offsets will be used
	 * @return true or false
	 */
	final public boolean getEarliest()
	{
		return (m_timestamp == EARLIESTTIME);
	}

	/**
	 * get timestamp
	 * @return timestamp
	 */
	final public long getTimestamp()
	{
		return m_timestamp;
	}

	/**
	 * set to use latest offsets
	 * @param latest true or false
	 */
	final public void setLatest()
	{
		m_timestamp = LATESTTIME;
	}

	/**
	 * set to use earliest offsets
	 * @param latest true or false
	 */
	final public void setEarliest()
	{
		m_timestamp = EARLIESTTIME;
	}

	/**
	 * set to use timestamp to retrieve offsets
	 * @param timestamp timestamp
	 */
	final public void setTimestamp(long timestamp)
	{
		m_timestamp = timestamp;
	}

	/**
	 * start consuming and processing kafka messages
	 */
	@Override
	public void start()
	{
		if (null == m_zkClient) connectZk();

		List<Integer> partitions = loadPartitions();

		m_executor = Executors.newFixedThreadPool(partitions.size());

		final AtomicLong cntRead = new AtomicLong(0L);

		for(final Integer partition: partitions)
		{
			m_consumers.put(partition, connectKafka(partition));

			m_executor.submit
			(() -> {
				SimpleConsumer consumer = m_consumers.get(partition);
				long curOffset = getOffsetBefore(partition, m_timestamp);

				FetchRequest req = null;
				FetchResponse res = null;
				whileloop:
				while(m_maxRead > cntRead.get() || 0 >= m_maxRead)
				{
					req = new FetchRequestBuilder()
						.clientId(m_clientnames.get(partition))
						.addFetch(m_topic, partition, curOffset, m_fetchSize)
						.build();
					res = consumer.fetch(req);

					if (res.hasError())
					{
						if
						(
							res.errorCode(m_topic, partition)
								== ErrorMapping.OffsetOutOfRangeCode()
						)
						{
							System.err.println
							(
								"Invalid offset: " + curOffset
							);

							curOffset = getOffsetBefore
							(
								partition
								, m_timestamp
							);

							System.err.println
							(
								"Valid offset obtained: " + curOffset
							);
						}
						else
						{
							System.err.println
							(
								"Errors detected in fetch response.  Code: "
									+ res.errorCode(m_topic, partition)
							);

							consumer.close();
							consumer = connectKafka(partition);
							m_consumers.put(partition, consumer);
						}

						continue;
					}

					for
					(
						MessageAndOffset mao:
							res.messageSet(m_topic, partition)
					)
					{
						if
						(
							0 < m_maxRead
							&& m_maxRead < cntRead.incrementAndGet()
						)
							break whileloop;

						if (mao.offset() < curOffset)
						{
							System.err.println
							(
								"Found an old offset: " + mao.offset()
									+ " Expecting: " + curOffset
							);

							continue;
						}

						curOffset = mao.nextOffset();
						ByteBuffer buf = mao.message().payload();
						byte[] bytes = new byte[buf.limit()];
						buf.get(bytes);
						process(bytes);
					}
				}
			});
		}
	}
}