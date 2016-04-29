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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import kafka.utils.ZkUtils;

/*
 * This is a class dedicated to consuming kafka messages and bookkeeping
 * offsets in zookeeper
 */
public abstract class TamedConsumer extends UntamedConsumer
{
	// C L A S S   C O N S T A N T S ---------------------------------------
	final public static int SAVE_INTERVAL = 60000;

	// C L A S S   M E M B E R S -------------------------------------------
	protected int m_saveInterval;

	// partition to offset map
	protected Map<Integer, Long> m_offsets;

	// C O N S T R U C T O R S ---------------------------------------------
	/**
	 * constructor
	 * @param groupId group ID
	 * @param topic kafka topic name
	 * @param endpoint a string representing zookeeper endpoint(s)
	 */
	public TamedConsumer(String groupId, String topic, String endpoint)
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
		setSaveInterval(SAVE_INTERVAL);

		m_offsets = new HashMap<Integer, Long>();
	}

	// P R O T E C T E D   M E T H O D S -----------------------------------
	/**
	 * ensure the group has topic ownership entry exists in zookeeper
	 */
	final protected void zkEnsureOwnership()
	{
		String sPath = getZkConsumerOwnerPath();

		if (m_zkClient.exists(sPath)) return;

		ZkUtils.makeSurePersistentPathExists
		(
			m_zkClient
			, sPath
		);
	}

	/**
	 * load offset specified by the given partition ID from
	 * zookeeper cluster
	 * @param partitionId partition ID
	 * @return offset
	 */
	final protected long loadOffset(int partitionId)
	{
		long offset = m_timestamp;
		String sPath = getZkConsumerOffsetPath(partitionId);

		if (m_zkClient.exists(sPath))
		{
			try
			{
				offset = Long.parseLong(m_zkClient.<String>readData(sPath));
			}
			catch(NumberFormatException e)
			{
				e.printStackTrace();
			}
		}

		return offset;
	}

	/**
	 * save offset with the given partition ID on zookeeper cluster
	 * @param partitionId partition ID
	 * @param offset offset
	 */
	final protected void saveOffset(int partitionId, long offset)
	{
		String sPath = getZkConsumerOffsetPath(partitionId);

		ZkUtils.updatePersistentPath
		(
			m_zkClient
			, sPath
			, Long.toString(offset)
		);
	}

	// P U B L I C   M E T H O D S -----------------------------------------
	/**
	 * get (offset) save interval
	 * @return save interval
	 */
	final public int getSaveInterval()
	{
		return m_saveInterval;
	}

	/**
	 * set (offset) save interval
	 * @param interval save interval
	 */
	final public void setSaveInterval(int interval)
	{
		m_saveInterval = interval;
	}

	/**
	 * start consuming and processing kafka messages
	 */
	@Override
	public void start()
	{
		if (null == m_zkClient) connectZk();

		zkEnsureOwnership();

		List<Integer> partitions = loadPartitions();

		m_executor = Executors.newFixedThreadPool(partitions.size());

		final AtomicLong cntRead = new AtomicLong(0L);

		for(final Integer partition: partitions)
		{
			m_consumers.put(partition, connectKafka(partition));
			m_offsets.put(partition, loadOffset(partition));

			m_executor.submit
			(() -> {
				SimpleConsumer consumer = m_consumers.get(partition);
				long offset = m_offsets.get(partition);
				long savemark = System.currentTimeMillis();
				long curOffset = offset;

				FetchRequest req = null;
				FetchResponse res = null;
				whileloop:
				while(m_maxRead > cntRead.get() || 0 >= m_maxRead)
				{
					req = new FetchRequestBuilder()
						.clientId(m_clientnames.get(partition))
						.addFetch(m_topic, partition, curOffset, m_fetchSize)
						.build();

					// XXX:2016-04-28:Chun:workaround of the next if block
					try
					{
						res = consumer.fetch(req);
					}
					catch(Exception e)
					{
						System.err.println
						(
							"Errors detected while fetching.  Details follow."
						);
						e.printStackTrace(System.err);

						consumer.close();
						consumer = connectKafka(partition);
						m_consumers.put(partition, consumer);

						continue;
					}

					// FIXME:2016-04-28:Chun:not work with disconnection??
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
						m_offsets.put(partition, curOffset);
						ByteBuffer buf = mao.message().payload();
						byte[] bytes = new byte[buf.limit()];
						buf.get(bytes);
						process(bytes);
					}

					if
					(
						(System.currentTimeMillis() - savemark)
							> m_saveInterval
					)
					{
						saveOffset(partition, curOffset);
						savemark = System.currentTimeMillis();
					}
				}

				saveOffset(partition, curOffset);
			});
		}
	}

	/**
	 * interrupt current kafka message consumption and processing
	 */
	@Override
	public void interrupt()
	{
		List<Runnable> runnables = m_executor.shutdownNow();

		for(Map.Entry<Integer, Long> entry: m_offsets.entrySet())
			saveOffset(entry.getKey(), entry.getValue());
	}
}
