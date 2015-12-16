package com.github.shinkou.zkafka.consumer.examples;

import com.github.shinkou.zkafka.consumer.UntamedConsumer;

public class UntamedConsumerExample extends UntamedConsumer
{
	public UntamedConsumerExample
	(
		String groupId
		, String topic
		, String endpoint
	)
	{
		super(groupId, topic, endpoint);
	}

	// this is the only function we need to override in most cases
	@Override
	public void process(byte[] data)
	{
		try
		{
			System.out.println(new String(data, "UTF-8"));
		}
		catch(Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public static void main(String[] args)
	{
		final UntamedConsumerExample consumer = new UntamedConsumerExample
		(
			System.getProperty("group.id", "tamedconsumerexample-test-group")
			, System.getProperty("topic", "tamedconsumerexample-test-queue")
			, System.getProperty("zookeeper", "localhost:2181")
		);

		consumer.setFetchSize(Integer.getInteger("fetchsize", FETCH_SIZE));
		consumer.setMaxRead(Long.getLong("maxread", MAX_READ));
		consumer.setTimestamp
		(
			(System.currentTimeMillis() / 1000)
				- Long.getLong("rewind", 3600)
		);
		if (Boolean.getBoolean("earliest")) consumer.setEarliest();
		else if (Boolean.getBoolean("latest")) consumer.setLatest();

		Runtime.getRuntime().addShutdownHook
		(
			new Thread(() -> consumer.interrupt())
		);

		consumer.start();
		consumer.stop();
	}
}
