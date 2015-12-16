package com.github.shinkou.zkafka.consumer.examples;

import com.github.shinkou.zkafka.consumer.TamedConsumer;

public class TamedConsumerExample extends TamedConsumer
{
	public TamedConsumerExample
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
		final TamedConsumerExample consumer = new TamedConsumerExample
		(
			System.getProperty("group.id", "tamedconsumerexample-test-group")
			, System.getProperty("topic", "tamedconsumerexample-test-queue")
			, System.getProperty("zookeeper", "localhost:2181")
		);

		consumer.setFetchSize(Integer.getInteger("fetchsize", FETCH_SIZE));
		consumer.setMaxRead(Long.getLong("maxread", MAX_READ));
		consumer.setSaveInterval(Integer.getInteger("saveinterval", 60) * 1000);

		Runtime.getRuntime().addShutdownHook
		(
			new Thread(() -> consumer.interrupt())
		);

		consumer.start();
		consumer.stop();
	}
}
