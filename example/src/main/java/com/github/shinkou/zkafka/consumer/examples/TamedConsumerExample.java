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
