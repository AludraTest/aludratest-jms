/*
 * Copyright (C) 2015 Hamburg Sud and the contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.aludratest.service.jms.impl;

import java.io.Closeable;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;

import org.aludratest.exception.AccessFailure;
import org.aludratest.exception.AutomationException;

/**
 * Encapsulates access to {@link MessageConsumer} and underlying {@link Connection}.
 * @author Volker Bergmann
 */

public class TopicHandler implements Closeable {
	
	private MessageConsumer subscriber;
	private Connection connection;
	private String subscriptionName;
	boolean durable;
	
	public TopicHandler(String subscriptionName, boolean durable, MessageConsumer subscriber, Connection connection) {
		this.subscriptionName = subscriptionName;
		this.durable = durable;
		this.subscriber = subscriber;
		this.connection = connection;
	}
	
	public boolean isDurable() {
		return durable;
	}

	/** Waits until a message arrives, applying no timeout */
	public Message receive() {
		try {
			return subscriber.receive();
		} catch (JMSException e) {
			throw new AccessFailure("Could not receive JMS message", e);
		}
	}

	/** Waits until a message arrives, applying a timeout
	 *  @param timeout the number of milliseconds to wait, <code>0</code> means to wait without timeout.
	 *  @return the received message or <code>null</code> if no message arrived within the timeout */
	public Message receive(long timeout) {
		try {
			return subscriber.receive(timeout);
		} catch (JMSException e) {
			throw new AccessFailure("Could not receive JMS message", e);
		}
	}

	public void start() {
		try {
			this.connection.start();
		} catch (JMSException e) {
			throw new AutomationException("Starting connection failed", e);
		}
	}

	public void stop() {
		try {
			this.connection.stop();
		} catch (JMSException e) {
			throw new AutomationException("Stopping connection failed for " + toString(), e);
		}
	}
	
	@Override
	public void close() {
		try {
			subscriber.close();
		} catch (JMSException e) {
			throw new AutomationException("Failed to close the subscriber for " + toString(), e);
		}
		try {
			connection.close();
		} catch (JMSException e) {
			throw new AutomationException("Failed to close the connection for " + toString(), e);
		}
	}
	
	@Override
	public String toString() {
		return "dynamic connection [ " + subscriptionName + " ]";
	}
	
}
