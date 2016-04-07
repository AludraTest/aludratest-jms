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

import org.aludratest.exception.*;
import org.aludratest.service.SystemConnector;
import org.aludratest.service.TechnicalArgument;
import org.aludratest.service.TechnicalLocator;
import org.aludratest.service.jms.JmsCondition;
import org.aludratest.service.jms.JmsInteraction;
import org.aludratest.service.jms.JmsVerification;
import org.aludratest.testcase.event.attachment.Attachment;
import org.apache.commons.lang.StringUtils;
import org.databene.commons.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JmsActionImpl implements JmsInteraction, JmsCondition, JmsVerification {

	private static final Logger LOGGER = LoggerFactory.getLogger(JmsActionImpl.class);

	private InitialContext context;

    private ConnectionFactory connectionFactory;

    private Connection connection;

	/**
	 * Using additional connections for topic subscriptions to avoid interferrence with
	 * starting / stopping the default connection during queue-messaging
	 */
    private Map<String, Connection> dynamicConnections;

	/**
	 * Need to keep track of durable subscribers to be abled to disconnect them on
	 * command.
	 */
    private Map<String, TopicSubscriber> durableConsumers;

	private Session session;

    private String userName;

    private String password;

	private String clientId;

	public JmsActionImpl(ConnectionFactory connectionFactory, InitialContext context, final String userName, final String password) {
        this.dynamicConnections = new HashMap<String, Connection>();
        this.durableConsumers = new HashMap<String, TopicSubscriber>();
        this.connectionFactory = connectionFactory;
		this.context = context;
        this.userName = userName;
        this.password = password;
		this.clientId = userName + "@" + JmsActionImpl.class.getSimpleName() + this.hashCode();
    }

	public void close() {
		LOGGER.info("Closing JmsServcie for clientId " + this.clientId );
		for (TopicSubscriber subscriber : this.durableConsumers.values()) {
			try {
				subscriber.close();
			}
			catch (JMSException e) {
				LOGGER.debug("Failed to close subscriber: ", e );
			}
		}
		for (Map.Entry<String, Connection> entry : this.dynamicConnections.entrySet()) {
			try {
				Connection dynC = entry.getValue();
				dynC.stop();
				dynC.close();
			}
			catch (JMSException e) {
				LOGGER.debug("Failed to close dynamic connection [ " + entry.getKey() + " ] : ", e );
			}
		}
		if (session != null) {
			try {
                session.close();
			}
			catch (JMSException e) {
				LOGGER.debug("Failed to close jms session : ", e );
			}
		}
		if (connection != null) {
			try {
				connection.stop();
				connection.close();
			}
			catch (JMSException e) {
				LOGGER.debug("Failed to close jms connection for client-id [ " + this.clientId + " ] : ", e );
			}
		}
	}

	@Override
	public List<Attachment> createDebugAttachments() {
		return null;
	}

	@Override
	public List<Attachment> createAttachments(Object object, String title) {
		return null;
	}

	@Override
	public void setSystemConnector(SystemConnector systemConnector) {
	}

	@Override
	public void assertDestinationAvailable(String destinationName) {
		if (!isDestinationAvailable(destinationName)) {
			throw new FunctionalFailure("Destination " + destinationName + " is not available");
		}
	}

	@Override
	public boolean isDestinationAvailable(String destinationName) {
		try {
			Object o = context.lookup(destinationName);
			return (o instanceof Destination);
		}
		catch (NamingException e) {
			return false;
		}
	}

	@Override
	public void sendTextMessage(String text, String destinationName) {
		TextMessage msg = createTextMessage();
		try {
			msg.setText(text);
		}
		catch (JMSException e) {
			throw new TechnicalException("Could not set text of text message", e);
		}
		sendMessage(msg, destinationName);
	}

	@Override
	public void sendObjectMessage(Serializable object, String destinationName) {
		ObjectMessage msg = createObjectMessage();
		try {
			msg.setObject(object);
		}
		catch (JMSException e) {
			throw new TechnicalException("Could not set object of object message", e);
		}
		sendMessage(msg, destinationName);
	}

	@Override
	public TextMessage createTextMessage() {
		try {
			return getSession().createTextMessage();
		}
		catch (JMSException e) {
			throw new AccessFailure("Could not create text message", e);
		}
	}

	@Override
	public ObjectMessage createObjectMessage() {
		try {
			return getSession().createObjectMessage();
		}
		catch (JMSException e) {
			throw new AccessFailure("Could not create object message", e);
		}
	}

	@Override
	public BytesMessage createBytesMessage() {
		try {
			return getSession().createBytesMessage();
		}
		catch (JMSException e) {
			throw new AccessFailure("Could not create bytes message", e);
		}
	}

	@Override
	public MapMessage createMapMessage() {
		try {
			return getSession().createMapMessage();
		}
		catch (JMSException e) {
			throw new AccessFailure("Could not create map message", e);
		}
	}

	@Override
	public StreamMessage createStreamMessage() {
		try {
			return getSession().createStreamMessage();
		}
		catch (JMSException e) {
			throw new AccessFailure("Could not create stream message", e);
		}
	}

	@Override
	public void sendMessage(Message message, String destinationName) {
		MessageProducer producer = null;
		try {
			LOGGER.debug("Sending message to destination "  + destinationName);
			Destination dest = (Destination) context.lookup(destinationName);
			producer = getSession().createProducer(dest);
			this.startConnection();
			producer.send(message);
			this.stopConnection();
		}
		catch (NamingException e) {
			throw new AutomationException("Could not lookup destination " + destinationName, e);
		}
		catch (ClassCastException e) {
			throw new AutomationException("JNDI object with name " + destinationName + " is no destination", e);
		}
		catch (JMSException e) {
			throw new AccessFailure("Could not send JMS message", e);
		}
		finally {
			if (producer != null) {
				try {
					producer.close();
				}
				catch (JMSException e) {
				}
			}
		}
	}

	@Override
	public Message receiveMessage(String destinationName, long timeout) {
		MessageConsumer consumer = null;
		try {
			Destination dest = (Destination) context.lookup(destinationName);
			consumer = getSession().createConsumer(dest);
            this.startConnection();
			Message msg;
			if (timeout == -1) {
				msg = consumer.receive();
			}
			else {
				msg = consumer.receive(timeout);
			}
			this.stopConnection();
			if (msg == null) {
				throw new PerformanceFailure("Destination " + destinationName + " did not deliver a message within timeout");
			}
			return msg;
		}
		catch (NamingException e) {
			throw new AutomationException("Could not lookup destination " + destinationName, e);
		}
		catch (ClassCastException e) {
			throw new AutomationException("JNDI object with name " + destinationName + " is no destination", e);
		}
		catch (JMSException e) {
			throw new AccessFailure("Could not receive JMS message", e);
		}
		finally {
			if (consumer != null) {
				try {
					consumer.close();
				}
				catch (JMSException e) {
				}
			}
		}
	}

	/**
	 * Build a new connection with the given clientId
	 *
	 * @param clientId	the clientId

	 * @return
     */
    private Connection buildConnection(String clientId) {
		try {
			Connection result;
            if (StringUtil.isEmpty(userName)) {
                result = this.connectionFactory.createConnection();
            }
            else {
				result = this.connectionFactory.createConnection(this.userName, this.password);
            }
			result.setClientID(clientId);

			return result;
        } catch (JMSException e) {
            throw new TechnicalException("Could not establish JMS connection", e);
        }
    }

    private Connection getConnection() {
        if (this.connection == null) {
            this.connection = buildConnection(this.clientId);
        }
        return this.connection;
    }

	/**
	 * Creates a dynamic connection for the given subscriptionName.
	 *
	 * According to JavaDoc of {@link TopicSession#createDurableSubscriber(Topic, String, String, boolean)}
	 * durablesubscriptions must use the same clientIds on every connection to a particular subscription.
	 * Therefore the clientId used for dynamic-connections is derived from the folloging rule:
	 *
	 * this.userName + "@" + this.getClass().getSimpleName() + "[" + @param subscriptionName + "]"
	 *
	 * Even new Instances of JmsActionImpl will produce a clientId that is abled to reconnect to existing
	 * durable subscriptions.
	 *
	 * @param subscriptionName	the subscriptionname
	 * @return	The connection.
	 *
	 * @throws JMSException
     */
    private Connection getDynamicConnection(String subscriptionName) throws  JMSException {

		final String dynClientID =
				this.userName + "@" + this.getClass().getSimpleName() + "[" +subscriptionName + "]";
        Connection dynC = this.dynamicConnections.get(dynClientID);
        if (dynC == null) {
            dynC = buildConnection(dynClientID);
            this.dynamicConnections.put(dynClientID,dynC);
            dynC.start();
        }
        return dynC;

    }


	private Session getSession() throws JMSException {
		if (session == null) {
			session = getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
		}
		return session;
	}

	@Override
	public void subscribeTopic(MessageListener listener, @TechnicalLocator String destinationName, @TechnicalArgument String messageSelector, @TechnicalArgument String subscriptionName, @TechnicalArgument boolean durable) throws JMSException  {
		if (StringUtils.isEmpty(subscriptionName)) {
			throw new IllegalArgumentException("subscriptionName must be provided to subscribe!");
		}
        Topic topic;
        try {
            topic = (Topic) context.lookup(destinationName);
        } catch (NamingException e) {
            throw new AutomationException("Could not lookup destination " + destinationName, e);
        }

		LOGGER.debug("Creating topic-subscriber for topic " + destinationName + " and subscriptionname " + subscriptionName);
	    Connection c = getDynamicConnection(subscriptionName);

        TopicSession ts = (TopicSession) c.createSession(false,Session.AUTO_ACKNOWLEDGE);
		if (durable) {
            TopicSubscriber subscriber = ts.createDurableSubscriber(topic,subscriptionName,messageSelector,false);
            subscriber.setMessageListener(listener);
            this.durableConsumers.put(subscriptionName,subscriber);
		} else {
            ts.createSubscriber(topic,messageSelector,true).setMessageListener(listener);
        }

	}


    @Override
    public void unsubscribeTopic(@TechnicalArgument String subscriptionName) throws JMSException {
        if (StringUtils.isEmpty(clientId)) {
            throw new IllegalArgumentException("Client-Id must be provided to unsubscribe");
        }
		LOGGER.debug("Unsubscribing from subscription " + subscriptionName);
        Connection c = getDynamicConnection(subscriptionName);
        c.stop();

        TopicSubscriber subscriber = this.durableConsumers.get(subscriptionName);
        if (subscriber != null) {
            subscriber.close();
            c.createSession(false,Session.AUTO_ACKNOWLEDGE).unsubscribe(subscriptionName);
            this.durableConsumers.remove(subscriptionName);
        }
        c.close();
        this.dynamicConnections.remove(c);

    }

    private void startConnection() throws JMSException {
        this.getConnection().start();
    }

    private void stopConnection() throws JMSException {
        this.getConnection().stop();
    }
}
