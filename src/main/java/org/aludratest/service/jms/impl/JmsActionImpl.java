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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.aludratest.exception.AccessFailure;
import org.aludratest.exception.AutomationException;
import org.aludratest.exception.FunctionalFailure;
import org.aludratest.exception.PerformanceFailure;
import org.aludratest.exception.TechnicalException;
import org.aludratest.service.SystemConnector;
import org.aludratest.service.TechnicalArgument;
import org.aludratest.service.TechnicalLocator;
import org.aludratest.service.jms.JmsCondition;
import org.aludratest.service.jms.JmsInteraction;
import org.aludratest.service.jms.JmsVerification;
import org.aludratest.service.jms.data.FileMessageData;
import org.aludratest.service.jms.data.ObjectMessageData;
import org.aludratest.service.jms.data.TextMessageData;
import org.aludratest.testcase.event.attachment.Attachment;
import org.aludratest.testcase.event.attachment.StringAttachment;
import org.databene.commons.Assert;
import org.databene.commons.Base64Codec;
import org.databene.commons.CollectionUtil;
import org.databene.commons.IOUtil;
import org.databene.commons.StringUtil;
import org.databene.commons.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsActionImpl implements JmsInteraction, JmsCondition, JmsVerification {

	private static final Logger LOGGER = LoggerFactory.getLogger(JmsActionImpl.class);

	private InitialContext context;

    private ConnectionFactory connectionFactory;

    private Connection connection;

	/**
	 * Need to keep track of durable subscribers to be able to disconnect them on
	 * command.
	 */
    private Map<String, TopicHandler> topicHandlers;

	private Session session;

    private String userName;

    private String password;

	private String clientId;
	
	private String recentMessage;


	public JmsActionImpl(ConnectionFactory connectionFactory, InitialContext context, final String userName, final String password) {
        this.connectionFactory = connectionFactory;
		this.context = context;
        this.userName = userName;
        this.password = password;
        this.topicHandlers = new HashMap<String, TopicHandler>();
		this.clientId = userName + "@" + JmsActionImpl.class.getSimpleName() + this.hashCode();
		this.recentMessage = null;
    }

	public void close() {
		LOGGER.info("Closing JmsService for clientId " + this.clientId );
		for (TopicHandler handler : this.topicHandlers.values()) {
			handler.stop();
			handler.close();
		}
		close(session);
		stopAndClose(connection, "jms connection for client-id [ " + this.clientId + " ]");
	}
	
	@Override
	public List<Attachment> createDebugAttachments() {
        List<Attachment> attachments = new ArrayList<Attachment>();
        if (this.recentMessage != null) {
            attachments.add(new StringAttachment("message content", this.recentMessage, "txt"));
        }
        return attachments;
	}

	@Override
	public List<Attachment> createAttachments(Object object, String title) {
		return CollectionUtil.toListOfType(Attachment.class, new StringAttachment(title, String.valueOf(object), ".txt"));
	}

	@Override
	public void setSystemConnector(SystemConnector systemConnector) {
		// not supported
	}

	@Override
	public void assertDestinationAvailable(String destinationName) {
		memorizeMessage(null);
		if (!isDestinationAvailable(destinationName)) {
			throw new FunctionalFailure("Destination " + destinationName + " is not available");
		}
	}

	@Override
	public boolean isDestinationAvailable(String destinationName) {
		memorizeMessage(null);
		try {
			Object o = context.lookup(destinationName);
			return (o instanceof Destination);
		}
		catch (NamingException e) {
			return false;
		}
	}
	

	@Override
	public void sendMessage(TextMessageData textMessageData, String destinationName) {
		memorizeMessage(textMessageData.getMessageText());
		try {
			TextMessage message = createTextMessage();
			message.setText(textMessageData.getMessageText());
						
			sendMessage(addPropertiesToMessage(message, textMessageData.getProperties()), destinationName);
		}
		catch (JMSException e) {
			throw new TechnicalException("Could not send text message", e);
		}
		
	}

	@Override
	public void sendMessage(ObjectMessageData objectMessageData, String destinationName) {
		memorizeMessage(objectMessageData.getMessageObject());
		try {
			ObjectMessage msg = createObjectMessage();
			msg.setObject(objectMessageData.getMessageObject());
			
			sendMessage(addPropertiesToMessage(msg, objectMessageData.getProperties()), destinationName);
			
		}
		catch (JMSException e) {
			throw new TechnicalException("Could not send object message", e);
		}
		
	}

	

	@Override
	public String sendMessage(FileMessageData fileMessageData, String destinationName) {
		try {
			String fileContent = IOUtil.getContentOfURI(fileMessageData.getFileUri());
			
			TextMessageData textMessageData = new TextMessageData(fileContent);
			textMessageData.setProperties(fileMessageData.getProperties());
			
			sendMessage(textMessageData, destinationName);
			return fileContent;
		}
		catch (IOException e) {
			throw new AccessFailure("File access failed", e);
		}
	}

	

	@Override
	public void sendTextMessage(String text, String destinationName) {
		memorizeMessage(text);
		try {
			TextMessage message = createTextMessage();
			message.setText(text);
			sendMessage(message, destinationName);
		}
		catch (JMSException e) {
			throw new TechnicalException("Could not set text of text message", e);
		}
	}

	@Override
	public void sendObjectMessage(Serializable object, String destinationName) {
		memorizeMessage(object);
		try {
			ObjectMessage msg = createObjectMessage();
			msg.setObject(object);
			sendMessage(msg, destinationName);
		}
		catch (JMSException e) {
			throw new TechnicalException("Could not set object of object message", e);
		}
	}

	@Override
	public String sendFileAsTextMessage(String fileUri, String destinationName) {
		try {
			String fileContent = IOUtil.getContentOfURI(fileUri);
			memorizeMessage(fileContent);
			sendTextMessage(fileContent, destinationName);
			return fileContent;
		}
		catch (IOException e) {
			throw new AccessFailure("File access failed", e);
		}
	}

	@Override
	public String sendFileAsBytesMessage(String fileUri, String destinationName) {
		try {
			byte[] fileContent = IOUtil.getBinaryContentOfUri(fileUri);
			memorizeMessage(fileContent);
			BytesMessage message = createBytesMessage();
			message.writeBytes(fileContent);
			sendMessage(message, destinationName);
			return Base64Codec.encode(fileContent);
		}
		catch (IOException e) {
			throw new AccessFailure("File access failed", e);
		} catch (JMSException e) {
			throw new TechnicalException("Message creation or sending failed", e);
		}
	}

	@Override
	public String receiveTextMessageFromQueue(String destinationName, String messageSelector, long timeout) {
		try {
			TextMessage message = receiveQueueMessage(destinationName, messageSelector, timeout, TextMessage.class);
			String text = message.getText();
			memorizeMessage(text);
			return text;
		} catch (JMSException e) {
			throw new AutomationException("Unable to read message text", e);
		}
	}
	
	@Override
	public Serializable receiveObjectMessageFromQueue(String destinationName, String messageSelector, long timeout) {
		try {
			ObjectMessage message = receiveQueueMessage(destinationName, messageSelector, timeout, ObjectMessage.class);
			Serializable object = message.getObject();
			memorizeMessage(object);
			return object;
		} catch (JMSException e) {
			throw new AutomationException("Unable to read message object", e);
		}
	}
	
	@Override
	public String receiveTextMessageFromQueueAndValidate(@TechnicalLocator String destinationName, String messageSelector,
			@TechnicalArgument long timeout, @TechnicalArgument Validator<String> validator) {
		String text = receiveTextMessageFromQueue(destinationName, messageSelector, timeout);
		memorizeMessage(text);
		if (!validator.valid(text))
			throw new FunctionalFailure("Message invalid");
		return text;
	}

	@Override
	public Serializable receiveObjectMessageFromQueueAndValidate(@TechnicalLocator String destinationName, String messageSelector,
			@TechnicalArgument long timeout, @TechnicalArgument Validator<Serializable> validator) {
		Serializable object = receiveTextMessageFromQueue(destinationName, messageSelector, timeout);
		memorizeMessage(object);
		if (!validator.valid(object))
			throw new FunctionalFailure("Message invalid");
		return object;
	}
    
	// topic subscription interface --------------------------------------------
	
	@Override
	public void startSubscriber(String subscriptionName, String destinationName, String messageSelector, boolean durable) {
		Assert.notEmpty(subscriptionName, "subscriptionName must be provided!");
		memorizeMessage(null);
		TopicHandler handler = getOrCreateTopicHandler(subscriptionName, destinationName, messageSelector, durable);
		handler.start();
	}

	@Override
	public void stopSubscriber(String subscriptionName) {
		getTopicHandler(subscriptionName).stop();
	}
	
	@Override
	public String receiveTextMessageFromTopic(String subscriptionName, String messageSelector, long timeout, boolean required) {
		try {
			TextMessage message = receiveTopicMessage(subscriptionName, messageSelector, timeout, required, TextMessage.class);
			String text = (message != null ? message.getText() : null);
			memorizeMessage(text);
			return text;
		} catch (JMSException e) {
			throw new AutomationException("Unable to read message text", e);
		}
	}
	
	@Override
	public Serializable receiveObjectMessageFromTopic(String subscriptionName, String messageSelector, long timeout, boolean required) {
		try {
			ObjectMessage message = receiveTopicMessage(subscriptionName, messageSelector, timeout, required, ObjectMessage.class);
			Serializable object = (message != null ? message.getObject() : null);
			memorizeMessage(object);
			return object;
		} catch (JMSException e) {
			throw new AutomationException("Unable to read message text", e);
		}
	}
	
	@Override
	public String receiveTextMessageFromTopicAndValidate(@TechnicalLocator String subscriptionName, String messageSelector,
			@TechnicalArgument long timeout, boolean required, @TechnicalArgument Validator<String> validator) {
		String text = receiveTextMessageFromTopic(subscriptionName, messageSelector, timeout, required);
		memorizeMessage(text);
		if (!validator.valid(text))
			throw new FunctionalFailure("Message invalid");
		return text;
	}

	@Override
	public Serializable receiveObjectMessageFromTopicAndValidate(@TechnicalLocator String subscriptionName, String messageSelector,
			@TechnicalArgument long timeout, boolean required, @TechnicalArgument Validator<Serializable> validator) {
		Serializable object = receiveTextMessageFromTopic(subscriptionName, messageSelector, timeout, required);
		memorizeMessage(object);
		if (!validator.valid(object))
			throw new FunctionalFailure("Message invalid");
		return object;
	}
    
	
    // private helper methods --------------------------------------------------
	
	private void memorizeMessage(Object message) {
		if (message == null) {
			this.recentMessage = null;
		} else if (message instanceof String) {
			this.recentMessage = (String) message;
		} else if (message instanceof byte[]) {
			this.recentMessage = Base64Codec.encode((byte[]) message);
		} else {
			this.recentMessage = message.toString();
		}
	}

	private TopicHandler getOrCreateTopicHandler(String subscriptionName, String destinationName, String messageSelector,
			boolean durable) {
		TopicHandler handler = getTopicHandler(subscriptionName);
		if (handler == null) {
			handler = createTopicHandler(subscriptionName, destinationName, messageSelector, durable);
		}
		return handler;
	}

	private TopicHandler getTopicHandler(String subscriptionName) {
		return this.topicHandlers.get(subscriptionName);
	}

	private TopicHandler createTopicHandler(String subscriptionName, String destinationName, String messageSelector,
			boolean durable) {
		LOGGER.debug("Creating topic-subscriber for topic " + destinationName + " and subscription name " + subscriptionName);
	    try {
			Connection c = createDynamicConnection(subscriptionName);
			TopicSession ts = (TopicSession) c.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Topic topic = lookupTopic(destinationName);
			TopicSubscriber subscriber;
			if (durable) {
				subscriber = ts.createDurableSubscriber(topic, subscriptionName, messageSelector, false);
			} else {
				subscriber = ts.createSubscriber(topic, messageSelector, true);
			}
			TopicHandler handler = new TopicHandler(subscriptionName, durable, subscriber, c);
		    this.topicHandlers.put(subscriptionName, handler);
		    return handler;
		} catch (JMSException e) {
			throw new AutomationException("Failed to subscribe", e);
		}
	}

	private Topic lookupTopic(String destinationName) {
		Topic topic;
        try {
            topic = (Topic) context.lookup(destinationName);
        } catch (NamingException e) {
            throw new AutomationException("Could not lookup destination " + destinationName, e);
        }
		return topic;
	}

    private void startConnection() throws JMSException {
		LOGGER.debug("starting connection");
        this.getOrCreateConnection().start();
    }

    private synchronized Connection getOrCreateConnection() {
        if (this.connection == null) {
            this.connection = createConnection(this.clientId);
        }
        return this.connection;
    }

	/**
	 * Build a new connection with the given clientId
	 *
	 * @param clientId	the clientId

	 * @return
     */
    private synchronized Connection createConnection(String clientId) {
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

	/**
	 * Creates a dynamic connection for the given subscriptionName.
	 *
	 * According to JavaDoc of {@link TopicSession#createDurableSubscriber(Topic, String, String, boolean)}
	 * durable subscriptions must use the same clientIds on every connection to a particular subscription.
	 * Therefore the clientId used for dynamic-connections is derived from the following rule:
	 *
	 * this.userName + "@" + this.getClass().getSimpleName() + "[" + @param subscriptionName + "]"
	 *
	 * Even new Instances of JmsActionImpl will produce a clientId that is able to reconnect to existing
	 * durable subscriptions.
	 *
	 * @param subscriptionName	the subscription name
	 * @return	The connection.
	 *
	 * @throws JMSException
     */
    private Connection createDynamicConnection(String subscriptionName) throws  JMSException {
		final String dynClientID =
				this.userName + "@" + this.getClass().getSimpleName() + "[" +subscriptionName + "]";
        Connection dynC = null;
        dynC = createConnection(dynClientID);
        return dynC;

    }

	private Session getSession() throws JMSException {
		if (session == null) {
			session = getOrCreateConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
		}
		return session;
	}

	private TextMessage createTextMessage() throws JMSException {
		return getSession().createTextMessage();
	}

	private ObjectMessage createObjectMessage() throws JMSException {
		return getSession().createObjectMessage();
	}

	private BytesMessage createBytesMessage() throws JMSException {
		return getSession().createBytesMessage();
	}

	private void sendMessage(Message message, String destinationName) {
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
			close(producer);
		}
	}

	@SuppressWarnings("unchecked")
	private <T extends Message> T receiveQueueMessage(String destinationName, String messageSelector, long timeout, Class<T> type) {
		MessageConsumer consumer = null;
		try {
			Destination dest = (Destination) context.lookup(destinationName);
			consumer = getSession().createConsumer(dest, messageSelector);
            this.startConnection();

			Message message;
			if (timeout == -1) {
				message = consumer.receive();
			}
			else {
				message = consumer.receive(timeout);
			}
			this.stopConnection();
			if (message == null) {
				throw new PerformanceFailure("Destination " + destinationName + " did not deliver a message within timeout");
			}
			if (!type.isAssignableFrom(message.getClass())) {
				throw new AutomationException("Received message is not a text message");
			}
			return (T) message;
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
			close(consumer);
		}
	}
	
	/** Waits until a message arrives, applying a timeout
	 *  @param timeout the number of milliseconds to wait, <code>0</code> means to wait without timeout.
	 *  @return the received message or <code>null</code> if no message arrived within the timeout */
	@SuppressWarnings("unchecked")
	private <T extends Message> T receiveTopicMessage(String subscriptionName, String messageSelector, long timeout, boolean required, Class<T> type) {
		TopicHandler handler = getTopicHandler(subscriptionName);
		Message message = handler.receive(timeout);
		if (message != null) {
			if (!type.isAssignableFrom(message.getClass())) {
				throw new AutomationException("Received message is not a text message. ");
			}
		} else if (required) {
			throw new PerformanceFailure("No message received within the timeout of " + timeout + " ms. ");
		}
		
		return (T) message;
	}
	
	private static void close(MessageProducer producer) {
		if (producer != null) {
			try {
				producer.close();
			}
			catch (JMSException e) {
				LOGGER.debug("Failed to close producer: ", e );
			}
		}
	}

	private static void close(MessageConsumer consumer) {
		if (consumer != null) {
			try {
				consumer.close();
			}
			catch (JMSException e) {
				LOGGER.debug("Failed to close consumer: ", e );
			}
		}
	}

	private static void close(Session session) {
		if (session != null) {
			try {
                session.close();
			}
			catch (JMSException e) {
				LOGGER.debug("Failed to close jms session : ", e );
			}
		}
	}

	private static void stopAndClose(Connection connection, String description) {
		if (connection != null) {
			try {
				connection.stop();
				connection.close();
			}
			catch (JMSException e) {
				LOGGER.debug("Failed to close " + description + " : ", e );
			}
		}
	}

    private void stopConnection() throws JMSException {
        this.getOrCreateConnection().stop();
    }
    
    /**
     * Applies the properties to javax.jms.Message
     *  
     * @param message the jms message
     * @param properties Map of properties. The list contains 'free' content, so if there is some property that is not supported by the javax.jms.Message, an exception will be thrown.
     * @return the update jms message with the properties, if they exist
     * @throws JMSException
     */
    private Message addPropertiesToMessage(Message message, Map<String, Object> properties) throws JMSException {
		// Adds the properties to the jms message.
		for(Map.Entry<String, Object> entry : properties.entrySet()) {
			message.setObjectProperty(entry.getKey(), entry.getValue());
		}
		return message;
	}

}
