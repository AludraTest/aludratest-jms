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
package org.aludratest.service.jms;

import org.aludratest.service.Interaction;
import org.aludratest.service.TechnicalArgument;
import org.aludratest.service.TechnicalLocator;

import javax.jms.*;
import java.io.Serializable;

public interface JmsInteraction extends Interaction {

	void sendTextMessage(String text, @TechnicalLocator String destinationName);

	void sendObjectMessage(Serializable object, @TechnicalLocator String destinationName);

	TextMessage createTextMessage();

	ObjectMessage createObjectMessage();

	BytesMessage createBytesMessage();

	MapMessage createMapMessage();

	StreamMessage createStreamMessage();

	void sendMessage(Message message, @TechnicalLocator String destinationName);

	Message receiveMessage(@TechnicalLocator String destinationName, @TechnicalArgument long timeout);

	/**
	 * Register the given MessageListener as TopicSubscriber on the given destination.
	 *
	 *
	 * @param listener	The listener to register as TopicSubscriber
	 * @param destinationName	The destination to subsribe to
	 * @param messageSelector	The messageselector to use on receiving topic-messages.
	 * @param subscriptionName 	The subscriptionname to be used to create the subscription.
	 * @param durable		If the subscriber should be registered durable or not
	 */
	void subscribeTopic(MessageListener listener, @TechnicalLocator String destinationName, @TechnicalArgument String messageSelector, @TechnicalArgument String subscriptionName, @TechnicalArgument boolean durable) throws JMSException;

	/**
	 * Unregister the given messagelistener that was registered with the given client-id
	 * MessageListener will stop receiving messages.
	 *
	 * @param subscriptionName	the subscriptionName that was used to register and should be removed now.
     */
	void unsubscribeTopic(@TechnicalArgument String subscriptionName) throws JMSException;


}
