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

import java.io.Serializable;

import org.aludratest.service.AttachParameter;
import org.aludratest.service.AttachResult;
import org.aludratest.service.Interaction;
import org.aludratest.service.TechnicalArgument;
import org.aludratest.service.TechnicalLocator;
import org.aludratest.service.jms.data.FileMessageData;
import org.aludratest.service.jms.data.ObjectMessageData;
import org.aludratest.service.jms.data.TextMessageData;
import org.databene.commons.Validator;

public interface JmsInteraction extends Interaction {
	
	// sending messages --------------------------------------------------------

	void sendTextMessage(@AttachParameter("Message text") String text, @TechnicalLocator String destinationName);

	void sendObjectMessage(@AttachParameter("Message object") Serializable object, @TechnicalLocator String destinationName);

	@AttachResult("Message text") String sendFileAsTextMessage(String fileUri, @TechnicalLocator String destinationName);

	@AttachResult("Message content (BASE64)") String sendFileAsBytesMessage(String fileUri, @TechnicalLocator String destinationName);

	/**
	 * Send a jms message (TextMessageData) with the jms properties support
	 * @param textMessageData the jms message
	 * @param destinationName jms destination.
	 */
	void sendMessage(@AttachParameter("Message textMessageData") TextMessageData textMessageData, @TechnicalLocator String destinationName);

	/**
	 * Send a jms message (ObjectMessageData) with the jms properties support
	 * @param objectMessageData the jms message
	 * @param destinationName jms destination.
	 */
	void sendMessage(@AttachParameter("Message objectMessageData") ObjectMessageData objectMessageData, @TechnicalLocator String destinationName);
	
	/**
	 * Send a jms message (FileMessageData) with the jms properties support
	 * @param fileMessageData the jms message
	 * @param destinationName jms destination.
	 * @return file content
	 */
	@AttachResult("Message text") String sendMessage(@AttachParameter("Message fileMessageData") FileMessageData fileMessageData, @TechnicalLocator String destinationName);
	
	// receiving messages from a queue -----------------------------------------
	
	String receiveTextMessageFromQueue(
			@TechnicalLocator String destinationName, 
			@TechnicalArgument String messageSelector, 
			@TechnicalArgument long timeout);

	String receiveTextMessageFromQueueAndValidate(
			@TechnicalLocator String destinationName, 
			@TechnicalArgument String messageSelector, 
			@TechnicalArgument long timeout, 
			@TechnicalArgument Validator<String> validator);

	Serializable receiveObjectMessageFromQueue(
			@TechnicalLocator String destinationName, 
			@TechnicalArgument String messageSelector, 
			@TechnicalArgument long timeout);

	Serializable receiveObjectMessageFromQueueAndValidate(
			@TechnicalLocator String destinationName, 
			@TechnicalArgument String messageSelector, 
			@TechnicalArgument long timeout, 
			@TechnicalArgument Validator<Serializable> validator);

	
	// subscribing a topic and receiving messages ------------------------------

	void startSubscriber(
			String subscriptionName, 
			String destinationName, 
			@TechnicalArgument String messageSelector, 
			boolean durable);

	void stopSubscriber(String subscriptionName);

	String receiveTextMessageFromTopic(
			@TechnicalLocator String subscriptionName, 
			@TechnicalArgument String messageSelector, 
			@TechnicalArgument long timeout,
			@TechnicalArgument boolean required);

	String receiveTextMessageFromTopicAndValidate(
			@TechnicalLocator String subscriptionName, 
			@TechnicalArgument String messageSelector, 
			@TechnicalArgument long timeout,
			@TechnicalArgument boolean required, 
			@TechnicalArgument Validator<String> validator);

	Serializable receiveObjectMessageFromTopic(
			@TechnicalLocator String subscriptionName, 
			@TechnicalArgument String messageSelector, 
			@TechnicalArgument long timeout,
			@TechnicalArgument boolean required);

	Serializable receiveObjectMessageFromTopicAndValidate(
			@TechnicalLocator String subscriptionName, 
			@TechnicalArgument String messageSelector, 
			@TechnicalArgument long timeout,
			@TechnicalArgument boolean required, 
			@TechnicalArgument Validator<Serializable> validator);

}
