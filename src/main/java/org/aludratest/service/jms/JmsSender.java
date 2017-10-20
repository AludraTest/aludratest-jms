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

import org.aludratest.dict.ActionWordLibrary;
import org.aludratest.service.jms.data.FileMessageData;
import org.aludratest.service.jms.data.ObjectMessageData;
import org.aludratest.service.jms.data.TextMessageData;

/**
 * Abstract parent class for specific JMS message senders.
 * @param E when subclassing JmsSender, use the subclass itself as generic class parameter, 
 * 			for example <code>class MyJmsSender extends JmsSender&lt;MyJmsSender&gt;</code>
 * @author Volker Bergmann
 */

public abstract class JmsSender<E extends JmsSender<E>> implements ActionWordLibrary<E> {

	private JmsService service;
	private String destinationName;
	
	/** Full constructor.
	 *  @param destinationName
	 *  @param service the {@link JmsService} to use for accessing the destination */
	public JmsSender(String destinationName, JmsService service) {
		this.destinationName = destinationName;
		this.service = service;
	}

	/** Sends a text message to this sender's destination.
	 *  @param data a data object holding the message text */
	public final E sendTextMessage(TextMessageData data) {
		service.perform().sendTextMessage(data.getMessageText(), destinationName);
		return verifyState();
	}

	/** Sends an object message to this sender's destination.
	 *  @param data a data object holding the message text */
	public final E sendObjectMessage(ObjectMessageData data) {
		service.perform().sendObjectMessage(data.getMessageObject(), destinationName);
		return verifyState();
	}

	/** Reads a text file and sends its content as text message to this sender's destination.
	 *  @param data a data object holding the URI of the file to read */
	public final E sendFileAsTextMessage(FileMessageData data) {
		service.perform().sendFileAsTextMessage(data.getFileUri(), destinationName);
		return verifyState();
	}
	
	/** Reads a binary file and sends its content as bytes message to this sender's destination.
	 *  @param data a data object holding the URI of the file to read */
	public final E sendFileAsBytesMessage(FileMessageData data) {
		service.perform().sendFileAsBytesMessage(data.getFileUri(), destinationName);
		return verifyState();
	}

	/** Sends a text message to this sender's destination. This method supports jms properties.
	 *  @param data a data object holding the message text */
	public final E sendMessage(TextMessageData data) {
		service.perform().sendMessage(data, destinationName);
		return verifyState();
	}

	/** Sends an object message to this sender's destination. This method supports jms properties.
	 *  @param data a data object holding the message data */
	public final E sendMessage(ObjectMessageData data) {
		service.perform().sendMessage(data, destinationName);
		return verifyState();
	}

	/** Reads a text file and sends its content as text message to this sender's destination. This method supports jms properties.
	 *  @param data a data object holding the URI of the file to read */
	public final E sendMessage(FileMessageData data) {
		service.perform().sendMessage(data, destinationName);
		return verifyState();
	}

	@SuppressWarnings("unchecked")
	@Override
	public E verifyState() {
		// no possibility of having an invalid state
		return (E) this;
	}

}
