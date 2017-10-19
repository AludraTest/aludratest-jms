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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.UUID;

import org.aludratest.service.jms.data.FileMessageData;
import org.aludratest.service.jms.data.ObjectMessageData;
import org.aludratest.service.jms.data.TextMessageData;
import org.aludratest.service.jms.util.ObjDataTest;
import org.aludratest.testcase.event.attachment.Attachment;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.databene.commons.Encodings;
import org.databene.commons.IOUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by ojurksch on 06.04.2016.
 */
public class JmsActionImplTest extends AbstractJmsTest {

    private static final Logger LOGGER = Logger.getLogger(JmsActionImplTest.class);

    private static final String QUEUE_NAME = "dynamicQueues/testQueue1";

    private static final String TOPIC_NAME = "dynamicTopics/testTopic1";

    @Test
    public void testBasicJms() throws UnsupportedEncodingException {
        String queueName = QUEUE_NAME;
        String textContent = UUID.randomUUID().toString();

        LOGGER.info("Begin testBasicJms");

        LOGGER.info("Creating and sending TextMessage to queue " + queueName);
        service.perform().sendTextMessage(textContent, queueName);
        // verify attachment
        Iterator<Attachment> atIterator = getLastTestStep().getAttachments().iterator();
        assertTrue(atIterator.hasNext());
        Attachment attachment = atIterator.next();
		assertEquals("Message text", attachment.getLabel());
        assertEquals(textContent, new String(attachment.getFileData(), Encodings.UTF_8));

        LOGGER.info("Receiving TextMessage from queue " + queueName);
        String receivedMessageText = service.perform().receiveTextMessageFromQueue(queueName, null, 20);

        Assert.assertNotNull(receivedMessageText);
        Assert.assertTrue(StringUtils.equalsIgnoreCase(textContent, receivedMessageText));

        LOGGER.info("End testBasicJms");
    }

    /**
     * Check nondurable topic messaging.
     * <ul>
     *     <li>subscribe to topic (non-durable) and connect a {@link }MessageListener} to that subscription</li>
     *     <li>send a message with a unique textcontent to that topic</li>
     *     <li>check if a message containing the unique textcontent was received</li>
     *     <li>unregister from the topic</li>
     *     <li>send another message with a new unique textcontent to the same topic</li>
     *     <li>verify that no more messages where recieved</li>
     * </ul>
      */
    @Test
    public void testDynamicTopicSubscriber() {

        LOGGER.info("Begin testTopicSubscriber");

        final String subscriptionName = "testTopicSubscriber@" + TOPIC_NAME;
        final String expectedText1 = UUID.randomUUID().toString();
        final String expectedText2 = UUID.randomUUID().toString();

        LOGGER.info("Subscribing to topic " + TOPIC_NAME);
        this.service.perform().startSubscriber(subscriptionName, TOPIC_NAME, null, false);

        LOGGER.info("Sending message to topic " + TOPIC_NAME);
        this.service.perform().sendTextMessage(expectedText1, TOPIC_NAME);

        LOGGER.info("Waiting a bit to receive message(s) from " + TOPIC_NAME);
        String receivedMessageText = this.service.perform().receiveTextMessageFromTopic(subscriptionName, null, 100, true);

        LOGGER.info("Checking if message(s) where received on " + TOPIC_NAME);
        Assert.assertEquals(expectedText1, receivedMessageText);

        LOGGER.info("Unsubscribing from topic " + TOPIC_NAME);
        this.service.perform().stopSubscriber(subscriptionName);

        LOGGER.info("Sending message to topic " + TOPIC_NAME);
        this.service.perform().sendTextMessage(expectedText2, TOPIC_NAME);
        
        LOGGER.info("Waiting a bit to receive message(s) from " + TOPIC_NAME);
        receivedMessageText = this.service.perform().receiveTextMessageFromTopic(subscriptionName, null, 100, false);
        
        LOGGER.info("Assert than NO message(s) where received on " + TOPIC_NAME);
        assertNull(receivedMessageText);

        LOGGER.info("End testTopicSubscriber");
    }

    /**
     *  Check durable messaging.
     *  <ul>
     *      <li>subscribe durable</li>
     *      <li>disconnect the {@link JmsService} that subscribed</li>
     *      <li>send a message with unique content to the topic</li>
     *      <li>connect a new {@link JmsService} to the subscription</li>
     *      <li>check if messages where received</li>
     *      <li>unsubscribe the durable subscription</li>
     *      <li>send a message with unique content to the topic</li>
     *      <li>check that no messages where received</li>
     *  </ul>
     */
    @Test
    public void testDurableTopicSubscriberRegisterUnregister() {
        LOGGER.info("Begin testDurableTopicSubscriberRegisterUnregister");

        final String subscriptionName = "testDurableTopicSubscriberRegisterUnregister@" + TOPIC_NAME;
        final String expectedText = UUID.randomUUID().toString();

        LOGGER.info("Create durable subscription [" + subscriptionName + "] on TOPIC " + TOPIC_NAME);
        JmsService service1 = getLoggingJmsService();
        service1.perform().startSubscriber(subscriptionName, TOPIC_NAME, null, true);
        service1.perform().stopSubscriber(subscriptionName);
        service1.close();

        LOGGER.info("Sending message to the TOPIC " + TOPIC_NAME);
        this.service.perform().sendTextMessage(expectedText, TOPIC_NAME);

        LOGGER.info("Connecting a new client using the subscription " + subscriptionName);
        JmsService service2 = getLoggingJmsService();
        service2.perform().startSubscriber(subscriptionName, TOPIC_NAME, null, true);

        LOGGER.info("Waiting for messages on subscription...");
        String receivedMessage = service2.perform().receiveTextMessageFromTopic(subscriptionName, null, 100, true);

        LOGGER.info("Check for expected message");
        Assert.assertNotNull("Expected message not received!", receivedMessage);
        assertEquals(expectedText, receivedMessage);

        LOGGER.info("Unsubscribing subscription " + subscriptionName);
        service2.perform().stopSubscriber(subscriptionName);

        LOGGER.info("Sending message to the TOPIC " + TOPIC_NAME);
        this.service.perform().sendTextMessage(expectedText, TOPIC_NAME);

        LOGGER.info("Waiting for messages on subscription...");
        String receivedMessage2 = service2.perform().receiveTextMessageFromTopic(subscriptionName, null, 100, false);
        
        Assert.assertNull("Received unexpected message!", receivedMessage2);
        service2.close();

        LOGGER.info("End testDurableTopicSubscriberRegisterUnregister");
    }
    
    @Test
    public void testBasicJmsTextMessageData() throws UnsupportedEncodingException {
        String queueName = QUEUE_NAME + ".TextMessage.1";
        String textContent = UUID.randomUUID().toString();

        LOGGER.info("Begin testBasicJmsWithTextMessageData");
        
        TextMessageData textMessageData = new TextMessageData(textContent);

        LOGGER.info("Creating and sending TextMessage to queue " + queueName);
        service.perform().sendMessage(textMessageData, queueName);
        // verify attachment
        Iterator<Attachment> atIterator = getLastTestStep().getAttachments().iterator();
        assertTrue(atIterator.hasNext());
        Attachment attachment = atIterator.next();
		assertEquals("Message textMessageData", attachment.getLabel());

        LOGGER.info("Receiving TextMessage from queue " + queueName);
        String receivedMessageText = service.perform().receiveTextMessageFromQueue(queueName, null, 20);

        Assert.assertNotNull(receivedMessageText);
        Assert.assertTrue(StringUtils.equalsIgnoreCase(textContent, receivedMessageText));

        LOGGER.info("End testBasicJmsWithTextMessageData");
    }
    
    @Test
    public void testBasicJmsTextMessageDataWithStringProp() throws UnsupportedEncodingException {
        String queueName = QUEUE_NAME + ".TextMessage.2";
        String textContent = UUID.randomUUID().toString();

        LOGGER.info("Begin testBasicJmsTextMessageDataWithStringProp");
        
        TextMessageData textMessageData = new TextMessageData(textContent);
        textMessageData.addProperty("KEY", "valueTest2");

        LOGGER.info("Creating and sending TextMessage to queue " + queueName);
        service.perform().sendMessage(textMessageData, queueName);
        // verify attachment
        Iterator<Attachment> atIterator = getLastTestStep().getAttachments().iterator();
        assertTrue(atIterator.hasNext());
        Attachment attachment = atIterator.next();
		assertEquals("Message textMessageData", attachment.getLabel());

        LOGGER.info("Receiving TextMessage from queue " + queueName);
        String receivedMessageText = service.perform().receiveTextMessageFromQueue(queueName, "KEY='valueTest2'", 20);

        Assert.assertNotNull(receivedMessageText);
        Assert.assertTrue(StringUtils.equalsIgnoreCase(textContent, receivedMessageText));

        LOGGER.info("End testBasicJmsTextMessageDataWithStringProp");
    }
    
    @Test
    public void testBasicJmsTextMessageDataWithStringProp_wrongSelector() throws UnsupportedEncodingException {
        String queueName = QUEUE_NAME + ".TextMessage.3";
        String textContent = UUID.randomUUID().toString();

        LOGGER.info("Begin testBasicJmsTextMessageDataWithStringProp_wrongSelector");
        
        TextMessageData textMessageData = new TextMessageData(textContent);
        textMessageData.addProperty("KEY", "valueTest3");

        LOGGER.info("Creating and sending TextMessage to queue " + queueName);
        service.perform().sendMessage(textMessageData, queueName);
        // verify attachment
        Iterator<Attachment> atIterator = getLastTestStep().getAttachments().iterator();
        assertTrue(atIterator.hasNext());
        Attachment attachment = atIterator.next();
		assertEquals("Message textMessageData", attachment.getLabel());

        LOGGER.info("Receiving TextMessage from queue " + queueName);
        String receivedMessageText = service.perform().receiveTextMessageFromQueue(queueName, "KEY='valueTestNOT-THERE'", 20);

        Assert.assertNotNull(receivedMessageText);
        Assert.assertTrue(StringUtils.equalsIgnoreCase("", receivedMessageText));

        LOGGER.info("End testBasicJmsTextMessageDataWithStringProp_wrongSelector");
    }
    
    @Test
    public void testBasicJmsFileMessageData() throws IOException {
    	
    	LOGGER.info("Begin testBasicJmsFileMessageData");
    	String queueName = QUEUE_NAME + ".FileMessage.1";
        String fileUri = "/config/jmsTest/jms.properties";
        FileMessageData fileMessageData = new FileMessageData(fileUri);

        LOGGER.info("Creating and sending FileMessage to queue " + queueName);

        service.perform().sendMessage(fileMessageData, queueName);
        
        // verify attachment
        Iterator<Attachment> atIterator = getLastTestStep().getAttachments().iterator();
        assertTrue(atIterator.hasNext());
        Attachment attachment = atIterator.next();
		assertEquals("Message fileMessageData", attachment.getLabel());
		
        LOGGER.info("Receiving TextMessage from queue " + queueName);
        String receivedMessageText = service.perform().receiveTextMessageFromQueue(queueName, null, 20);

        Assert.assertNotNull(receivedMessageText);
        
        //verify content
        Assert.assertEquals(IOUtil.getContentOfURI(fileUri), receivedMessageText);

        LOGGER.info("End testBasicJmsFileMessageData");
    }
    
    @Test
    public void testBasicJmsFileMessageDataWithStringProp() throws IOException {
    	
    	LOGGER.info("Begin testBasicJmsFileMessageDataWithStringProp");
        String queueName = QUEUE_NAME + ".FileMessage.2";
        String fileUri = "/config/jmsTest/jms.properties";
        FileMessageData fileMessageData = new FileMessageData(fileUri);
        fileMessageData.addProperty("KEYPROP", "propValue");

        
        LOGGER.info("Creating and sending FileMessage to queue " + queueName);

        service.perform().sendMessage(fileMessageData, queueName);
        
        // verify attachment
        Iterator<Attachment> atIterator = getLastTestStep().getAttachments().iterator();
        assertTrue(atIterator.hasNext());
        Attachment attachment = atIterator.next();
		assertEquals("Message fileMessageData", attachment.getLabel());
		
        LOGGER.info("Receiving TextMessage from queue " + queueName);
        String receivedMessageText = service.perform().receiveTextMessageFromQueue(queueName, "KEYPROP='propValue'", 20);

        Assert.assertNotNull(receivedMessageText);
        
        //verify content
        Assert.assertEquals(IOUtil.getContentOfURI(fileUri), receivedMessageText);

        LOGGER.info("End testBasicJmsFileMessageDataWithStringProp");
    }
    
    
    @Test
    public void testBasicJmsObjectMessageDataInteger() throws IOException {
    	
    	LOGGER.info("Begin testBasicJmsObjectMessageDataInteger");
    	String queueName = QUEUE_NAME + ".ObjectMessage.1";
        
    	Integer messageObject = new Integer(12);
    	
    	ObjectMessageData objectMessageData = new ObjectMessageData();
    	objectMessageData.setMessageObject(messageObject);

        LOGGER.info("Creating and sending ObjectMessage to queue " + queueName);

        service.perform().sendMessage(objectMessageData, queueName);
        
        // verify attachment
        Iterator<Attachment> atIterator = getLastTestStep().getAttachments().iterator();
        assertTrue(atIterator.hasNext());
        Attachment attachment = atIterator.next();
		assertEquals("Message objectMessageData", attachment.getLabel());
		
        LOGGER.info("Receiving ObjectMessage from queue " + queueName);
        Object receivedMessageObject = service.perform().receiveObjectMessageFromQueue(queueName, null, 20);

        Assert.assertNotNull(receivedMessageObject);
        
        Assert.assertTrue(receivedMessageObject instanceof Integer);
        //apply cast
        receivedMessageObject = (Integer) receivedMessageObject;
        
        //verify content
        Assert.assertEquals(messageObject, receivedMessageObject);

        LOGGER.info("End testBasicJmsObjectMessageDataInteger");
    }
    
    @Test
    public void testBasicJmsObjectMessageDataIntegerWithProp() throws IOException {
    	
    	LOGGER.info("Begin testBasicJmsObjectMessageDataIntegerWithProp");
    	String queueName = QUEUE_NAME + ".ObjectMessage.2";
        
    	Integer messageObject = new Integer(12);
    	
    	ObjectMessageData objectMessageData = new ObjectMessageData();
    	objectMessageData.setMessageObject(messageObject);
    	objectMessageData.addProperty("KEYOBJPROP", "valueInt");

        LOGGER.info("Creating and sending ObjectMessage to queue " + queueName);

        service.perform().sendMessage(objectMessageData, queueName);
        
        // verify attachment
        Iterator<Attachment> atIterator = getLastTestStep().getAttachments().iterator();
        assertTrue(atIterator.hasNext());
        Attachment attachment = atIterator.next();
		assertEquals("Message objectMessageData", attachment.getLabel());
		
        LOGGER.info("Receiving ObjectMessage from queue " + queueName);
        Object receivedMessageObject = service.perform().receiveObjectMessageFromQueue(queueName, "KEYOBJPROP='valueInt'", 20);

        Assert.assertNotNull(receivedMessageObject);
        
        Assert.assertTrue(receivedMessageObject instanceof Integer);
        //apply cast
        receivedMessageObject = (Integer) receivedMessageObject;
        
        //verify content
        Assert.assertEquals(messageObject, receivedMessageObject);

        LOGGER.info("End testBasicJmsObjectMessageDataIntegerWithProp");
    }
    
    
    @Test
    public void testBasicJmsObjectMessageDataOtherObj() throws IOException {
    	
    	LOGGER.info("Begin testBasicJmsObjectMessageDataOtherObj");
    	String queueName = QUEUE_NAME + ".ObjectMessage.3";
        
    	String name = "testBasicJmsObjectMessageDataOtherObj";
    	ObjDataTest objectToSend = new ObjDataTest(name);
    	
    	
    	ObjectMessageData objectMessageData = new ObjectMessageData();
    	objectMessageData.setMessageObject(objectToSend);

        LOGGER.info("Creating and sending ObjectMessage to queue " + queueName);

        service.perform().sendMessage(objectMessageData, queueName);
        
        // verify attachment
        Iterator<Attachment> atIterator = getLastTestStep().getAttachments().iterator();
        assertTrue(atIterator.hasNext());
        Attachment attachment = atIterator.next();
		assertEquals("Message objectMessageData", attachment.getLabel());
		
        LOGGER.info("Receiving ObjectMessage from queue " + queueName);
        Object receivedMessageObject = service.perform().receiveObjectMessageFromQueue(queueName, null, 20);

        Assert.assertNotNull(receivedMessageObject);
        
        Assert.assertTrue(receivedMessageObject instanceof ObjDataTest);
        //apply cast
        ObjDataTest objDataTest = (ObjDataTest) receivedMessageObject;
        
        //verify content
        Assert.assertEquals(objectToSend.getName(), objDataTest.getName());

        LOGGER.info("End testBasicJmsObjectMessageDataOtherObj");
    }
    
    @Test
    public void testBasicJmsObjectMessageDataOtherObjWithProp() throws IOException {
    	
    	LOGGER.info("Begin testBasicJmsObjectMessageDataOtherObjWithProp");
    	String queueName = QUEUE_NAME + ".ObjectMessage.4";       

    	String name = "testBasicJmsObjectMessageDataOtherObjWithProp";
    	ObjDataTest objectToSend = new ObjDataTest(name);

    	ObjectMessageData objectMessageData = new ObjectMessageData();
    	objectMessageData.setMessageObject(objectToSend);
    	objectMessageData.addProperty("KEYOBJPROP", "valueObj");

        LOGGER.info("Creating and sending ObjectMessage to queue " + queueName);

        service.perform().sendMessage(objectMessageData, queueName);
        
        // verify attachment
        Iterator<Attachment> atIterator = getLastTestStep().getAttachments().iterator();
        assertTrue(atIterator.hasNext());
        Attachment attachment = atIterator.next();
		assertEquals("Message objectMessageData", attachment.getLabel());
		
        LOGGER.info("Receiving ObjectMessage from queue " + queueName);
        Object receivedMessageObject = service.perform().receiveObjectMessageFromQueue(queueName, "KEYOBJPROP='valueObj'", 20);

        Assert.assertNotNull(receivedMessageObject);
        
        Assert.assertTrue(receivedMessageObject instanceof ObjDataTest);
        //apply cast
        ObjDataTest objDataTest = (ObjDataTest) receivedMessageObject;
        
        //verify content
        Assert.assertEquals(objectToSend.getName(), objDataTest.getName());

        LOGGER.info("End testBasicJmsObjectMessageDataOtherObjWithProp");
    }
    
}
