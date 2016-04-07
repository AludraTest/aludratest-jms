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

import org.aludratest.config.impl.SimplePreferences;
import org.aludratest.service.jms.impl.JmsActionImpl;
import org.aludratest.service.jms.impl.JmsServiceImpl;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.jndi.ActiveMQInitialContextFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.junit.*;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * Created by ojurksch on 06.04.2016.
 */
public class JmsActionImplTest {

    private static final Logger LOGGER = Logger.getLogger(JmsActionImpl.class);

    private static final String[] queues = {"dynamicQueues/testQueue1"};

    private static final String[] topics = {"dynamicTopics/testTopic1"};

    private static BrokerService testBroker;

    private static String testBrokerUri = "vm://localhost";


    private JmsService testObject;

    private JmsInteraction perform;

    @BeforeClass
    public static void startTestBroker() {
        LOGGER.info("Setting up embedded ActiveMQ broker for URL " + testBrokerUri);
        testBroker = new BrokerService();
        try {
            testBroker.setPersistent(false);
            testBroker.addConnector(testBrokerUri);
            testBroker.start();
        } catch (Exception e) {
            Assert.fail("Failed to setup testbroker for url "
                    + testBrokerUri + " : " + e.getMessage());
        }
        LOGGER.info("Done setting up embedded ActiveMQ broker for URL " + testBrokerUri);
    }

    @AfterClass
    public static void stopTestBroker() {
        LOGGER.info("Stopping embedded ActiveMQ broker for URL " + testBrokerUri);
        if (testBroker != null && testBroker.isStarted()) {
            try {
                testBroker.stop();
            } catch (Exception e)  {
                Assert.fail("Failed to stop testbroker for url "
                        + testBrokerUri + " : " + e.getMessage());
            }
        }
        LOGGER.info("Done stopping embedded ActiveMQ broker for URL " + testBrokerUri);
    }


    @Before
    public void prepareJmsService() {
        LOGGER.info("Setting up JmsService object connected to URL " + testBrokerUri);
        try {
            this.testObject = buildJmsService();
            this.perform = this.testObject.perform();


        } catch (Exception e) {
            Assert.fail("Failed to connect to testbroker on url "
                    + testBrokerUri + " : " + e.getMessage());
        }
        LOGGER.info("Done setting up JmsService object connected to URL " + testBrokerUri);
    }

    @Test
    public void testBasicJms() {
        String queueName = queues[0];
        String textContent = UUID.randomUUID().toString();

        try {
            LOGGER.info("Begin testBasicJms");

            LOGGER.info("Creating and sending TextMessage to queue " + queueName);
            TextMessage sentMessage = perform.createTextMessage();
            sentMessage.setText(textContent);
            perform.sendMessage(sentMessage,queueName);

            LOGGER.info("Receiving TextMessage from queue " + queueName);
            Message receivedMessage = perform.receiveMessage(queues[0],20);

            Assert.assertNotNull(receivedMessage);
            Assert.assertTrue(receivedMessage instanceof TextMessage);
            Assert.assertTrue(StringUtils.equalsIgnoreCase(sentMessage.getText(),((TextMessage) receivedMessage).getText()));

            LOGGER.info("End testBasicJms");
        } catch (Exception e) {
            Assert.fail("Failed on testBasicJms "
                    + " : " + e.getMessage());
        }

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
    public void testTopicSubscriber() {

        LOGGER.info("Begin testTopicSubscriber");

        final List<Message> received = new ArrayList<Message>();
        MessageListener listener = new MessageListener() {
            @Override
            public void onMessage(Message message) {
                LOGGER.info("Got Message! ");
                received.add(message);
            }
        };

        final String topicName = topics[0];
        final String subscriptionName = "testTopicSubscriber@" + topicName;
        final String expectedText1 = UUID.randomUUID().toString();
        final String expectedText2 = UUID.randomUUID().toString();

        try {
            LOGGER.info("Subscribing to topic " + topicName);
            this.perform.subscribeTopic(listener,topicName,null,subscriptionName,true);

            //  If anything distracting is in there
            received.clear();

            LOGGER.info("Sending message to topic " + topicName);
            this.perform.sendTextMessage(expectedText1,topicName);

            LOGGER.info("Waiting a bit to receive message(s) from " + topicName);
            Thread.sleep(100);

            LOGGER.info("Checking if message(s) where received on " + topicName);
            Assert.assertTrue(containtsTextMessage(received,expectedText1));

            received.clear();

            LOGGER.info("Unsubscribing from topic " + topicName);
            this.perform.unsubscribeTopic(subscriptionName);

            LOGGER.info("Sending message to topic " + topicName);
            this.perform.sendTextMessage(expectedText1,topicName);
            LOGGER.info("Waiting a bit to receive message(s) from " + topicName);
            Thread.sleep(100);
            LOGGER.info("Assert than NO message(s) where received on " + topicName);
            Assert.assertTrue(received.isEmpty());

        } catch (Exception e)  {
            Assert.fail("Unexpected excpetion on testTopicSubscriber "
                    + " : " + e.getMessage());
        }
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

        final List<Message> received = new ArrayList<Message>();
        final MessageListener listener = new MessageListener() {
            @Override
            public void onMessage(Message message) {
                LOGGER.info("Got message!");
                received.add(message);
            }
        };

        final String topicName = topics[0];
        final String subscriptionName = "testDurableTopicSubscriberRegisterUnregister@" + topicName;
        final String expectedText = UUID.randomUUID().toString();

        try {
            LOGGER.info("Create durable subscription [" + subscriptionName + "] on TOPIC " + topicName);
            JmsService subscriber1 = buildJmsService();
            subscriber1.perform().subscribeTopic(listener,topicName,null,subscriptionName,true);
            subscriber1.close();

            LOGGER.info("Sending message to the TOPIC " + topicName);
            received.clear();
            this.perform.sendTextMessage(expectedText,topicName);

            LOGGER.info("Connecting a new client using the subscription " + subscriptionName);
            subscriber1 = buildJmsService();
            subscriber1.perform().subscribeTopic(listener,topicName,null,subscriptionName,true);

            LOGGER.info("Waiting for messages on subscription...");
            Thread.sleep(100);

            LOGGER.info("Check for expected message");
            Assert.assertTrue("Expected message not received!", containtsTextMessage(received,expectedText));
            received.clear();

            LOGGER.info("Unsubscribing subscription " + subscriptionName);
            subscriber1.perform().unsubscribeTopic(subscriptionName);

            LOGGER.info("Sending message to the TOPIC " + topicName);
            received.clear();
            this.perform.sendTextMessage(expectedText,topicName);

            LOGGER.info("Waiting for messages on subscription...");
            Thread.sleep(100);
            Assert.assertTrue("Received unexpected message!", received.isEmpty());
            subscriber1.close();

        } catch (Exception e)  {
            Assert.fail("Unexpected excpetion on testTopicSubscriber "
                    + " : " + e.getMessage());
        }
        LOGGER.info("End testDurableTopicSubscriberRegisterUnregister");

    }

    //------------------------------------  HELPERS -------------------------------------

    /**
     * Check if a message of type {@link TextMessage} exists, where the textcontent matches
     * the given expectedText.
     *
     * Compared with {@link StringUtils#equals(String, String)}
     *
     * Messages of types <b>other than</b> {@link TextMessage} are ignored.
     *
     * @param messages  The list of messages to check
     * @param expectedText  The text to match against.
     * @return  true if at least one message was found
     * @throws JMSException On error accessing the message-objects.
     */
    private boolean containtsTextMessage(Collection<Message> messages, String expectedText) throws JMSException {
        boolean foundExpected = false;
        for (Message m : messages) {
            if (m instanceof TextMessage) {
                String actualText = ((TextMessage) m).getText();
                if (StringUtils.equals(expectedText,actualText)) {
                    return true;
                }

            }
        }
        return false;
    }

    /**
     * Build a JmsService connected to the jms at testBrokerUri
     *
     * @return  the JmsService.
     * @throws org.aludratest.exception.TechnicalException On error initiating the service.
     */
    private JmsService buildJmsService() {
        SimplePreferences preferences = new  SimplePreferences();
        preferences.setValue("connectionFactoryJndiName","ConnectionFactory");
        preferences.setValue("providerUrl",testBrokerUri);
        preferences.setValue("initialContextFactory",ActiveMQInitialContextFactory.class.getName());

        JmsServiceImpl prepareTestObject = new JmsServiceImpl();
        prepareTestObject.configure(preferences);
        return prepareTestObject;
    }

}
