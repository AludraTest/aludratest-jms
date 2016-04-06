package org.aludratest.service.jms;

import org.aludratest.config.impl.SimplePreferences;
import org.aludratest.service.jms.impl.JmsServiceImpl;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.jndi.ActiveMQInitialContextFactory;
import org.apache.commons.lang.StringUtils;
import org.junit.*;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ojurksch on 06.04.2016.
 */
public class TestJmsActionImpl {

    private static final String[] queues = {"dynamicQueues/testQueue1"};

    private static final String[] topics = {"dynamicTopics/testTopic1"};

    private static BrokerService testBroker;

    private static String testBrokerUri = "vm://localhost";


    private JmsService testObject;

    private JmsInteraction perform;

    @BeforeClass
    public static void startTestBroker() {

        testBroker = new BrokerService();
        try {
            testBroker.setPersistent(false);
            testBroker.addConnector(testBrokerUri);
            testBroker.start();
        } catch (Exception e) {
            Assert.fail("Failed to setup testbroker for url "
                    + testBrokerUri + " : " + e.getMessage());
        }
    }

    @AfterClass
    public static void stopTestBroker() {
        if (testBroker != null && testBroker.isStarted()) {
            try {
                testBroker.stop();
            } catch (Exception e)  {
                Assert.fail("Failed to stop testbroker for url "
                        + testBrokerUri + " : " + e.getMessage());
            }
        }
    }

    @Before
    public void prepareJmsService() {
        try {
            SimplePreferences preferences = new  SimplePreferences();
            preferences.setValue("connectionFactoryJndiName","ConnectionFactory");
            preferences.setValue("providerUrl",testBrokerUri);
            preferences.setValue("initialContextFactory",ActiveMQInitialContextFactory.class.getName());

            JmsServiceImpl prepareTestObject = new JmsServiceImpl();
            prepareTestObject.configure(preferences);
            this.testObject = prepareTestObject;
            this.perform = this.testObject.perform();
            this.perform.startConnection();

        } catch (Exception e) {
            Assert.fail("Failed to connect to testbroker on url "
                    + testBrokerUri + " : " + e.getMessage());
        }
    }

    @Test
    public void testBasicJms() {
        try {

            TextMessage sentMessage = perform.createTextMessage();
            sentMessage.setText(System.currentTimeMillis()+"");
            perform.sendMessage(sentMessage,queues[0]);

            Message receivedMessage = perform.receiveMessage(queues[0],20);
            Assert.assertNotNull(receivedMessage);
            Assert.assertTrue(receivedMessage instanceof TextMessage);
            Assert.assertTrue(StringUtils.equalsIgnoreCase(sentMessage.getText(),((TextMessage) receivedMessage).getText()));

        } catch (Exception e) {
            Assert.fail("Failed on testBasicJms "
                    + " : " + e.getMessage());
        }

    }

    @Test
    public void testTopicSubscriber() {

        final List<Message> received = new ArrayList<Message>();

        final String clientId = "client";
        final String topicName = topics[0];
        final String expectedText = System.currentTimeMillis() + "";

        try {
            MessageListener listener = new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    received.add(message);
                }
            };

            this.perform.subscribeTopic(listener,topicName,null,clientId,true);

            //  If anything distracting is in there
            received.clear();

            this.perform.sendTextMessage(expectedText,topicName);

            Assert.assertTrue(received.size() >= 1);

            boolean foundExpected = false;
            for (Message m : received) {
                if (m instanceof TextMessage) {
                    String actualText = ((TextMessage) m).getText();
                    if (StringUtils.equals(expectedText,actualText)) {
                        foundExpected = true;
                        break;
                    }

                }
            }

            received.clear();

            this.perform.unsubscribeTopic(clientId);

            Assert.assertTrue(foundExpected);

        } catch (Exception e)  {
            Assert.fail("Unexpected excpetion on testTopicSubscriber "
                    + " : " + e.getMessage());
        }
    }

}
