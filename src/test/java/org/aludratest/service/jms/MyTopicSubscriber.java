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

/**
 * Custom {@link TopicSubscriber} implementation for testing.
 * @author Volker Bergmann
 */

public class MyTopicSubscriber extends TopicSubscriber<MyTopicSubscriber> {

	public MyTopicSubscriber(String subscriptionName, String destinationName, boolean durable, JmsService service) {
		super(subscriptionName, destinationName, durable, service);
	}

}
