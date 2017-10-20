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
package org.aludratest.service.jms.data;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.aludratest.dict.Data;

/**
 * Abstract parent class for specific JMS message data classes.
 * @author Volker Bergmann
 */

public abstract class JmsMessageData extends Data {
	// parent class
	
	/**
	 * Properties to apply the jms message.
	 * Supports the properties of javax.jms.Message
	 */
	private Map<String, Object> properties;
	
	JmsMessageData() {
		//default, empty property list
		properties = new HashMap<String, Object>();
	}

	/**
	 * Returns the list of properties
	 * @return
	 */
	public Map<String, Object> getProperties() {
		return Collections.unmodifiableMap(properties);
	}
	
	/**
	 * Set the properties
	 * @param props
	 */
	public void setProperties(Map<String, Object> props) {
		this.properties.clear();
		this.properties.putAll(props);
	}
	
	/**
	 * Adds one property
	 * @param key
	 * @param value
	 */
	public void addProperty(String key, Object value) {
		this.properties.put(key, value);
	}
	
	
}
