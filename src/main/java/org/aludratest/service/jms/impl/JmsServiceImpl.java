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

import org.aludratest.config.ConfigurationException;
import org.aludratest.config.Preferences;
import org.aludratest.config.ValidatingPreferencesWrapper;
import org.aludratest.exception.TechnicalException;
import org.aludratest.service.AbstractConfigurableAludraService;
import org.aludratest.service.jms.JmsCondition;
import org.aludratest.service.jms.JmsInteraction;
import org.aludratest.service.jms.JmsService;
import org.aludratest.service.jms.JmsVerification;

import javax.jms.ConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Hashtable;

public class JmsServiceImpl extends AbstractConfigurableAludraService implements JmsService {

	private String providerUrl;

	private InitialContext initialContext;

	private ConnectionFactory connectionFactory;

	private JmsActionImpl action;

	@Override
	public String getDescription() {
		return "JMS @ " + providerUrl;
	}

	@Override
	public void close() {
		if (action != null) {
			action.close();
		}
	}

	@Override
	public String getPropertiesBaseName() {
		return "jms";
	}

	@Override
	public void configure(Preferences preferences) {
		ValidatingPreferencesWrapper prefs = new ValidatingPreferencesWrapper(preferences);
		providerUrl = prefs.getRequiredStringValue("providerUrl");

		String initialContextFactory = prefs.getRequiredStringValue("initialContextFactory");
		String connectionFactoryName = prefs.getRequiredStringValue("connectionFactoryJndiName");

		String userName = prefs.getStringValue("jmsUser");
		String password = prefs.getStringValue("jmsPassword");

		Hashtable<String, String> env = new Hashtable<String, String>();
		env.put(Context.INITIAL_CONTEXT_FACTORY, initialContextFactory);
		env.put(Context.PROVIDER_URL, providerUrl);
		try {
			initialContext = new InitialContext(env);
			connectionFactory = (ConnectionFactory) initialContext.lookup(connectionFactoryName);
			if (connectionFactory == null) {
				throw new ConfigurationException("The connection factory could not be found.");
			}

		}
		catch (NamingException e) {
			throw new TechnicalException("Could not retrieve objects from JNDI context", e);
		}

		action = new JmsActionImpl(connectionFactory, initialContext, userName, password);
	}

	@Override
	public JmsInteraction perform() {
		return action;
	}

	@Override
	public JmsCondition check() {
		return action;
	}

	@Override
	public JmsVerification verify() {
		return action;
	}

	@Override
	public void initService() {
	}

}
