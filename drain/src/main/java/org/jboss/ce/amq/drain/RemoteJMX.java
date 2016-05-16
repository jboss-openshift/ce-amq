/*
 * JBoss, Home of Professional Open Source
 * Copyright 2016 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.ce.amq.drain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.management.AttributeList;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
class RemoteJMX extends AbstractJMX implements JMX {
    private static final Logger log = LoggerFactory.getLogger(RemoteJMX.class);
    private static final String brokerQueryString = "type=Broker,brokerName=%s";
    private static final String connectionQueryString = "type=Broker,brokerName=%s,connectionViewType=clientId,connectionName=%s";

    private static String BROKER_NAME;

    static {
        BROKER_NAME = Utils.getSystemPropertyOrEnvVar("broker.name", "localhost");
    }

    public Collection<DestinationHandle> queues() throws Exception {
        return destinations("Queues");
    }

    public Collection<DestinationHandle> durableTopicSubscribers() throws Exception {
        return destinations("InactiveDurableTopicSubscribers");
    }

    public void disconnect(String clientId) throws Exception {
        String query = connectionQuery(clientId);
        List<ObjectInstance> mbeans = queryMBeans(createJmxConnection(), query);
        for (ObjectInstance mbean : mbeans) {
            createJmxConnection().invoke(mbean.getObjectName(), "stop", new Object[0], new String[0]);
        }
    }

    public boolean hasNextMessage(DestinationHandle handle, String attributeName) throws Exception {
        AttributeList attributes = createJmxConnection().getAttributes(handle.getObjectName(), new String[]{attributeName});
        if (attributes.size() > 0) {
            Object value = attributes.asList().get(0).getValue();
            Number number = Number.class.cast(value);
            return (number.longValue() > 0);
        }
        return false;
    }

    public <T> T getAttribute(Class<T> type, DestinationHandle handle, String attributeName) throws Exception {
        return getAttribute(type, handle.getObjectName(), attributeName);
    }

    private String brokerQuery() {
        return String.format(brokerQueryString, BROKER_NAME);
    }

    private String connectionQuery(String clientId) {
        return String.format(connectionQueryString, BROKER_NAME, clientId);
    }

    private Collection<DestinationHandle> destinations(String type) throws Exception {
        String query = brokerQuery();
        List<ObjectInstance> mbeans = queryMBeans(createJmxConnection(), query);
        List<DestinationHandle> destinations = new ArrayList<>();
        for (ObjectInstance mbean : mbeans) {
            ObjectName objectName = mbean.getObjectName();
            ObjectName[] names = getAttribute(ObjectName[].class, objectName, type);
            for (ObjectName on : names) {
                destinations.add(new DestinationHandle(on));
            }
        }
        return destinations;
    }

    protected void print(String msg) {
        log.info(msg);
    }
}
