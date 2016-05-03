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

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
class DTSFunction implements Function<DTSFunction.Tuple> {
    public Tuple apply(JMX jmx, DestinationHandle handle) throws Exception {
        return apply(jmx.createJmxConnection(), handle.getObjectName());
    }

    private Tuple apply(MBeanServerConnection connection, ObjectName objectName) throws Exception {
        String clientId = JMX.getAttribute(String.class, connection, objectName, "ClientId");
        String topic = JMX.getAttribute(String.class, connection, objectName, "DestinationName");
        String subscriptionName = JMX.getAttribute(String.class, connection, objectName, "SubscriptionName");
        return new Tuple(clientId, topic, subscriptionName);
    }

    public static class Tuple {
        public final String clientId;
        public final String topic;
        public final String subscriptionName;

        public Tuple(String clientId, String topic, String subscriptionName) {
            this.clientId = clientId;
            this.topic = topic;
            this.subscriptionName = subscriptionName;
        }
    }
}
