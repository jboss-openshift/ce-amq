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

import java.util.Iterator;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import sun.security.krb5.internal.crypto.Des;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public class Consumer extends Client {
    public Consumer(String url, String username, String password) {
        this(url, username, password, null);
    }

    public Consumer(String url, String username, String password, String clientId) {
        super(url, username, password, clientId);
    }

    private Queue createQueue(String queueName) throws JMSException {
        return getSession().createQueue(queueName);
    }

    public MessageConsumer queueConsumer(String queueName) throws JMSException {
        return getSession().createConsumer(createQueue(queueName));
    }

    public Iterator<Message> consumeQueue(DestinationHandle handle, String queueName) throws JMSException {
        final Queue queue = createQueue(queueName);
        return consumeMessages(queue, handle, "QueueSize");
    }

    public Iterator<Message> consumeDurableTopicSubscriptions(DestinationHandle handle, String topicName, String subscriptionName) throws JMSException {
        TopicSubscriber subscriber = getTopicSubscriber(topicName, subscriptionName);
        return consumeMessages(subscriber, handle, "PendingQueueSize");
    }

    private Iterator<Message> consumeMessages(Destination destination, DestinationHandle handle, String attributeName) throws JMSException {
        final MessageConsumer consumer = getSession().createConsumer(destination);
        return consumeMessages(consumer, handle, attributeName);
    }

    private Iterator<Message> consumeMessages(final MessageConsumer consumer, final DestinationHandle handle, final String attributeName) throws JMSException {
        return new Iterator<Message>() {
            public boolean hasNext() {
                try {
                    return getJMX().hasNextMessage(handle.getObjectName(), attributeName);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }

            public Message next() {
                try {
                    return consumer.receive();
                } catch (JMSException e) {
                    throw new IllegalStateException(e);
                }
            }

            public void remove() {
            }
        };
    }
}
