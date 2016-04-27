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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Topic;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public class Consumer extends Client {
    public Consumer(String url, String username, String password) {
        super(url, username, password);
    }

    public Iterator<Message> consumeQueue(String queueName) throws JMSException {
        final Queue queue = getSession().createQueue(queueName);
        DestinationInfo info = new DestinationInfo() {
            public String getType() {
                return "Queue";
            }

            public String getName() throws JMSException {
                return queue.getQueueName();
            }

            public String getAttribute() {
                return "QueueSize";
            }
        };
        return consumeMessages(queue, info);
    }

    public Iterator<Message> consumeTopic(String topicName) throws JMSException {
        final Topic topic = getSession().createTopic(topicName);
        DestinationInfo info = new DestinationInfo() {
            public String getType() {
                return "Topic";
            }

            public String getName() throws JMSException {
                return topic.getTopicName();
            }

            public String getAttribute() {
                return "InFlightCount"; // TODO -- which attribute??
            }
        };
        return consumeMessages(topic, info);
    }

    private Iterator<Message> consumeMessages(Destination destination, final DestinationInfo info) throws JMSException {
        final MessageConsumer consumer = getSession().createConsumer(destination);
        return new Iterator<Message>() {
            public boolean hasNext() {
                try {
                    return getJMX().hasNextMessage(info);
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
