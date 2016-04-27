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

    public Iterator<Message> consumeQueue(String queue) throws JMSException {
        return consumeMessages(getSession().createQueue(queue));
    }

    public Iterator<Message> consumeTopic(String topic) throws JMSException {
        return consumeMessages(getSession().createTopic(topic));
    }

    private Iterator<Message> consumeMessages(final Destination destination) throws JMSException {
        final MessageConsumer consumer = getSession().createConsumer(destination);
        return new Iterator<Message>() {
            public boolean hasNext() {
                try {
                    return getJMX().hasNextMessage(getDestinationInfo(destination));
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

    private DestinationInfo getDestinationInfo(final Destination destination) throws JMSException {
        if (destination instanceof Queue) {
            final Queue queue = (Queue) destination;
            return new DestinationInfo() {
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
        } else if (destination instanceof Topic) {
            final Topic topic = (Topic) destination;
            return new DestinationInfo() {
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
        } else {
            throw new IllegalArgumentException("Unknown destination type: " + destination);
        }
    }
}
