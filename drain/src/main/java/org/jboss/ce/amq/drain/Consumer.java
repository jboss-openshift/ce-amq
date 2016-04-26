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

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public class Consumer extends Client {
    public Consumer(String url, String username, String password) {
        super(url, username, password);
    }

    public Iterator<Message> consumeQueue(String queue) throws JMSException {
        return consumeMessages(session.createQueue(queue));
    }

    public Iterator<Message> consumeTopic(String topic) throws JMSException {
        return consumeMessages(session.createTopic(topic));
    }

    private Iterator<Message> consumeMessages(Destination destination) throws JMSException {
        final MessageConsumer consumer = session.createConsumer(destination);
        return new Iterator<Message>() {
            private Message message;

            public boolean hasNext() {
                try {
                    message = consumer.receiveNoWait();
                    return (message != null);
                } catch (JMSException e) {
                    throw new IllegalStateException(e);
                }
            }

            public Message next() {
                return message;
            }

            public void remove() {
            }
        };
    }
}
