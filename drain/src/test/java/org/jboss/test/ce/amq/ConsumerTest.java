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

package org.jboss.test.ce.amq;

import java.util.Iterator;

import javax.jms.Message;

import org.jboss.ce.amq.drain.Consumer;
import org.jboss.ce.amq.drain.Utils;
import org.junit.Test;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public class ConsumerTest {

    private static final String URL = Utils.getSystemPropertyOrEnvVar("consumer.test.url", "tcp://localhost:61616");

    private static final String QUEUE = Utils.getSystemPropertyOrEnvVar("consumer.test.queue", "QUEUES.FOO");
    private static final String TOPIC = Utils.getSystemPropertyOrEnvVar("consumer.test.topic", "TOPICS.FOO");

    @Test
    public void testConsumeQueue() throws Exception {
        try (Consumer consumer = new Consumer(URL, null, null)) {
            consumer.start();

            Iterator<Message> iterator = consumer.consumeQueue(QUEUE);
            while (iterator.hasNext()) {
                System.out.println(iterator.next());
            }
        }
    }

    @Test
    public void testConsumeTopic() throws Exception {
        try (Consumer consumer = new Consumer(URL, null, null)) {
            consumer.start();

            Iterator<Message> iterator = consumer.consumeTopic(TOPIC);
            while (iterator.hasNext()) {
                System.out.println(iterator.next());
            }
        }
    }

}
