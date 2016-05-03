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

import java.util.Collection;
import java.util.Iterator;

import javax.jms.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private String consumerURL = Utils.getSystemPropertyOrEnvVar("consumer.url");
    private String consumerUsername = Utils.getSystemPropertyOrEnvVar("consumer.username");
    private String consumerPassword = Utils.getSystemPropertyOrEnvVar("consumer.password");
    private String consumerClientId = Utils.getSystemPropertyOrEnvVar("consumer.clientId", "CE-AMQ");

    private String producerURL = Utils.getSystemPropertyOrEnvVar("producer.url");
    private String producerUsername = Utils.getSystemPropertyOrEnvVar("producer.username");
    private String producerPassword = Utils.getSystemPropertyOrEnvVar("producer.password");

    public static void main(String[] args) {
        try {
            Main main = new Main();
            main.run();
        } catch (Exception e) {
            log.error("Error draining broker: " + e.getMessage(), e);
            System.exit(1);
        }
    }

    public void run() throws Exception {
        try (Consumer consumer = new Consumer(consumerURL, consumerUsername, consumerPassword, consumerClientId)) {
            consumer.start();
            try (Producer producer = new Producer(producerURL, producerUsername, producerPassword)) {
                producer.start();

                int counter;

                // drain queues
                Function<String> qFn = new QueueFunction();
                Collection<DestinationHandle> queues = consumer.getJMX().queues();
                log.info(String.format("Found queues: %s", queues));
                for (DestinationHandle handle : queues) {
                    counter = 0;
                    String queue = qFn.apply(consumer.getJMX(), handle);
                    Producer.ProducerProcessor processor = producer.processQueueMessages(queue);
                    Iterator<Message> iter = consumer.consumeQueue(handle, queue);
                    while (iter.hasNext()) {
                        Message next = iter.next();
                        processor.processMessage(next);
                        counter++;
                    }
                    log.info(String.format("Handled %s messages for queue '%s'.", counter, queue));
                }

                // drain durable topic subscribers
                Function<DTSFunction.Pair> tFn = new DTSFunction();
                Collection<DestinationHandle> topics = consumer.getJMX().durableTopicSubscribers();
                log.info(String.format("Found durable topic subscribers: %s", topics));
                for (DestinationHandle handle : topics) {
                    counter = 0;
                    DTSFunction.Pair pair = tFn.apply(consumer.getJMX(), handle);
                    Producer.ProducerProcessor processor = producer.processTopicMessages(pair.topic);
                    Iterator<Message> iter = consumer.consumeDurableTopicSubscriptions(handle, pair.topic, pair.subscriptionName);
                    while (iter.hasNext()) {
                        Message next = iter.next();
                        processor.processMessage(next);
                        counter++;
                    }
                    log.info(String.format("Handled %s messages for topic '%s' [%s].", counter, pair.topic, pair.subscriptionName));
                }
            }
        }
    }
}
