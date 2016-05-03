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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
        try (Producer queueProducer = new Producer(producerURL, producerUsername, producerPassword)) {
            queueProducer.start();

            try (Consumer queueConsumer = new Consumer(consumerURL, consumerUsername, consumerPassword)) {
                queueConsumer.start();

                int counter;

                // drain queues
                Function<String> qFn = new QueueFunction();
                Collection<DestinationHandle> queues = queueConsumer.getJMX().queues();
                log.info(String.format("Found queues: %s", queues));
                for (DestinationHandle handle : queues) {
                    counter = 0;
                    String queue = qFn.apply(queueConsumer.getJMX(), handle);
                    Producer.ProducerProcessor processor = queueProducer.processQueueMessages(queue);
                    Iterator<Message> iter = queueConsumer.consumeQueue(handle, queue);
                    while (iter.hasNext()) {
                        Message next = iter.next();
                        processor.processMessage(next);
                        counter++;
                    }
                    log.info(String.format("Handled %s messages for queue '%s'.", counter, queue));
                }
            }
        }

        try (Consumer dtsConsumer = new Consumer(consumerURL, consumerUsername, consumerPassword)) {
            int counter;
            // drain durable topic subscribers
            Set<String> ids = new HashSet<>();
            Function<DTSFunction.Tuple> tFn = new DTSFunction();
            Collection<DestinationHandle> topics = dtsConsumer.getJMX().durableTopicSubscribers();
            log.info(String.format("Found durable topic subscribers: %s", topics));
            for (DestinationHandle handle : topics) {
                counter = 0;
                DTSFunction.Tuple tuple = tFn.apply(dtsConsumer.getJMX(), handle);
                try (Producer dtsProducer = new Producer(producerURL, producerUsername, producerPassword)) {
                    dtsProducer.start(tuple.clientId);

                    dtsProducer.getTopicSubscriber(tuple.topic, tuple.subscriptionName).close(); // just create dts on producer-side

                    Producer.ProducerProcessor processor = dtsProducer.processTopicMessages(tuple.topic);
                    dtsConsumer.getJMX().disconnect(tuple.clientId);
                    dtsConsumer.start(tuple.clientId);
                    try {
                        Iterator<Message> iter = dtsConsumer.consumeDurableTopicSubscriptions(handle, tuple.topic, tuple.subscriptionName);
                        while (iter.hasNext()) {
                            Message next = iter.next();
                            if (ids.add(next.getJMSMessageID())) {
                                processor.processMessage(next);
                                counter++;
                            }
                        }
                        log.info(String.format("Handled %s messages for topic '%s' [%s].", counter, tuple.topic, tuple.subscriptionName));
                    } finally {
                        //noinspection ThrowFromFinallyBlock
                        dtsConsumer.stop();
                    }
                }
            }
            log.info(String.format("Consumed %s messages.", ids.size()));
        }
    }
}
