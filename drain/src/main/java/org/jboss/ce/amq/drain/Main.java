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

import java.net.URL;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Message;

import org.jboss.ce.amq.drain.jms.Consumer;
import org.jboss.ce.amq.drain.jms.Producer;
import org.jboss.ce.amq.drain.jmx.DTSTuple;
import org.jboss.ce.amq.drain.jmx.DestinationHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private String consumerURL = Utils.getSystemPropertyOrEnvVar("consumer.url", "tcp://" + Utils.getSystemPropertyOrEnvVar("hostname", "localhost") + ":61616");
    private String consumerUsername = Utils.getSystemPropertyOrEnvVar("consumer.username", Utils.getSystemPropertyOrEnvVar("amq.user"));
    private String consumerPassword = Utils.getSystemPropertyOrEnvVar("consumer.password", Utils.getSystemPropertyOrEnvVar("amq.password"));

    private String producerURL = Utils.getSystemPropertyOrEnvVar("producer.url");
    private String producerUsername = Utils.getSystemPropertyOrEnvVar("producer.username", Utils.getSystemPropertyOrEnvVar("amq.user"));
    private String producerPassword = Utils.getSystemPropertyOrEnvVar("producer.password", Utils.getSystemPropertyOrEnvVar("amq.password"));

    public static void main(String[] args) {
        try {
            Main main = new Main();
            main.validate();
            main.run();
        } catch (Exception e) {
            log.error("Error draining broker: " + e.getMessage(), e);
            System.exit(1);
        }
    }

    protected void validate() throws Exception {
        if (consumerUsername == null) {
            throw new IllegalArgumentException("Missing consumer username!");
        }
        if (consumerPassword == null) {
            throw new IllegalArgumentException("Missing consumer password!");
        }
        if (producerUsername == null) {
            throw new IllegalArgumentException("Missing producer username!");
        }
        if (producerPassword == null) {
            throw new IllegalArgumentException("Missing producer password!");
        }
        if (producerURL == null && getProducerURLRaw() == null) {
            throw new IllegalArgumentException("Missing producer url!");
        }
    }

    protected String getProducerURL() {
        if (producerURL == null) {
            producerURL = "tcp://" + getProducerURLRaw() + ":61616";
        }
        return producerURL;
    }

    private String getProducerURLRaw() {
        String appName = Utils.getSystemPropertyOrEnvVar("application.name");
        String url = Utils.getSystemPropertyOrEnvVar(appName + ".amq.tcp.service.host");
        if (url == null) {
            url = Utils.getSystemPropertyOrEnvVar("amq.service.name");
        }
        return url;
    }

    protected void info() {
        log.info("Running A-MQ migration ...");
    }

    protected void check() throws Exception {
        new URL(getProducerURL()).openConnection().connect();
        log.info("A-MQ service checked ...");
    }

    public void run() throws Exception {
        final AtomicBoolean terminating = new AtomicBoolean();
        final Semaphore statsSemaphore = new Semaphore(0);

        final Stats stats = new Stats();
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                public void run() {
                    terminating.set(true);
                    try {
                        statsSemaphore.acquire();
                    } catch (InterruptedException ignored) {
                        // ignore as we are terminating
                    }
                    stats.dumpStats();
                }
            }));

            info();

            check();

            if (!terminating.get()) {
                try (Producer queueProducer = new Producer(getProducerURL(), producerUsername, producerPassword)) {
                    queueProducer.start();

                    try (Consumer queueConsumer = new Consumer(consumerURL, consumerUsername, consumerPassword)) {
                        queueConsumer.start();

                        int msgsCounter;

                        // drain queues
                        Collection<DestinationHandle> queues = queueConsumer.getJMX().queues();
                        log.info("Found queues: {}", queues);
                        for (DestinationHandle handle : queues) {
                            if (terminating.get()) {
                                break;
                            }
                            msgsCounter = 0;
                            String queue = queueConsumer.getJMX().queueName(handle);
                            log.info("Processing queue: '{}'", queue);
                            stats.setSize(queue, queueConsumer.currentQueueSize(handle));
                            Producer.ProducerProcessor processor = queueProducer.processQueueMessages(queue);
                            Iterator<Message> iter = queueConsumer.consumeQueue(handle, queue);
                            while (iter.hasNext() && !terminating.get()) {
                                Message next = iter.next();
                                processor.processMessage(next);
                                msgsCounter++;
                                stats.increment(queue);
                            }
                            log.info("Handled {} messages for queue '{}'.", msgsCounter, queue);
                        }
                    }
                }
            }

            if (!terminating.get()) {
                try (Consumer dtsConsumer = new Consumer(consumerURL, consumerUsername, consumerPassword)) {
                    int msgsCounter;
                    // drain durable topic subscribers
                    Set<String> ids = new HashSet<>();
                    Collection<DestinationHandle> subscribers = dtsConsumer.getJMX().durableTopicSubscribers();
                    log.info("Found durable topic subscribers: {}", subscribers);
                    for (DestinationHandle handle : subscribers) {
                        if (terminating.get()) {
                            break;
                        }
                        msgsCounter = 0;
                        DTSTuple tuple = dtsConsumer.getJMX().dtsTuple(handle);
                        try (Producer dtsProducer = new Producer(getProducerURL(), producerUsername, producerPassword)) {
                            dtsProducer.start(tuple.clientId);

                            dtsProducer.getTopicSubscriber(tuple.topic, tuple.subscriptionName).close(); // just create dts on producer-side

                            Producer.ProducerProcessor processor = dtsProducer.processTopicMessages(tuple.topic);
                            dtsConsumer.getJMX().disconnect(tuple.clientId);
                            dtsConsumer.start(tuple.clientId);
                            try {
                                log.info("Processing topic subscriber : '{}' [{}]", tuple.topic, tuple.subscriptionName);
                                stats.setSize(tuple.topic + "/" + tuple.subscriptionName, dtsConsumer.currentTopicSubscriptionSize(handle));
                                Iterator<Message> iter = dtsConsumer.consumeDurableTopicSubscriptions(handle, tuple.topic, tuple.subscriptionName);
                                while (iter.hasNext() && !terminating.get()) {
                                    Message next = iter.next();
                                    if (ids.add(next.getJMSMessageID())) {
                                        processor.processMessage(next);
                                        msgsCounter++;
                                        stats.increment(tuple.topic + "/" + tuple.subscriptionName);
                                    }
                                }
                                log.info("Handled {} messages for topic subscriber '{}' [{}].", msgsCounter, tuple.topic, tuple.subscriptionName);
                            } finally {
                                //noinspection ThrowFromFinallyBlock
                                dtsConsumer.close();
                            }
                        }
                    }
                    log.info("Consumed {} messages.", ids.size());
                }
            }

            if (!terminating.get()) {
                log.info("-- [CE] A-MQ migration finished. --");
            }
        } finally {
            statsSemaphore.release();
        }
    }
}
