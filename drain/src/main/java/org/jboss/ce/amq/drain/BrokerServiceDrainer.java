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

import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.network.ConditionalNetworkBridgeFilterFactory;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public class BrokerServiceDrainer {
    private static final Logger log = LoggerFactory.getLogger(BrokerServiceDrainer.class);

    private static final String ACTIVEMQ_DATA = "activemq.data" ;

    private static final String MESH_URL_FORMAT = "%s://%s:61616/?queryInterval=%s";

    public static final String AMQ_DRAINER_DLQ_PROCESS_EXPIRED = "amq.drainer.dlq.process.expired";

    public static void main(final String[] args) throws Exception {
        final String dataDir;
        switch(args.length) {
        case 0:
            dataDir = Utils.getSystemPropertyOrEnvVar(ACTIVEMQ_DATA);
            break;
        default:
            dataDir = args[0];
        }

        if (dataDir == null) {
            throw new IllegalArgumentException("Missing ActiveMQ data directory!");
        } else {
            final File dataDirFile = new File(dataDir);
            if (!dataDirFile.exists()) {
                throw new IllegalArgumentException("Missing ActiveMQ data directory!");
            } else if (!dataDirFile.isDirectory()) {
                throw new IllegalArgumentException("ActiveMQ data directory is not a directory!");
            }
        }

        final BrokerService broker = new BrokerService();
        broker.setAdvisorySupport(false);
        broker.setBrokerName(getBrokerName());
        broker.setUseJmx(true);

        broker.setDataDirectory(dataDir);
        log.info(String.format("Data directory %s.", dataDir));

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setExpireMessagesPeriod(0);
        String dlqProcessExpired = Utils.getSystemPropertyOrEnvVar(AMQ_DRAINER_DLQ_PROCESS_EXPIRED);
        if (dlqProcessExpired != null) {
           defaultEntry.getDeadLetterStrategy().setProcessExpired(Boolean.parseBoolean(dlqProcessExpired));
        }
        ConditionalNetworkBridgeFilterFactory filterFactory = new ConditionalNetworkBridgeFilterFactory();
        filterFactory.setReplayWhenNoConsumers(true);
        defaultEntry.setNetworkBridgeFilterFactory(filterFactory);
        policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);

        String meshServiceName = Utils.getSystemPropertyOrEnvVar("amq.mesh.service.name");
        if (meshServiceName == null) {
            meshServiceName = getApplicationName() + "-amq-tcp";
        }
        String queryInterval = Utils.getSystemPropertyOrEnvVar("amq.mesh.query.interval", "3");
        String drainerDiscoveryType = Utils.getSystemPropertyOrEnvVar("amq.mesh.discovery.type", "kube");
        String meshURL = String.format(MESH_URL_FORMAT, drainerDiscoveryType, meshServiceName, queryInterval);

        // programmatically add the draining bridge, depends on the mesh url only (could be in the xml config either)
        log.info("Creating network connector.");
        NetworkConnector drainingNetworkConnector = broker.addNetworkConnector(meshURL);
        drainingNetworkConnector.setUserName(getUsername());
        drainingNetworkConnector.setPassword(getPassword());
        drainingNetworkConnector.setMessageTTL(-1);
        drainingNetworkConnector.setConsumerTTL(1);
        drainingNetworkConnector.setStaticBridge(true);
        drainingNetworkConnector.setStaticallyIncludedDestinations(Arrays.asList(new ActiveMQDestination[]{new ActiveMQQueue(">")}));

        for (File kahaDbDir : findKahaDbInstances(new File(dataDir, "kahadb"))) {

            final PersistenceAdapter adaptor = new KahaDBPersistenceAdapter();
            ((KahaDBPersistenceAdapter) adaptor).setConcurrentStoreAndDispatchQueues(false);
            adaptor.setDirectory(kahaDbDir);
            broker.setPersistenceAdapter(adaptor);

            log.info("Starting broker with data directory " + kahaDbDir);
            broker.start(true);
            broker.waitUntilStarted();
            log.info("Started broker.");

            long msgs;
            while ((msgs = ((RegionBroker)broker.getRegionBroker()).getDestinationStatistics().getMessages().getCount()) > 0) {
                log.info(String.format("Still %s msgs left to migrate ...", msgs));
                TimeUnit.SECONDS.sleep(5);
            }

            broker.stop();
            broker.waitUntilStopped();
        }
        log.info("-- [CE] A-MQ migration finished. --");
    }

    private static Iterable<? extends File> findKahaDbInstances(File root) {
        LinkedList<File> dirs = new LinkedList<File>();
        FileFilter destinationNames = new FileFilter() {
            public boolean accept(File file) {
                return file.getName().startsWith("queue#") || file.getName().startsWith("#210");
            }
        };
        File[] candidates = root.listFiles(destinationNames);
        if (candidates == null || candidates.length == 0) {
            // single instance kahaDb
            dirs.add(root);
        } else for (File mKahaDBDir : candidates) {
            dirs.add(mKahaDBDir);
        }
        return dirs;
    }

    public static String getApplicationName() {
        return Utils.getSystemPropertyOrEnvVar("application.name");
    }

    public static String getBrokerName() {
        return Utils.getSystemPropertyOrEnvVar("broker.name", Utils.getSystemPropertyOrEnvVar("hostname", "drainer"));
    }

    public static String getPassword() {
        return Utils.getSystemPropertyOrEnvVar("amq.password");
    }

    public static String getUsername() {
        return Utils.getSystemPropertyOrEnvVar("amq.user");
    }
}
