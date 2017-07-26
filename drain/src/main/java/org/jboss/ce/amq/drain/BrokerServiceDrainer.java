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
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
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

    private static final String MESH_URL_FORMAT = "kube://%s:61616/?queryInterval=%s";

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

        final PersistenceAdapter adaptor = new KahaDBPersistenceAdapter();
        adaptor.setDirectory(new File(dataDir + "/kahadb"));
        broker.setPersistenceAdapter(adaptor);

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setExpireMessagesPeriod(0);
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
        String meshURL = String.format(MESH_URL_FORMAT, meshServiceName, queryInterval);

        // programmatically add the draining bridge, depends on the mesh url only (could be in the xml config either)
        log.info("Creating network connector.");
        NetworkConnector drainingNetworkConnector = broker.addNetworkConnector(meshURL);
        drainingNetworkConnector.setUserName(getUsername());
        drainingNetworkConnector.setPassword(getPassword());
        drainingNetworkConnector.setMessageTTL(-1);
        drainingNetworkConnector.setConsumerTTL(1);
        drainingNetworkConnector.setStaticBridge(true);
        drainingNetworkConnector.setStaticallyIncludedDestinations(Arrays.asList(new ActiveMQDestination[]{new ActiveMQQueue(">"), new ActiveMQQueue("*")}));

        log.info("Starting broker.");
        broker.start();
        broker.waitUntilStarted();
        log.info("Started broker.");

        long msgs;
        while ((msgs = broker.getAdminView().getTotalMessageCount()) > 0) {
            log.info(String.format("Still %s msgs left to migrate ...", msgs));
            TimeUnit.SECONDS.sleep(5);
        }

        broker.stop();

        log.info("-- [CE] A-MQ migration finished. --");
    }

    public static String getApplicationName() {
        return Utils.getSystemPropertyOrEnvVar("application.name");
    }

    public static String getBrokerName() {
        return Utils.getSystemPropertyOrEnvVar("broker.name", Utils.getSystemPropertyOrEnvVar("hostname", "localhost"));
    }

    public static String getPassword() {
        return Utils.getSystemPropertyOrEnvVar("amq.password");
    }

    public static String getUsername() {
        return Utils.getSystemPropertyOrEnvVar("amq.user");
    }
}
