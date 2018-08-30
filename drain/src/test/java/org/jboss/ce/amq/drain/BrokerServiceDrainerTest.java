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
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.FilteredKahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MultiKahaDBPersistenceAdapter;
import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryAgentFactory;
import org.apache.activemq.transport.discovery.DiscoveryListener;
import org.apache.activemq.util.FactoryFinder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class BrokerServiceDrainerTest {
   private static final Logger log = LoggerFactory.getLogger(BrokerServiceDrainerTest.class);

   @Rule
   public TemporaryFolder folder= new TemporaryFolder();

   final static FactoryFinder.ObjectFactory fallback = FactoryFinder.getObjectFactory();

   BrokerService meshBroker;
   static String meshUrl;

   @Before
   public void initMeshBrokerTarget() throws Exception {
      meshBroker = new BrokerService();
      meshBroker.setBrokerName("mesh");
      meshBroker.setPersistent(false);
      meshBroker.setUseJmx(false);
      meshBroker.setAdvisorySupport(false);
      TransportConnector tcp = meshBroker.addConnector("tcp://localhost:0");
      meshBroker.start();
      meshUrl = tcp.getPublishableConnectString();
      // ensure broker registry findFirst does not mess with drain embedded broker
      BrokerRegistry.getInstance().unbind(meshBroker.getBrokerName());
   }

   @After
   public void stopMeshBroker() throws Exception {
      if (meshBroker != null) {
         meshBroker.stop();
      }
   }

   @BeforeClass
   public static void setUpDiscoveryKubeAgent() throws Exception {
      FactoryFinder.setObjectFactory(new FactoryFinder.ObjectFactory() {
         @Override
         public Object create(String s) throws IllegalAccessException, InstantiationException, IOException, ClassNotFoundException {
            log.info("Create: " + s);
            if (s.endsWith("kube")) {
               return new DiscoveryAgentFactory() {
                  @Override
                  protected DiscoveryAgent doCreateDiscoveryAgent(URI uri) throws IOException {
                     final DiscoveryListener[] listener = new DiscoveryListener[1];
                     return new DiscoveryAgent() {
                        @Override
                        public void setDiscoveryListener(DiscoveryListener discoveryListener) {
                           listener[0] = discoveryListener;
                        }

                        @Override
                        public void registerService(String s) throws IOException {

                        }

                        @Override
                        public void serviceFailed(DiscoveryEvent discoveryEvent) throws IOException {

                        }

                        @Override
                        public void start() throws Exception {
                           DiscoveryEvent event = new DiscoveryEvent();
                           event.setServiceName(getMeshUrl());
                           listener[0].onServiceAdd(event);
                        }

                        @Override
                        public void stop() throws Exception {

                        }
                     };
                  }
               };
            } else {
               return fallback.create(s);
            }
         }
      });
   }

   private static String getMeshUrl() {
      return meshUrl;
   }

   @org.junit.Test
   public void testDrainKahaDb() throws Exception {
         File dataDir = folder.newFolder("data-dir");

         final BrokerService brokerToDrain = new BrokerService();
         brokerToDrain.setUseJmx(false);
         brokerToDrain.setAdvisorySupport(false);
         brokerToDrain.setBrokerName("source");

         brokerToDrain.setDataDirectoryFile(dataDir);

         File kahaDataDir = new File(dataDir, "kahadb");
         final PersistenceAdapter adaptor = new KahaDBPersistenceAdapter();
         adaptor.setDirectory(kahaDataDir);
         brokerToDrain.setPersistenceAdapter(adaptor);

         brokerToDrain.start();
         populateBroker(brokerToDrain);

         assertEquals("all messages enqueued", 20,
                      ((RegionBroker)brokerToDrain.getRegionBroker()).getDestinationStatistics().getEnqueues().getCount());

         assertEquals("all messages present", 20,
                   ((RegionBroker)brokerToDrain.getRegionBroker()).getDestinationStatistics().getMessages().getCount());

         brokerToDrain.stop();
         brokerToDrain.waitUntilStopped();

         log.info("do drain..");

         //((RegionBroker)meshBroker.getRegionBroker()).getDestinationStatistics().reset();

         // default is kahadb sub dir
         BrokerServiceDrainer.main(new String[]{dataDir.getAbsolutePath()});

         assertEquals("all messages forwarded to mesh", 20,
                      ((RegionBroker)meshBroker.getRegionBroker()).getDestinationStatistics().getEnqueues().getCount());


   }



   @org.junit.Test
   public void testDrainMultiKahaDb() throws Exception {
      File dataDir = folder.newFolder("data-dir");

      final BrokerService brokerToDrain = new BrokerService();
      brokerToDrain.setUseJmx(false);
      brokerToDrain.setAdvisorySupport(false);
      brokerToDrain.setBrokerName("source-mkahadb");

      brokerToDrain.setDataDirectoryFile(dataDir);

      File kahaDataDir = new File(dataDir, "kahadb");
      final MultiKahaDBPersistenceAdapter multiKahaDBPersistenceAdapter = new MultiKahaDBPersistenceAdapter();
      multiKahaDBPersistenceAdapter.setDirectory(kahaDataDir);

      ArrayList<FilteredKahaDBPersistenceAdapter> adapters = new ArrayList<>();

      FilteredKahaDBPersistenceAdapter template = new FilteredKahaDBPersistenceAdapter();
      template.setPersistenceAdapter(new KahaDBPersistenceAdapter());
      template.setPerDestination(true);
      adapters.add(template);
      multiKahaDBPersistenceAdapter.setFilteredPersistenceAdapters(adapters);
      brokerToDrain.setPersistenceAdapter(multiKahaDBPersistenceAdapter);

      brokerToDrain.start();
      populateBroker(brokerToDrain);

      assertEquals("all messages enqueued", 20,
                   ((RegionBroker)brokerToDrain.getRegionBroker()).getDestinationStatistics().getEnqueues().getCount());

      assertEquals("all messages present", 20,
                   ((RegionBroker)brokerToDrain.getRegionBroker()).getDestinationStatistics().getMessages().getCount());

      brokerToDrain.stop();
      brokerToDrain.waitUntilStopped();

      log.info("do drain..");


      BrokerServiceDrainer.main(new String[]{dataDir.getAbsolutePath(), "mkahadb"});

      assertEquals("all messages forwarded to mesh", 20,
                   ((RegionBroker)meshBroker.getRegionBroker()).getDestinationStatistics().getEnqueues().getCount());


   }



   @org.junit.Test
   public void testDrainMultiKahaDbWithAnyDestMatch() throws Exception {
      File dataDir = folder.newFolder("data-dir");

      final BrokerService brokerToDrain = new BrokerService();
      brokerToDrain.setUseJmx(false);
      brokerToDrain.setAdvisorySupport(false);
      brokerToDrain.setBrokerName("source-mkahadb-match-all");

      brokerToDrain.setDataDirectoryFile(dataDir);

      File kahaDataDir = new File(dataDir, "kahadb");
      final MultiKahaDBPersistenceAdapter multiKahaDBPersistenceAdapter = new MultiKahaDBPersistenceAdapter();
      multiKahaDBPersistenceAdapter.setDirectory(kahaDataDir);

      ArrayList<FilteredKahaDBPersistenceAdapter> adapters = new ArrayList<>();

      FilteredKahaDBPersistenceAdapter template = new FilteredKahaDBPersistenceAdapter();
      template.setPersistenceAdapter(new KahaDBPersistenceAdapter());
      // no dest, will match any, which will be TWO.A
      adapters.add(template);

      template = new FilteredKahaDBPersistenceAdapter();
      template.setPersistenceAdapter(new KahaDBPersistenceAdapter());
      template.setDestination(new ActiveMQQueue("ONE.>"));
      adapters.add(template);

      multiKahaDBPersistenceAdapter.setFilteredPersistenceAdapters(adapters);
      brokerToDrain.setPersistenceAdapter(multiKahaDBPersistenceAdapter);

      brokerToDrain.start();
      populateBroker(brokerToDrain);

      assertEquals("all messages enqueued", 20,
                   ((RegionBroker)brokerToDrain.getRegionBroker()).getDestinationStatistics().getEnqueues().getCount());

      assertEquals("all messages present", 20,
                   ((RegionBroker)brokerToDrain.getRegionBroker()).getDestinationStatistics().getMessages().getCount());

      brokerToDrain.stop();
      brokerToDrain.waitUntilStopped();

      log.info("do drain..");


      BrokerServiceDrainer.main(new String[]{dataDir.getAbsolutePath(), "mkahadb"});

      assertEquals("all messages forwarded to mesh", 20,
                   ((RegionBroker)meshBroker.getRegionBroker()).getDestinationStatistics().getEnqueues().getCount());


   }

   private void populateBroker(BrokerService broker) throws Exception {

      ProducerBrokerExchange exchange = new ProducerBrokerExchange();
      exchange.setConnectionContext(broker.getAdminConnectionContext());
      ProducerInfo producerInfo = new ProducerInfo();
      ProducerState producerState = new ProducerState(producerInfo);
      exchange.setProducerState(producerState);
      ActiveMQTextMessage activeMQTextMessage = new ActiveMQTextMessage();
      activeMQTextMessage.setPersistent(true);
      activeMQTextMessage.setDestination(new ActiveMQQueue("ONE.A,TWO.A"));
      for (int i=0; i<10; i++) {
         Message copy = activeMQTextMessage.copy();
         copy.setMessageId(new MessageId("1:2:3:4:" + i));
         broker.getBroker().send(exchange, copy);
      }
   }
}