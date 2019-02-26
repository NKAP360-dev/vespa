// Copyright 2018 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.provision.provisioning;

import com.yahoo.component.Version;
import com.yahoo.config.provision.ApplicationId;
import com.yahoo.config.provision.ApplicationName;
import com.yahoo.config.provision.Capacity;
import com.yahoo.config.provision.ClusterSpec;
import com.yahoo.config.provision.DockerImage;
import com.yahoo.config.provision.Flavor;
import com.yahoo.config.provision.FlavorType;
import com.yahoo.config.provision.HostFilter;
import com.yahoo.config.provision.HostSpec;
import com.yahoo.config.provision.InstanceName;
import com.yahoo.config.provision.NodeFlavors;
import com.yahoo.config.provision.NodeType;
import com.yahoo.config.provision.ProvisionLogger;
import com.yahoo.config.provision.TenantName;
import com.yahoo.config.provision.Zone;
import com.yahoo.config.provisioning.FlavorsConfig;
import com.yahoo.test.ManualClock;
import com.yahoo.transaction.NestedTransaction;
import com.yahoo.vespa.curator.Curator;
import com.yahoo.vespa.curator.mock.MockCurator;
import com.yahoo.vespa.curator.transaction.CuratorTransaction;
import com.yahoo.vespa.flags.FlagSource;
import com.yahoo.vespa.flags.InMemoryFlagSource;
import com.yahoo.vespa.hosted.provision.Node;
import com.yahoo.vespa.hosted.provision.NodeList;
import com.yahoo.vespa.hosted.provision.NodeRepository;
import com.yahoo.vespa.hosted.provision.lb.LoadBalancerServiceMock;
import com.yahoo.vespa.hosted.provision.node.Agent;
import com.yahoo.vespa.hosted.provision.node.filter.NodeHostFilter;
import com.yahoo.vespa.hosted.provision.persistence.NameResolver;
import com.yahoo.vespa.hosted.provision.testutils.MockNameResolver;
import com.yahoo.vespa.hosted.provision.testutils.MockProvisionServiceProvider;
import com.yahoo.vespa.orchestrator.OrchestrationException;
import com.yahoo.vespa.orchestrator.Orchestrator;
import com.yahoo.vespa.service.duper.ConfigServerApplication;

import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * A test utility for provisioning tests.
 *
 * @author bratseth
 */
public class ProvisioningTester {

    private final Curator curator;
    private final NodeFlavors nodeFlavors;
    private final ManualClock clock;
    private final NodeRepository nodeRepository;
    private final Orchestrator orchestrator;
    private final NodeRepositoryProvisioner provisioner;
    private final CapacityPolicies capacityPolicies;
    private final ProvisionLogger provisionLogger;
    private final LoadBalancerServiceMock loadBalancerService;

    private int nextHost = 0;
    private int nextIP = 0;

    public ProvisioningTester(
            Curator curator, NodeFlavors nodeFlavors, Zone zone, NameResolver nameResolver,
            Orchestrator orchestrator, HostProvisioner hostProvisioner,
            LoadBalancerServiceMock loadBalancerService, FlagSource flagSource) {
        this.curator = curator;
        this.nodeFlavors = nodeFlavors;
        this.clock = new ManualClock();
        this.nodeRepository = new NodeRepository(nodeFlavors, curator, clock, zone, nameResolver,
                new DockerImage("docker-registry.domain.tld:8080/dist/vespa"), true);
        this.orchestrator = orchestrator;
        ProvisionServiceProvider provisionServiceProvider = new MockProvisionServiceProvider(loadBalancerService, hostProvisioner);
        this.provisioner = new NodeRepositoryProvisioner(nodeRepository, nodeFlavors, zone, provisionServiceProvider, flagSource);
        this.capacityPolicies = new CapacityPolicies(zone, nodeFlavors);
        this.provisionLogger = new NullProvisionLogger();
        this.loadBalancerService = loadBalancerService;
    }

    public static FlavorsConfig createConfig() {
        FlavorConfigBuilder b = new FlavorConfigBuilder();
        b.addFlavor("default", 2., 4., 100, FlavorType.BARE_METAL).cost(3);
        b.addFlavor("small", 1., 2., 50, FlavorType.BARE_METAL).cost(2);
        b.addFlavor("dockerSmall", 1., 1., 10, FlavorType.DOCKER_CONTAINER).cost(1);
        b.addFlavor("dockerLarge", 2., 1., 20, FlavorType.DOCKER_CONTAINER).cost(3);
        b.addFlavor("v-4-8-100", 4., 8., 100, FlavorType.VIRTUAL_MACHINE).cost(4);
        b.addFlavor("old-large1", 2., 4., 100, FlavorType.BARE_METAL).cost(6);
        b.addFlavor("old-large2", 2., 5., 100, FlavorType.BARE_METAL).cost(14);
        FlavorsConfig.Flavor.Builder large = b.addFlavor("large", 4., 8., 100, FlavorType.BARE_METAL).cost(10);
        b.addReplaces("old-large1", large);
        b.addReplaces("old-large2", large);
        FlavorsConfig.Flavor.Builder largeVariant = b.addFlavor("large-variant", 3., 9., 101, FlavorType.BARE_METAL).cost(9);
        b.addReplaces("large", largeVariant);
        FlavorsConfig.Flavor.Builder largeVariantVariant = b.addFlavor("large-variant-variant", 4., 9., 101, FlavorType.BARE_METAL).cost(11);
        b.addReplaces("large-variant", largeVariantVariant);
        return b.build();
    }

    public Curator getCurator() {
        return curator;
    }

    public void advanceTime(TemporalAmount duration) { clock.advance(duration); }
    public NodeRepository nodeRepository() { return nodeRepository; }
    public Orchestrator orchestrator() { return orchestrator; }
    public ManualClock clock() { return clock; }
    public NodeRepositoryProvisioner provisioner() { return provisioner; }
    public LoadBalancerServiceMock loadBalancerService() { return loadBalancerService; }
    public CapacityPolicies capacityPolicies() { return capacityPolicies; }
    public NodeList getNodes(ApplicationId id, Node.State ... inState) { return new NodeList(nodeRepository.getNodes(id, inState)); }

    public void patchNode(Node node) { nodeRepository.write(node); }

    public List<HostSpec> prepare(ApplicationId application, ClusterSpec cluster, int nodeCount, int groups, String flavor) {
        return prepare(application, cluster, nodeCount, groups, false, flavor);
    }

    public List<HostSpec> prepare(ApplicationId application, ClusterSpec cluster, int nodeCount, int groups, boolean required, String flavor) {
        return prepare(application, cluster, Capacity.fromNodeCount(nodeCount, Optional.ofNullable(flavor), required, true), groups);
    }

    public List<HostSpec> prepare(ApplicationId application, ClusterSpec cluster, Capacity capacity, int groups) {
        Set<String> reservedBefore = toHostNames(nodeRepository.getNodes(application, Node.State.reserved));
        Set<String> inactiveBefore = toHostNames(nodeRepository.getNodes(application, Node.State.inactive));
        // prepare twice to ensure idempotence
        List<HostSpec> hosts1 = provisioner.prepare(application, cluster, capacity, groups, provisionLogger);
        List<HostSpec> hosts2 = provisioner.prepare(application, cluster, capacity, groups, provisionLogger);
        assertEquals(hosts1, hosts2);
        Set<String> newlyActivated = toHostNames(nodeRepository.getNodes(application, Node.State.reserved));
        newlyActivated.removeAll(reservedBefore);
        newlyActivated.removeAll(inactiveBefore);
        return hosts2;
    }

    public void activate(ApplicationId application, Collection<HostSpec> hosts) {
        NestedTransaction transaction = new NestedTransaction();
        transaction.add(new CuratorTransaction(curator));
        provisioner.activate(transaction, application, hosts);
        transaction.commit();
        assertEquals(toHostNames(hosts), toHostNames(nodeRepository.getNodes(application, Node.State.active)));
    }

    public void deactivate(ApplicationId applicationId) {
        NestedTransaction deactivateTransaction = new NestedTransaction();
        nodeRepository.deactivate(applicationId, deactivateTransaction);
        deactivateTransaction.commit();
    }

    public Collection<String> toHostNames(Collection<HostSpec> hosts) {
        return hosts.stream().map(HostSpec::hostname).collect(Collectors.toSet());
    }

    public Set<String> toHostNames(List<Node> nodes) {
        return nodes.stream().map(Node::hostname).collect(Collectors.toSet());
    }

    /**
     * Asserts that each active node in this application has a restart count equaling the
     * number of matches to the given filters
     */
    public void assertRestartCount(ApplicationId application, HostFilter... filters) {
        for (Node node : nodeRepository.getNodes(application, Node.State.active)) {
            int expectedRestarts = 0;
            for (HostFilter filter : filters)
                if (NodeHostFilter.from(filter).matches(node))
                    expectedRestarts++;
            assertEquals(expectedRestarts, node.allocation().get().restartGeneration().wanted());
        }
    }

    public void fail(HostSpec host) {
        int beforeFailCount = nodeRepository.getNode(host.hostname(), Node.State.active).get().status().failCount();
        Node failedNode = nodeRepository.fail(host.hostname(), Agent.system, "Failing to unit test");
        assertTrue(nodeRepository.getNodes(NodeType.tenant, Node.State.failed).contains(failedNode));
        assertEquals(beforeFailCount + 1, failedNode.status().failCount());
    }

    public void assertMembersOf(ClusterSpec requestedCluster, Collection<HostSpec> hosts) {
        Set<Integer> indices = new HashSet<>();
        for (HostSpec host : hosts) {
            ClusterSpec nodeCluster = host.membership().get().cluster();
            assertTrue(requestedCluster.equalsIgnoringGroupAndVespaVersion(nodeCluster));
            if (requestedCluster.group().isPresent())
                assertEquals(requestedCluster.group(), nodeCluster.group());
            else
                assertEquals(0, nodeCluster.group().get().index());

            indices.add(host.membership().get().index());
        }
        assertEquals("Indexes in " + requestedCluster + " are disjunct", hosts.size(), indices.size());
    }

    public HostSpec removeOne(Set<HostSpec> hosts) {
        Iterator<HostSpec> i = hosts.iterator();
        HostSpec removed = i.next();
        i.remove();
        return removed;
    }

    public ApplicationId makeApplicationId() {
        return ApplicationId.from(
                TenantName.from(UUID.randomUUID().toString()),
                ApplicationName.from(UUID.randomUUID().toString()),
                InstanceName.from(UUID.randomUUID().toString()));
    }

    public List<Node> makeReadyNodes(int n, String flavor) {
        return makeReadyNodes(n, flavor, NodeType.tenant);
    }

    public List<Node> makeReadyNodes(int n, String flavor, NodeType type) {
        return makeReadyNodes(n, flavor, type, 0);
    }

    public List<Node> makeProvisionedNodes(int count, String flavor, NodeType type, int ipAddressPoolSize) {
        return makeProvisionedNodes(count, flavor, type, ipAddressPoolSize, false);
    }

    public List<Node> makeProvisionedNodes(int n, String flavor, NodeType type, int ipAddressPoolSize, boolean dualStack) {
        List<Node> nodes = new ArrayList<>(n);

        for (int i = 0; i < n; i++) {
            nextHost++;
            nextIP++;

            // One test involves two provision testers - to detect this we check if the
            // name resolver already contains the next host - if this is the case - bump the indices and move on
            String testIp = String.format("127.0.0.%d", nextIP);
            MockNameResolver nameResolver = (MockNameResolver)nodeRepository().nameResolver();
            if (nameResolver.getHostname(testIp).isPresent()) {
                nextHost += 100;
                nextIP += 100;
            }

            String hostname = String.format("host-%d.yahoo.com", nextHost);
            String ipv4 = String.format("127.0.0.%d", nextIP);
            String ipv6 = String.format("::%d", nextIP);

            nameResolver.addRecord(hostname, ipv4, ipv6);
            HashSet<String> hostIps = new HashSet<>();
            hostIps.add(ipv4);
            hostIps.add(ipv6);

            Set<String> ipAddressPool = new LinkedHashSet<>();
            for (int poolIp = 1; poolIp < ipAddressPoolSize; poolIp++) {
                nextIP++;
                String ipv6Addr = String.format("::%d", nextIP);
                ipAddressPool.add(ipv6Addr);
                nameResolver.addRecord(String.format("node-%d-of-%s", poolIp, hostname), ipv6Addr);
                if (dualStack) {
                    String ipv4Addr = String.format("127.0.127.%d", nextIP);
                    ipAddressPool.add(ipv4Addr);
                    nameResolver.addRecord(String.format("node-%d-of-%s", poolIp, hostname), ipv4Addr);
                }
            }

            nodes.add(nodeRepository.createNode(hostname,
                                                hostname,
                                                hostIps,
                                                ipAddressPool,
                                                Optional.empty(),
                                                nodeFlavors.getFlavorOrThrow(flavor),
                                                type));
        }
        nodes = nodeRepository.addNodes(nodes);
        return nodes;
    }

    public List<Node> makeConfigServers(int n, String flavor, Version configServersVersion) {
        List<Node> nodes = new ArrayList<>(n);
        MockNameResolver nameResolver = (MockNameResolver)nodeRepository().nameResolver();

        for (int i = 1; i <= n; i++) {
            String hostname = "cfg" + i;
            String ipv4 = "127.0.1." + i;

            nameResolver.addRecord(hostname, ipv4);
            Node node = nodeRepository.createNode(hostname,
                    hostname,
                    Collections.singleton(ipv4),
                    Optional.empty(),
                    nodeFlavors.getFlavorOrThrow(flavor),
                    NodeType.config);
            nodes.add(node);
        }

        nodes = nodeRepository.addNodes(nodes);
        nodes = nodeRepository.setDirty(nodes, Agent.system, getClass().getSimpleName());
        nodeRepository.setReady(nodes, Agent.system, getClass().getSimpleName());

        ConfigServerApplication application = new ConfigServerApplication();
        List<HostSpec> hosts = prepare(
                application.getApplicationId(),
                application.getClusterSpecWithVersion(configServersVersion),
                application.getCapacity(),
                1);
        activate(application.getApplicationId(), new HashSet<>(hosts));
        return nodeRepository.getNodes(application.getApplicationId(), Node.State.active);
    }

    public List<Node> makeReadyNodes(int n, String flavor, NodeType type, int ipAddressPoolSize) {
        return makeReadyNodes(n, flavor, type, ipAddressPoolSize, false);
    }

    public List<Node> makeReadyNodes(int n, String flavor, NodeType type, int ipAddressPoolSize, boolean dualStack) {
        List<Node> nodes = makeProvisionedNodes(n, flavor, type, ipAddressPoolSize, dualStack);
        nodes = nodeRepository.setDirty(nodes, Agent.system, getClass().getSimpleName());
        return nodeRepository.setReady(nodes, Agent.system, getClass().getSimpleName());
    }

    /** Creates a set of virtual docker hosts */
    public List<Node> makeReadyVirtualDockerHosts(int n, String flavor) {
        return makeReadyVirtualNodes(n, 1, flavor, Optional.empty(),
                i -> "dockerHost" + i, NodeType.host);
    }

    /** Creates a set of virtual docker nodes on a single docker host starting with index 1 and increasing */
    public List<Node> makeReadyVirtualDockerNodes(int n, String flavor, String dockerHostId) {
        return makeReadyVirtualNodes(n, 1, flavor, Optional.of(dockerHostId),
                i -> String.format("%s-%03d", dockerHostId, i), NodeType.tenant);
    }

    /** Creates a single of virtual docker node on a single parent host */
    public List<Node> makeReadyVirtualDockerNode(int index, String flavor, String dockerHostId) {
        return makeReadyVirtualNodes(1, index, flavor, Optional.of(dockerHostId),
                i -> String.format("%s-%03d", dockerHostId, i), NodeType.tenant);
    }

    /** Creates a set of virtual nodes without a parent host */
    public List<Node> makeReadyVirtualNodes(int n, String flavor) {
        return makeReadyVirtualNodes(n, 0, flavor, Optional.empty(),
                i -> UUID.randomUUID().toString(), NodeType.tenant);
    }

    /** Creates a set of virtual nodes on a single parent host */
    private List<Node> makeReadyVirtualNodes(int count, int startIndex, String flavor, Optional<String> parentHostId,
                                             Function<Integer, String> nodeNamer, NodeType nodeType) {
        List<Node> nodes = new ArrayList<>(count);
        for (int i = startIndex; i < count + startIndex; i++) {
            String hostname = nodeNamer.apply(i);
            nodes.add(nodeRepository.createNode("openstack-id", hostname, parentHostId,
                                                nodeFlavors.getFlavorOrThrow(flavor), nodeType));
        }
        nodes = nodeRepository.addNodes(nodes);
        nodes = nodeRepository.setDirty(nodes, Agent.system, getClass().getSimpleName());
        nodeRepository.setReady(nodes, Agent.system, getClass().getSimpleName());
        return nodes;
    }

    /** Returns the hosts from the input list which are not retired */
    public List<HostSpec> nonRetired(Collection<HostSpec> hosts) {
        return hosts.stream().filter(host -> ! host.membership().get().retired()).collect(Collectors.toList());
    }

    public void assertNumberOfNodesWithFlavor(List<HostSpec> hostSpecs, String flavor, int expectedCount) {
        long actualNodesWithFlavor = hostSpecs.stream()
                .map(HostSpec::hostname)
                .map(this::getNodeFlavor)
                .map(Flavor::flavorName)
                .filter(name -> name.equals(flavor))
                .count();
        assertEquals(expectedCount, actualNodesWithFlavor);
    }

    private Flavor getNodeFlavor(String hostname) {
        return nodeRepository.getNode(hostname).map(Node::flavor).orElseThrow(() -> new RuntimeException("No flavor for host " + hostname));
    }

    public static final class Builder {
        private Curator curator;
        private FlavorsConfig flavorsConfig;
        private Zone zone;
        private NameResolver nameResolver;
        private Orchestrator orchestrator;
        private HostProvisioner hostProvisioner;
        private LoadBalancerServiceMock loadBalancerService;
        private FlagSource flagSource;

        public Builder curator(Curator curator) {
            this.curator = curator;
            return this;
        }

        public Builder flavorsConfig(FlavorsConfig flavorsConfig) {
            this.flavorsConfig = flavorsConfig;
            return this;
        }

        public Builder zone(Zone zone) {
            this.zone = zone;
            return this;
        }

        public Builder nameResolver(NameResolver nameResolver) {
            this.nameResolver = nameResolver;
            return this;
        }

        public Builder orchestrator(Orchestrator orchestrator) {
            this.orchestrator = orchestrator;
            return this;
        }

        public Builder hostProvisioner(HostProvisioner hostProvisioner) {
            this.hostProvisioner = hostProvisioner;
            return this;
        }

        public Builder loadBalancerService(LoadBalancerServiceMock loadBalancerService) {
            this.loadBalancerService = loadBalancerService;
            return this;
        }

        public Builder flagSource(FlagSource flagSource) {
            this.flagSource = flagSource;
            return this;
        }

        public ProvisioningTester build() {
            Orchestrator orchestrator = Optional.ofNullable(this.orchestrator)
                    .orElseGet(() -> {
                        Orchestrator orch = mock(Orchestrator.class);
                        try {
                            doThrow(new RuntimeException()).when(orch).acquirePermissionToRemove(any());
                        } catch (OrchestrationException e) {
                            throw new RuntimeException(e);
                        }
                        return orch;
                    });

            return new ProvisioningTester(
                    Optional.ofNullable(curator).orElseGet(MockCurator::new),
                    new NodeFlavors(Optional.ofNullable(flavorsConfig).orElseGet(ProvisioningTester::createConfig)),
                    Optional.ofNullable(zone).orElseGet(Zone::defaultZone),
                    Optional.ofNullable(nameResolver).orElseGet(() -> new MockNameResolver().mockAnyLookup()),
                    orchestrator,
                    hostProvisioner,
                    Optional.ofNullable(loadBalancerService).orElseGet(LoadBalancerServiceMock::new),
                    Optional.ofNullable(flagSource).orElseGet(InMemoryFlagSource::new));
        }
    }

    private static class NullProvisionLogger implements ProvisionLogger {
        @Override public void log(Level level, String message) { }
    }

}
