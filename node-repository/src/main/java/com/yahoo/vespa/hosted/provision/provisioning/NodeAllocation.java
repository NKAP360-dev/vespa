// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.provision.provisioning;

import com.yahoo.config.provision.ApplicationId;
import com.yahoo.config.provision.ClusterMembership;
import com.yahoo.config.provision.ClusterSpec;
import com.yahoo.config.provision.Flavor;
import com.yahoo.config.provision.FlavorType;
import com.yahoo.config.provision.SystemName;
import com.yahoo.config.provision.TenantName;
import com.yahoo.config.provision.Zone;
import com.yahoo.lang.MutableInteger;
import com.yahoo.vespa.hosted.provision.Node;
import com.yahoo.vespa.hosted.provision.NodeList;
import com.yahoo.vespa.hosted.provision.node.Agent;
import com.yahoo.vespa.hosted.provision.node.Allocation;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Used to manage a list of nodes during the node reservation process
 * in order to fulfill the nodespec.
 * 
 * @author bratseth
 */
class NodeAllocation {

    /** List of all nodes in node-repository */
    private final NodeList allNodes;

    /** The application this list is for */
    private final ApplicationId application;

    /** The cluster this list is for */
    private final ClusterSpec cluster;

    /** The requested nodes of this list */
    private final NodeSpec requestedNodes;

    /** The nodes this has accepted so far */
    private final Set<PrioritizableNode> nodes = new LinkedHashSet<>();

    /** The number of nodes in the accepted nodes which are of the requested flavor */
    private int acceptedOfRequestedFlavor = 0;

    /** The number of nodes rejected because of clashing parentHostname */
    private int rejectedWithClashingParentHost = 0;

    /** The number of nodes rejected due to exclusivity constraints */
    private int rejectedDueToExclusivity = 0;

    /** The number of nodes that just now was changed to retired */
    private int wasRetiredJustNow = 0;

    /** The node indexes to verify uniqueness of each members index */
    private final Set<Integer> indexes = new HashSet<>();

    /** The next membership index to assign to a new node */
    private final MutableInteger highestIndex;

    private final Zone zone;

    private final Clock clock;

    NodeAllocation(NodeList allNodes, ApplicationId application, ClusterSpec cluster, NodeSpec requestedNodes,
                   MutableInteger highestIndex, Zone zone, Clock clock) {
        this.allNodes = allNodes;
        this.application = application;
        this.cluster = cluster;
        this.requestedNodes = requestedNodes;
        this.highestIndex = highestIndex;
        this.zone = zone;
        this.clock = clock;
    }

    /**
     * Offer some nodes to this. The nodes may have an allocation to a different application or cluster,
     * an allocation to this cluster, or no current allocation (in which case one is assigned).
     * 
     * Note that if unallocated nodes are offered before allocated nodes, this will unnecessarily
     * reject allocated nodes due to index duplicates.
     *
     * @param nodesPrioritized the nodes which are potentially on offer. These may belong to a different application etc.
     * @return the subset of offeredNodes which was accepted, with the correct allocation assigned
     */
    List<Node> offer(List<PrioritizableNode> nodesPrioritized) {
        List<Node> accepted = new ArrayList<>();
        for (PrioritizableNode offeredPriority : nodesPrioritized) {
            Node offered = offeredPriority.node;

            if (offered.allocation().isPresent()) {
                boolean wantToRetireNode = false;
                ClusterMembership membership = offered.allocation().get().membership();
                if ( ! offered.allocation().get().owner().equals(application)) continue; // wrong application
                if ( ! membership.cluster().equalsIgnoringGroupAndVespaVersion(cluster)) continue; // wrong cluster id/type
                if ((! offeredPriority.isSurplusNode || saturated()) && ! membership.cluster().group().equals(cluster.group())) continue; // wrong group and we can't or have no reason to change it
                if ( offered.allocation().get().isRemovable()) continue; // don't accept; causes removal
                if ( indexes.contains(membership.index())) continue; // duplicate index (just to be sure)

                // conditions on which we want to retire nodes that were allocated previously
                if ( violatesParentHostPolicy(this.nodes, offered)) wantToRetireNode = true;
                if ( ! hasCompatibleFlavor(offered)) wantToRetireNode = true;
                if ( offered.flavor().isRetired()) wantToRetireNode = true;
                if ( offered.status().wantToRetire()) wantToRetireNode = true;
                if ( requestedNodes.isExclusive() &&
                     ! hostsOnly(application.tenant(), offered.parentHostname())) wantToRetireNode = true;

                if (( ! saturated() && hasCompatibleFlavor(offered)) || acceptToRetire(offered) ) {
                    accepted.add(acceptNode(offeredPriority, wantToRetireNode));
                }
            }
            else if ( ! saturated() && hasCompatibleFlavor(offered)) {
                if ( violatesParentHostPolicy(this.nodes, offered)) {
                    ++rejectedWithClashingParentHost;
                    continue;
                }
                if ( ! exclusiveTo(application.tenant(), offered.parentHostname())) {
                    ++rejectedDueToExclusivity;
                    continue;
                }
                if ( requestedNodes.isExclusive() && ! hostsOnly(application.tenant(), offered.parentHostname())) {
                    ++rejectedDueToExclusivity;
                    continue;
                }
                if (offered.flavor().isRetired()) {
                    continue;
                }
                if (offered.status().wantToRetire()) {
                    continue;
                }
                offeredPriority.node = offered.allocate(application,
                                                        ClusterMembership.from(cluster, highestIndex.add(1)),
                                                        clock.instant());
                accepted.add(acceptNode(offeredPriority, false));
            }
        }

        return accepted;
    }


    private boolean violatesParentHostPolicy(Collection<PrioritizableNode> accepted, Node offered) {
        return checkForClashingParentHost() && offeredNodeHasParentHostnameAlreadyAccepted(accepted, offered);
    }

    private boolean checkForClashingParentHost() {
        return zone.system() == SystemName.main && zone.environment().isProduction();
    }

    private boolean offeredNodeHasParentHostnameAlreadyAccepted(Collection<PrioritizableNode> accepted, Node offered) {
        for (PrioritizableNode acceptedNode : accepted) {
            if (acceptedNode.node.parentHostname().isPresent() && offered.parentHostname().isPresent() &&
                    acceptedNode.node.parentHostname().get().equals(offered.parentHostname().get())) {
                return true;
            }
        }
        return false;
    }

    /**
     * If a parent host is given, and it hosts another tenant with an application which requires exclusive access
     * to the physical host, then we cannot host this application on it.
     */
    private boolean exclusiveTo(TenantName tenant, Optional<String> parentHostname) {
        if ( ! parentHostname.isPresent()) return true;
        for (Node nodeOnHost : allNodes.childrenOf(parentHostname.get())) {
            if ( ! nodeOnHost.allocation().isPresent()) continue;

            if ( nodeOnHost.allocation().get().membership().cluster().isExclusive() &&
                 ! nodeOnHost.allocation().get().owner().tenant().equals(tenant))
                return false;
        }
        return true;
    }

    private boolean hostsOnly(TenantName tenant, Optional<String> parentHostname) {
        if ( ! parentHostname.isPresent()) return true; // yes, as host is exclusive

        for (Node nodeOnHost : allNodes.childrenOf(parentHostname.get())) {
            if ( ! nodeOnHost.allocation().isPresent()) continue;
            if ( ! nodeOnHost.allocation().get().owner().tenant().equals(tenant))
                return false;
        }
        return true;
    }

    /**
     * Returns whether this node should be accepted into the cluster even if it is not currently desired
     * (already enough nodes, or wrong flavor).
     * Such nodes will be marked retired during finalization of the list of accepted nodes.
     * The conditions for this are
     * <ul>
     * <li>This is a content node. These must always be retired before being removed to allow the cluster to
     * migrate away data.
     * <li>This is a container node and it is not desired due to having the wrong flavor. In this case this
     * will (normally) obtain for all the current nodes in the cluster and so retiring before removing must
     * be used to avoid removing all the current nodes at once, before the newly allocated replacements are
     * initialized. (In the other case, where a container node is not desired because we have enough nodes we
     * do want to remove it immediately to get immediate feedback on how the size reduction works out.)
     * </ul>
     */
    private boolean acceptToRetire(Node node) {
        if (node.state() != Node.State.active) return false;
        if (! node.allocation().get().membership().cluster().group().equals(cluster.group())) return false;

        return (cluster.type() == ClusterSpec.Type.content) ||
                (cluster.type() == ClusterSpec.Type.container && ! hasCompatibleFlavor(node));
    }

    private boolean hasCompatibleFlavor(Node node) {
        return requestedNodes.isCompatible(node.flavor());
    }

    private Node acceptNode(PrioritizableNode prioritizableNode, boolean wantToRetire) {
        Node node = prioritizableNode.node;
        if (! wantToRetire) {
            if ( ! node.state().equals(Node.State.active)) {
                // reactivated node - make sure its not retired
                node = node.unretire();
                prioritizableNode.node = node;
            }
            acceptedOfRequestedFlavor++;
        } else {
            ++wasRetiredJustNow;
            // Retire nodes which are of an unwanted flavor, retired flavor or have an overlapping parent host
            node = node.retire(clock.instant());
            prioritizableNode.node = node;
        }
        if ( ! node.allocation().get().membership().cluster().equals(cluster)) {
            // group may be different
            node = setCluster(cluster, node);
            prioritizableNode.node = node;
        }
        indexes.add(node.allocation().get().membership().index());
        highestIndex.set(Math.max(highestIndex.get(), node.allocation().get().membership().index()));
        nodes.add(prioritizableNode);
        return node;
    }

    private Node setCluster(ClusterSpec cluster, Node node) {
        ClusterMembership membership = node.allocation().get().membership().with(cluster);
        return node.with(node.allocation().get().with(membership));
    }

    /** Returns true if no more nodes are needed in this list */
    private boolean saturated() {
        return requestedNodes.saturatedBy(acceptedOfRequestedFlavor);
    }

    /** Returns true if the content of this list is sufficient to meet the request */
    boolean fulfilled() {
        return requestedNodes.fulfilledBy(acceptedOfRequestedFlavor);
    }

    boolean wouldBeFulfilledWithRetiredNodes() {
        return requestedNodes.fulfilledBy(acceptedOfRequestedFlavor + wasRetiredJustNow);
    }

    boolean wouldBeFulfilledWithClashingParentHost() {
        return requestedNodes.fulfilledBy(acceptedOfRequestedFlavor + rejectedWithClashingParentHost);
    }

    boolean wouldBeFulfilledWithoutExclusivity() {
        return requestedNodes.fulfilledBy(acceptedOfRequestedFlavor + rejectedDueToExclusivity);
    }

    /**
     * Returns {@link FlavorCount} describing the docker node deficit for the given {@link NodeSpec}.
     *
     * @return empty if the requested spec is not count based or the requested flavor type is not docker or
     * the request is already fulfilled. Otherwise returns {@link FlavorCount} containing the required flavor
     * and node count to cover the deficit.
     */
    Optional<FlavorCount> getFulfilledDockerDeficit() {
        return Optional.of(requestedNodes)
                .filter(NodeSpec.CountNodeSpec.class::isInstance)
                .map(NodeSpec.CountNodeSpec.class::cast)
                .map(spec -> new FlavorCount(spec.getFlavor(), spec.fulfilledDeficitCount(acceptedOfRequestedFlavor)))
                .filter(flavorCount -> flavorCount.getFlavor().getType() == FlavorType.DOCKER_CONTAINER)
                .filter(flavorCount -> flavorCount.getCount() > 0);
    }

    /**
     * Make the number of <i>non-retired</i> nodes in the list equal to the requested number
     * of nodes, and retire the rest of the list. Only retire currently active nodes.
     * Prefer to retire nodes of the wrong flavor.
     * Make as few changes to the retired set as possible.
     *
     * @param surplusNodes this will add nodes not any longer needed by this group to this list
     * @return the final list of nodes
     */
    List<Node> finalNodes(List<Node> surplusNodes) {
        int currentRetiredCount = (int) nodes.stream().filter(node -> node.node.allocation().get().membership().retired()).count();
        int deltaRetiredCount = requestedNodes.idealRetiredCount(nodes.size(), currentRetiredCount) - currentRetiredCount;

        if (deltaRetiredCount > 0) { // retire until deltaRetiredCount is 0, prefer to retire higher indexes to minimize redistribution
            for (PrioritizableNode node : byDecreasingIndex(nodes)) {
                if ( ! node.node.allocation().get().membership().retired() && node.node.state().equals(Node.State.active)) {
                    node.node = node.node.retire(Agent.application, clock.instant());
                    surplusNodes.add(node.node); // offer this node to other groups
                    if (--deltaRetiredCount == 0) break;
                }
            }
        }
        else if (deltaRetiredCount < 0) { // unretire until deltaRetiredCount is 0
            for (PrioritizableNode node : byIncreasingIndex(nodes)) {
                if ( node.node.allocation().get().membership().retired() && hasCompatibleFlavor(node.node)) {
                    node.node = node.node.unretire();
                    if (++deltaRetiredCount == 0) break;
                }
            }
        }
        
        for (PrioritizableNode node : nodes) {
            node.node = requestedNodes.assignRequestedFlavor(node.node);

            // Set whether the node is exclusive
            Allocation allocation = node.node.allocation().get();
            node.node = node.node.with(allocation.with(allocation.membership()
                           .with(allocation.membership().cluster().exclusive(requestedNodes.isExclusive()))));
        }

        return nodes.stream().map(n -> n.node).collect(Collectors.toList());
    }

    List<Node> reservableNodes() {
        return nodesFilter(n -> n.node.state() == Node.State.inactive || n.node.state() == Node.State.ready);
    }

    List<Node> surplusNodes() {
        return nodesFilter(n -> n.isSurplusNode);
    }

    List<Node> newNodes() {
        return nodesFilter(n -> n.isNewNode);
    }

    private List<Node> nodesFilter(Predicate<PrioritizableNode> predicate) {
        return nodes.stream()
                .filter(predicate)
                .map(n -> n.node)
                .collect(Collectors.toList());
    }

    private List<PrioritizableNode> byDecreasingIndex(Set<PrioritizableNode> nodes) {
        return nodes.stream().sorted(nodeIndexComparator().reversed()).collect(Collectors.toList());
    }

    private List<PrioritizableNode> byIncreasingIndex(Set<PrioritizableNode> nodes) {
        return nodes.stream().sorted(nodeIndexComparator()).collect(Collectors.toList());
    }

    private Comparator<PrioritizableNode> nodeIndexComparator() {
        return Comparator.comparing((PrioritizableNode n) -> n.node.allocation().get().membership().index());
    }

    static class FlavorCount {
        private final Flavor flavor;
        private final int count;

        private FlavorCount(Flavor flavor, int count) {
            this.flavor = flavor;
            this.count = count;
        }

        Flavor getFlavor() {
            return flavor;
        }

        int getCount() {
            return count;
        }
    }
}
