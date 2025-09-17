// src/path/tests.rs

use super::types::{GraphSnapshot, PoolEdge};
use ethers::types::Address;
use petgraph::graph::UnGraph;
use petgraph::visit::EdgeRef;

fn sample_address(val: u8) -> Address {
    let mut bytes = [0u8; 20];
    bytes[19] = val;
    Address::from(bytes)
}

fn build_sample_graph() -> (UnGraph<Address, PoolEdge>, Vec<Address>) {
    let mut graph = UnGraph::<Address, PoolEdge>::new_undirected();
    let addr1 = sample_address(1);
    let addr2 = sample_address(2);
    let addr3 = sample_address(3);

    let n1 = graph.add_node(addr1);
    let n2 = graph.add_node(addr2);
    let n3 = graph.add_node(addr3);

    graph.add_edge(
        n1,
        n2,
        PoolEdge {
            pool_address: sample_address(10),
        },
    );
    graph.add_edge(
        n2,
        n3,
        PoolEdge {
            pool_address: sample_address(11),
        },
    );
    graph.add_edge(
        n1,
        n3,
        PoolEdge {
            pool_address: sample_address(12),
        },
    );

    (graph, vec![addr1, addr2, addr3])
}

#[test]
fn test_graph_snapshot_node_and_edge_count() {
    let (graph, _) = build_sample_graph();
    let snapshot = GraphSnapshot {
        node_count: graph.node_count(),
        edge_count: graph.edge_count(),
        graph: graph.clone(),
    };
    assert_eq!(snapshot.node_count(), 3);
    assert_eq!(snapshot.edge_count(), 3);
}

#[test]
fn test_graph_snapshot_node_index_for_token_found() {
    let (graph, addrs) = build_sample_graph();
    let snapshot = GraphSnapshot {
        node_count: graph.node_count(),
        edge_count: graph.edge_count(),
        graph: graph.clone(),
    };
    let idx = snapshot.node_index_for_token(&addrs[1]);
    assert!(idx.is_some());
    let node_idx = idx.unwrap();
    assert_eq!(snapshot.graph[node_idx], addrs[1]);
}

#[test]
fn test_graph_snapshot_node_index_for_token_not_found() {
    let (graph, _) = build_sample_graph();
    let snapshot = GraphSnapshot {
        node_count: graph.node_count(),
        edge_count: graph.edge_count(),
        graph: graph.clone(),
    };
    let not_present = sample_address(99);
    assert!(snapshot.node_index_for_token(&not_present).is_none());
}

#[test]
fn test_graph_snapshot_edges() {
    let (graph, addrs) = build_sample_graph();
    let snapshot = GraphSnapshot {
        node_count: graph.node_count(),
        edge_count: graph.edge_count(),
        graph: graph.clone(),
    };
    let idx = snapshot.node_index_for_token(&addrs[0]).unwrap();
    let mut edge_count = 0;
    for edge in snapshot.edges(idx) {
        let pool_addr = edge.weight().pool_address;
        let token_a = edge.source();
        let token_b = edge.target();
        let token_a = snapshot.graph[token_a];
        let token_b = snapshot.graph[token_b];
        assert!(
            (pool_addr == sample_address(10) && ((token_a == addrs[0] && token_b == addrs[1]) || (token_a == addrs[1] && token_b == addrs[0]))) ||
            (pool_addr == sample_address(12) && ((token_a == addrs[0] && token_b == addrs[2]) || (token_a == addrs[2] && token_b == addrs[0])))
        );
        edge_count += 1;
    }
    assert_eq!(edge_count, 2);
}

#[test]
fn test_graph_snapshot_clone() {
    let (graph, _) = build_sample_graph();
    let snapshot = GraphSnapshot {
        node_count: graph.node_count(),
        edge_count: graph.edge_count(),
        graph: graph.clone(),
    };
    let cloned = snapshot.clone();
    assert_eq!(cloned.node_count, snapshot.node_count);
    assert_eq!(cloned.edge_count, snapshot.edge_count);
    assert_eq!(cloned.graph.node_count(), snapshot.graph.node_count());
    assert_eq!(cloned.graph.edge_count(), snapshot.graph.edge_count());
}

#[test]
fn test_token_to_node_mapping_consistency() {
    let (graph, addrs) = build_sample_graph();
    let snapshot = GraphSnapshot {
        node_count: graph.node_count(),
        edge_count: graph.edge_count(),
        graph: graph.clone(),
    };

    for (i, addr) in addrs.iter().enumerate() {
        let idx = snapshot.node_index_for_token(addr);
        assert!(
            idx.is_some(),
            "Token at index {} ({:?}) not found in node_index_for_token mapping!",
            i,
            addr
        );
        if let Some(node_idx) = idx {
            assert_eq!(
                snapshot.graph[node_idx],
                *addr,
                "Node index mapping mismatch for token {:?}",
                addr
            );
        }
    }

    for edge in snapshot.graph.edge_indices() {
        let (a, b) = snapshot.graph.edge_endpoints(edge).unwrap();
        let pool_addr = snapshot.graph[edge].pool_address;
        let token_a = snapshot.graph[a];
        let token_b = snapshot.graph[b];
        assert!(
            (pool_addr == sample_address(10) && ((token_a == addrs[0] && token_b == addrs[1]) || (token_a == addrs[1] && token_b == addrs[0]))) ||
            (pool_addr == sample_address(11) && ((token_a == addrs[1] && token_b == addrs[2]) || (token_a == addrs[2] && token_b == addrs[1]))) ||
            (pool_addr == sample_address(12) && ((token_a == addrs[0] && token_b == addrs[2]) || (token_a == addrs[2] && token_b == addrs[0])))
        );
        assert!(
            addrs.contains(&token_a) && addrs.contains(&token_b),
            "Edge connects unknown tokens: {:?} <-> {:?}",
            token_a,
            token_b
        );
    }
}
