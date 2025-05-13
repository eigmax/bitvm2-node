// #![feature(trivial_bounds)]
use libp2p::identity::Keypair;
use libp2p::{gossipsub, kad, kad::store::MemoryStore, swarm::StreamProtocol};
use libp2p_swarm_derive::NetworkBehaviour;
use std::time::Duration;
use tokio::io::{self};

// We create a custom network behaviour that combines Kademlia and mDNS.
#[derive(NetworkBehaviour)]
pub struct AllBehaviours {
    pub kademlia: kad::Behaviour<MemoryStore>,
    //pub mdns: mdns::tokio::Behaviour,
    pub gossipsub: gossipsub::Behaviour,
}

impl AllBehaviours {
    pub fn new(key: &Keypair) -> Self {
        let mut cfg = kad::Config::new(get_proto_name());
        cfg.set_query_timeout(Duration::from_secs(5 * 60));
        let store = kad::store::MemoryStore::new(key.public().to_peer_id());
        let kademlia = kad::Behaviour::with_config(key.public().to_peer_id(), store, cfg);
        //let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())
        //    .unwrap();

        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .max_transmit_size(4194304) // 4 MB
            .build()
            .map_err(io::Error::other)
            .unwrap();
        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(key.clone()),
            gossipsub_config,
        )
        .expect("Valid configuration");
        Self { kademlia, gossipsub }
    }
}

pub fn get_proto_name() -> StreamProtocol {
    let version = env!("CARGO_PKG_VERSION");
    let protocol = crate::env::get_proto_base();
    let kad_proto = format!("/{protocol}/kad/{version}");
    StreamProtocol::try_from_owned(kad_proto).expect("Valid kad proto")
}
