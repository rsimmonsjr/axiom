//! Implements a cluster manager for Axiom that manages connections to remote actor systems
//! over TCP.
//!
//! This is a reference implmentation for creating a cluster manager for Axiom. The developer can
//! use any technology they want for managing an Axiom cluster so long as it supports bridging two
//! actor systems with channels. This implementation achieves that bridge through generic
//! run-of-the-mill TCP connections. This is not to say that this code is simple, or usable only
//! for a reference. It is designed to be the default way Axiom is clustered and thus it will be
//! robust and well tested like the rest of Axiom.

use crate::prelude::*;
use log::{error, info};
use secc::*;
use std::collections::HashMap;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::sync::{Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use uuid::Uuid;

/// Encapsulates information on a connection to another actor system.
struct ConnectionData {
    /// Uuid of the system that this manager is connected to.
    pub system_uuid: Uuid,
    /// The uuid of the system that this connection data references.
    pub address: SocketAddr,
    /// The sender used to send wire messages to the connected actor system.
    pub sender: SeccSender<WireMessage>,
    /// The receiver used to send wire messages to the connected actor system.
    pub receiver: SeccReceiver<WireMessage>,
    /// A join handle for the thread managing the transmitting.
    pub tx_handle: JoinHandle<()>,
    /// A join handle for the thread managing the receiving.
    pub rx_handle: JoinHandle<()>,
}

/// Data for the [`TcpClusterMgr`].
struct TcpClusterMgrData {
    /// Address that this manager is listening for connections on.
    listen_address: SocketAddr,
    /// Actor System that this manager is attached to.
    system: ActorSystem,
    /// Handle to the thread that is listening for connections.
    listener: RwLock<Option<JoinHandle<()>>>,
    /// A map containing the data for all of the connections to this server.
    connections: RwLock<HashMap<Uuid, ConnectionData>>,
    /// A flag to exit the loops.
    running: AtomicBool,
}

#[derive(Clone)]
pub struct TcpClusterMgr {
    data: Arc<TcpClusterMgrData>,
}

impl TcpClusterMgr {
    /// Creates a new manager attached to the given actor system that manages connections to other
    /// [`TcpClusterMgr`]s.
    pub fn create(system: &ActorSystem, address: SocketAddr) -> TcpClusterMgr {
        let result = TcpClusterMgr {
            data: Arc::new(TcpClusterMgrData {
                listen_address: address,
                system: system.clone(),
                listener: RwLock::new(None),
                connections: RwLock::new(HashMap::new()),
                running: AtomicBool::new(true),
            }),
        };

        {
            // We will create a condvar so we wait for the listener to be up before the function
            // returning to the caller of this function. Note that we lock before starting the
            // listener so that we dont miss a notify and loop endlessly.
            let pair = Arc::new((Mutex::new(false), Condvar::new()));
            let (mutex, condvar) = &*pair;
            let mut started = mutex.lock().unwrap();
            let join_handle = result.start_tcp_listener(pair.clone());
            while !*started {
                started = condvar.wait(started).unwrap();
            }

            // We store the handle for the thread for later shutdown reasons.
            let mut handle = result.data.listener.write().unwrap();
            *handle = Some(join_handle);
        }

        result
    }

    // Starts a TCP listener that listens for incomming connections from other [`TcpClusterMgr`]s
    // and then creates a remote channel thread with the other actor system.
    fn start_tcp_listener(&self, pair: Arc<(Mutex<bool>, Condvar)>) -> JoinHandle<()> {
        let system = self.data.system.clone();
        let address = self.data.listen_address;
        let manager = self.clone();
        thread::spawn(move || {
            system.init_current();
            let sys_uuid = system.uuid();
            let listener = TcpListener::bind(address).unwrap();
            info!("{}: Listening for connections on {}.", sys_uuid, address);

            // Notify the cluster manager that the listener is ready.
            let (mutex, condvar) = &*pair;
            let mut started = mutex.lock().unwrap();
            *started = true;
            condvar.notify_all();
            drop(started);

            // Starts a loop waiting for connections from other managers.
            // FIXME Create a Shutdown Mechanism
            while manager.data.running.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, socket_address)) => {
                        info!(
                            "{}: Accepting connection from: {}.",
                            sys_uuid, socket_address
                        );
                        manager.start_tcp_threads(stream, socket_address);
                    }
                    Err(e) => {
                        error!("couldn't get client: {:?}", e);
                    }
                }
            }
        })
    }

    /// Connects to another [`TcpClusterMgr`] with TCP at the given socket address.
    pub fn connect(&self, address: SocketAddr, timeout: Duration) -> std::io::Result<()> {
        // FIXME Error handling needs to be improved.
        let stream = TcpStream::connect_timeout(&address, timeout)?;
        self.start_tcp_threads(stream, address);
        Ok(())
    }

    /// Connects this actor system to a remote actor system using the given string which contains
    /// `host:port` for the other actor sytem.
    fn start_tcp_threads(&self, stream: TcpStream, address: SocketAddr) {
        let arc_stream = Arc::new(stream);

        // FIXME: Allow channel size and poll to be configurable.
        let (sender, receiver) = secc::create::<WireMessage>(32, Duration::from_millis(10));
        let system_uuid = self.data.system.connect(&sender, &receiver);

        // Create the threads that manage the connections between the two systems.
        let tx_handle = self.start_tx_thread(arc_stream.clone(), receiver.clone());
        let rx_handle = self.start_rx_thread(arc_stream, sender.clone());

        let data = ConnectionData {
            system_uuid,
            address,
            receiver,
            sender,
            tx_handle,
            rx_handle,
        };

        info!(
            "{:?}: Connected to {:?}@{:?}",
            self.data.system.uuid(),
            system_uuid,
            address
        );

        let mut connections = self.data.connections.write().unwrap();
        connections.insert(data.system_uuid, data);
    }

    /// Starts the thread that takes messages off the receiver from the actor system channel
    /// and sends them to the remote system.
    fn start_tx_thread(
        &self,
        stream: Arc<TcpStream>,
        receiver: SeccReceiver<WireMessage>,
    ) -> JoinHandle<()> {
        // This thread manages transmitting messages to the stream.
        let system = self.data.system.clone();
        let manager = self.clone();
        thread::spawn(move || {
            system.init_current();
            let mut writer = BufWriter::new(&*stream);

            // FIXME Put in a mechanism for soft shutdown.
            // FIXME Allow configurable timeout.
            // FIXME Errors are not currently handled!
            while manager.data.running.load(Ordering::Relaxed) {
                if let Ok(message) = receiver.receive_await_timeout(Duration::from_millis(10)) {
                    bincode::serialize_into(&mut writer, &message).unwrap();
                    writer.flush().unwrap();
                }
            }
        })
    }

    /// Starts the thread that receives messages from the wire and puts them on the sender
    /// to send them to the actor system for processing.
    fn start_rx_thread(
        &self,
        stream: Arc<TcpStream>,
        sender: SeccSender<WireMessage>,
    ) -> JoinHandle<()> {
        let system = self.data.system.clone();
        let manager = self.clone();

        // This thread manages receiving messages from the stream.
        // FIXME Errors are not currently handled!
        // FIXME No mechanism to exit softly now.
        thread::spawn(move || {
            system.init_current();
            let mut reader = BufReader::new(&*stream);
            while manager.data.running.load(Ordering::Relaxed) {
                let msg: WireMessage = bincode::deserialize_from(&mut reader).unwrap();
                sender.send(msg).unwrap();
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::*;

    use super::*;

    #[test]
    fn test_tcp_remote_connect() {
        init_test_log();

        let socket_addr1 = SocketAddr::from(([127, 0, 0, 1], 7717));
        let system1 = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        let cluster_mgr1 = TcpClusterMgr::create(&system1, socket_addr1);

        let socket_addr2 = SocketAddr::from(([127, 0, 0, 1], 7727));
        let system2 = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        let _cluster_mgr2 = TcpClusterMgr::create(&system2, socket_addr2);

        // thread::sleep(Duration::from_millis(5000));
        cluster_mgr1
            .connect(socket_addr2, Duration::from_millis(2000))
            .unwrap();
    }
}
