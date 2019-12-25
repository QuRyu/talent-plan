use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use futures::future;
use futures::sync::mpsc::UnboundedSender;
use futures::Future;
use rand::Rng;

use labrpc::RpcFuture;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

const ELECTION_TIMEOUT_LOWER_BOUND: u32 = 500;
const ELECTION_TIMEOUT_UPPER_BOUND: u32 = 1000;
const HEARTBEAT_TIMEOUT: u32 = 300;

const MILLIS: Duration = Duration::from_millis(1);

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

/// Role of a raft peer.
#[derive(PartialEq, Eq, Clone, Debug)]
enum Role {
    /// The peer is a follower.
    Follower,
    /// The peer is a leader.
    Leader,
    /// The peer might become a leader.
    Candidate,
}

impl Default for Role {
    fn default() -> Role {
        Role::Follower
    }
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    state: Arc<State>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    term: u64,
    voted_for: Option<u64>,
    leader_id: Option<u64>,
    role: Role,
    timer: Instant,
    election_timeout: Duration,
    heartbeat_timeout: Duration,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: Arc::default(),
            term: 0,
            voted_for: None,
            leader_id: None,
            role: Default::default(),
            timer: Instant::now(),
            election_timeout: MILLIS,
            heartbeat_timeout: HEARTBEAT_TIMEOUT * MILLIS,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf.reset_election_timeout();

        //crate::your_code_here((rf, apply_ch))

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        sender: SyncSender<Result<RequestVoteReply>>,
        args: &RequestVoteArgs,
    ) {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let (tx, rx) = channel();
        // peer.spawn(
        //     peer.request_vote(&args)
        //         .map_err(Error::Rpc)
        //         .then(move |res| {
        //             tx.send(res);
        //             Ok(())
        //         }),
        // );
        // rx
        // ```
        //let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);

        let peer = &self.peers[server];
        peer.spawn(
            peer.request_vote(&args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    match sender.send(res) {
                        Ok(_) => {}
                        Err(e) => error!("request vote sender error {:?}", e),
                    };
                    Ok(())
                }),
        );
    }

    fn append_log_entries(
        &self, 
        server: usize, 
        sender: SyncSender<Result<AppendEntriesReply>>,
        args: &AppendEntriesArgs
    ) {
        let peer = &self.peers[server];
        peer.spawn(
            peer.append_entries(&args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    match sender.send(res) {
                        Ok(_) => {}
                        Err(e) => error!("log entries sender error {:?}", e),
                    };
                    Ok(())
                }),
        );
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn reset_timer(&mut self) {
        self.timer = Instant::now();
    }

    fn reset_election_timeout(&mut self) {
        let timeout = rand::thread_rng()
            .gen_range(ELECTION_TIMEOUT_LOWER_BOUND, ELECTION_TIMEOUT_UPPER_BOUND);
        self.election_timeout = timeout * MILLIS;
    }

    fn pass_election_timeout(&self) -> bool {
        self.timer.elapsed() > self.election_timeout
    }

    fn pass_heartbeat_timeout(&self) -> bool {
        self.timer.elapsed() > self.heartbeat_timeout
    }

    fn is_leader(&self) -> bool {
        self.leader_id.is_some() && self.leader_id.unwrap() == self.me as u64
    }

    /// reset peer state to become follower
    fn become_follower(&mut self, term: u64) {
        self.term = term;
        self.role = Role::Follower;
        self.leader_id = None;
        self.voted_for = None;

        self.persist();
        self.reset_election_timeout();
        self.reset_timer();
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = &self.state;
        let _ = &self.persister;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    raft: Arc<Mutex<Raft>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        let n = Node {
            raft: Arc::new(Mutex::new(raft)),
        };

        // thread handling election
        let node = n.clone();
        thread::Builder::new()
            .spawn(move || {
                loop {
                    let raft = node.raft.lock().unwrap();
                    if raft.is_leader() && raft.pass_heartbeat_timeout() {
                        std::mem::drop(raft);
                        let node = node.clone();
                        thread::Builder::new()
                            .spawn(move || {
                                node.replicate(); // trick the node to send empty logs
                            })
                            .expect("fail to spawn replicate thread");
                    } else if !raft.is_leader() && raft.pass_election_timeout() {
                        std::mem::drop(raft);
                        let node = node.clone();
                        thread::Builder::new()
                            .spawn(move || {
                                node.campaign(); // start election
                            })
                            .expect("fail to spawn campaign thread");
                    } else {
                        thread::sleep(5 * MILLIS);
                    }
                }
            })
            .expect("failed to spawn election thread");

        n
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let raft = self.raft.lock().unwrap();
        raft.start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        let raft = self.raft.lock().unwrap();
        raft.term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        let raft = self.raft.lock().unwrap();
        raft.is_leader()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }

    /// Start a new term and request votes from peers
    /// if the node believes the primary has died
    fn campaign(&self) {
        let vote_count = Arc::new(AtomicUsize::new(1));
        let mut raft = self.raft.lock().unwrap();

        let (tx, rx) = sync_channel(raft.peers.len() - 1);
        let votes_needed = raft.peers.len() / 2 + 1;
        let me = raft.me;

        raft.term += 1;
        raft.role = Role::Candidate;
        raft.leader_id = None;
        raft.voted_for = Some(raft.me as u64);
        println!("server {} campaigns for votes, term {}", me, raft.term);
         
        raft.persist();

        let args = RequestVoteArgs {
            term: raft.term,
            candidate_id: raft.me as u64,
            // TODO: change these two
            last_log_index: 1,
            last_log_term: 1,
        };
        for i in 0..raft.peers.len() {
            if i == raft.me {
                continue;
            }

            let tx = tx.clone();
            raft.send_request_vote(i, tx, &args);
        }

        raft.reset_election_timeout();
        raft.reset_timer();

        let now = raft.timer;
        let timeout = raft.election_timeout;
        let current_term = raft.term;
        std::mem::drop(raft);

        println!("server {} finished sending vote requests, term {}", me, current_term);

        while now.elapsed() < timeout && vote_count.load(Ordering::SeqCst) < votes_needed+1 {
            if let Ok(resp) = rx.recv() {
                match resp {
                    Ok(RequestVoteReply { term, vote_granted }) => {
                        println!("server {} received vote reply", me);
                        if vote_granted {
                            println!("server {} vote_granted", me);
                            vote_count.fetch_add(1, Ordering::SeqCst);
                        } else if term > current_term {
                            let mut raft = self.raft.lock().unwrap();
                            raft.become_follower(term);
                            return;
                        }
                    }

                    Err(e) => {
                        error!("server {} campaign error {:?}", me, e);
                        // resend request vote
                        //let tx = tx.clone();
                        //let raft = self.raft.lock().unwrap();
                        //raft.send_request_vote(
                        //server as usize, tx, &args
                        //);
                        //std::mem::drop(raft);
                    }
                }
            }
        }

        let mut raft = self.raft.lock().unwrap();
        if vote_count.load(Ordering::SeqCst) >= votes_needed && raft.role == Role::Candidate {
            println!("server {} receives enough votes ({}) to become leader, term {}", me, vote_count.load(Ordering::SeqCst), current_term); // TODO: info
            raft.role = Role::Leader;
            raft.leader_id = Some(raft.me as u64);

            let node = self.clone();
            thread::spawn(move || {
                node.replicate();
            });
            // notify that new leader and coordinate entries 
        } else {
            println!("server {} campaign failed: vote count {}, term {}", me, vote_count.load(Ordering::SeqCst), current_term);
        }
        raft.reset_election_timeout();
        raft.reset_timer();
    }

    /// Send logs to peers
    fn replicate(&self) {
        // just send empty logs here 
        let mut raft = self.raft.lock().unwrap();
        
        if !raft.is_leader() {
            return;
        }

        let me = raft.me; 
        let (tx, rx) = sync_channel(raft.peers.len()-1);

        // TODO: move this to entries 
        let args = AppendEntriesArgs {
            term: raft.term, 
            leader_id: raft.me as u64,
        };

        for i in 0..raft.peers.len() {
            if i == raft.me {
                continue;
            }

            let tx = tx.clone();
            raft.append_log_entries(i, tx, &args);
        }

        raft.reset_timer();
        let now = raft.timer;
        let timeout = raft.heartbeat_timeout;
        std::mem::drop(raft);

        while now.elapsed() < timeout {
            if let Ok(resp) = rx.recv() {
                match resp {
                    Ok(AppendEntriesReply {term, success}) => {
                        if success {
                            // TODO: update log entries 
                            continue;
                        } else {
                            let mut raft = self.raft.lock().unwrap();
                            if raft.term < term { // become follower 
                                raft.become_follower(term);
                                return;
                            } else { // follower lags behind, send snapshot 
                            }
                        }
                    }

                    Err(e) => error!("server {} append entries error {:?}", me, e),
                }
            }
        }

    }

}

impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        // TODO: add conditions on logs later
        let mut raft = self.raft.lock().unwrap();
        let me = raft.me;
        let mut reply = RequestVoteReply {
            term: raft.term,
            vote_granted: false,
        };

        if raft.term == args.term
            && raft.voted_for.is_some()
            && raft.voted_for.unwrap() == args.candidate_id as u64
        {
            reply.vote_granted = true;
        } else if raft.term < args.term {
            // add log condition
            raft.role = Role::Follower;
            raft.leader_id = Some(args.candidate_id);
            raft.term = args.term;
            raft.voted_for = Some(args.candidate_id);
            reply.vote_granted = true;
        }

        raft.reset_timer();
        std::mem::drop(raft);

        println!(
            "server {} received vote request from {} for term {}, granted {}",
            me, args.candidate_id, args.term, reply.vote_granted
        );

        Box::new(future::ok(reply))
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        let mut raft = self.raft.lock().unwrap();
        let mut reply = AppendEntriesReply {
            term: raft.term, 
            success: false,
        };

        if raft.term <= args.term { 
            reply.success = true; 
            raft.leader_id = Some(args.leader_id);
            raft.role = Role::Follower;
            raft.reset_timer();

            if raft.term < args.term {
                raft.term = args.term; 
                raft.voted_for = None; 
            } 

            // apply logs here 
            // use apply_ch as well 

            raft.persist();
            raft.reset_timer();
        }
         
        std::mem::drop(raft);
        Box::new(future::ok(reply))
    }
}
