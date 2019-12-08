use std::sync::{Arc, Mutex, mpsc};
use std::sync::atomic::{AtomicU64, AtomicUsize, AtomicBool, Ordering};
use std::thread;
use std::time::{Instant, Duration};
use futures::sync::mpsc::UnboundedSender;
use futures::Future;
use futures_cpupool::CpuPool;
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

const ELECTION_TIMEOUT_LOWER_BOUND: usize = 300;
const ELECTION_TIMEOUT_UPPER_BOUND: usize = 500;
const HEART_BEAT_INTERVAL: u32 = 200;
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

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    //state: Arc<State>,
    term: u64, 
    is_leader: bool,

    // state maintained for a raft peer
    voted_for: (u64, usize), // (current_term, leader_id)
    commit_index: u64, // index of highest entries committed
    last_applied: u64, // index of highest entries applied to state machine

    timer: Instant, // timer to keep track when the last communication happened 
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
        _apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            //state: Arc::default(),
            term: 0, 
            is_leader: false, 
            voted_for: (0, 0),
            commit_index: 0,
            last_applied: 0,
            timer: Instant::now(),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

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
        args: &RequestVoteArgs,
    ) -> mpsc::Receiver<Result<RequestVoteReply>> {
        let peer = &self.peers[server];
        let (tx, rx) = mpsc::sync_channel::<Result<RequestVoteReply>>(1);
        peer.spawn(
            peer.request_vote(&args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    tx.send(res).unwrap();
                    Ok(())
                }),
        );
        rx
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

    fn update_timer(&mut self) {
        self.timer = Instant::now();
    }

    fn increment_term(&mut self) {
        self.term += 1;
    }

    fn term(&self) -> u64 {
        self.term 
    }

    // TODO: change this for 2C
    fn last_log_index(&self) -> u64 {
        self.commit_index
    }

    // TODO: change this for 2C
    fn last_log_term(&self) -> u64 {
        self.term 
    }

    /// Change state to follower 
    fn become_follower(&mut self, new_term: u64) {
        self.is_leader = false; 
        self.term = new_term;
        self.update_timer();
    }


}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.send_request_vote(0, &Default::default());
        self.persist();
        //let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
        let _ = &self.last_applied;
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
    worker:  CpuPool,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        let node = Node {
            raft: Arc::new(Mutex::new(raft)),
            worker: CpuPool::new_num_cpus(),
        };

        let raft = node.raft.lock().expect("raft unlock failed");
        let me = raft.me;
        std::mem::drop(raft);

        let node_share = node.clone();
        let election_thread = 
            thread::Builder::new().name(format!("election thread for peer {}", me));
        election_thread.spawn(move || {
            node_share.election()
        }).expect("election thread failed to spawn");


        let node_share = node.clone();
        let heartbeat_thread = 
            thread::Builder::new().name(format!("heartbeat thread for peer {}", me));
        heartbeat_thread.spawn(move || {
            node_share.heartbeat()
        }).expect("heartbeat thread failed to spawn");

        node 
    }

    // manage election  
    fn election(&self) { 
        let mut rng = rand::thread_rng();
        let mut timeout_gen = move || -> Duration {
            MILLIS * rng.gen_range(ELECTION_TIMEOUT_LOWER_BOUND, 
                          ELECTION_TIMEOUT_UPPER_BOUND) as u32
        };

        let raft = self.raft.lock().unwrap();
        let peer_count = raft.peers.len(); // #peers 
        let votes_needed = peer_count / 2; 
        let me = raft.me; // id of current server 
        std::mem::drop(raft);

        let mut election_timeout = timeout_gen();
        loop {

            let mut raft = self.raft.lock().unwrap();

            if raft.is_leader {
                std::mem::drop(raft);
                thread::sleep(10 * MILLIS);
                continue; 
            }

            // election times out, start the election 
            if raft.timer.elapsed() >= election_timeout {
                let vote_count = Arc::new(AtomicUsize::new(1));
                let vote_cancelled = Arc::new(AtomicBool::new(false));
                let highest_term = Arc::new(AtomicU64::new(raft.term));
                let mut become_leader = false; 

                election_timeout = timeout_gen();
                raft.increment_term();

                info!("server {} starts election process, new term {}", me, raft.term);

                let request_vote_args = RequestVoteArgs {
                    term: raft.term(),
                    candidate_id: me as u64, 
                    last_log_index: raft.last_log_index(),
                    last_log_term: raft.last_log_term(),
                };

                let mut receivers = vec![];
                for i in 0..peer_count {
                    if i == me { continue; }

                    if vote_cancelled.load(Ordering::Relaxed) {
                        break;
                    }

                    let rx = raft.send_request_vote(i, &request_vote_args);
                    receivers.push(rx);
                }

                let current = raft.timer;
                std::mem::drop(raft);

                // continue to poll responses while election has not timed out 
                while current.elapsed() < election_timeout && !vote_cancelled.load(Ordering::SeqCst) { 
                    for (i, rx) in receivers.iter().enumerate() {
                        match rx.try_recv() {
                            Ok(res) => {
                                match res {
                                    Ok(res) => {
                                        if !res.vote_granted { 
                                            highest_term.store(res.term, Ordering::SeqCst);
                                            vote_cancelled.store(false, Ordering::SeqCst);
                                        } else {
                                            vote_count.fetch_add(1, Ordering::SeqCst); 
                                        }
                                    }
                                    Err(e) => error!("send request vote from server {} to server {}: {:?}", me, i, e),
                                }
                            }
                            Err(_) => {} 
                        }
                    }


                    if !vote_cancelled.load(Ordering::SeqCst) && vote_count.load(Ordering::SeqCst) >= votes_needed {
                        become_leader = true; 
                        break; 
                    } 

                    thread::sleep(1 * MILLIS);
                }

                let mut raft = self.raft.lock().unwrap();
                if become_leader { 
                    // become the leader
                    raft.is_leader = true; 
                    raft.voted_for = (raft.term, raft.me);
                    raft.update_timer();
                    std::mem::drop(raft);

                    // send heartbeats to all followers 
                    // start agreement process  
                    self.start_agreement();
                } else { 
                    // request rejected
                    // update term and become follower 
                    raft.become_follower(highest_term.load(Ordering::SeqCst));
                }
            }

            thread::sleep(1 * MILLIS);
        }
    }

    // managing heartbeats
    // send regular heartbeats to followers to keep the lead 
    fn heartbeat(&self) {
        loop {
            let mut raft = self.raft.lock().unwrap();

            if raft.is_leader {
                if raft.timer.elapsed() > MILLIS * HEART_BEAT_INTERVAL  { 
                    raft.update_timer();
                    std::mem::drop(raft);

                    self.send_heart_beats();
                }
            }

            thread::sleep(1 * MILLIS);
        }

    }

    fn send_heart_beats(&self) {
        let raft = self.raft.lock().unwrap();

        let cur_term = raft.term; 
        let me = raft.me;
        let peers_len = raft.peers.len();

        std::mem::drop(raft);

        let highest_term = Arc::new(AtomicU64::new(cur_term));

        for i in 0..peers_len { 
            if i == me {
                continue; 
            }

            let raft = self.raft.lock().unwrap();
            let peer = raft.peers[i].clone();
            std::mem::drop(raft);

            let highest_term = highest_term.clone();

            self.worker.spawn_fn(move || -> RpcFuture<()> {
                let heart_beat_args = AppendEntriesArgs { 
                    term: cur_term, 
                    leader_id: me as u64, 
                    //prev_log_index: 0, // TODO: placeholder 
                };

                Box::new(peer.append_entries(&heart_beat_args).map(move |rep| {
                    let AppendEntriesReply { term, success } = rep; 


                    if term > cur_term { // become follower 
                        highest_term.store(term, Ordering::SeqCst);
                    } else if !success { // retry 
                    } else { // update pointer to follower's index
                    }

                }))
            }).forget();
        }


        if highest_term.load(Ordering::SeqCst) > cur_term { // become follower 
            let mut raft = self.raft.lock().unwrap();
            raft.become_follower(highest_term.load(Ordering::SeqCst));
        }
    }

    fn start_agreement(&self) {
        
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
        self.raft.lock().unwrap().start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        let raft = self.raft.lock().unwrap();
        raft.term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        let raft = self.raft.lock().unwrap();
        raft.is_leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    fn commit_index(&self) -> u64 {
        let raft = self.raft.lock().unwrap();
        raft.commit_index
    }

    fn voted_for(&self) -> usize {
        let raft = self.raft.lock().unwrap();
        raft.voted_for.1
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
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        // RequestVoteArgs{term, candidateId, lastLogIndex, lastLogTerm}

        let mut raft = self.raft.lock().unwrap();
        raft.update_timer();
        std::mem::drop(raft);

        let reply = |granted| {
            Box::new(futures::future::ok(RequestVoteReply {
                term: self.term(),
                vote_granted: granted,
            }))
        };

        if self.term() > args.term {
            reply(false)
        } else if self.voted_for() as u64 == args.candidate_id
            && self.commit_index() <= args.last_log_index
        {
            reply(true)
        } else {
            reply(false)
        }
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        // TODO: this is not gonna work 
        let mut raft = self.raft.lock().unwrap();
        if raft.term < args.term {
            raft.term = args.term;
        }

        raft.update_timer();
        Box::new(futures::future::ok(AppendEntriesReply {
            term: raft.term,
            success: true 
        }))
    }
}
