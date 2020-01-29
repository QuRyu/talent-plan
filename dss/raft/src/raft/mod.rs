use std::sync::mpsc::{sync_channel, SyncSender, Sender, channel};
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

//struct LogEntry {
//term: u64,
//index: u64,
//command: Vec<u8>,
//}

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

    log_entries: Vec<LogEntry>,
    commit_index: u64,
    last_applied: u64,
    log_index: u64,
    //last_included_index: u64,
    next_index: Vec<u64>,  // index of the next log entry to send to each server
    match_index: Vec<u64>, // index of highest log entry known to be replicated on server
    apply_ch: UnboundedSender<ApplyMsg>,
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
        let empty_entry: LogEntry = LogEntry {
            term: 0,
            index: 0,
            command: vec![],
        };

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

            log_entries: vec![empty_entry],
            commit_index: 0,
            last_applied: 0,
            log_index: 1,
            next_index: Default::default(),
            match_index: Default::default(),
            apply_ch,
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
        let peer = &self.peers[server];
        let me = self.me;
        peer.spawn(
            peer.request_vote(&args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    match sender.send(res) {
                        Ok(_) => {}
                        Err(e) => debug!("server {}: request vote sender error {:?}", me, e),
                    };
                    Ok(())
                }),
        );
    }

    /// clear timer
    fn reset_timer(&mut self) {
        self.timer = Instant::now();
    }

    /// set election timeout to a new random value between preset lower and upper bound
    fn reset_election_timeout(&mut self) {
        let timeout = rand::thread_rng()
            .gen_range(ELECTION_TIMEOUT_LOWER_BOUND, ELECTION_TIMEOUT_UPPER_BOUND);
        self.election_timeout = timeout * MILLIS;
    }

    /// test if Raft should start a new election
    fn pass_election_timeout(&self) -> bool {
        self.timer.elapsed() > self.election_timeout
    }

    /// test if Raft should send heartbeats
    fn pass_heartbeat_timeout(&self) -> bool {
        self.timer.elapsed() > self.heartbeat_timeout
    }

    fn is_leader(&self) -> bool {
        self.role == Role::Leader
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

    /// Test if `index` has been replicated on majority of servers
    fn can_commit(&self, index: u64) -> bool {
        if index < self.log_index
            && self.commit_index < index
            && self.log_entries[index as usize].term == self.term
        {
            // leader only commits entries for its own term
            let majority = self.peers.len() / 2 + 1;
            let mut rep_count = 1;

            for server_idx in self.match_index.iter() {
                if *server_idx >= index {
                    rep_count += 1;
                }
            }

            rep_count >= majority
        } else {
            false
        }
    }

    /// return log entries specified by [start, end)
    fn make_entries(&self, start: u64, end: u64) -> Vec<LogEntry> {
        let (start, end) = (start as usize, end as usize);
        let mut entries = Vec::with_capacity(end - start);

        for i in start..end {
            entries.push(self.log_entries[i].clone());
        }

        entries
    }

    /// returns last log entry in Raft
    fn last_log_entry(&self) -> &LogEntry {
        &self.log_entries[self.log_index as usize - 1]
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        //let _ = self.start(&0);
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
    send_ch: Sender<()>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        let (tx, rx) = channel();

        let n = Node {
            raft: Arc::new(Mutex::new(raft)),
            send_ch: tx,
        };

        // thread handling election
        let node = n.clone();
        thread::Builder::new()
            .spawn(move || {
                loop {
                    let raft = node.raft.lock().unwrap();

                    match raft.role {
                        Role::Follower => {
                            if raft.pass_election_timeout() {
                                let node = node.clone();
                                thread::Builder::new()
                                    .spawn(move || {
                                        node.campaign(); // start election
                                    })
                                    .expect("fail to spawn campaign thread");
                            }
                        }

                        Role::Leader => {
                            if raft.pass_heartbeat_timeout() {
                                let node = node.clone();
                                thread::Builder::new()
                                    .spawn(move || {
                                        node.replicate(); // trick the node to send empty logs
                                    })
                                    .expect("fail to spawn replicate thread");
                            }
                        }

                        Role::Candidate => {}
                    }
                    std::mem::drop(raft);
                    thread::sleep(5 * MILLIS);
                }
            })
            .expect("failed to spawn election thread");

        let node = n.clone();
        thread::Builder::new().spawn(move || {
            loop {
                match rx.recv() {
                    Ok(_) => node.apply_logs(),
                    Err(e) => info!("{:?}", e),
                }
            }
        }).expect("fail to spawn apply_logs thread");



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
        let mut raft = self.raft.lock().unwrap();

        let me = raft.me;
        let index = raft.log_index;
        let term = raft.term;
        let is_leader = raft.is_leader();

        if is_leader {
            let mut buf = vec![];
            labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
            let entry = LogEntry {
                term,
                index,
                command: buf,
            };

            raft.log_entries.push(entry);

            raft.match_index[me] = raft.log_index;
            raft.log_index += 1;

            std::mem::drop(raft);

            info!("Server {} start entry {:?} at index {}", me, command, index);

            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
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
        let raft = self.raft.lock().unwrap();
        State {
            term: raft.term,
            is_leader: raft.is_leader(),
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
        let raft = self.raft.lock().unwrap();
        error!("server {} killed", raft.me);
    }

    /// Start a new term and request votes from peers
    /// if the node believes the primary has died
    fn campaign(&self) {
        let mut raft = self.raft.lock().unwrap();

        if raft.role == Role::Leader {
            return;
        }

        let mut vote_count = 1;
        let (tx, rx) = sync_channel(raft.peers.len() - 1);
        let votes_needed = raft.peers.len() / 2 + 1;
        let me = raft.me;

        raft.term += 1;
        raft.role = Role::Candidate;
        raft.leader_id = None;
        raft.voted_for = Some(raft.me as u64);
        info!("server {}: campaign for votes, term {}", me, raft.term);

        raft.persist();

        let last_log = raft.last_log_entry();
        let args = RequestVoteArgs {
            term: raft.term,
            candidate_id: raft.me as u64,
            // TODO: change these two
            last_log_index: last_log.index,
            last_log_term: last_log.term,
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

        debug!(
            "server {} finished sending vote requests, term {}",
            me, current_term
        );

        while now.elapsed() < timeout && vote_count < votes_needed {
            if let Ok(resp) = rx.try_recv() {
                match resp {
                    Ok(RequestVoteReply { term, vote_granted }) => {
                        if vote_granted {
                            debug!(
                                "server {}: received vote reply for term {}, granted {}",
                                me, current_term, vote_granted
                            );
                            vote_count += 1;
                        } else if term > current_term {
                            debug!(
                                "server {}: vote reply rejected by higher term {}, current term {}",
                                me, term, current_term
                            );
                            let mut raft = self.raft.lock().unwrap();
                            raft.become_follower(term);
                            return;
                        }
                    }

                    Err(e) => {
                        debug!("server {}: RPC error: {:?}", me, e);
                    }
                }
            }
        }

        let mut raft = self.raft.lock().unwrap();
        if vote_count >= votes_needed && raft.role == Role::Candidate {
            info!(
                "server {}: receive enough votes ({}) to become leader for term {}",
                me, vote_count, raft.term
            );
            let n = raft.peers.len();
            let me = raft.me;

            raft.role = Role::Leader;
            raft.leader_id = Some(raft.me as u64);
            raft.next_index = vec![raft.log_index; n];
            raft.match_index = vec![0; n];
            raft.match_index[me] = raft.log_index;

            raft.reset_election_timeout();
            raft.reset_timer();

            let node = self.clone();
            thread::spawn(move || {
                node.replicate();
            });
        } else {
            // another server has established itself as the leader
            info!(
                "server {}: campaign failed, vote count {}, term {}",
                me, vote_count, current_term
            );
            raft.role = Role::Follower; // trigger another round of campaign if network partitions
            raft.reset_election_timeout();
            raft.reset_timer();
        }
    }

    /// Send logs to peers
    fn replicate(&self) {
        // just send empty logs here
        let mut raft = self.raft.lock().unwrap();

        if !raft.is_leader() {
            return;
        }

        debug!(
            "server {} replicate entries for term {}, log entry length {}",
            raft.me, raft.term, raft.log_entries.len()
        );

        for i in 0..raft.peers.len() {
            if i == raft.me {
                continue;
            }

            //let tx = tx.clone();
            //raft.append_log_entries(i, tx, &args);

            let node = self.clone();
            thread::Builder::new()
                .spawn(move || {
                    node.append_log_entries(i);
                })
                .expect("fail to spawn replicate");
        }

        raft.reset_timer();
    }

    fn append_log_entries(&self, server: usize) {
        let raft = self.raft.lock().unwrap();

        let me = raft.me;
        let peer = raft.peers[server].clone();
        let prev_log_index = raft.next_index[server] - 1;
        let prev_log_term = raft.log_entries[prev_log_index as usize].term;

        let (entries, len) = if raft.log_index > raft.next_index[server] {
            let entries = raft.make_entries(raft.next_index[server], raft.log_index);
            let len = entries.len() as u64;
            (entries, len)
        } else {
            (vec![], 0)
        };
        //println!("Server {}: raft leader log index {}, server {}, next_index {}, send entries of length {}", me, raft.log_index, server, raft.next_index[server], entries.len());

        let args = AppendEntriesArgs {
            term: raft.term,
            leader_id: raft.me as u64,
            prev_log_index,
            prev_log_term,
            leader_commit: raft.commit_index,
            entries,
        };

        std::mem::drop(raft);

        let (tx, rx) = sync_channel(1);
        peer.spawn(
            peer.append_entries(&args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    tx.send(res)
                        .expect("append_log_entries tx should not panic");
                    Ok(())
                }),
        );

        match rx.recv() {
            Err(_) => {}

            Ok(res) => {
                match res {
                    Err(e) => error!("server {}: append_log_entries error, {:?}", me, e),

                    Ok(AppendEntriesReply {
                        term,
                        success,
                        conflict_index,
                    }) => {
                        let mut raft = self.raft.lock().unwrap();
                        if success {
                            if prev_log_index + len > raft.next_index[server] {
                                // update only if reply arrives in order
                                raft.next_index[server] = prev_log_index + len + 1;
                                raft.match_index[server] = prev_log_index + len;
                            }

                            let commit_index = prev_log_index + len;
                            if raft.can_commit(commit_index) {
                                info!("server {}: commit entries up to {}", raft.me, commit_index);
                                raft.commit_index = commit_index;
                                raft.persist();

                                std::mem::drop(raft);
                                self.send_ch.send(()).unwrap();
                            }
                        } else if raft.term < term {
                            // is no longer the leader
                            raft.become_follower(term);
                        } else {
                            // follower is inconsistent
                            raft.next_index[server] = 1.max(raft.log_index.min(conflict_index));
                            // instead of sending the new logs immediately, simplify the design
                            // by waiting till the next AppendEntries RPC
                        }
                    }
                }
            }
        }
    }

    /// given that there are logs safely replicated on majority of servers,
    /// apply logs to state machine
    fn apply_logs(&self) {
        let mut raft = self.raft.lock().unwrap();

        let command_valid = true;
        let sender = raft.apply_ch.clone();
        let me = raft.me;
        info!("server {}: apply logs of range {}..{},", raft.me, raft.last_applied+1, raft.log_index);

        let entries = raft.make_entries(raft.last_applied + 1, raft.log_index);

        raft.last_applied = raft.commit_index;
        raft.persist();
        std::mem::drop(raft);

        for entry in entries {
            let msg = ApplyMsg {
                command_valid,
                command: entry.command,
                command_index: entry.index,
            };

            match sender.unbounded_send(msg) {
                Ok(_) => debug!(
                    "Server {}: sent log at index {}, command validity {}",
                    me, entry.index, command_valid
                ),
                Err(e) => error!(
                    "Server {}: failed to send log at index {}, error: {:?}",
                    me, entry.index, e
                ),
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
        } else if raft.term > args.term || raft.term == args.term && raft.voted_for.is_some() {
            reply.vote_granted = false;
        } else if raft.term < args.term {
            // log check
            let last_entry = raft.last_log_entry();

            if last_entry.term < args.last_log_term
                || (last_entry.term == args.last_log_term
                    && last_entry.index <= args.last_log_index)
            {
                raft.role = Role::Follower;
                raft.leader_id = None;
                raft.voted_for = Some(args.candidate_id);
                raft.term = args.term;
                reply.vote_granted = true;
                reply.term = args.term;

                raft.persist()
            }
        }

        raft.voted_for = Some(args.candidate_id);

        raft.reset_timer();
        std::mem::drop(raft);

        info!(
            "server {} received vote request from server {} for term {}, granted {}",
            me, args.candidate_id, args.term, reply.vote_granted
        );

        Box::new(future::ok(reply))
    }

    fn append_entries(&self, mut args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        let mut raft = self.raft.lock().unwrap();
        let mut reply = AppendEntriesReply {
            term: raft.term,
            success: false,
            conflict_index: 0,
        };

        debug!(
            "server {}: received append_entries from leader {}",
            raft.me, args.leader_id
        );
        raft.reset_timer();
        if raft.term <= args.term {
            reply.success = true;
            raft.leader_id = Some(args.leader_id);
            raft.role = Role::Follower;

            if raft.term < args.term {
                raft.term = args.term;
                raft.voted_for = None;
            }

            let arg_prev_log_index = args.prev_log_index as usize;
            if raft.log_index <= args.prev_log_index
                || raft.log_entries[arg_prev_log_index].term != args.prev_log_term
            {
                // leader and follower disagree on log entries
                let mut conflict_index = (raft.log_index - 1).min(args.prev_log_index); 
                let conflict_term = raft.log_entries[conflict_index as usize].term;

                while conflict_index > raft.commit_index && raft.log_entries[conflict_index as usize].term == conflict_term {
                    conflict_index -= 1;
                }

                reply.conflict_index = conflict_index as u64;
            } else {
                let drop_index = raft.log_index.min(args.prev_log_index + 1) as usize;
                let args_entries_len = args.entries.len();
                raft.log_entries.truncate(drop_index);
                raft.log_entries.append(&mut args.entries);

                raft.log_index = (drop_index + args_entries_len) as u64;
                raft.persist();

                if args.leader_commit > raft.commit_index {
                    raft.commit_index = args.leader_commit;
                    self.send_ch.send(()).unwrap();

                }
                //if raft.log_entries.len() > 1 {
                    //println!("log entry {:?}", raft.log_entries[1]);
                //}
                //println!("Server {}: drop index {}, appending entries of length {}, prev_log_index {}, log_index {}, log entries len {}", raft.me, drop_index, args_entries_len, args.prev_log_index, raft.log_index, raft.log_entries.len());


                reply.success = true;
            }
        }

        std::mem::drop(raft);
        Box::new(future::ok(reply))
    }
}
