package raft

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// Estados //
const (
	StateFollower = iota
	StateCandidate
	StateLeader
)

// Tempos de eleicao //
const (
	DefaultElectionTimeoutMin   = 150
	DefaultElectionTimeoutMax   = 300
	DefaultElectionTimeoutRange = DefaultElectionTimeoutMax - DefaultElectionTimeoutMin
	DefaultHeartbeatInterval    = 50
	DefaultChannelBufferSize    = 20
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int

	state              int
	votes              int
	requestVoteReplied chan bool
	winner             chan bool
	appendEntriesRec   chan bool
	commandApplied     chan ApplyMsg

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry // vazio para heartbeat
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int // se houver erro, avisar lider do index do conflito
	ConflictTerm  int // se houver erro, avisar o lider o termo no log do ConflictIndex
}

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool

	term = rf.currentTerm
	isLeader = rf.state == StateLeader

	return term, isLeader
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

func (rf *Raft) GetLastEntryIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) GetLastEntryTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// Retornar falso quando o termo for menor do que o termo do atual //
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// Caso a requisicao ou resposta RPC conter um termo maior que o termo atual  //
	// entao setar o currentTerm = T e converter para seguidor                    //
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = StateFollower
		rf.votedFor = -1 // new term, reset votedFor
	}

	// Se votedFor eh null ou igual ao CandidateId,                   //
	// e o log do candidato eh pelo menos igual ao log do recebedor   //
	// entao dar o voto                                               //
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.GetLastEntryTerm() ||
			args.LastLogTerm == rf.GetLastEntryTerm() && args.LastLogIndex >= rf.GetLastEntryIndex()) {
		rf.state = StateFollower
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		rf.requestVoteReplied <- true
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		// Caso a requisicao ou resposta RPC tem o termo maior que o termo atual //
		// entao setar currentTerm = T, e converter para seguidor                //
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = StateFollower
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		if rf.state == StateCandidate {
			if reply.VoteGranted {
				rf.votes++
				if rf.votes > (len(rf.peers) / 2) {
					rf.winner <- true
				}
			}
		}

		return ok
	}
	return ok
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// Retornar falso se o termo for menor que o termo atual //
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.appendEntriesRec <- true
	// Se a requisicao ou a resposta RPC contem um termo maior que o currentTerm, entao //
	// atualizar o termo e virar seguidor                                              //
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = StateFollower
		rf.votedFor = -1 // resetar pois tem novo termo //
	}
	reply.Term = args.Term
	// Retorna falso se o log nao contem uma entrada prevLogIndex que bate com o termo //
	if rf.GetLastEntryIndex() < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false

		// Se o seguidor nao tem um prevLogIndex no log //
		if rf.GetLastEntryIndex() < args.PrevLogIndex {
			reply.ConflictIndex = len(rf.log)
			reply.ConflictTerm = -1 // sem entradas, entao é invalido //
		} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {

			// Se tiver o mesmo index mas nao o mesmo termo //
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

			// Procurar pelo primeiro index com o mesmo termo conflictTerm //
			for i := args.PrevLogIndex - 1; i > 0; i-- {
				if rf.log[args.PrevLogIndex].Term != rf.log[i].Term {
					reply.ConflictIndex = i + 1
					break
				}
			}
		}
		return
	}

	// Caso uma entrada existente conflite com uma nova de mesmo index e termos diferentes,  //
	// entao deletar a entrada existente e todas que subsequentes                            //
	var i int
	for i = args.PrevLogIndex + 1; i <= rf.GetLastEntryIndex() && i-args.PrevLogIndex-1 < len(args.Entries); i++ {
		if rf.log[i].Term != args.Entries[i-args.PrevLogIndex-1].Term {
			rf.log = rf.log[:i]
			break
		}
	}
	rf.log = append(rf.log, args.Entries[i-args.PrevLogIndex-1:]...)
	rf.persist()
	// Caso LeaderCommit > commitIndex, entao setar commitIndex como min(LeaderCommit, index da ultima entrada) //
	if args.LeaderCommit > rf.commitIndex {

		if args.LeaderCommit < rf.GetLastEntryIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.GetLastEntryIndex()
		}

		for i = rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msg := ApplyMsg{
				Index:   i,
				Command: rf.log[i].Command,
			}
			rf.commandApplied <- msg
		}
		rf.lastApplied = rf.commitIndex
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		// Se a requisicao ou a resposta RPC contem um termo maior que o currentTerm, entao //
		// atualizar o termo e virar seguidor                                              //
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = StateFollower
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		if rf.state == StateLeader {
			if reply.Success {
				// Atualizar o nextIndex e matchIndex //
				rf.nextIndex[server] = rf.GetLastEntryIndex() + 1
				rf.matchIndex[server] = rf.GetLastEntryIndex()
				// Se existir um N tal que N > commitIndex //
				for N := rf.commitIndex + 1; N < len(rf.log); N++ {
					count := 1
					foundN := false
					for _, v := range rf.matchIndex {
						if v >= N {
							count++
						}
						if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
							rf.commitIndex = N
							foundN = true
							break
						}
					}
					if foundN {
						break
					}
				}
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					msg := ApplyMsg{
						Index:   i,
						Command: rf.log[i].Command,
					}
					rf.commandApplied <- msg
				}
				rf.lastApplied = rf.commitIndex
			} else {
				// Inconsistencia no log //
				if reply.ConflictTerm == -1 {
					rf.nextIndex[server] = reply.ConflictIndex
				} else {
					var i int
					for i = len(rf.log) - 1; i >= 0; i-- {
						if rf.log[i].Term == reply.ConflictTerm {
							rf.nextIndex[server] = i + 1
							break
						}
					}
					if i == -1 {
						rf.nextIndex[server] = reply.ConflictIndex
					}
				}
			}
		}
	}
	return ok
}

func (rf *Raft) doAsFollower() {
	// Tempo de eleicao randomizado //
	electionTimeout := rand.Intn(DefaultElectionTimeoutRange) + DefaultElectionTimeoutMin

	select {
	case <-time.After(time.Duration(electionTimeout) * time.Millisecond):
		// Qualquer msg que nao chegou e timeout do tempo de eleicao -- virar candidato //
		rf.state = StateCandidate
	case <-rf.requestVoteReplied:
		// Servidor votou em outro servidor //
	case <-rf.appendEntriesRec:
		// Lider esta vivo //
	}
}

func (rf *Raft) doAsCandidate() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votes = 1
	rf.persist()
	rf.mu.Unlock()
	// Enviar RPCs de RequestVote para todos os outros servidores //
	go func() {
		var args RequestVoteArgs
		rf.mu.Lock()
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastLogTerm = rf.GetLastEntryTerm()
		args.LastLogIndex = rf.GetLastEntryIndex()
		rf.mu.Unlock()
		for i := range rf.peers {
			if i != rf.me {
				var reply RequestVoteReply
				go rf.sendRequestVote(i, args, &reply)
			}
		}
	}()
	electionTimeout := rand.Intn(DefaultElectionTimeoutRange) + DefaultElectionTimeoutMin
	select {
	case <-rf.winner:
		// Caso receba a maioria dos votos //
		rf.mu.Lock()
		rf.state = StateLeader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.GetLastEntryIndex() + 1
			rf.matchIndex[i] = 0
		}
		rf.mu.Unlock()
	case <-rf.appendEntriesRec:
		// Se RPC receber do novo lider entao converter para seguidor //
		rf.state = StateFollower
	case <-time.After(time.Duration(electionTimeout) * time.Millisecond):
		// Se o tempo de eleicao termina, entao comecar nova eleicao //
	}
}

func (rf *Raft) doAsLeader() {
	// Enviar AppendEntries periodicamente //
	for i := range rf.peers {
		if i != rf.me {
			var args AppendEntriesArgs
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Entries = make([]LogEntry, len(rf.log[rf.nextIndex[i]:]))
			copy(args.Entries, rf.log[rf.nextIndex[i]:])
			args.LeaderCommit = rf.commitIndex

			var reply AppendEntriesReply
			go rf.sendAppendEntries(i, args, &reply)
		}
	}
	// Enviar heartbeats periodicamente //
	time.Sleep(DefaultHeartbeatInterval * time.Millisecond)
}

func (rf *Raft) stateLoop() {
	for {
		switch rf.state {
		case StateFollower:
			rf.doAsFollower()
		case StateCandidate:
			rf.doAsCandidate()
		case StateLeader:
			rf.doAsLeader()
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := rf.state == StateLeader
	if isLeader {
		rf.mu.Lock()
		// Anexar entrada ao log local //
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		rf.persist()
		index = rf.GetLastEntryIndex()
		term = rf.currentTerm
		rf.mu.Unlock()
	}
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = StateFollower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.votes = 0
	rf.appendEntriesRec = make(chan bool, DefaultChannelBufferSize)
	rf.requestVoteReplied = make(chan bool, DefaultChannelBufferSize)
	rf.winner = make(chan bool, DefaultChannelBufferSize)
	rf.commandApplied = applyCh

	rf.log = []LogEntry{{Term: 0, Command: -1}}
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Corrigir tempo para windows //
	rand.Seed(time.Now().UnixNano() + int64(rf.me))
	go rf.stateLoop()

	// Inicializar a partir do estado persistente antes de uma falha  //
	rf.readPersist(persister.ReadRaftState())

	return rf
}
