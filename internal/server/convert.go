package server

import (
	"errors"
	"fmt"

	"github.com/abishekprabakar/raftkv/internal/pb"
	"github.com/abishekprabakar/raftkv/internal/raft"
)

// The raft package knows nothing about protobuf, which keeps the algorithm free of
// any dependency and lets the simulator run it with no encoding at all. These two
// functions are the only place the two meet. TestMessagesRoundTrip checks that every
// field of every message survives the trip.

var errEmptyEnvelope = errors.New("server: envelope holds no message")

func toEnvelope(from, to raft.NodeID, msg raft.Message) *pb.Envelope {
	env := &pb.Envelope{From: uint64(from), To: uint64(to)}
	switch m := msg.(type) {
	case raft.RequestVoteArgs:
		env.Message = &pb.Envelope_RequestVote{RequestVote: &pb.RequestVoteArgs{
			Term: m.Term, CandidateId: uint64(m.CandidateID), LastLogIndex: m.LastLogIndex, LastLogTerm: m.LastLogTerm,
		}}
	case raft.RequestVoteReply:
		env.Message = &pb.Envelope_RequestVoteReply{RequestVoteReply: &pb.RequestVoteReply{
			Term: m.Term, VoteGranted: m.VoteGranted,
		}}
	case raft.AppendEntriesArgs:
		entries := make([]*pb.Entry, len(m.Entries))
		for i, e := range m.Entries {
			entries[i] = &pb.Entry{Index: e.Index, Term: e.Term, Kind: pb.EntryKind(e.Kind), Data: e.Data}
		}
		env.Message = &pb.Envelope_AppendEntries{AppendEntries: &pb.AppendEntriesArgs{
			Term: m.Term, LeaderId: uint64(m.LeaderID), PrevLogIndex: m.PrevLogIndex, PrevLogTerm: m.PrevLogTerm,
			Entries: entries, LeaderCommit: m.LeaderCommit, ReadSeq: m.ReadSeq,
		}}
	case raft.AppendEntriesReply:
		env.Message = &pb.Envelope_AppendEntriesReply{AppendEntriesReply: &pb.AppendEntriesReply{
			Term: m.Term, Success: m.Success, ReadSeq: m.ReadSeq, MatchIndex: m.MatchIndex,
			ConflictTerm: m.ConflictTerm, ConflictIndex: m.ConflictIndex,
		}}
	case raft.InstallSnapshotArgs:
		env.Message = &pb.Envelope_InstallSnapshot{InstallSnapshot: &pb.InstallSnapshotArgs{
			Term: m.Term, LeaderId: uint64(m.LeaderID), LastIncludedIndex: m.LastIncludedIndex,
			LastIncludedTerm: m.LastIncludedTerm, Data: m.Data, ReadSeq: m.ReadSeq,
		}}
	case raft.InstallSnapshotReply:
		env.Message = &pb.Envelope_InstallSnapshotReply{InstallSnapshotReply: &pb.InstallSnapshotReply{
			Term: m.Term, MatchIndex: m.MatchIndex, ReadSeq: m.ReadSeq,
		}}
	default:
		// Only this package's own code calls toEnvelope, with what a Node sent, so
		// an unknown type is a programming error, not bad input.
		panic(fmt.Sprintf("server: no encoding for %T", msg))
	}
	return env
}

func fromEnvelope(env *pb.Envelope) (raft.Message, error) {
	switch m := env.Message.(type) {
	case *pb.Envelope_RequestVote:
		r := m.RequestVote
		return raft.RequestVoteArgs{
			Term: r.Term, CandidateID: raft.NodeID(r.CandidateId), LastLogIndex: r.LastLogIndex, LastLogTerm: r.LastLogTerm,
		}, nil
	case *pb.Envelope_RequestVoteReply:
		r := m.RequestVoteReply
		return raft.RequestVoteReply{Term: r.Term, VoteGranted: r.VoteGranted}, nil
	case *pb.Envelope_AppendEntries:
		r := m.AppendEntries
		var entries []raft.Entry
		if len(r.Entries) > 0 {
			entries = make([]raft.Entry, len(r.Entries))
			for i, e := range r.Entries {
				entries[i] = raft.Entry{Index: e.Index, Term: e.Term, Kind: raft.EntryKind(e.Kind), Data: e.Data}
			}
		}
		return raft.AppendEntriesArgs{
			Term: r.Term, LeaderID: raft.NodeID(r.LeaderId), PrevLogIndex: r.PrevLogIndex, PrevLogTerm: r.PrevLogTerm,
			Entries: entries, LeaderCommit: r.LeaderCommit, ReadSeq: r.ReadSeq,
		}, nil
	case *pb.Envelope_AppendEntriesReply:
		r := m.AppendEntriesReply
		return raft.AppendEntriesReply{
			Term: r.Term, Success: r.Success, ReadSeq: r.ReadSeq, MatchIndex: r.MatchIndex,
			ConflictTerm: r.ConflictTerm, ConflictIndex: r.ConflictIndex,
		}, nil
	case *pb.Envelope_InstallSnapshot:
		r := m.InstallSnapshot
		return raft.InstallSnapshotArgs{
			Term: r.Term, LeaderID: raft.NodeID(r.LeaderId), LastIncludedIndex: r.LastIncludedIndex,
			LastIncludedTerm: r.LastIncludedTerm, Data: r.Data, ReadSeq: r.ReadSeq,
		}, nil
	case *pb.Envelope_InstallSnapshotReply:
		r := m.InstallSnapshotReply
		return raft.InstallSnapshotReply{Term: r.Term, MatchIndex: r.MatchIndex, ReadSeq: r.ReadSeq}, nil
	}
	// This one is input from the network, so it is an error, not a panic.
	return nil, errEmptyEnvelope
}
