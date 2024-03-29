package kafka

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"net"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type Server struct {
	handler RequestHandler
	logger  zerolog.Logger
}

func NewServer(handler RequestHandler, logger zerolog.Logger) *Server {
	return &Server{
		handler: handler,
		logger:  logger.With().Str("component", "kafka-server").Logger(),
	}
}

func (s *Server) Run(ctx context.Context, addr string) error {
	s.logger.Info().Str("addr", addr).Msg("starting kafka server")
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return err
	}
	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	s.logger.Info().Str("addr", addr).Msg("server running")
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			s.logger.Err(err).Msg("listener accept error")
			continue
		}

		s.logger.Info().Str("remoteAddr", conn.RemoteAddr().String()).Msg("accepted connection")
		go s.handleConn(ctx, conn)
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		// Read request
		sizeRaw, err := reader.Peek(4)
		if err != nil {
			if err != io.EOF {
				s.logger.Err(err).Msg("conn read error")
			}
			return
		}

		size := int32(binary.BigEndian.Uint32(sizeRaw))
		buf := make([]byte, size+4)

		_, err = io.ReadFull(reader, buf)
		if err != nil {
			s.logger.Err(err).Msg("conn read error")
			return
		}

		s.logger.Debug().
			Str("remoteAddr", conn.RemoteAddr().String()).
			Int32("size", size).
			Msg("received request")
		// fmt.Printf("\n%s\n", hex.Dump(buf))

		// Decode request
		var req Request
		err = req.Decode(buf)
		if err != nil {
			s.logger.Err(err).Msg("request decode error")
			return
		}

		// Handle request
		resp, err := s.handleRequest(ctx, req)
		if err != nil {
			s.logger.Err(err).Msg("handle request error")
			return
		}

		// Encode response
		_, err = resp.Encode(conn)
		if err != nil {
			s.logger.Err(err).Msg("response encode error")
			return
		}
	}
}

func (s *Server) handleRequest(ctx context.Context, req Request) (Response, error) {
	resp := Response{CorrelationID: req.Header.CorrelationID}
	switch kmsgReq := req.Request.(type) {
	case *kmsg.ProduceRequest:
		resp.Body = s.handler.Produce(ctx, kmsgReq)
	case *kmsg.FetchRequest:
		resp.Body = s.handler.Fetch(ctx, kmsgReq)
	case *kmsg.ListOffsetsRequest:
		resp.Body = s.handler.ListOffsets(ctx, kmsgReq)
	case *kmsg.MetadataRequest:
		resp.Body = s.handler.Metadata(ctx, kmsgReq)
	case *kmsg.LeaderAndISRRequest:
		resp.Body = s.handler.LeaderAndISR(ctx, kmsgReq)
	case *kmsg.StopReplicaRequest:
		resp.Body = s.handler.StopReplica(ctx, kmsgReq)
	case *kmsg.UpdateMetadataRequest:
		resp.Body = s.handler.UpdateMetadata(ctx, kmsgReq)
	case *kmsg.ControlledShutdownRequest:
		resp.Body = s.handler.ControlledShutdown(ctx, kmsgReq)
	case *kmsg.OffsetCommitRequest:
		resp.Body = s.handler.OffsetCommit(ctx, kmsgReq)
	case *kmsg.OffsetFetchRequest:
		resp.Body = s.handler.OffsetFetch(ctx, kmsgReq)
	case *kmsg.FindCoordinatorRequest:
		resp.Body = s.handler.FindCoordinator(ctx, kmsgReq)
	case *kmsg.JoinGroupRequest:
		resp.Body = s.handler.JoinGroup(ctx, kmsgReq)
	case *kmsg.HeartbeatRequest:
		resp.Body = s.handler.Heartbeat(ctx, kmsgReq)
	case *kmsg.LeaveGroupRequest:
		resp.Body = s.handler.LeaveGroup(ctx, kmsgReq)
	case *kmsg.SyncGroupRequest:
		resp.Body = s.handler.SyncGroup(ctx, kmsgReq)
	case *kmsg.DescribeGroupsRequest:
		resp.Body = s.handler.DescribeGroups(ctx, kmsgReq)
	case *kmsg.ListGroupsRequest:
		resp.Body = s.handler.ListGroups(ctx, kmsgReq)
	case *kmsg.SASLHandshakeRequest:
		resp.Body = s.handler.SASLHandshake(ctx, kmsgReq)
	case *kmsg.ApiVersionsRequest:
		resp.Body = s.handler.ApiVersions(ctx, kmsgReq)
	case *kmsg.CreateTopicsRequest:
		resp.Body = s.handler.CreateTopics(ctx, kmsgReq)
	case *kmsg.DeleteTopicsRequest:
		resp.Body = s.handler.DeleteTopics(ctx, kmsgReq)
	case *kmsg.DeleteRecordsRequest:
		resp.Body = s.handler.DeleteRecords(ctx, kmsgReq)
	case *kmsg.InitProducerIDRequest:
		resp.Body = s.handler.InitProducerID(ctx, kmsgReq)
	case *kmsg.OffsetForLeaderEpochRequest:
		resp.Body = s.handler.OffsetForLeaderEpoch(ctx, kmsgReq)
	case *kmsg.AddPartitionsToTxnRequest:
		resp.Body = s.handler.AddPartitionsToTxn(ctx, kmsgReq)
	case *kmsg.AddOffsetsToTxnRequest:
		resp.Body = s.handler.AddOffsetsToTxn(ctx, kmsgReq)
	case *kmsg.EndTxnRequest:
		resp.Body = s.handler.EndTxn(ctx, kmsgReq)
	case *kmsg.WriteTxnMarkersRequest:
		resp.Body = s.handler.WriteTxnMarkers(ctx, kmsgReq)
	case *kmsg.TxnOffsetCommitRequest:
		resp.Body = s.handler.TxnOffsetCommit(ctx, kmsgReq)
	case *kmsg.DescribeACLsRequest:
		resp.Body = s.handler.DescribeACLs(ctx, kmsgReq)
	case *kmsg.CreateACLsRequest:
		resp.Body = s.handler.CreateACLs(ctx, kmsgReq)
	case *kmsg.DeleteACLsRequest:
		resp.Body = s.handler.DeleteACLs(ctx, kmsgReq)
	case *kmsg.DescribeConfigsRequest:
		resp.Body = s.handler.DescribeConfigs(ctx, kmsgReq)
	case *kmsg.AlterConfigsRequest:
		resp.Body = s.handler.AlterConfigs(ctx, kmsgReq)
	case *kmsg.AlterReplicaLogDirsRequest:
		resp.Body = s.handler.AlterReplicaLogDirs(ctx, kmsgReq)
	case *kmsg.DescribeLogDirsRequest:
		resp.Body = s.handler.DescribeLogDirs(ctx, kmsgReq)
	case *kmsg.SASLAuthenticateRequest:
		resp.Body = s.handler.SASLAuthenticate(ctx, kmsgReq)
	case *kmsg.CreatePartitionsRequest:
		resp.Body = s.handler.CreatePartitions(ctx, kmsgReq)
	case *kmsg.CreateDelegationTokenRequest:
		resp.Body = s.handler.CreateDelegationToken(ctx, kmsgReq)
	case *kmsg.RenewDelegationTokenRequest:
		resp.Body = s.handler.RenewDelegationToken(ctx, kmsgReq)
	case *kmsg.ExpireDelegationTokenRequest:
		resp.Body = s.handler.ExpireDelegationToken(ctx, kmsgReq)
	case *kmsg.DescribeDelegationTokenRequest:
		resp.Body = s.handler.DescribeDelegationToken(ctx, kmsgReq)
	case *kmsg.DeleteGroupsRequest:
		resp.Body = s.handler.DeleteGroups(ctx, kmsgReq)
	case *kmsg.ElectLeadersRequest:
		resp.Body = s.handler.ElectLeaders(ctx, kmsgReq)
	case *kmsg.IncrementalAlterConfigsRequest:
		resp.Body = s.handler.IncrementalAlterConfigs(ctx, kmsgReq)
	case *kmsg.AlterPartitionAssignmentsRequest:
		resp.Body = s.handler.AlterPartitionAssignments(ctx, kmsgReq)
	case *kmsg.ListPartitionReassignmentsRequest:
		resp.Body = s.handler.ListPartitionReassignments(ctx, kmsgReq)
	case *kmsg.OffsetDeleteRequest:
		resp.Body = s.handler.OffsetDelete(ctx, kmsgReq)
	case *kmsg.DescribeClientQuotasRequest:
		resp.Body = s.handler.DescribeClientQuotas(ctx, kmsgReq)
	case *kmsg.AlterClientQuotasRequest:
		resp.Body = s.handler.AlterClientQuotas(ctx, kmsgReq)
	case *kmsg.DescribeUserSCRAMCredentialsRequest:
		resp.Body = s.handler.DescribeUserSCRAMCredentials(ctx, kmsgReq)
	case *kmsg.AlterUserSCRAMCredentialsRequest:
		resp.Body = s.handler.AlterUserSCRAMCredentials(ctx, kmsgReq)
	case *kmsg.VoteRequest:
		resp.Body = s.handler.Vote(ctx, kmsgReq)
	case *kmsg.BeginQuorumEpochRequest:
		resp.Body = s.handler.BeginQuorumEpoch(ctx, kmsgReq)
	case *kmsg.EndQuorumEpochRequest:
		resp.Body = s.handler.EndQuorumEpoch(ctx, kmsgReq)
	case *kmsg.DescribeQuorumRequest:
		resp.Body = s.handler.DescribeQuorum(ctx, kmsgReq)
	case *kmsg.AlterPartitionRequest:
		resp.Body = s.handler.AlterPartition(ctx, kmsgReq)
	case *kmsg.UpdateFeaturesRequest:
		resp.Body = s.handler.UpdateFeatures(ctx, kmsgReq)
	case *kmsg.EnvelopeRequest:
		resp.Body = s.handler.Envelope(ctx, kmsgReq)
	case *kmsg.FetchSnapshotRequest:
		resp.Body = s.handler.FetchSnapshot(ctx, kmsgReq)
	case *kmsg.DescribeClusterRequest:
		resp.Body = s.handler.DescribeCluster(ctx, kmsgReq)
	case *kmsg.DescribeProducersRequest:
		resp.Body = s.handler.DescribeProducers(ctx, kmsgReq)
	case *kmsg.BrokerRegistrationRequest:
		resp.Body = s.handler.BrokerRegistration(ctx, kmsgReq)
	case *kmsg.BrokerHeartbeatRequest:
		resp.Body = s.handler.BrokerHeartbeat(ctx, kmsgReq)
	case *kmsg.UnregisterBrokerRequest:
		resp.Body = s.handler.UnregisterBroker(ctx, kmsgReq)
	case *kmsg.DescribeTransactionsRequest:
		resp.Body = s.handler.DescribeTransactions(ctx, kmsgReq)
	case *kmsg.ListTransactionsRequest:
		resp.Body = s.handler.ListTransactions(ctx, kmsgReq)
	case *kmsg.AllocateProducerIDsRequest:
		resp.Body = s.handler.AllocateProducerIDs(ctx, kmsgReq)
	default:
		return Response{}, kerr.InvalidRequest
	}
	resp.Body.SetVersion(req.Header.APIVersion)
	return resp, nil
}
