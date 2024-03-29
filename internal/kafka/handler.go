package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kmsg"
)

type RequestHandler interface {
	Produce(context.Context, *kmsg.ProduceRequest) *kmsg.ProduceResponse
	Fetch(context.Context, *kmsg.FetchRequest) *kmsg.FetchResponse
	ListOffsets(context.Context, *kmsg.ListOffsetsRequest) *kmsg.ListOffsetsResponse
	Metadata(context.Context, *kmsg.MetadataRequest) *kmsg.MetadataResponse
	LeaderAndISR(context.Context, *kmsg.LeaderAndISRRequest) *kmsg.LeaderAndISRResponse
	StopReplica(context.Context, *kmsg.StopReplicaRequest) *kmsg.StopReplicaResponse
	UpdateMetadata(context.Context, *kmsg.UpdateMetadataRequest) *kmsg.UpdateMetadataResponse
	ControlledShutdown(context.Context, *kmsg.ControlledShutdownRequest) *kmsg.ControlledShutdownResponse
	OffsetCommit(context.Context, *kmsg.OffsetCommitRequest) *kmsg.OffsetCommitResponse
	OffsetFetch(context.Context, *kmsg.OffsetFetchRequest) *kmsg.OffsetFetchResponse
	FindCoordinator(context.Context, *kmsg.FindCoordinatorRequest) *kmsg.FindCoordinatorResponse
	JoinGroup(context.Context, *kmsg.JoinGroupRequest) *kmsg.JoinGroupResponse
	Heartbeat(context.Context, *kmsg.HeartbeatRequest) *kmsg.HeartbeatResponse
	LeaveGroup(context.Context, *kmsg.LeaveGroupRequest) *kmsg.LeaveGroupResponse
	SyncGroup(context.Context, *kmsg.SyncGroupRequest) *kmsg.SyncGroupResponse
	DescribeGroups(context.Context, *kmsg.DescribeGroupsRequest) *kmsg.DescribeGroupsResponse
	ListGroups(context.Context, *kmsg.ListGroupsRequest) *kmsg.ListGroupsResponse
	SASLHandshake(context.Context, *kmsg.SASLHandshakeRequest) *kmsg.SASLHandshakeResponse
	ApiVersions(context.Context, *kmsg.ApiVersionsRequest) *kmsg.ApiVersionsResponse
	CreateTopics(context.Context, *kmsg.CreateTopicsRequest) *kmsg.CreateTopicsResponse
	DeleteTopics(context.Context, *kmsg.DeleteTopicsRequest) *kmsg.DeleteTopicsResponse

	DeleteRecords(context.Context, *kmsg.DeleteRecordsRequest) *kmsg.DeleteRecordsResponse
	InitProducerID(context.Context, *kmsg.InitProducerIDRequest) *kmsg.InitProducerIDResponse
	OffsetForLeaderEpoch(context.Context, *kmsg.OffsetForLeaderEpochRequest) *kmsg.OffsetForLeaderEpochResponse
	AddPartitionsToTxn(context.Context, *kmsg.AddPartitionsToTxnRequest) *kmsg.AddPartitionsToTxnResponse
	AddOffsetsToTxn(context.Context, *kmsg.AddOffsetsToTxnRequest) *kmsg.AddOffsetsToTxnResponse
	EndTxn(context.Context, *kmsg.EndTxnRequest) *kmsg.EndTxnResponse
	WriteTxnMarkers(context.Context, *kmsg.WriteTxnMarkersRequest) *kmsg.WriteTxnMarkersResponse
	TxnOffsetCommit(context.Context, *kmsg.TxnOffsetCommitRequest) *kmsg.TxnOffsetCommitResponse
	DescribeACLs(context.Context, *kmsg.DescribeACLsRequest) *kmsg.DescribeACLsResponse
	CreateACLs(context.Context, *kmsg.CreateACLsRequest) *kmsg.CreateACLsResponse
	DeleteACLs(context.Context, *kmsg.DeleteACLsRequest) *kmsg.DeleteACLsResponse
	DescribeConfigs(context.Context, *kmsg.DescribeConfigsRequest) *kmsg.DescribeConfigsResponse
	AlterConfigs(context.Context, *kmsg.AlterConfigsRequest) *kmsg.AlterConfigsResponse
	AlterReplicaLogDirs(context.Context, *kmsg.AlterReplicaLogDirsRequest) *kmsg.AlterReplicaLogDirsResponse
	DescribeLogDirs(context.Context, *kmsg.DescribeLogDirsRequest) *kmsg.DescribeLogDirsResponse
	SASLAuthenticate(context.Context, *kmsg.SASLAuthenticateRequest) *kmsg.SASLAuthenticateResponse
	CreatePartitions(context.Context, *kmsg.CreatePartitionsRequest) *kmsg.CreatePartitionsResponse
	CreateDelegationToken(context.Context, *kmsg.CreateDelegationTokenRequest) *kmsg.CreateDelegationTokenResponse
	RenewDelegationToken(context.Context, *kmsg.RenewDelegationTokenRequest) *kmsg.RenewDelegationTokenResponse
	ExpireDelegationToken(context.Context, *kmsg.ExpireDelegationTokenRequest) *kmsg.ExpireDelegationTokenResponse
	DescribeDelegationToken(context.Context, *kmsg.DescribeDelegationTokenRequest) *kmsg.DescribeDelegationTokenResponse
	DeleteGroups(context.Context, *kmsg.DeleteGroupsRequest) *kmsg.DeleteGroupsResponse
	ElectLeaders(context.Context, *kmsg.ElectLeadersRequest) *kmsg.ElectLeadersResponse
	IncrementalAlterConfigs(context.Context, *kmsg.IncrementalAlterConfigsRequest) *kmsg.IncrementalAlterConfigsResponse
	AlterPartitionAssignments(context.Context, *kmsg.AlterPartitionAssignmentsRequest) *kmsg.AlterPartitionAssignmentsResponse
	ListPartitionReassignments(context.Context, *kmsg.ListPartitionReassignmentsRequest) *kmsg.ListPartitionReassignmentsResponse
	OffsetDelete(context.Context, *kmsg.OffsetDeleteRequest) *kmsg.OffsetDeleteResponse
	DescribeClientQuotas(context.Context, *kmsg.DescribeClientQuotasRequest) *kmsg.DescribeClientQuotasResponse
	AlterClientQuotas(context.Context, *kmsg.AlterClientQuotasRequest) *kmsg.AlterClientQuotasResponse
	DescribeUserSCRAMCredentials(context.Context, *kmsg.DescribeUserSCRAMCredentialsRequest) *kmsg.DescribeUserSCRAMCredentialsResponse
	AlterUserSCRAMCredentials(context.Context, *kmsg.AlterUserSCRAMCredentialsRequest) *kmsg.AlterUserSCRAMCredentialsResponse
	Vote(context.Context, *kmsg.VoteRequest) *kmsg.VoteResponse
	BeginQuorumEpoch(context.Context, *kmsg.BeginQuorumEpochRequest) *kmsg.BeginQuorumEpochResponse
	EndQuorumEpoch(context.Context, *kmsg.EndQuorumEpochRequest) *kmsg.EndQuorumEpochResponse
	DescribeQuorum(context.Context, *kmsg.DescribeQuorumRequest) *kmsg.DescribeQuorumResponse
	AlterPartition(context.Context, *kmsg.AlterPartitionRequest) *kmsg.AlterPartitionResponse
	UpdateFeatures(context.Context, *kmsg.UpdateFeaturesRequest) *kmsg.UpdateFeaturesResponse
	Envelope(context.Context, *kmsg.EnvelopeRequest) *kmsg.EnvelopeResponse
	FetchSnapshot(context.Context, *kmsg.FetchSnapshotRequest) *kmsg.FetchSnapshotResponse
	DescribeCluster(context.Context, *kmsg.DescribeClusterRequest) *kmsg.DescribeClusterResponse
	DescribeProducers(context.Context, *kmsg.DescribeProducersRequest) *kmsg.DescribeProducersResponse
	BrokerRegistration(context.Context, *kmsg.BrokerRegistrationRequest) *kmsg.BrokerRegistrationResponse
	BrokerHeartbeat(context.Context, *kmsg.BrokerHeartbeatRequest) *kmsg.BrokerHeartbeatResponse
	UnregisterBroker(context.Context, *kmsg.UnregisterBrokerRequest) *kmsg.UnregisterBrokerResponse
	DescribeTransactions(context.Context, *kmsg.DescribeTransactionsRequest) *kmsg.DescribeTransactionsResponse
	ListTransactions(context.Context, *kmsg.ListTransactionsRequest) *kmsg.ListTransactionsResponse
	AllocateProducerIDs(context.Context, *kmsg.AllocateProducerIDsRequest) *kmsg.AllocateProducerIDsResponse
}
