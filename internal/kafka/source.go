package kafka

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// SourceRequestHandler is a RequestHandler used in the source connector and
// only allows Produce requests.
type SourceRequestHandler struct {
	replica *Replica
}

func NewSourceRequestHandler(replica *Replica) *SourceRequestHandler {
	return &SourceRequestHandler{replica: replica}
}

func (h *SourceRequestHandler) Produce(ctx context.Context, req *kmsg.ProduceRequest) *kmsg.ProduceResponse {
	resp := kmsg.ProduceResponse{
		Version: req.GetVersion(),
		Topics:  make([]kmsg.ProduceResponseTopic, 0, len(req.Topics)),
	}
	for _, reqTopic := range req.Topics {
		respTopic := kmsg.ProduceResponseTopic{
			Topic:      reqTopic.Topic,
			Partitions: make([]kmsg.ProduceResponseTopicPartition, 0, len(reqTopic.Partitions)),
		}
		for _, reqPartition := range reqTopic.Partitions {
			var batch kmsg.RecordBatch
			if err := batch.ReadFrom(reqPartition.Records); err != nil {
				respTopic.Partitions = append(respTopic.Partitions, kmsg.ProduceResponseTopicPartition{
					Partition:      reqPartition.Partition,
					ErrorCode:      kerr.CorruptMessage.Code,
					LogAppendTime:  -1,
					LogStartOffset: -1,
				})
				continue
			}
			offset, err := h.replica.Produce(ctx, reqTopic.Topic, reqPartition.Partition, batch)
			if err != nil {
				respTopic.Partitions = append(respTopic.Partitions, kmsg.ProduceResponseTopicPartition{
					Partition:      reqPartition.Partition,
					ErrorCode:      err.Code,
					LogAppendTime:  -1,
					LogStartOffset: -1,
				})
				continue
			}

			respTopic.Partitions = append(respTopic.Partitions, kmsg.ProduceResponseTopicPartition{
				Partition:      reqPartition.Partition,
				BaseOffset:     offset,
				LogAppendTime:  time.Now().UnixMilli(),
				LogStartOffset: -1,
				UnknownTags:    kmsg.Tags{},
			})
		}
		resp.Topics = append(resp.Topics, respTopic)
	}
	return &resp
}

func (h *SourceRequestHandler) CreateTopics(ctx context.Context, req *kmsg.CreateTopicsRequest) *kmsg.CreateTopicsResponse {
	resp := kmsg.CreateTopicsResponse{
		Version: req.GetVersion(),
		Topics:  make([]kmsg.CreateTopicsResponseTopic, 0, len(req.Topics)),
	}
	for _, reqTopic := range req.Topics {
		numPartitions := reqTopic.NumPartitions
		if numPartitions <= 0 {
			numPartitions = int32(len(reqTopic.ReplicaAssignment))
		}

		t, err := h.replica.CreateTopic(reqTopic.Topic, [16]byte{}, numPartitions)
		if err != nil {
			resp.Topics = append(resp.Topics, kmsg.CreateTopicsResponseTopic{
				Topic:     reqTopic.Topic,
				ErrorCode: err.Code,
			})
			continue
		}
		resp.Topics = append(resp.Topics, kmsg.CreateTopicsResponseTopic{
			Topic:             reqTopic.Topic,
			TopicID:           t.TopicID,
			NumPartitions:     int32(len(t.Partitions)),
			ReplicationFactor: 1,
		})
	}
	return &resp
}

func (h *SourceRequestHandler) DeleteTopics(ctx context.Context, req *kmsg.DeleteTopicsRequest) *kmsg.DeleteTopicsResponse {
	resp := kmsg.DeleteTopicsResponse{
		Version: req.GetVersion(),
		Topics:  make([]kmsg.DeleteTopicsResponseTopic, 0, len(req.Topics)),
	}
	for _, reqTopic := range req.Topics {
		t, err := h.replica.DeleteTopic(*reqTopic.Topic)
		if err != nil {
			resp.Topics = append(resp.Topics, kmsg.DeleteTopicsResponseTopic{
				TopicID:   reqTopic.TopicID,
				Topic:     reqTopic.Topic,
				ErrorCode: err.Code,
			})
			continue
		}
		resp.Topics = append(resp.Topics, kmsg.DeleteTopicsResponseTopic{
			TopicID: t.TopicID,
			Topic:   reqTopic.Topic,
		})
	}
	return &resp
}

func (h *SourceRequestHandler) Metadata(ctx context.Context, req *kmsg.MetadataRequest) *kmsg.MetadataResponse {
	resp := kmsg.MetadataResponse{
		Version: req.GetVersion(),
		Brokers: []kmsg.MetadataResponseBroker{{
			NodeID: 0,
			Host:   h.replica.Host,
			Port:   h.replica.Port,
		}},
		ControllerID: 0,
		Topics:       make([]kmsg.MetadataResponseTopic, 0, len(req.Topics)),
	}

	buildMetadataResponseTopic := func(t *Topic, err *kerr.Error) kmsg.MetadataResponseTopic {
		if err != nil {
			return kmsg.MetadataResponseTopic{
				TopicID:   t.TopicID,
				Topic:     &t.Topic,
				ErrorCode: err.Code,
			}
		}
		respTopic := kmsg.MetadataResponseTopic{
			TopicID:              t.TopicID,
			Topic:                &t.Topic,
			IsInternal:           false,
			Partitions:           make([]kmsg.MetadataResponseTopicPartition, 0, len(t.Partitions)),
			AuthorizedOperations: -2147483648,
		}
		for _, tp := range t.Partitions {
			respPartition := kmsg.MetadataResponseTopicPartition{
				Partition:       tp.Partition,
				Leader:          0,
				LeaderEpoch:     -1,
				Replicas:        []int32{0},
				ISR:             []int32{0},
				OfflineReplicas: []int32{},
				UnknownTags:     kmsg.Tags{},
			}
			respTopic.Partitions = append(respTopic.Partitions, respPartition)
		}
		return respTopic
	}

	if req.Topics == nil {
		topics := h.replica.GetTopics()
		resp.Topics = make([]kmsg.MetadataResponseTopic, 0, len(topics))
		for _, t := range topics {
			resp.Topics = append(resp.Topics, buildMetadataResponseTopic(t, nil))
		}
	} else {
		for _, reqTopic := range req.Topics {
			t, err := h.replica.GetTopic(*reqTopic.Topic)
			if err != nil {
				t = &Topic{Topic: *reqTopic.Topic, TopicID: reqTopic.TopicID}
				if err == kerr.UnknownTopicOrPartition && req.AllowAutoTopicCreation {
					// auto create topic
					t, err = h.replica.CreateTopic(*reqTopic.Topic, reqTopic.TopicID, 1)
					if err != nil {
						t = &Topic{Topic: *reqTopic.Topic, TopicID: reqTopic.TopicID}
					}
				}
			}
			resp.Topics = append(resp.Topics, buildMetadataResponseTopic(t, err))
		}
	}

	return &resp
}

func (h *SourceRequestHandler) ApiVersions(ctx context.Context, req *kmsg.ApiVersionsRequest) *kmsg.ApiVersionsResponse {
	return &kmsg.ApiVersionsResponse{
		Version: req.GetVersion(),
		ApiKeys: []kmsg.ApiVersionsResponseApiKey{
			{ApiKey: kmsg.Produce.Int16(), MinVersion: 0, MaxVersion: 5},
			{ApiKey: kmsg.CreateTopics.Int16(), MinVersion: 0, MaxVersion: 7},
			{ApiKey: kmsg.DeleteTopics.Int16(), MinVersion: 0, MaxVersion: 6},
			{ApiKey: kmsg.Metadata.Int16(), MinVersion: 0, MaxVersion: 12},
		},
	}
}

func (h *SourceRequestHandler) ListOffsets(ctx context.Context, req *kmsg.ListOffsetsRequest) *kmsg.ListOffsetsResponse {
	resp := kmsg.ListOffsetsResponse{
		Version: req.GetVersion(),
		Topics:  make([]kmsg.ListOffsetsResponseTopic, 0, len(req.Topics)),
	}
	for _, reqTopic := range req.Topics {
		respTopic := kmsg.ListOffsetsResponseTopic{
			Topic:      reqTopic.Topic,
			Partitions: make([]kmsg.ListOffsetsResponseTopicPartition, 0, len(reqTopic.Partitions)),
		}
		t, err := h.replica.GetTopic(reqTopic.Topic)
		if err != nil {
			for range reqTopic.Partitions {
				respTopic.Partitions = append(respTopic.Partitions, kmsg.ListOffsetsResponseTopicPartition{
					Partition: reqTopic.Partitions[0].Partition,
					ErrorCode: err.Code,
				})
			}
			resp.Topics = append(resp.Topics, respTopic)
			continue
		}
		for _, tp := range t.Partitions {
			for _, reqPartition := range reqTopic.Partitions {
				if tp.Partition != reqPartition.Partition {
					continue
				}
				respPartition := kmsg.ListOffsetsResponseTopicPartition{
					Partition:   tp.Partition,
					LeaderEpoch: -1,
					Offset:      tp.Offset,
				}
				respTopic.Partitions = append(respTopic.Partitions, respPartition)
				break
			}
		}
		resp.Topics = append(resp.Topics, respTopic)
	}
	return &resp
}

func (h *SourceRequestHandler) Fetch(ctx context.Context, req *kmsg.FetchRequest) *kmsg.FetchResponse {
	return &kmsg.FetchResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) LeaderAndISR(ctx context.Context, req *kmsg.LeaderAndISRRequest) *kmsg.LeaderAndISRResponse {
	return &kmsg.LeaderAndISRResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) StopReplica(ctx context.Context, req *kmsg.StopReplicaRequest) *kmsg.StopReplicaResponse {
	return &kmsg.StopReplicaResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) UpdateMetadata(ctx context.Context, req *kmsg.UpdateMetadataRequest) *kmsg.UpdateMetadataResponse {
	return &kmsg.UpdateMetadataResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) ControlledShutdown(ctx context.Context, req *kmsg.ControlledShutdownRequest) *kmsg.ControlledShutdownResponse {
	return &kmsg.ControlledShutdownResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) OffsetCommit(ctx context.Context, req *kmsg.OffsetCommitRequest) *kmsg.OffsetCommitResponse {
	resp := kmsg.OffsetCommitResponse{
		Version: req.GetVersion(),
		Topics:  make([]kmsg.OffsetCommitResponseTopic, 0, len(req.Topics)),
	}
	for _, reqTopic := range req.Topics {
		respTopic := kmsg.OffsetCommitResponseTopic{
			Topic:      reqTopic.Topic,
			Partitions: make([]kmsg.OffsetCommitResponseTopicPartition, 0, len(reqTopic.Partitions)),
		}
		for _, reqPartition := range reqTopic.Partitions {
			respTopic.Partitions = append(respTopic.Partitions, kmsg.OffsetCommitResponseTopicPartition{
				Partition: reqPartition.Partition,
				ErrorCode: kerr.InvalidRequest.Code,
			})
		}
		resp.Topics = append(resp.Topics, respTopic)
	}
	return &resp
}

func (h *SourceRequestHandler) OffsetFetch(ctx context.Context, req *kmsg.OffsetFetchRequest) *kmsg.OffsetFetchResponse {
	return &kmsg.OffsetFetchResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) FindCoordinator(ctx context.Context, req *kmsg.FindCoordinatorRequest) *kmsg.FindCoordinatorResponse {
	return &kmsg.FindCoordinatorResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) JoinGroup(ctx context.Context, req *kmsg.JoinGroupRequest) *kmsg.JoinGroupResponse {
	return &kmsg.JoinGroupResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) Heartbeat(ctx context.Context, req *kmsg.HeartbeatRequest) *kmsg.HeartbeatResponse {
	return &kmsg.HeartbeatResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) LeaveGroup(ctx context.Context, req *kmsg.LeaveGroupRequest) *kmsg.LeaveGroupResponse {
	return &kmsg.LeaveGroupResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) SyncGroup(ctx context.Context, req *kmsg.SyncGroupRequest) *kmsg.SyncGroupResponse {
	return &kmsg.SyncGroupResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) DescribeGroups(ctx context.Context, req *kmsg.DescribeGroupsRequest) *kmsg.DescribeGroupsResponse {
	resp := kmsg.DescribeGroupsResponse{
		Version: req.GetVersion(),
		Groups:  make([]kmsg.DescribeGroupsResponseGroup, 0, len(req.Groups)),
	}
	for _, group := range req.Groups {
		resp.Groups = append(resp.Groups, kmsg.DescribeGroupsResponseGroup{
			ErrorCode: kerr.GroupIDNotFound.Code,
			Group:     group,
		})
	}
	return &resp
}

func (h *SourceRequestHandler) ListGroups(ctx context.Context, req *kmsg.ListGroupsRequest) *kmsg.ListGroupsResponse {
	return &kmsg.ListGroupsResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) SASLHandshake(ctx context.Context, req *kmsg.SASLHandshakeRequest) *kmsg.SASLHandshakeResponse {
	return &kmsg.SASLHandshakeResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) DeleteRecords(ctx context.Context, req *kmsg.DeleteRecordsRequest) *kmsg.DeleteRecordsResponse {
	resp := kmsg.DeleteRecordsResponse{
		Version: req.GetVersion(),
		Topics:  make([]kmsg.DeleteRecordsResponseTopic, 0, len(req.Topics)),
	}
	for _, reqTopic := range req.Topics {
		respTopic := kmsg.DeleteRecordsResponseTopic{
			Topic:      reqTopic.Topic,
			Partitions: make([]kmsg.DeleteRecordsResponseTopicPartition, 0, len(reqTopic.Partitions)),
		}
		for _, reqPartition := range reqTopic.Partitions {
			respTopic.Partitions = append(respTopic.Partitions, kmsg.DeleteRecordsResponseTopicPartition{
				Partition: reqPartition.Partition,
				ErrorCode: kerr.InvalidRequest.Code,
			})
		}
		resp.Topics = append(resp.Topics, respTopic)
	}
	return &resp
}

func (h *SourceRequestHandler) InitProducerID(ctx context.Context, req *kmsg.InitProducerIDRequest) *kmsg.InitProducerIDResponse {
	return &kmsg.InitProducerIDResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) OffsetForLeaderEpoch(ctx context.Context, req *kmsg.OffsetForLeaderEpochRequest) *kmsg.OffsetForLeaderEpochResponse {
	resp := kmsg.OffsetForLeaderEpochResponse{
		Version: req.GetVersion(),
		Topics:  make([]kmsg.OffsetForLeaderEpochResponseTopic, 0, len(req.Topics)),
	}
	for _, reqTopic := range req.Topics {
		respTopic := kmsg.OffsetForLeaderEpochResponseTopic{
			Topic:      reqTopic.Topic,
			Partitions: make([]kmsg.OffsetForLeaderEpochResponseTopicPartition, 0, len(reqTopic.Partitions)),
		}
		for _, reqPartition := range reqTopic.Partitions {
			respTopic.Partitions = append(respTopic.Partitions, kmsg.OffsetForLeaderEpochResponseTopicPartition{
				Partition: reqPartition.Partition,
				ErrorCode: kerr.InvalidRequest.Code,
			})
		}
		resp.Topics = append(resp.Topics, respTopic)
	}
	return &resp
}

func (h *SourceRequestHandler) AddPartitionsToTxn(ctx context.Context, req *kmsg.AddPartitionsToTxnRequest) *kmsg.AddPartitionsToTxnResponse {
	return &kmsg.AddPartitionsToTxnResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) AddOffsetsToTxn(ctx context.Context, req *kmsg.AddOffsetsToTxnRequest) *kmsg.AddOffsetsToTxnResponse {
	return &kmsg.AddOffsetsToTxnResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) EndTxn(ctx context.Context, req *kmsg.EndTxnRequest) *kmsg.EndTxnResponse {
	return &kmsg.EndTxnResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) WriteTxnMarkers(ctx context.Context, req *kmsg.WriteTxnMarkersRequest) *kmsg.WriteTxnMarkersResponse {
	resp := kmsg.WriteTxnMarkersResponse{
		Version: req.GetVersion(),
		Markers: make([]kmsg.WriteTxnMarkersResponseMarker, 0, len(req.Markers)),
	}
	for _, reqMarker := range req.Markers {
		respMarker := kmsg.WriteTxnMarkersResponseMarker{
			ProducerID: reqMarker.ProducerID,
			Topics:     make([]kmsg.WriteTxnMarkersResponseMarkerTopic, 0, len(reqMarker.Topics)),
		}
		for _, reqTopic := range reqMarker.Topics {
			respTopic := kmsg.WriteTxnMarkersResponseMarkerTopic{
				Topic:      reqTopic.Topic,
				Partitions: make([]kmsg.WriteTxnMarkersResponseMarkerTopicPartition, 0, len(reqTopic.Partitions)),
			}
			for _, reqPartition := range reqTopic.Partitions {
				respTopic.Partitions = append(respTopic.Partitions, kmsg.WriteTxnMarkersResponseMarkerTopicPartition{
					Partition: reqPartition,
					ErrorCode: kerr.InvalidRequest.Code,
				})
			}
			respMarker.Topics = append(respMarker.Topics, respTopic)
		}
		resp.Markers = append(resp.Markers, respMarker)
	}
	return &resp
}

func (h *SourceRequestHandler) TxnOffsetCommit(ctx context.Context, req *kmsg.TxnOffsetCommitRequest) *kmsg.TxnOffsetCommitResponse {
	resp := kmsg.TxnOffsetCommitResponse{
		Version: req.GetVersion(),
		Topics:  make([]kmsg.TxnOffsetCommitResponseTopic, 0, len(req.Topics)),
	}
	for _, reqTopic := range req.Topics {
		respTopic := kmsg.TxnOffsetCommitResponseTopic{
			Topic:      reqTopic.Topic,
			Partitions: make([]kmsg.TxnOffsetCommitResponseTopicPartition, 0, len(reqTopic.Partitions)),
		}
		for _, reqPartition := range reqTopic.Partitions {
			respTopic.Partitions = append(respTopic.Partitions, kmsg.TxnOffsetCommitResponseTopicPartition{
				Partition: reqPartition.Partition,
				ErrorCode: kerr.InvalidRequest.Code,
			})
		}
		resp.Topics = append(resp.Topics, respTopic)
	}
	return &resp
}

func (h *SourceRequestHandler) DescribeACLs(ctx context.Context, req *kmsg.DescribeACLsRequest) *kmsg.DescribeACLsResponse {
	return &kmsg.DescribeACLsResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) CreateACLs(ctx context.Context, req *kmsg.CreateACLsRequest) *kmsg.CreateACLsResponse {
	resp := kmsg.CreateACLsResponse{
		Version: req.GetVersion(),
		Results: make([]kmsg.CreateACLsResponseResult, 0, len(req.Creations)),
	}
	for range req.Creations {
		respResult := kmsg.CreateACLsResponseResult{
			ErrorCode: kerr.InvalidRequest.Code,
		}
		resp.Results = append(resp.Results, respResult)
	}
	return &resp
}

func (h *SourceRequestHandler) DeleteACLs(ctx context.Context, req *kmsg.DeleteACLsRequest) *kmsg.DeleteACLsResponse {
	resp := kmsg.DeleteACLsResponse{
		Version: req.GetVersion(),
		Results: make([]kmsg.DeleteACLsResponseResult, 0, len(req.Filters)),
	}
	for range req.Filters {
		respResult := kmsg.DeleteACLsResponseResult{
			ErrorCode: kerr.InvalidRequest.Code,
		}
		resp.Results = append(resp.Results, respResult)
	}
	return &resp
}

func (h *SourceRequestHandler) DescribeConfigs(ctx context.Context, req *kmsg.DescribeConfigsRequest) *kmsg.DescribeConfigsResponse {
	resp := kmsg.DescribeConfigsResponse{
		Version:   req.GetVersion(),
		Resources: make([]kmsg.DescribeConfigsResponseResource, 0, len(req.Resources)),
	}
	for _, reqResource := range req.Resources {
		respResource := kmsg.DescribeConfigsResponseResource{
			ResourceType: reqResource.ResourceType,
			ResourceName: reqResource.ResourceName,
			ErrorCode:    kerr.InvalidRequest.Code,
		}
		resp.Resources = append(resp.Resources, respResource)
	}
	return &resp
}

func (h *SourceRequestHandler) AlterConfigs(ctx context.Context, req *kmsg.AlterConfigsRequest) *kmsg.AlterConfigsResponse {
	resp := kmsg.AlterConfigsResponse{
		Version:   req.GetVersion(),
		Resources: make([]kmsg.AlterConfigsResponseResource, 0, len(req.Resources)),
	}
	for _, reqResource := range req.Resources {
		respResource := kmsg.AlterConfigsResponseResource{
			ResourceType: reqResource.ResourceType,
			ResourceName: reqResource.ResourceName,
			ErrorCode:    kerr.InvalidRequest.Code,
		}
		resp.Resources = append(resp.Resources, respResource)
	}
	return &resp
}

func (h *SourceRequestHandler) AlterReplicaLogDirs(ctx context.Context, req *kmsg.AlterReplicaLogDirsRequest) *kmsg.AlterReplicaLogDirsResponse {
	resp := kmsg.AlterReplicaLogDirsResponse{
		Version: req.GetVersion(),
		Topics:  make([]kmsg.AlterReplicaLogDirsResponseTopic, 0, len(req.Dirs)),
	}
	for _, reqDir := range req.Dirs {
		for _, reqTopic := range reqDir.Topics {
			respTopic := kmsg.AlterReplicaLogDirsResponseTopic{
				Topic:      reqTopic.Topic,
				Partitions: make([]kmsg.AlterReplicaLogDirsResponseTopicPartition, 0, len(reqTopic.Partitions)),
			}
			for _, reqPartition := range reqTopic.Partitions {
				respPartition := kmsg.AlterReplicaLogDirsResponseTopicPartition{
					Partition: reqPartition,
					ErrorCode: kerr.InvalidRequest.Code,
				}
				respTopic.Partitions = append(respTopic.Partitions, respPartition)
			}
			resp.Topics = append(resp.Topics, respTopic)
		}
	}
	return &resp
}

func (h *SourceRequestHandler) DescribeLogDirs(ctx context.Context, req *kmsg.DescribeLogDirsRequest) *kmsg.DescribeLogDirsResponse {
	return &kmsg.DescribeLogDirsResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) SASLAuthenticate(ctx context.Context, req *kmsg.SASLAuthenticateRequest) *kmsg.SASLAuthenticateResponse {
	return &kmsg.SASLAuthenticateResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) CreatePartitions(ctx context.Context, req *kmsg.CreatePartitionsRequest) *kmsg.CreatePartitionsResponse {
	resp := kmsg.CreatePartitionsResponse{
		Version: req.GetVersion(),
		Topics:  make([]kmsg.CreatePartitionsResponseTopic, 0, len(req.Topics)),
	}
	for _, reqTopic := range req.Topics {
		respTopic := kmsg.CreatePartitionsResponseTopic{
			Topic:     reqTopic.Topic,
			ErrorCode: kerr.InvalidRequest.Code,
		}
		resp.Topics = append(resp.Topics, respTopic)
	}
	return &resp
}

func (h *SourceRequestHandler) CreateDelegationToken(ctx context.Context, req *kmsg.CreateDelegationTokenRequest) *kmsg.CreateDelegationTokenResponse {
	return &kmsg.CreateDelegationTokenResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) RenewDelegationToken(ctx context.Context, req *kmsg.RenewDelegationTokenRequest) *kmsg.RenewDelegationTokenResponse {
	return &kmsg.RenewDelegationTokenResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) ExpireDelegationToken(ctx context.Context, req *kmsg.ExpireDelegationTokenRequest) *kmsg.ExpireDelegationTokenResponse {
	return &kmsg.ExpireDelegationTokenResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) DescribeDelegationToken(ctx context.Context, req *kmsg.DescribeDelegationTokenRequest) *kmsg.DescribeDelegationTokenResponse {
	return &kmsg.DescribeDelegationTokenResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) DeleteGroups(ctx context.Context, req *kmsg.DeleteGroupsRequest) *kmsg.DeleteGroupsResponse {
	resp := kmsg.DeleteGroupsResponse{
		Version: req.GetVersion(),
		Groups:  make([]kmsg.DeleteGroupsResponseGroup, 0, len(req.Groups)),
	}
	for _, group := range req.Groups {
		resp.Groups = append(resp.Groups, kmsg.DeleteGroupsResponseGroup{
			ErrorCode: kerr.GroupIDNotFound.Code,
			Group:     group,
		})
	}
	return &resp
}

func (h *SourceRequestHandler) ElectLeaders(ctx context.Context, req *kmsg.ElectLeadersRequest) *kmsg.ElectLeadersResponse {
	return &kmsg.ElectLeadersResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) IncrementalAlterConfigs(ctx context.Context, req *kmsg.IncrementalAlterConfigsRequest) *kmsg.IncrementalAlterConfigsResponse {
	resp := kmsg.IncrementalAlterConfigsResponse{
		Version:   req.GetVersion(),
		Resources: make([]kmsg.IncrementalAlterConfigsResponseResource, 0, len(req.Resources)),
	}
	for _, reqResource := range req.Resources {
		respResource := kmsg.IncrementalAlterConfigsResponseResource{
			ResourceType: reqResource.ResourceType,
			ResourceName: reqResource.ResourceName,
			ErrorCode:    kerr.InvalidRequest.Code,
		}
		resp.Resources = append(resp.Resources, respResource)
	}
	return &resp
}

func (h *SourceRequestHandler) AlterPartitionAssignments(ctx context.Context, req *kmsg.AlterPartitionAssignmentsRequest) *kmsg.AlterPartitionAssignmentsResponse {
	return &kmsg.AlterPartitionAssignmentsResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) ListPartitionReassignments(ctx context.Context, req *kmsg.ListPartitionReassignmentsRequest) *kmsg.ListPartitionReassignmentsResponse {
	return &kmsg.ListPartitionReassignmentsResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) OffsetDelete(ctx context.Context, req *kmsg.OffsetDeleteRequest) *kmsg.OffsetDeleteResponse {
	return &kmsg.OffsetDeleteResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) DescribeClientQuotas(ctx context.Context, req *kmsg.DescribeClientQuotasRequest) *kmsg.DescribeClientQuotasResponse {
	return &kmsg.DescribeClientQuotasResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) AlterClientQuotas(ctx context.Context, req *kmsg.AlterClientQuotasRequest) *kmsg.AlterClientQuotasResponse {
	resp := kmsg.AlterClientQuotasResponse{
		Version: req.GetVersion(),
		Entries: make([]kmsg.AlterClientQuotasResponseEntry, 0, len(req.Entries)),
	}
	for range req.Entries {
		respEntry := kmsg.AlterClientQuotasResponseEntry{
			ErrorCode: kerr.InvalidRequest.Code,
		}
		resp.Entries = append(resp.Entries, respEntry)
	}
	return &resp
}

func (h *SourceRequestHandler) DescribeUserSCRAMCredentials(ctx context.Context, req *kmsg.DescribeUserSCRAMCredentialsRequest) *kmsg.DescribeUserSCRAMCredentialsResponse {
	return &kmsg.DescribeUserSCRAMCredentialsResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) AlterUserSCRAMCredentials(ctx context.Context, req *kmsg.AlterUserSCRAMCredentialsRequest) *kmsg.AlterUserSCRAMCredentialsResponse {
	resp := kmsg.AlterUserSCRAMCredentialsResponse{
		Version: req.GetVersion(),
		Results: make([]kmsg.AlterUserSCRAMCredentialsResponseResult, 0, len(req.Upsertions)+len(req.Deletions)),
	}
	for _, reqUpsert := range req.Upsertions {
		respResult := kmsg.AlterUserSCRAMCredentialsResponseResult{
			User:      reqUpsert.Name,
			ErrorCode: kerr.InvalidRequest.Code,
		}
		resp.Results = append(resp.Results, respResult)
	}
	for _, reqDelete := range req.Deletions {
		respResult := kmsg.AlterUserSCRAMCredentialsResponseResult{
			User:      reqDelete.Name,
			ErrorCode: kerr.InvalidRequest.Code,
		}
		resp.Results = append(resp.Results, respResult)
	}
	return &resp

}

func (h *SourceRequestHandler) Vote(ctx context.Context, req *kmsg.VoteRequest) *kmsg.VoteResponse {
	return &kmsg.VoteResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) BeginQuorumEpoch(ctx context.Context, req *kmsg.BeginQuorumEpochRequest) *kmsg.BeginQuorumEpochResponse {
	return &kmsg.BeginQuorumEpochResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) EndQuorumEpoch(ctx context.Context, req *kmsg.EndQuorumEpochRequest) *kmsg.EndQuorumEpochResponse {
	return &kmsg.EndQuorumEpochResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) DescribeQuorum(ctx context.Context, req *kmsg.DescribeQuorumRequest) *kmsg.DescribeQuorumResponse {
	return &kmsg.DescribeQuorumResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) AlterPartition(ctx context.Context, req *kmsg.AlterPartitionRequest) *kmsg.AlterPartitionResponse {
	return &kmsg.AlterPartitionResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) UpdateFeatures(ctx context.Context, req *kmsg.UpdateFeaturesRequest) *kmsg.UpdateFeaturesResponse {
	return &kmsg.UpdateFeaturesResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) Envelope(ctx context.Context, req *kmsg.EnvelopeRequest) *kmsg.EnvelopeResponse {
	return &kmsg.EnvelopeResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) FetchSnapshot(ctx context.Context, req *kmsg.FetchSnapshotRequest) *kmsg.FetchSnapshotResponse {
	return &kmsg.FetchSnapshotResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) DescribeCluster(ctx context.Context, req *kmsg.DescribeClusterRequest) *kmsg.DescribeClusterResponse {
	return &kmsg.DescribeClusterResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) DescribeProducers(ctx context.Context, req *kmsg.DescribeProducersRequest) *kmsg.DescribeProducersResponse {
	resp := kmsg.DescribeProducersResponse{
		Version: req.GetVersion(),
		Topics:  make([]kmsg.DescribeProducersResponseTopic, 0, len(req.Topics)),
	}
	for _, reqTopic := range req.Topics {
		respTopic := kmsg.DescribeProducersResponseTopic{
			Topic:      reqTopic.Topic,
			Partitions: make([]kmsg.DescribeProducersResponseTopicPartition, 0, len(reqTopic.Partitions)),
		}
		for _, reqPartition := range reqTopic.Partitions {
			respTopic.Partitions = append(respTopic.Partitions, kmsg.DescribeProducersResponseTopicPartition{
				Partition: reqPartition,
				ErrorCode: kerr.InvalidRequest.Code,
			})
		}
		resp.Topics = append(resp.Topics, respTopic)
	}
	return &resp
}

func (h *SourceRequestHandler) BrokerRegistration(ctx context.Context, req *kmsg.BrokerRegistrationRequest) *kmsg.BrokerRegistrationResponse {
	return &kmsg.BrokerRegistrationResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) BrokerHeartbeat(ctx context.Context, req *kmsg.BrokerHeartbeatRequest) *kmsg.BrokerHeartbeatResponse {
	return &kmsg.BrokerHeartbeatResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) UnregisterBroker(ctx context.Context, req *kmsg.UnregisterBrokerRequest) *kmsg.UnregisterBrokerResponse {
	return &kmsg.UnregisterBrokerResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) DescribeTransactions(ctx context.Context, req *kmsg.DescribeTransactionsRequest) *kmsg.DescribeTransactionsResponse {
	resp := kmsg.DescribeTransactionsResponse{
		Version:           req.GetVersion(),
		TransactionStates: make([]kmsg.DescribeTransactionsResponseTransactionState, 0, len(req.TransactionalIDs)),
	}
	for _, reqTransactionalID := range req.TransactionalIDs {
		respEntry := kmsg.DescribeTransactionsResponseTransactionState{
			TransactionalID: reqTransactionalID,
			ErrorCode:       kerr.InvalidRequest.Code,
		}
		resp.TransactionStates = append(resp.TransactionStates, respEntry)
	}
	return &resp
}

func (h *SourceRequestHandler) ListTransactions(ctx context.Context, req *kmsg.ListTransactionsRequest) *kmsg.ListTransactionsResponse {
	return &kmsg.ListTransactionsResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}

func (h *SourceRequestHandler) AllocateProducerIDs(ctx context.Context, req *kmsg.AllocateProducerIDsRequest) *kmsg.AllocateProducerIDsResponse {
	return &kmsg.AllocateProducerIDsResponse{
		Version:   req.GetVersion(),
		ErrorCode: kerr.InvalidRequest.Code,
	}
}
