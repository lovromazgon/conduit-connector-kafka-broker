package kafkabroker

//go:generate paramgen -output=paramgen_src.go SourceConfig

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/lovromazgon/conduit-connector-kafka-broker/internal"
	"github.com/travisjeffery/jocko/jocko"
	"github.com/travisjeffery/jocko/jocko/config"
	"github.com/travisjeffery/jocko/jocko/structs"
)

type Source struct {
	sdk.UnimplementedSource

	config SourceConfig

	broker *jocko.Broker
	server *jocko.Server

	messages *internal.Fanin[Message]
}

type SourceConfig struct {
	Config
}

func NewSource() sdk.Source {
	// Create Source and wrap it in the default middleware.
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

func (s *Source) Parameters() map[string]sdk.Parameter {
	return s.config.Parameters()
}

func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	err := sdk.Util.ParseConfig(cfg, &s.config)
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	return nil
}

func (s *Source) Open(ctx context.Context, _ sdk.Position) error {
	s.messages = internal.NewFanin[Message]()

	brokerCfg := config.DefaultConfig()

	brokerCfg.Bootstrap = true
	brokerCfg.BootstrapExpect = 1
	brokerCfg.DataDir = os.TempDir() + "/jocko"
	brokerCfg.RaftAddr = "127.0.0.1:0"
	brokerCfg.SerfLANConfig.MemberlistConfig.BindAddr = "127.0.0.1:0"
	brokerCfg.CommitLogMiddleware = func(log structs.CommitLog, partition structs.Partition) structs.CommitLog {
		tcl := NewTeeCommitLog(log, partition)
		s.messages.Add(tcl.Messages())
		return log
	}

	brokerCfg.Addr = s.config.Addr

	broker, err := jocko.NewBroker(brokerCfg)
	if err != nil {
		return err
	}

	srv := jocko.NewServer(brokerCfg, broker, nil)
	if err := srv.Start(context.Background()); err != nil {
		if shutdownErr := broker.Shutdown(); shutdownErr != nil {
			sdk.Logger(ctx).Err(shutdownErr).Msg("broker shutdown failed")
		}
		return err
	}

	s.broker = broker
	s.server = srv

	return nil
}

func (s *Source) onNewReplica(log structs.CommitLog, partition structs.Partition) {

}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	msg, err := s.messages.Recv(ctx)
	if err != nil {
		return sdk.Record{}, err
	}

	rec := sdk.Util.Source.NewRecordCreate(
		[]byte(fmt.Sprintf("%s:%d:%d", msg.Topic, msg.Partition, msg.Offset)),
		sdk.Metadata{
			"kafkabroker.topic":     msg.Topic,
			"kafkabroker.partition": strconv.FormatInt(int64(msg.Partition), 10),
			"kafkabroker.offset":    strconv.FormatInt(msg.Offset, 10),
		},
		sdk.RawData(msg.Key()),
		sdk.RawData(msg.Value()),
	)
	return rec, nil
}

func (s *Source) Ack(context.Context, sdk.Position) error { return nil }

func (s *Source) Teardown(context.Context) error {
	var errs []error
	if s.broker != nil {
		err := s.broker.Shutdown()
		errs = append(errs, err)
	}
	if s.server != nil {
		err := s.server.Shutdown()
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
