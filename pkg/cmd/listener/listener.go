// © 2022 Nokia.
//
// This code is a Contribution to the gNMIc project (“Work”) made under the Google Software Grant and Corporate Contributor License Agreement (“CLA”) and governed by the Apache License 2.0.
// No other rights or licenses in or to any of Nokia’s intellectual property are granted for any other purpose.
// This code is provided on an “as is” basis without any warranties of any kind.
//
// SPDX-License-Identifier: Apache-2.0

package listener

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"

	"github.com/fullstorydev/grpcurl"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	nokiasros "github.com/karimra/sros-dialout"
	"github.com/openconfig/gnmi/proto/gnmi"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/openconfig/gnmic/pkg/api/utils"
	"github.com/openconfig/gnmic/pkg/app"
	"github.com/openconfig/gnmic/pkg/outputs"
)

// SonicDialoutServer is the server-side interface for the SONiC gNMIDialOut service.
// This avoids importing the sonic-gnmi module which has broken file paths.
type SonicDialoutServer interface {
	SonicPublish(grpc.ServerStream) error
}

// sonicDialoutServiceDesc is the gRPC service descriptor for gnmi.sonic.gNMIDialOut.
var sonicDialoutServiceDesc = grpc.ServiceDesc{
	ServiceName: "gnmi.sonic.gNMIDialOut",
	HandlerType: (*SonicDialoutServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Publish",
			Handler:       sonicPublishHandler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "dial_out.proto",
}

func sonicPublishHandler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(SonicDialoutServer).SonicPublish(stream)
}

// New returns the listen command tree.
func New(gApp *app.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "listen",
		Short: "listens for telemetry dialout updates from the node",
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			gApp.Config.SetLocalFlagsFromFile(cmd)
			if len(gApp.Config.Address) == 0 {
				return fmt.Errorf("no address specified")
			}
			if len(gApp.Config.Address) > 1 {
				fmt.Fprintf(os.Stderr, "multiple addresses specified, listening only on %s\n", gApp.Config.Address[0])
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()
			server := new(dialoutTelemetryServer)
			server.ctx = ctx

			opts := []grpc.ServerOption{
				grpc.MaxConcurrentStreams(gApp.Config.LocalFlags.ListenMaxConcurrentStreams),
			}
			if gApp.Config.MaxMsgSize > 0 {
				opts = append(opts, grpc.MaxRecvMsgSize(gApp.Config.MaxMsgSize))
			}

			if gApp.Config.LocalFlags.ListenPrometheusAddress != "" {
				server.reg = prometheus.NewRegistry()
				grpcMetrics := grpc_prometheus.NewServerMetrics()
				opts = append(opts,
					grpc.StreamInterceptor(grpcMetrics.StreamServerInterceptor()),
				)
				server.reg.MustRegister(grpcMetrics)
			}

			if len(gApp.Config.ProtoFile) > 0 {
				gApp.Logger.Info("loading proto files")
				descSource, err := grpcurl.DescriptorSourceFromProtoFiles(gApp.Config.ProtoDir, gApp.Config.ProtoFile...)
				if err != nil {
					gApp.Logger.Info("failed to load proto files", "err", err)
					return err
				}
				server.rootDesc, err = descSource.FindSymbol("Nokia.SROS.root")
				if err != nil {
					gApp.Logger.Info("could not get proto symbol", "symbol", "Nokia.SROS.root", "err", err)
					return err
				}
				gApp.Logger.Info("loaded proto files")
			}

			server.Outputs = make(map[string]outputs.Output)
			outCfgs, err := gApp.Config.GetOutputs()
			if err != nil {
				return err
			}

			for name, outConf := range outCfgs {
				if outType, ok := outConf["type"]; ok {
					if initializer, ok := outputs.Outputs[outType.(string)]; ok {
						out := initializer()
						go out.Init(ctx, name, outConf,
							outputs.WithLogger(gApp.Logger),
							outputs.WithName(gApp.Config.InstanceName),
							outputs.WithClusterName(gApp.Config.ClusterName),
							outputs.WithRegistry(server.reg),
							outputs.WithConfigStore(gApp.Store),
						)
						server.Outputs[name] = out
					}
				}
			}

			defer func() {
				for _, o := range server.Outputs {
					o.Close()
				}
			}()

			server.listener, err = net.Listen("tcp", gApp.Config.Address[0])
			if err != nil {
				return err
			}
			gApp.Logger.Info("waiting for connections", "address", gApp.Config.Address[0])

			clientAuth := "request"
			if gApp.Config.LocalFlags.ListenAllowNoClientAuth {
				clientAuth = "request"
			}

			if gApp.Config.LocalFlags.ListenInsecure {
				tlsConfig, err := utils.NewTLSConfig(
					"", "", "",
					clientAuth,
					false, true,
				)
				if err != nil {
					return err
				}
				opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
				gApp.Logger.Info("TLS enabled with auto-generated self-signed certificate")
			} else if gApp.Config.TLSKey != "" && gApp.Config.TLSCert != "" {
				tlsConfig, err := utils.NewTLSConfig(
					gApp.Config.TLSCa,
					gApp.Config.TLSCert,
					gApp.Config.TLSKey,
					clientAuth,
					false,
					true,
				)
				if err != nil {
					return err
				}
				opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
			}

			server.grpcServer = grpc.NewServer(opts...)
			nokiasros.RegisterDialoutTelemetryServer(server.grpcServer, server)
			server.grpcServer.RegisterService(&sonicDialoutServiceDesc, server)

			if gApp.Config.LocalFlags.ListenPrometheusAddress != "" {
				grpc_prometheus.Register(server.grpcServer)

				httpServer := &http.Server{
					Handler: promhttp.HandlerFor(server.reg, promhttp.HandlerOpts{}),
					Addr:    gApp.Config.LocalFlags.ListenPrometheusAddress,
				}
				go func() {
					if err := httpServer.ListenAndServe(); err != nil {
						gApp.Logger.Info("unable to start prometheus HTTP server", "err", err)
					}
				}()
				defer httpServer.Close()
			}
			server.gApp = gApp
			server.grpcServer.Serve(server.listener)
			defer server.grpcServer.Stop()
			return nil
		},
		SilenceUsage: true,
	}
	cmd.Flags().Uint32P("max-concurrent-streams", "", 256, "max concurrent streams gnmic can receive per transport")
	cmd.Flags().StringP("prometheus-address", "", "", "prometheus server address")
	cmd.Flags().BoolP("insecure", "", false, "use auto-generated self-signed TLS certificate for the listen server")
	cmd.Flags().BoolP("allow-no-client-auth", "", false, "request but do not require a client certificate")
	gApp.Config.FileConfig.BindPFlag("listen-max-concurrent-streams", cmd.LocalFlags().Lookup("max-concurrent-streams"))
	gApp.Config.FileConfig.BindPFlag("listen-prometheus-address", cmd.LocalFlags().Lookup("prometheus-address"))
	gApp.Config.FileConfig.BindPFlag("listen-insecure", cmd.LocalFlags().Lookup("insecure"))
	gApp.Config.FileConfig.BindPFlag("listen-allow-no-client-auth", cmd.LocalFlags().Lookup("allow-no-client-auth"))
	return cmd
}

type dialoutTelemetryServer struct {
	listener   net.Listener
	grpcServer *grpc.Server
	rootDesc   desc.Descriptor

	Outputs map[string]outputs.Output

	ctx context.Context

	gApp *app.App
	reg  *prometheus.Registry
}

func (s *dialoutTelemetryServer) Publish(stream nokiasros.DialoutTelemetry_PublishServer) error {
	peer, ok := peer.FromContext(stream.Context())
	if ok && s.gApp.Config.Debug {
		b, err := json.Marshal(peer)
		if err != nil {
			s.gApp.Logger.Debug("failed to marshal peer data", "err", err)
		} else {
			s.gApp.Logger.Debug("received Publish RPC", "peer", string(b))
		}
	}
	md, ok := metadata.FromIncomingContext(stream.Context())
	if ok && s.gApp.Config.Debug {
		b, err := json.Marshal(md)
		if err != nil {
			s.gApp.Logger.Debug("failed to marshal context metadata", "err", err)
		} else {
			s.gApp.Logger.Debug("received http2 headers", "headers", string(b))
		}
	}
	outMeta := outputs.Meta{}
	if sn, ok := md["subscription-name"]; ok {
		if len(sn) > 0 {
			outMeta["subscription-name"] = sn[0]
		}
	} else {
		s.gApp.Logger.Info("could not find subscription-name in http2 headers")
	}
	outMeta["source"] = peer.Addr.String()
	if systemName, ok := md["system-name"]; ok {
		if len(systemName) > 0 {
			outMeta["system-name"] = systemName[0]
		}
	} else {
		s.gApp.Logger.Info("could not find system-name in http2 headers")
	}
	for {
		subResp, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				s.gApp.Logger.Info("gRPC dialout receive error", "err", err)
			}
			break
		}
		err = stream.Send(&nokiasros.PublishResponse{})
		if err != nil {
			s.gApp.Logger.Info("error sending publish response to server", "err", err)
		}
		switch resp := subResp.Response.(type) {
		case *gnmi.SubscribeResponse_Update:
			if s.rootDesc != nil {
				for _, update := range resp.Update.Update {
					switch update.Val.Value.(type) {
					case *gnmi.TypedValue_ProtoBytes:
						m := dynamic.NewMessage(s.rootDesc.GetFile().FindMessage("Nokia.SROS.root"))
						err := m.Unmarshal(update.Val.GetProtoBytes())
						if err != nil {
							s.gApp.Logger.Info("failed to unmarshal dynamic proto message", "err", err)
						}
						jsondata, err := m.MarshalJSON()
						if err != nil {
							s.gApp.Logger.Info("failed to marshal dynamic proto message", "err", err)
							continue
						}
						if s.gApp.Config.Debug {
							s.gApp.Logger.Debug("dynamic proto JSON", "json", string(jsondata))
						}
						update.Val.Value = &gnmi.TypedValue_JsonVal{JsonVal: jsondata}
					}
				}
			}
			for _, o := range s.Outputs {
				go o.Write(s.ctx, subResp, outMeta)
			}

		case *gnmi.SubscribeResponse_SyncResponse:
			s.gApp.Logger.Info("received sync response", "sync_response", resp.SyncResponse, "source", outMeta["source"])
		}
	}
	return nil
}

func (s *dialoutTelemetryServer) SonicPublish(stream grpc.ServerStream) error {
	pr, ok := peer.FromContext(stream.Context())
	if ok && s.gApp.Config.Debug {
		b, err := json.Marshal(pr)
		if err != nil {
			s.gApp.Logger.Debug("failed to marshal peer data", "err", err)
		} else {
			s.gApp.Logger.Debug("received SonicPublish RPC", "peer", string(b))
		}
	}
	md, ok := metadata.FromIncomingContext(stream.Context())
	if ok && s.gApp.Config.Debug {
		b, err := json.Marshal(md)
		if err != nil {
			s.gApp.Logger.Debug("failed to marshal context metadata", "err", err)
		} else {
			s.gApp.Logger.Debug("received http2 headers", "headers", string(b))
		}
	}
	outMeta := outputs.Meta{}
	if sn, ok := md["subscription-name"]; ok {
		if len(sn) > 0 {
			outMeta["subscription-name"] = sn[0]
		}
	}
	outMeta["source"] = pr.Addr.String()
	if systemName, ok := md["system-name"]; ok {
		if len(systemName) > 0 {
			outMeta["system-name"] = systemName[0]
		}
	}
	for {
		subResp := new(gnmi.SubscribeResponse)
		if err := stream.RecvMsg(subResp); err != nil {
			if err != io.EOF {
				s.gApp.Logger.Info("gRPC sonic dialout receive error", "err", err)
			}
			break
		}
		switch resp := subResp.Response.(type) {
		case *gnmi.SubscribeResponse_Update:
			if s.rootDesc != nil {
				for _, update := range resp.Update.Update {
					switch update.Val.Value.(type) {
					case *gnmi.TypedValue_ProtoBytes:
						m := dynamic.NewMessage(s.rootDesc.GetFile().FindMessage("Nokia.SROS.root"))
						err := m.Unmarshal(update.Val.GetProtoBytes())
						if err != nil {
							s.gApp.Logger.Info("failed to unmarshal dynamic proto message", "err", err)
						}
						jsondata, err := m.MarshalJSON()
						if err != nil {
							s.gApp.Logger.Info("failed to marshal dynamic proto message", "err", err)
							continue
						}
						if s.gApp.Config.Debug {
							s.gApp.Logger.Debug("dynamic proto JSON", "json", string(jsondata))
						}
						update.Val.Value = &gnmi.TypedValue_JsonVal{JsonVal: jsondata}
					}
				}
			}
			for _, o := range s.Outputs {
				go o.Write(s.ctx, subResp, outMeta)
			}
		case *gnmi.SubscribeResponse_SyncResponse:
			s.gApp.Logger.Info("received sync response", "sync_response", resp.SyncResponse, "source", outMeta["source"])
		}
	}
	return nil
}
