package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/je4/filesystem/v3/pkg/vfsrw"
	genericproto "github.com/je4/genericproto/v2/pkg/generic/proto"
	"github.com/je4/mediaserverdeleter/v2/configs"
	"github.com/je4/mediaserverdeleter/v2/pkg/service"
	mediaserveractionclient "github.com/je4/mediaserverproto/v2/pkg/mediaserveraction/client"
	mediaserveractionproto "github.com/je4/mediaserverproto/v2/pkg/mediaserveraction/proto"
	mediaserverdbClient "github.com/je4/mediaserverproto/v2/pkg/mediaserverdb/client"
	mediaserverdbproto "github.com/je4/mediaserverproto/v2/pkg/mediaserverdb/proto"
	pb "github.com/je4/mediaserverproto/v2/pkg/mediaserverdeleter/proto"
	resolverclient "github.com/je4/miniresolver/v2/pkg/client"
	resolverhelper "github.com/je4/miniresolver/v2/pkg/grpchelper"
	"github.com/je4/trustutil/v2/pkg/certutil"
	"github.com/je4/trustutil/v2/pkg/loader"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"io/fs"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

var cfg = flag.String("config", "", "location of toml configuration file")

func main() {
	flag.Parse()
	var cfgFS fs.FS
	var cfgFile string
	if *cfg != "" {
		cfgFS = os.DirFS(filepath.Dir(*cfg))
		cfgFile = filepath.Base(*cfg)
	} else {
		cfgFS = configs.ConfigFS
		cfgFile = "mediaserverdeleter.toml"
	}
	conf := &MediaserverDeleterConfig{
		LocalAddr:   "localhost:8443",
		LogLevel:    "DEBUG",
		Concurrency: 3,
	}
	if err := LoadMediaserverDeleterConfig(cfgFS, cfgFile, conf); err != nil {
		log.Fatalf("cannot load toml from [%v] %s: %v", cfgFS, cfgFile, err)
	}
	// create logger instance
	var out io.Writer = os.Stdout
	if conf.LogFile != "" {
		fp, err := os.OpenFile(conf.LogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("cannot open logfile %s: %v", conf.LogFile, err)
		}
		defer fp.Close()
		out = fp
	}

	/*
		addrs, err := net.InterfaceAddrs()
		if err != nil {
			log.Fatalf("cannot get interface addresses: %v", err)
		}
		addrStr := make([]string, 0, len(addrs))
		for _, addr := range addrs {
			addrStr = append(addrStr, addr.String())
		}
	*/
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("cannot get hostname: %v", err)
	}

	output := zerolog.ConsoleWriter{Out: out, TimeFormat: time.RFC3339}
	_logger := zerolog.New(output).With().Timestamp().Str("service", "mediaserverdeleter"). /*.Array("addrs", zLogger.StringArray(addrStr))*/ Str("host", hostname).Str("addr", conf.LocalAddr).Logger()
	_logger.Level(zLogger.LogLevel(conf.LogLevel))
	var logger zLogger.ZLogger = &_logger
	//	var dbLogger = zerologadapter.NewLogger(_logger)

	vfs, err := vfsrw.NewFS(conf.VFS, logger)
	if err != nil {
		logger.Panic().Err(err).Msg("cannot create vfs")
	}
	defer func() {
		if err := vfs.Close(); err != nil {
			logger.Error().Err(err).Msg("cannot close vfs")
		}
	}()

	// create client TLS certificate
	// the certificate MUST contain "grpc:miniresolverproto.MiniResolver" or "*" in URIs
	clientTLSConfig, clientLoader, err := loader.CreateClientLoader(&conf.ClientTLS, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create client loader")
	}
	defer clientLoader.Close()

	var dbClientAddr, actionControllerClientAddr string
	// create resolver client
	resolver, resolverCloser, err := resolverclient.CreateClient(conf.ResolverAddr, clientTLSConfig)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create resolver client")
	}
	defer resolverCloser.Close()
	resolverhelper.RegisterResolver(resolver, time.Duration(conf.ResolverTimeout), time.Duration(conf.ResolverNotFoundTimeout), logger)

	dbClientAddr = resolverhelper.GetAddress(mediaserverdbproto.DBController_Ping_FullMethodName)
	actionControllerClientAddr = resolverhelper.GetAddress(mediaserveractionproto.ActionController_Ping_FullMethodName)

	logger.Info().Msgf("resolver address is %s", conf.ResolverAddr)
	miniResolverClient, miniResolverCloser, err := resolverclient.CreateClient(conf.ResolverAddr, clientTLSConfig)
	if err != nil {
		logger.Fatal().Msgf("cannot create resolver client: %v", err)
	}
	defer miniResolverCloser.Close()
	resolverhelper.RegisterResolver(miniResolverClient, time.Duration(conf.ResolverTimeout), time.Duration(conf.ResolverNotFoundTimeout), logger)

	dbClient, dbClientConn, err := mediaserverdbClient.CreateClient(dbClientAddr, clientTLSConfig)
	if err != nil {
		logger.Panic().Msgf("cannot create mediaserverdb grpc client: %v", err)
	}
	defer dbClientConn.Close()
	if resp, err := dbClient.Ping(context.Background(), &emptypb.Empty{}); err != nil {
		logger.Error().Msgf("cannot ping mediaserverdb: %v", err)
	} else {
		if resp.GetStatus() != genericproto.ResultStatus_OK {
			logger.Error().Msgf("cannot ping mediaserverdb: %v", resp.GetStatus())
		} else {
			logger.Info().Msgf("mediaserverdb ping response: %s", resp.GetMessage())
		}
	}

	actionControllerClient, actionControllerClientConn, err := mediaserveractionclient.CreateControllerClient(actionControllerClientAddr, clientTLSConfig)
	if err != nil {
		logger.Panic().Msgf("cannot create mediaserveractioncontroller grpc client: %v", err)
	}
	defer actionControllerClientConn.Close()
	if resp, err := actionControllerClient.Ping(context.Background(), &emptypb.Empty{}); err != nil {
		logger.Error().Msgf("cannot ping mediaserveractioncontroller: %v", err)
	} else {
		if resp.GetStatus() != genericproto.ResultStatus_OK {
			logger.Error().Msgf("cannot ping mediaserveractioncontroller: %v", resp.GetStatus())
		} else {
			logger.Info().Msgf("mediaserveractioncontroller ping response: %s", resp.GetMessage())
		}
	}

	host, portStr, err := net.SplitHostPort(conf.LocalAddr)
	if err != nil {
		logger.Fatal().Err(err).Msgf("invalid addr '%s' in config", conf.LocalAddr)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		logger.Fatal().Err(err).Msgf("invalid port '%s'", portStr)
	}
	srv, err := service.NewDeleterController(host, uint32(port), vfs, dbClient, actionControllerClient, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create service")
	}

	// create TLS Certificate.
	// the certificate MUST contain <package>.<service> as DNS name
	certutil.AddDefaultDNSNames(resolverhelper.GetService(pb.DeleterController_Ping_FullMethodName))
	serverTLSConfig, serverLoader, err := loader.CreateServerLoader(true, &conf.ServerTLS, nil, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create server loader")
	}
	defer serverLoader.Close()

	// create grpc server with resolver for name resolution
	grpcServer, err := resolverhelper.NewServer(conf.LocalAddr, serverTLSConfig, resolver, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create server")
	}
	// register the server
	pb.RegisterDeleterControllerServer(grpcServer, srv)

	grpcServer.Startup()

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	fmt.Println("press ctrl+c to stop server")
	s := <-done
	fmt.Println("got signal:", s)

	defer grpcServer.Shutdown()

}
