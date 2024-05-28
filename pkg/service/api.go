package service

import (
	"context"
	"emperror.dev/errors"
	"fmt"
	generic "github.com/je4/genericproto/v2/pkg/generic/proto"
	"github.com/je4/mediaserverdeleter/v2/pkg/deleter"
	mediaserverproto "github.com/je4/mediaserverproto/v2/pkg/mediaserver/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"io/fs"
)

func NewDeleterController(host string, port uint32, vfs fs.FS, db mediaserverproto.DatabaseClient, actionController mediaserverproto.ActionClient, logger zLogger.ZLogger) (*deleterController, error) {
	_logger := logger.With().Str("rpcService", "deleterController").Logger()
	del, err := deleter.NewDeleter(db, actionController, vfs, logger)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create deleter")
	}
	return &deleterController{
		host:    host,
		port:    port,
		vFS:     vfs,
		db:      db,
		deleter: del,
		logger:  &_logger,
	}, nil
}

type deleterController struct {
	mediaserverproto.UnimplementedDeleterServer
	logger  zLogger.ZLogger
	host    string
	port    uint32
	vFS     fs.FS
	db      mediaserverproto.DatabaseClient
	deleter *deleter.Deleter
}

func (*deleterController) Ping(context.Context, *emptypb.Empty) (*generic.DefaultResponse, error) {
	return &generic.DefaultResponse{
		Status:  generic.ResultStatus_OK,
		Message: "pong",
		Data:    nil,
	}, nil
}

func (*deleterController) DeleteItem(context.Context, *mediaserverproto.ItemIdentifier) (*generic.DefaultResponse, error) {
	return &generic.DefaultResponse{
		Status:  generic.ResultStatus_Error,
		Message: "not implemented",
		Data:    nil,
	}, nil
}
func (*deleterController) DeleteItemCaches(context.Context, *mediaserverproto.ItemIdentifier) (*generic.DefaultResponse, error) {
	return &generic.DefaultResponse{
		Status:  generic.ResultStatus_Error,
		Message: "not implemented",
		Data:    nil,
	}, nil
}
func (dc *deleterController) DeleteCache(ctx context.Context, cr *mediaserverproto.CacheRequest) (*generic.DefaultResponse, error) {
	if err := dc.deleter.DeleteCache(cr.GetIdentifier().GetCollection(), cr.GetIdentifier().GetSignature(), cr.GetAction(), cr.GetParams()); err != nil {
		return nil, status.Errorf(codes.Internal, "error deleting cache %s/%s/%s/%s: %v", cr.GetIdentifier().GetCollection(), cr.GetIdentifier().GetSignature(), cr.GetAction(), cr.GetParams(), err)
	}
	return &generic.DefaultResponse{
		Status:  generic.ResultStatus_OK,
		Message: fmt.Sprintf("cache %s/%s/%s/%s deleted", cr.GetIdentifier().GetCollection(), cr.GetIdentifier().GetSignature(), cr.GetAction(), cr.GetParams()),
		Data:    nil,
	}, nil
}
