package service

import (
	"context"
	"emperror.dev/errors"
	"fmt"
	generic "github.com/je4/genericproto/v2/pkg/generic/proto"
	"github.com/je4/mediaserverdeleter/v2/pkg/deleter"
	mediaserveractionproto "github.com/je4/mediaserverproto/v2/pkg/mediaserveraction/proto"
	mediaserverdbproto "github.com/je4/mediaserverproto/v2/pkg/mediaserverdb/proto"
	pb "github.com/je4/mediaserverproto/v2/pkg/mediaserverdeleter/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"io/fs"
)

func NewDeleterController(host string, port uint32, vfs fs.FS, db mediaserverdbproto.DBControllerClient, actionController mediaserveractionproto.ActionControllerClient, logger zLogger.ZLogger) (*deleterController, error) {
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
	pb.UnimplementedDeleterControllerServer
	logger  zLogger.ZLogger
	host    string
	port    uint32
	vFS     fs.FS
	db      mediaserverdbproto.DBControllerClient
	deleter *deleter.Deleter
}

func (*deleterController) Ping(context.Context, *emptypb.Empty) (*generic.DefaultResponse, error) {
	return &generic.DefaultResponse{
		Status:  generic.ResultStatus_OK,
		Message: "pong",
		Data:    nil,
	}, nil
}

func (*deleterController) DeleteItem(context.Context, *mediaserverdbproto.ItemIdentifier) (*generic.DefaultResponse, error) {
	return &generic.DefaultResponse{
		Status:  generic.ResultStatus_Error,
		Message: "not implemented",
		Data:    nil,
	}, nil
}
func (*deleterController) DeleteItemCaches(context.Context, *mediaserverdbproto.ItemIdentifier) (*generic.DefaultResponse, error) {
	return &generic.DefaultResponse{
		Status:  generic.ResultStatus_Error,
		Message: "not implemented",
		Data:    nil,
	}, nil
}
func (dc *deleterController) DeleteCache(ctx context.Context, cr *mediaserverdbproto.CacheRequest) (*generic.DefaultResponse, error) {
	if err := dc.deleter.DeleteCache(cr.GetIdentifier().GetCollection(), cr.GetIdentifier().GetSignature(), cr.GetAction(), cr.GetParams()); err != nil {
		return nil, status.Errorf(codes.Internal, "error deleting cache %s/%s/%s/%s: %v", cr.GetIdentifier().GetCollection(), cr.GetIdentifier().GetSignature(), cr.GetAction(), cr.GetParams(), err)
	}
	return &generic.DefaultResponse{
		Status:  generic.ResultStatus_OK,
		Message: fmt.Sprintf("cache %s/%s/%s/%s deleted", cr.GetIdentifier().GetCollection(), cr.GetIdentifier().GetSignature(), cr.GetAction(), cr.GetParams()),
		Data:    nil,
	}, nil
}
