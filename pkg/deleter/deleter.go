package deleter

import (
	"context"
	"emperror.dev/errors"
	"fmt"
	"github.com/je4/filesystem/v3/pkg/writefs"
	genericproto "github.com/je4/genericproto/v2/pkg/generic/proto"
	"github.com/je4/mediaserveraction/v2/pkg/actionCache"
	mediaserverproto "github.com/je4/mediaserverproto/v2/pkg/mediaserver/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io/fs"
	"regexp"
	"strings"
)

func NewDeleter(db mediaserverproto.DatabaseClient, actionController mediaserverproto.ActionClient, vfs fs.FS, logger zLogger.ZLogger) (*Deleter, error) {
	return &Deleter{
		db:               db,
		actionController: actionController,
		vfs:              vfs,
		actionParams:     map[string][]string{},
		logger:           logger,
	}, nil
}

type Deleter struct {
	db               mediaserverproto.DatabaseClient
	vfs              fs.FS
	logger           zLogger.ZLogger
	actionParams     map[string][]string
	actionController mediaserverproto.ActionClient
}

func (d *Deleter) getParams(mediaType string, action string) ([]string, error) {
	sig := fmt.Sprintf("%s::%s", mediaType, action)
	if params, ok := d.actionParams[sig]; ok {
		return params, nil
	}
	resp, err := d.actionController.GetParams(context.Background(), &mediaserverproto.ParamsParam{
		Type:   mediaType,
		Action: action,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get params for %s::%s", mediaType, action)
	}
	d.logger.Debug().Msgf("params for %s::%s: %v", mediaType, action, resp.GetValues())
	d.actionParams[sig] = resp.GetValues()
	return resp.GetValues(), nil
}

var isUrlRegexp = regexp.MustCompile(`^[a-z]+://`)

func (d *Deleter) DeleteItem(collection, signature string) (numItems, numCaches int64, err error) {
	d.logger.Debug().Msgf("deleting item %s/%s", collection, signature)
	var page genericproto.PageRequest = genericproto.PageRequest{
		PageRequest: &genericproto.PageRequest_Page{
			Page: &genericproto.Page{
				PageSize: 100,
				PageNo:   0,
			},
		},
	}
	for {
		itemsResult, err := d.db.GetChildItems(context.Background(), &mediaserverproto.ItemsRequest{
			Identifier: &mediaserverproto.ItemIdentifier{
				Collection: collection,
				Signature:  signature,
			},
			PageRequest: &page,
		})
		if err != nil {
			if status, ok := status.FromError(err); ok && status.Code() == codes.NotFound {
				d.logger.Debug().Msgf("no child items for %s/%s", collection, signature)
				break
			} else {
				return 0, 0, errors.Wrapf(err, "error getting child items for %s/%s", collection, signature)
			}
		}
		for _, item := range itemsResult.GetItems() {
			if ni, nc, err := d.DeleteItem(item.GetIdentifier().GetCollection(), item.GetIdentifier().GetSignature()); err != nil {
				return 0, 0, errors.Wrapf(err, "error deleting item %s/%s", item.GetIdentifier().GetCollection(), item.GetIdentifier().GetSignature())
			} else {
				numItems += ni
				numCaches += nc
			}
		}
		currentPage := itemsResult.GetPageResponse().GetPageResult()
		if currentPage == nil {
			break
		}
	}
	if num, err := d.DeleteItemCaches(collection, signature, true); err != nil {
		return 0, 0, errors.Wrapf(err, "error deleting caches for %s/%s", collection, signature)
	} else {
		numCaches += num
		d.logger.Debug().Msgf("deleted %d caches for %s/%s", num, collection, signature)
	}
	resp, err := d.db.DeleteItem(context.Background(), &mediaserverproto.ItemIdentifier{
		Collection: collection,
		Signature:  signature,
	})
	if err != nil {
		return 0, 0, errors.Wrapf(err, "error deleting item %s/%s", collection, signature)
	}
	numItems++
	d.logger.Debug().Msgf("deleted item %s/%s: [%d] %s", collection, signature, resp.GetStatus(), resp.GetMessage())
	return
}

func (d *Deleter) DeleteItemCaches(collection, signature string, withItem bool) (int64, error) {
	var num int64
	var page genericproto.PageRequest = genericproto.PageRequest{
		PageRequest: &genericproto.PageRequest_Page{
			Page: &genericproto.Page{
				PageSize: 100,
				PageNo:   0,
			},
		},
	}
	for {
		cacheResult, err := d.db.GetCaches(context.Background(), &mediaserverproto.CachesRequest{
			Identifier: &mediaserverproto.ItemIdentifier{
				Collection: collection,
				Signature:  signature,
			},
			PageRequest: &page,
		})
		if err != nil {
			return 0, errors.Wrapf(err, "error getting caches for %s/%s", collection, signature)
		}
		for _, cache := range cacheResult.GetCaches() {
			metadata := cache.GetMetadata()
			action := metadata.GetAction()
			params := metadata.GetParams()
			if !withItem && action == "item" {
				continue
			}
			if err := d.DeleteCache(collection, signature, action, params); err != nil {
				return 0, errors.Wrapf(err, "error deleting cache for %s/%s/%s/%s", collection, signature, action, params)
			}
			num++
		}
		currentPage := cacheResult.GetPageResponse().GetPageResult()

		if currentPage == nil {
			break
		}
		if currentPage.GetPageNo() >= currentPage.GetTotal()-1 {
			break
		}
	}
	return num, nil
}

func (d *Deleter) DeleteCache(collection, signature, action, params string) error {
	item, err := d.db.GetItem(context.Background(), &mediaserverproto.ItemIdentifier{
		Collection: collection,
		Signature:  signature,
	})
	if err != nil {
		return errors.Wrapf(err, "error getting item %s/%s", collection, signature)
	}
	ps := actionCache.ActionParams{}
	aparams, err := d.getParams(item.GetMetadata().GetType(), action)
	if err != nil {
		return errors.Wrapf(err, "error getting params for %s::%s", item.GetMetadata().GetType(), action)
	}
	ps.SetString(params, aparams)

	d.logger.Debug().Msgf("deleting cache %s/%s/%s/%s", collection, signature, action, ps.String())
	resp, err := d.db.GetCache(context.Background(), &mediaserverproto.CacheRequest{
		Identifier: &mediaserverproto.ItemIdentifier{
			Collection: collection,
			Signature:  signature,
		},
		Action: action,
		Params: ps.String(),
	})
	if err != nil {
		if status, ok := status.FromError(err); ok && status.Code() == codes.NotFound {
			d.logger.Debug().Msgf("cache %s/%s/%s/%s not found", collection, signature, action, ps.String())
			return nil
		}
		return errors.Wrapf(err, "error getting cache for %s/%s/%s/%s", collection, signature, action, ps.String())
	}
	metadata := resp.GetMetadata()
	fullpath := metadata.GetPath()
	if !isUrlRegexp.MatchString(fullpath) {
		storage := metadata.GetStorage()
		fullpath = fmt.Sprintf("%s/%s", storage.GetFilebase(), strings.TrimPrefix(fullpath, "/"))
		d.logger.Debug().Msgf("deleting file %s", fullpath)
		if err := writefs.Remove(d.vfs, fullpath); err != nil {
			d.logger.Error().Err(err).Msgf("error removing file %s", fullpath)
			return errors.Wrapf(err, "error removing file %s", fullpath)
		}
	} else {
		d.logger.Debug().Msgf("not deleting url %s", fullpath)
	}
	d.logger.Debug().Msgf("deleting cache %s/%s/%s/%s: %s", collection, signature, action, params, fullpath)
	_, err = d.db.DeleteCache(context.Background(), &mediaserverproto.CacheRequest{
		Identifier: &mediaserverproto.ItemIdentifier{
			Collection: collection,
			Signature:  signature,
		},
		Action: action,
		Params: ps.String(),
	})
	if err != nil {
		d.logger.Error().Err(err).Msgf("error deleting cache for %s/%s/%s/%s", collection, signature, action, params)
		return errors.Wrapf(err, "error deleting cache for %s/%s/%s/%s", collection, signature, action, params)
	}
	return nil

}
