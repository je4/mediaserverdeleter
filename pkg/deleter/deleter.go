package deleter

import (
	"context"
	"emperror.dev/errors"
	"fmt"
	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/je4/mediaserveraction/v2/pkg/actionCache"
	mediaserverproto "github.com/je4/mediaserverproto/v2/pkg/mediaserver/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
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

func (d *Deleter) DeleteItem(collection, signature string) error {
	return nil
}

func (d *Deleter) DeleteItemCaches(collection, signature string) error {
	return nil
}

var isUrlRegexp = regexp.MustCompile(`^[a-z]+://`)

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

	resp, err := d.db.GetCache(context.Background(), &mediaserverproto.CacheRequest{
		Identifier: &mediaserverproto.ItemIdentifier{
			Collection: collection,
			Signature:  signature,
		},
		Action: action,
		Params: ps.String(),
	})
	if err != nil {
		return errors.Wrapf(err, "error getting cache for %s/%s", collection, signature)
	}
	metadata := resp.GetMetadata()
	fullpath := metadata.GetPath()
	if !isUrlRegexp.MatchString(fullpath) {
		storage := metadata.GetStorage()
		fullpath = fmt.Sprintf("%s/%s", storage.GetFilebase(), strings.TrimPrefix(fullpath, "/"))
	}
	d.logger.Debug().Msgf("deleting file %s", fullpath)
	if err := writefs.Remove(d.vfs, fullpath); err != nil {
		d.logger.Error().Err(err).Msgf("error removing file %s", fullpath)
		return errors.Wrapf(err, "error removing file %s", fullpath)
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
