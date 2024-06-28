package main

import (
	"emperror.dev/errors"
	"github.com/BurntSushi/toml"
	"github.com/je4/filesystem/v3/pkg/vfsrw"
	loaderConfig "github.com/je4/trustutil/v2/pkg/config"
	"github.com/je4/utils/v2/pkg/config"
	"github.com/je4/utils/v2/pkg/zLogger"
	"io/fs"
	"os"
)

type MediaserverDeleterConfig struct {
	LocalAddr               string                 `toml:"localaddr"`
	ServerDomains           []string               `toml:"serverdomains"`
	SingleService           bool                   `toml:"singleservice"`
	ResolverAddr            string                 `toml:"resolveraddr"`
	ResolverTimeout         config.Duration        `toml:"resolvertimeout"`
	ResolverNotFoundTimeout config.Duration        `toml:"resolvernotfoundtimeout"`
	ServerTLS               loaderConfig.TLSConfig `toml:"servertls"`
	ClientTLS               loaderConfig.TLSConfig `toml:"clienttls"`
	GRPCClient              map[string]string      `toml:"grpcclient"`
	VFS                     map[string]*vfsrw.VFS  `toml:"vfs"`
	Log                     zLogger.Config         `toml:"log"`
}

func LoadMediaserverDeleterConfig(fSys fs.FS, fp string, conf *MediaserverDeleterConfig) error {
	if _, err := fs.Stat(fSys, fp); err != nil {
		path, err := os.Getwd()
		if err != nil {
			return errors.Wrap(err, "cannot get current working directory")
		}
		fSys = os.DirFS(path)
		fp = "mediaserverdeleter.toml"
	}
	data, err := fs.ReadFile(fSys, fp)
	if err != nil {
		return errors.Wrapf(err, "cannot read file [%v] %s", fSys, fp)
	}
	_, err = toml.Decode(string(data), conf)
	if err != nil {
		return errors.Wrapf(err, "error loading config file %v", fp)
	}
	return nil
}
