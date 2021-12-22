package zkhelper

import (
	"fmt"
	"strings"

	"github.com/samuel/go-zookeeper/zk"

	"github.com/mengjunwei/go-utils/logger"
)

const (
	rootPath      = "/go-utils/common"
	leaderPath    = "leader"
	discoveryPath = "discovery"

	sessionTimeout = 15
)

var (
	logInstance logger.Logger
)

func init() {
	logInstance = logger.NewNonLogger()
}

func SetLogger(logger logger.Logger) {
	logInstance = logger
}

//相当于 mkdir -p
func makePath(path string, zkConn *zk.Conn, flags int32, acl []zk.ACL) (bool, error) {
	exists, _, err := zkConn.Exists(path)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}

	paths := strings.Split(path, "/")
	tempPath := ""
	for i, p := range paths {
		if i == 0 {
			continue
		}
		tempPath = fmt.Sprintf("%s/%s", tempPath, p)
		if err := doMakePath(tempPath, zkConn, flags, acl); err != nil {
			return false, err
		}
	}

	return true, nil
}

func doMakePath(path string, zkConn *zk.Conn, flags int32, acl []zk.ACL) error {
	exists, _, err := zkConn.Exists(path)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	_, err = zkConn.Create(path, []byte(path), flags, acl)
	return err
}
