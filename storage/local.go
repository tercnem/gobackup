package storage

import (
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/gobackup/gobackup/helper"
	"github.com/gobackup/gobackup/logger"
)

// Local storage
//
// type: local
// path: /data/backups
type Local struct {
	Base
	path string
}

func (s *Local) open() error {
	s.path = s.viper.GetString("path")
	return helper.MkdirP(s.path)
}

func (s *Local) close() {}

func (s *Local) upload(fileKey string) (err error) {
	logger := logger.Tag("Local")

	// Related path
	if !path.IsAbs(s.path) {
		s.path = path.Join(s.model.WorkDir, s.path)
	}

	targetPath := path.Join(s.path, fileKey)
	targetDir := path.Dir(targetPath)
	helper.MkdirP(targetDir)

	_, err = helper.Exec("cp", "-a", s.archivePath, targetPath)
	if err != nil {
		return err
	}

	if s.viper != nil && s.viper.GetBool("google_drive_sync") {
		logger.Info("Sync google drive")
		_, err = helper.Exec("rclone", "sync", targetDir, s.viper.GetString("google_drive_rclone_remote_name")+":"+s.model.Name)
		if err != nil {
			logger.Info("error sync google drive")
			return err
		}
		logger.Info("success sync google drive")
	}
	logger.Info("Store succeeded", targetPath)
	return nil
}

func (s *Local) delete(fileKey string) (err error) {
	targetPath := filepath.Join(s.path, fileKey)
	logger.Info("Deleting", targetPath)

	return os.Remove(targetPath)
}

// List all files
func (s *Local) list(parent string) ([]FileItem, error) {
	remotePath := filepath.Join(s.path, parent)
	var items = []FileItem{}

	files, err := os.ReadDir(remotePath)
	if err != nil {
		return nil, err
	}

	for _, de := range files {
		if !de.IsDir() {
			file, err := de.Info()
			if err != nil {
				return nil, err
			}
			items = append(items, FileItem{
				Filename:     file.Name(),
				Size:         file.Size(),
				LastModified: file.ModTime(),
			})
		}

	}

	return items, nil
}

func (s *Local) download(fileKey string) (string, error) {
	return "", fmt.Errorf("Local is not support download")
}
