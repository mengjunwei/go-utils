package services

import (
	"context"

	"github.com/mengjunwei/go-utils/logger"
)

type Priority int

const (
	High Priority = 100
	Low  Priority = 0
)

var (
	logInstance logger.Logger
)

func init() {
	logInstance = logger.NewNonLogger()
}

type Service interface {
	Init() error
}

type CanBeDisabled interface {
	IsDisabled() bool
}

type BackgroundService interface {
	Run(ctx context.Context) error
}

func IsDisabled(srv Service) bool {
	canBeDisabled, ok := srv.(CanBeDisabled)
	return ok && canBeDisabled.IsDisabled()
}
