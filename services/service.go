package services

import (
	"context"
)

type Priority int

const (
	High Priority = 100
	Low  Priority = 0
)

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
