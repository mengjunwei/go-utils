package services

import (
	"reflect"
	"sort"
)

var Services = make(map[string]*Descriptor)
var overrides []OverrideServiceFunc

type Descriptor struct {
	Name         string
	Instance     Service
	InitPriority Priority
	Detail       interface{}
}

type OverrideServiceFunc func(descriptor Descriptor) (*Descriptor, bool)

func RegisterService(instance Service) {
	d := &Descriptor{
		Name:         reflect.TypeOf(instance).Elem().Name(),
		Instance:     instance,
		InitPriority: Low,
	}
	Register(d)
}

func RegisterServiceDetail(s interface{}) {
	instance := s.(Service)
	d := &Descriptor{
		Name:         reflect.TypeOf(instance).Elem().Name(),
		Instance:     instance,
		InitPriority: Low,
		Detail:       s,
	}
	Register(d)
}

func Register(descriptor *Descriptor) {
	Services[descriptor.Name] = descriptor
}

func GetServices() []*Descriptor {
	slice := getServicesWithOverrides()

	sort.Slice(slice, func(i, j int) bool {
		return slice[i].InitPriority > slice[j].InitPriority
	})

	return slice
}

func GetServicesDetail(name string) (interface{}, bool) {
	ret, ok := Services[name]
	if ok {
		return ret.Detail, ok
	}
	return nil, false
}

func RegisterOverride(fn OverrideServiceFunc) {
	overrides = append(overrides, fn)
}

func getServicesWithOverrides() []*Descriptor {
	slice := []*Descriptor{}
	for _, s := range Services {
		var descriptor *Descriptor
		for _, fn := range overrides {
			if newDescriptor, override := fn(*s); override {
				descriptor = newDescriptor
				break
			}
		}

		if descriptor != nil {
			slice = append(slice, descriptor)
		} else {
			slice = append(slice, s)
		}
	}

	return slice
}
