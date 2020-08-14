package reflect

import (
	"errors"
	"reflect"
)

type Reflect interface {
	AddObj(alias string, obj interface{}, params ...interface{}) ([]string, error)
	Call(alias, method string, req interface{}) error
}

type IReflect struct {
	obj    map[string]interface{}
	params map[string][]reflect.Value
}

func NewReflect() Reflect {
	return &IReflect{obj: make(map[string]interface{}), params: make(map[string][]reflect.Value)}
}

func (r *IReflect) AddObj(alias string, obj interface{}, params ...interface{}) ([]string, error) {
	if _, ok := r.obj[alias]; ok {
		return nil, errors.New("alias already exists")
	}
	r.obj[alias] = obj
	if _, ok := r.params[alias]; !ok {
		r.params[alias] = make([]reflect.Value, len(params)+1)
	}
	for i, _ := range params {
		r.params[alias][i+1] = reflect.ValueOf(params[i])
	}

	ref := reflect.TypeOf(obj)

	var name []string
	for i := 0; i < ref.NumMethod(); i++ {
		//	log.Println("ref add obj",ref.Method(i).Index,ref.Method(i).Name)
		name = append(name, ref.Method(i).Name)
	}

	return name, nil
}

func (r *IReflect) Call(alias, method string, req interface{}) error {
	r.params[alias][0] = reflect.ValueOf(req)
	obj, ok := r.obj[alias]
	if !ok {
		return errors.New("reflect call error alias does not exist")
	}
	reflect.ValueOf(obj).MethodByName(method).Call(r.params[alias])
	return nil

}
