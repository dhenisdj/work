package utils

import (
	"crypto/md5"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"io"
	rand2 "math/rand"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func CleanKeyspace(namespace string, pool *redis.Pool) {
	conn := pool.Get()
	defer conn.Close()

	keys, err := redis.Strings(conn.Do("KEYS", namespace+"*"))
	if err != nil {
		panic("could not get keys: " + err.Error())
	}
	for _, k := range keys {
		if _, err := conn.Do("DEL", k); err != nil {
			panic("could not del: " + err.Error())
		}
	}
}

func ValidateEnv(env string) bool {
	env = strings.ToLower(env)
	if env == "live" || env == "uat" {
		return true
	}
	if !(strings.Contains(env, "test") && strings.Contains(env, "_")) {
		panic("Test env for configuration initialization key must be test_{cid} format\n")
	}
	return true
}

func Contain(target interface{}, obj interface{}) bool {
	targetValue := reflect.ValueOf(target)
	switch reflect.TypeOf(target).Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < targetValue.Len(); i++ {
			if targetValue.Index(i).Interface() == obj {
				return true
			}
		}
	case reflect.Map:
		if targetValue.MapIndex(reflect.ValueOf(obj)).IsValid() {
			return true
		}
	}

	return false
}

func CreateNonce() string {
	h := md5.New()
	now := time.Now().Unix()
	io.WriteString(h, strconv.FormatInt(now, 10))
	io.WriteString(h, strconv.FormatInt(rand2.Int63(), 10))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// ValidateContextType will panic if context is invalid
func ValidateContextType(ctxType reflect.Type) {
	if ctxType.Kind() == reflect.Ptr {
		ctxType = ctxType.Elem()
	}
	if ctxType.Kind() != reflect.Struct {
		panic(fmt.Sprintf("work: Context %s needs to be a struct type", ctxType.Kind()))
	}
}

// Since it's easy to pass the wrong method as a middleware/handler, and since the user can't rely on static type checking since we use reflection,
// lets be super helpful about what they did and what they need to do.
// Arguments:
//  - vfn is the failed method
//  - addingType is for "You are adding {addingType} to a work pool...". Eg, "middleware" or "a handler"
//  - yourType is for "Your {yourType} function can have...". Eg, "middleware" or "handler" or "error handler"
//  - args is like "rw web.ResponseWriter, req *web.Request, next web.NextMiddlewareFunc"
//    - NOTE: args can be calculated if you pass in each type. BUT, it doesn't have example argument name, so it has less copy/paste value.
func InstructiveMessage(vfn reflect.Value, addingType string, yourType string, args string, ctxType reflect.Type) string {
	// Get context type without package.
	ctxString := ctxType.String()
	splitted := strings.Split(ctxString, ".")
	if len(splitted) <= 1 {
		ctxString = splitted[0]
	} else {
		ctxString = splitted[1]
	}

	str := "\n" + strings.Repeat("*", 120) + "\n"
	str += "* You are adding " + addingType + " to a work pool with context type '" + ctxString + "'\n"
	str += "*\n*\n"
	str += "* Your " + yourType + " function can have one of these signatures:\n"
	str += "*\n"
	str += "* // If you don't need context:\n"
	str += "* func YourFunctionName(" + args + ") error\n"
	str += "*\n"
	str += "* // If you want your " + yourType + " to accept a context:\n"
	str += "* func (c " + ctxString + ") YourFunctionName(" + args + ") error  // or,\n"
	str += "* func YourFunctionName(c " + ctxString + ", " + args + ") error\n"
	str += "*\n"
	str += "* Unfortunately, your function has this signature: " + vfn.Type().String() + "\n"
	str += "*\n"
	str += strings.Repeat("*", 120) + "\n"

	return str
}

func GetIP() (ip string, err error) {
	conn, err := net.Dial("udp", "8.8.8.8:53")
	if err != nil {
		fmt.Println(err)
		return
	}
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	ip = strings.Split(localAddr.String(), ":")[0]
	return
}
