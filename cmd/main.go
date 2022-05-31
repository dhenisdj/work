package main

import (
	"fmt"
	"github.com/dhenisdj/scheduler/component/context"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"time"
)

func main() {

	//timer1 := time.NewTimer(1 * time.Second)
	//timer2 := time.NewTimer(5 * time.Second)
	//
	//for {
	//	select {
	//	case <-timer1.C:
	//		fmt.Println("process job")
	//		timer1.Reset(1 * time.Second)
	//		select {
	//		case <-timer2.C:
	//			fmt.Println("the interactive session renewing")
	//			timer2.Reset(5 * time.Second)
	//		default:
	//			fmt.Println("session not reach limit")
	//		}
	//	}
	//}

	//ctx := context.New("test_sg", true)
	//fmt.Printf("type %s, required type %s\n", reflect.TypeOf(ctx).Elem().String(), reflect.TypeOf(context.AContext{}))
	//
	//refCtx := reflect.NewAt(reflect.TypeOf(ctx).Elem(), unsafe.Pointer(&ctx))
	//refCtx.MethodByName("TestReflect").Call(nil)
	//refCtx.MethodByName("LE").Call([]reflect.Value{reflect.ValueOf("runJob.panic"), reflect.ValueOf(fmt.Errorf("%s", "error"))})
	//
	//vfn := reflect.ValueOf(handler.LivyHandlerBatch).Type()
	//paramType := vfn.In(0)
	//actuallyType := reflect.TypeOf(ctx)
	//fmt.Printf("actual type %s impl param type %s %s", actuallyType.String(), paramType.String(), actuallyType.Implements(paramType))

	client := http.Client{
		Timeout: time.Second * 30,
	}

	name := "sztoc_audiencemanager"
	pwd := "ufrSvqgAx80d"
	url := "http://livy-rest.data-infra.shopee.io"

	data := ""

	req, err := http.NewRequest("POST", url, strings.NewReader(data))
	if err != nil {
		fmt.Printf("request error with %s", err.Error())
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")
	req.SetBasicAuth(name, pwd)

	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("response error with %s", err.Error())
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("read body error with %s", err.Error())
	}
	fmt.Printf("response %s", body)

}

func pp(ctx2 context.Context) {
	ctxT := reflect.TypeOf(ctx2)
	fmt.Printf("type of ctx2 %s\n", ctxT.String())
}
