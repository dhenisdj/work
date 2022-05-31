package api

import (
	"encoding/json"
	"fmt"
	"github.com/dhenisdj/scheduler/component/common/entities"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type API struct {
	Client http.Client
	Livy   *entities.Livy
	retry  int
}

func NewAPI(livy *entities.Livy) *API {
	if livy.Retry <= 1 {
		panic(fmt.Errorf("api retry config must be greater than 1"))
	}
	return &API{
		Client: http.Client{
			Timeout: time.Second * 30,
		},
		Livy:  livy,
		retry: livy.Retry,
	}
}

func (a *API) Request(method, url string, data interface{}) (*http.Request, error) {
	var request *http.Request

	for i := 1; i <= a.retry; i++ {
		body, err := json.Marshal(data)
		if i >= a.retry && err != nil {
			return nil, fmt.Errorf("retry %d times marshal json error when build request %s", i, body)
		}
		stringBody := string(body)
		if data == nil {
			stringBody = ""
		}

		request, err = http.NewRequest(method, url, strings.NewReader(stringBody))
		if i >= a.retry && err != nil {
			return nil, fmt.Errorf("retry %d times new request error when build request %s", i, stringBody)
		}
	}

	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Accept", "application/json")
	return request, nil
}

func (a *API) DoRequest(req *http.Request) (*http.Response, error) {
	var response *http.Response
	var err error
	for i := 1; i <= a.retry; i++ {
		response, err = a.Client.Do(req)
		if i >= a.retry && err != nil {
			body, _ := ioutil.ReadAll(response.Body)
			return nil, fmt.Errorf("retry %d times with status code %d mesage %s response %s", i, response.StatusCode, response.Status, body)
		}
	}

	return response, nil
}

func (a *API) buildBatchRequest(method, uri string, account *entities.Account, args map[string]interface{}) (*http.Request, error) {
	name := account.Name
	pwd := account.Password
	rest := a.Livy.Rest
	protocol := rest.Protocol
	baseUrl := rest.Url
	var data interface{}

	link := fmt.Sprintf("%s://%s%s", protocol, baseUrl, uri)
	if args != nil {
		data = args
	}
	req, err := a.Request(method, link, data)
	if err != nil {
		return nil, fmt.Errorf("error build livy batch session request: %s", err.Error())
	}
	req.SetBasicAuth(name, pwd)
	return req, nil
}

func (a *API) BatchSessionInit(account *entities.Account, args map[string]interface{}) (*entities.LivyBatchSession, error) {
	uri := a.Livy.Rest.Single.Create
	req, err := a.buildBatchRequest("POST", uri, account, args)

	if err != nil {
		return nil, fmt.Errorf("BatchSessionInit build request error %s", err.Error())
	}

	resp, err := a.DoRequest(req)
	if err != nil {
		return nil, fmt.Errorf("BatchSessionInit do request error %s", err.Error())
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("BatchSessionInit read body %s error with %s", body, err.Error())
	}

	session := &entities.LivyBatchSession{}
	err = json.Unmarshal(body, session)
	if err != nil {
		return nil, fmt.Errorf("BatchSessionInit unmarshal body %s error with %s", body, err.Error())
	}

	return session, nil
}

func (a *API) BatchSessionState(id int, account *entities.Account) (*entities.LivyBatchSession, error) {
	uri := a.Livy.Rest.Single.State
	uri = strings.ReplaceAll(uri, "{batchId}", strconv.Itoa(id))
	req, err := a.buildBatchRequest("GET", uri, account, nil)

	if err != nil {
		return nil, fmt.Errorf("BatchSessionState error %s", err.Error())
	}

	resp, err := a.DoRequest(req)

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("BatchSessionState read body %s error with %s", body, err.Error())
	}

	session := &entities.LivyBatchSession{}
	err = json.Unmarshal(body, session)
	if err != nil {
		return nil, fmt.Errorf("BatchSessionState unmarshal body %s error with %s", body, err.Error())
	}

	return session, nil
}

func (a *API) BatchSessionInfo(id int, account *entities.Account) (*entities.LivyBatchSession, error) {
	uri := a.Livy.Rest.Single.Get
	uri = strings.ReplaceAll(uri, "{batchId}", strconv.Itoa(id))
	req, err := a.buildBatchRequest("GET", uri, account, nil)

	if err != nil {
		return nil, fmt.Errorf("BatchSessionInfo error %s", err.Error())
	}

	resp, err := a.DoRequest(req)

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("BatchSessionInfo read body %s error with %s", body, err.Error())
	}

	session := &entities.LivyBatchSession{}
	err = json.Unmarshal(body, session)
	if err != nil {
		return nil, fmt.Errorf("BatchSessionInfo unmarshal body %s error with %s", body, err.Error())
	}

	return session, nil
}

func (a *API) BatchSessionLogs(logType string, id, from, size int, account *entities.Account) (*entities.LivyBatchSession, error) {
	uri := a.Livy.Rest.Single.Logs
	uri = strings.ReplaceAll(uri, "{type}", logType)
	uri = strings.ReplaceAll(uri, "{batchId}", strconv.Itoa(id))
	uri = strings.ReplaceAll(uri, "{start}", strconv.Itoa(from))
	uri = strings.ReplaceAll(uri, "{logSize}", strconv.Itoa(size))
	req, err := a.buildBatchRequest("GET", uri, account, nil)

	if err != nil {
		return nil, fmt.Errorf("BatchSessionLogs error %s", err.Error())
	}

	resp, err := a.DoRequest(req)

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("BatchSessionLogs read body %s error with %s", body, err.Error())
	}

	logs := &entities.LivyBatchSession{}
	err = json.Unmarshal(body, logs)
	if err != nil {
		return nil, fmt.Errorf("BatchSessionLogs unmarshal body %s error with %s", body, err.Error())
	}

	return logs, nil
}

func (a *API) BatchSessionDel(id int, account *entities.Account) (*entities.LivyBatchSession, error) {
	uri := a.Livy.Rest.Single.Delete
	strings.ReplaceAll(uri, "{batchId}", strconv.Itoa(id))
	req, err := a.buildBatchRequest("DELETE", uri, account, nil)

	if err != nil {
		return nil, fmt.Errorf("BatchSessionDel error %s", err.Error())
	}

	resp, err := a.DoRequest(req)

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("BatchSessionDel read body %s error with %s", body, err.Error())
	}

	del := &entities.LivyBatchSession{}
	err = json.Unmarshal(body, del)
	if err != nil {
		return nil, fmt.Errorf("BatchSessionDel unmarshal body %s error with %s", body, err.Error())
	}

	return del, nil
}

func (a *API) InteractiveSessionInit(account *entities.Account, thrift *entities.LivyThrift, args map[string]interface{}) error {
	return nil
}

func (a *API) InteractiveSessionState(account *entities.Account, thrift *entities.LivyThrift, args map[string]interface{}) error {
	return nil
}

func (a *API) InteractiveSessionInfo(account *entities.Account, thrift *entities.LivyThrift, args map[string]interface{}) error {
	return nil
}

func (a *API) InteractiveSessionLogs(account *entities.Account, thrift *entities.LivyThrift, args map[string]interface{}) error {
	return nil
}

func (a *API) InteractiveSessionDel(account *entities.Account, thrift *entities.LivyThrift, args map[string]interface{}) error {
	return nil
}
