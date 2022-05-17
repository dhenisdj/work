package common

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/dhenisdj/scheduler/component/utils"
	"github.com/pkg/errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Caller struct {
	Client http.Client `json:"client"`
}

func NewCaller() *Caller {
	return &Caller{
		Client: http.Client{
			Timeout: time.Second * 30,
		},
	}
}

// SignedURL can change attributes of cl
func (cl *Caller) SignedURL(appKey, appSecret, rawUrl, uri, executor, businessGroup string) string {
	rawUrl = rawUrl + uri

	nonce := utils.CreateNonce()
	timeStamp := strconv.FormatInt(time.Now().Unix(), 10)
	text := strings.Join([]string{appKey, nonce, timeStamp}, "|")
	mac := hmac.New(sha1.New, []byte(appSecret))
	mac.Write([]byte(text))
	sign := hex.EncodeToString(mac.Sum(nil))

	//fmt.Printf("sign %s\n", sign)

	params := url.Values{}
	Url, err := url.Parse(rawUrl)
	if err != nil {
		fmt.Printf("Error building url path: %s", err.Error())
	}

	if executor == "SyncDistributionCommon" {
		params.Set("business_name", businessGroup)
	} else {
		params.Set("business_group", businessGroup)
	}
	params.Set("executor", executor)
	params.Set("nonce", nonce)
	params.Set("timestamp", timeStamp)
	params.Set("sign", sign)

	Url.RawQuery = params.Encode()
	urlPath := Url.String()

	return urlPath
}

func (cl *Caller) Request(method, url string, data interface{}) (*http.Request, error) {
	body, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	stringBody := string(body)
	if data == nil {
		stringBody = ""
	}

	request, err := http.NewRequest(method, url, strings.NewReader(stringBody))
	if err != nil {
		return nil, err
	}

	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Accept", "application/json")
	return request, nil
}

func (cl *Caller) DoRequest(req *http.Request) (*http.Response, error) {
	resp, err := cl.Client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, errors.New(fmt.Sprintf("Http status code %d mesage %s", resp.StatusCode, resp.Body))
	}

	return resp, nil
}

func (cl *Caller) BatchSessionInit() error {
	return nil
}

func (cl *Caller) BatchSessionState() error {
	return nil
}

func (cl *Caller) BatchSessionInfo() error {
	return nil
}

func (cl *Caller) BatchSessionLogs() error {
	return nil
}

func (cl *Caller) BatchSessionDel() error {
	return nil
}

func (cl *Caller) InteractiveSessionInit() error {
	return nil
}

func (cl *Caller) InteractiveSessionState() error {
	return nil
}

func (cl *Caller) InteractiveSessionInfo() error {
	return nil
}

func (cl *Caller) InteractiveSessionLogs() error {
	return nil
}

func (cl *Caller) InteractiveSessionDel() error {
	return nil
}
