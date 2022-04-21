package work

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	URIInternalTaskPoll   = "/internal/task/poll"
	URIInternalTaskUpdate = "/internal/task"

	URIDependenciesPoll   = "/open/dep_table/list"
	URIDependenciesUpdate = "/open/dep_table/update_batch"
)

type Caller struct {
	Client http.Client `json:"client"`
}

func newCaller() *Caller {
	return &Caller{
		Client: http.Client{
			Timeout: time.Second * 30,
		},
	}
}

// SignedURL can change attributes of cl
func (cl *Caller) signedURL(appKey, appSecret, rawUrl, uri, executor, businessGroup string) string {
	rawUrl = rawUrl + uri

	nonce := createNonce()
	timeStamp := strconv.FormatInt(time.Now().Unix(), 10)
	text := strings.Join([]string{appKey, nonce, timeStamp}, "|")
	mac := hmac.New(sha1.New, []byte(appSecret))
	mac.Write([]byte(text))
	sign := hex.EncodeToString(mac.Sum(nil))

	fmt.Printf("sign %s\n", sign)

	params := url.Values{}
	Url, err := url.Parse(rawUrl)
	if err != nil {
		log.Fatal("Error building url path: ", err)
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

func (cl *Caller) request(method, url string, data interface{}) (*http.Request, error) {
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

func (cl *Caller) doRequest(req *http.Request) (*http.Response, error) {
	resp, err := cl.Client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, errors.New(fmt.Sprintf("Http status code %d mesage %s", resp.StatusCode, resp.Body))
	}

	return resp, nil
}
