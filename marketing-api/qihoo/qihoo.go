package qihoo

import (
	"encoding/json"
	"fmt"
	"github.com/golang-module/carbon/v2"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

var (
	ApiUrl = "https://api.e.360.cn"
)

// 客户登录
func UcAccountClientLogin(apiKey string, argv map[string]string) (string, error) {

	params := url.Values{}
	for k, v := range argv {
		params.Add(k, v)
	}

	formData := params.Encode()

	r, _ := http.NewRequest("POST", fmt.Sprintf("%s/uc/account/clientLogin", ApiUrl), strings.NewReader(formData)) // URL-encoded payload
	r.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	r.Header.Add("apiKey", apiKey)

	client := &http.Client{}
	resp, err := client.Do(r)
	if err != nil {
		return ``, err
	}

	defer resp.Body.Close()

	cookies := []string{}
	for _, cookie := range resp.Cookies() {
		cookies = append(cookies, fmt.Sprintf("%s=%s", cookie.Name, cookie.Value))
	}

	// 读取响应体
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ``, err
	}
	// 输出响应
	return string(body), nil
}

// 获取账户详情
func UcAccountGetInfo(apiKey, accessToken string, argv map[string]string) (string, error) {

	params := url.Values{}
	for k, v := range argv {
		params.Add(k, v)
	}

	formData := params.Encode()

	r, _ := http.NewRequest("POST", fmt.Sprintf("%s/uc/account/getInfo", ApiUrl), strings.NewReader(formData)) // URL-encoded payload
	r.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	r.Header.Add("apiKey", apiKey)
	r.Header.Add("accessToken", accessToken)

	client := &http.Client{}
	resp, err := client.Do(r)
	if err != nil {
		return ``, err
	}

	defer resp.Body.Close()

	cookies := []string{}
	for _, cookie := range resp.Cookies() {
		cookies = append(cookies, fmt.Sprintf("%s=%s", cookie.Name, cookie.Value))
	}

	// 读取响应体
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ``, err
	}
	// 输出响应
	return string(body), nil
}

func UcAccountGetmccuserlist(apiKey, accessToken string, argv map[string]string) (string, error) {

	params := url.Values{}
	for k, v := range argv {
		params.Add(k, v)
	}

	formData := params.Encode()

	r, _ := http.NewRequest("POST", fmt.Sprintf("%s/uc/account/Getmccuserlist", ApiUrl), strings.NewReader(formData)) // URL-encoded payload
	r.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	r.Header.Add("apiKey", apiKey)
	r.Header.Add("accessToken", accessToken)

	client := &http.Client{}
	resp, err := client.Do(r)
	if err != nil {
		return ``, err
	}

	defer resp.Body.Close()

	cookies := []string{}
	for _, cookie := range resp.Cookies() {
		cookies = append(cookies, fmt.Sprintf("%s=%s", cookie.Name, cookie.Value))
	}

	// 读取响应体
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ``, err
	}
	// 输出响应
	return string(body), nil
}

func DisplayAdvertiser(apiKey, accessToken string, argv map[string]string) (string, error) {

	params := url.Values{}
	for k, v := range argv {
		params.Add(k, v)
	}

	formData := params.Encode()

	r, _ := http.NewRequest("GET", fmt.Sprintf("%s/display/advertiser/getinfobyid", ApiUrl), strings.NewReader(formData)) // URL-encoded payload
	r.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	r.Header.Add("apiKey", apiKey)
	r.Header.Add("accessToken", accessToken)

	client := &http.Client{}
	resp, err := client.Do(r)
	if err != nil {
		return ``, err
	}

	defer resp.Body.Close()

	cookies := []string{}
	for _, cookie := range resp.Cookies() {
		cookies = append(cookies, fmt.Sprintf("%s=%s", cookie.Name, cookie.Value))
	}

	// 读取响应体
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ``, err
	}
	// 输出响应
	return string(body), nil
}

type Cost struct {
	Date         string   `json:"date"`
	CampaignId   int      `json:"campaignId"`
	CampaignName string   `json:"campaignName"`
	CampaignType string   `json:"campaignType"`
	GroupId      int      `json:"groupId"`
	GroupName    string   `json:"groupName"`
	CreativeId   int      `json:"creativeId"`
	CreativeName string   `json:"creativeName"`
	Link         string   `json:"link"`
	Shows        int      `json:"shows"`
	Clicks       int      `json:"clicks"`
	Cost         float64  `json:"cost"`
	MaterialUrls []string `json:"materialUrls"`
}

type ReportCostResponse struct {
	Count int     `json:"count"`
	Cost  []*Cost `json:"cost"`
}

// 账户报表
func ReportAdRealTimeCost(apiKey, accessToken string, argv map[string]string) (*ReportCostResponse, error) {
	params := url.Values{}
	for k, v := range argv {
		params.Add(k, v)
	}
	formData := params.Encode()

	// 实时
	erportType := "Adrealtimecost"
	if carbon.Now().StartOfDay().Timestamp() >= carbon.Parse(argv["endDate"]).Timestamp() {
		erportType = "Adofflinecost" // 离线
	}

	r, _ := http.NewRequest("POST", fmt.Sprintf("%s/display/report/%s", ApiUrl, erportType), strings.NewReader(formData)) // URL-encoded payload
	r.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	r.Header.Add("apiKey", apiKey)
	r.Header.Add("accessToken", accessToken)

	client := &http.Client{}
	resp, err := client.Do(r)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	// 读取响应体
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	reportCostResponse := &ReportCostResponse{}
	if err := json.Unmarshal(body, &reportCostResponse); err != nil {
		return nil, err
	}

	// 输出响应
	return reportCostResponse, nil
}

// 消耗数据
func ReportCost(apiKey, accessToken string, argv map[string]string) (string, error) {
	params := url.Values{}
	for k, v := range argv {
		params.Add(k, v)
	}

	formData := params.Encode()
	r, _ := http.NewRequest("POST", fmt.Sprintf("%s/display/report/cost", ApiUrl), strings.NewReader(formData)) // URL-encoded payload
	r.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	r.Header.Add("apiKey", apiKey)
	r.Header.Add("accessToken", accessToken)

	client := &http.Client{}
	resp, err := client.Do(r)
	if err != nil {
		return ``, err
	}

	defer resp.Body.Close()

	cookies := []string{}
	for _, cookie := range resp.Cookies() {
		cookies = append(cookies, fmt.Sprintf("%s=%s", cookie.Name, cookie.Value))
	}

	// 读取响应体
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ``, err
	}
	// 输出响应
	return string(body), nil
}
