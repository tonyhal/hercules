package bilibili

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/golang-module/carbon/v2"
	"github.com/tonyhal/hercules/utils"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

var (
	apiUrl = "https://cm.bilibili.com/takumi/api"
)

func getSign(values map[string]interface{}, token string) string {

	params := []string{}
	for k, v := range values {
		if k == "sign" {
			continue
		}
		switch v.(type) {
		case []int:
			marshal, _ := json.Marshal(v.([]int))
			params = append(params, fmt.Sprintf("%s=%v", k, string(marshal)))
		default:
			params = append(params, fmt.Sprintf("%s=%v", k, v))
		}

	}

	sort.Strings(params)

	paramsStr := strings.ReplaceAll(strings.Join(params, "&"), "\"", "")

	return utils.Md5(fmt.Sprintf("%s%s", paramsStr, token))
}

// 生成/刷新token
func GenOauthToken(accountId int64, appkey, secret string) (string, error) {
	values := map[string]interface{}{
		"account_id": accountId,
		"appkey":     appkey,
		"ts":         carbon.Now().TimestampMilli(),
	}
	// 签名
	values["sign"] = getSign(values, secret)
	// PUT请求的Body
	valuesJson, _ := json.Marshal(values)
	// 创建PUT请求
	req, err := http.NewRequest("PUT", fmt.Sprintf("%s%s", apiUrl, "/open_api/v2/auth/refresh_token"), bytes.NewBufferString(string(valuesJson)))
	if err != nil {
		return ``, err
	}
	// 设置Content-Type
	req.Header.Set("Content-Type", "application/json")
	// 发送请求
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return ``, err
	}
	defer resp.Body.Close()

	// 读取响应体
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ``, err
	}
	// 输出响应
	return string(responseBody), nil
}

// 获取appkey对应的所有账户信息
func AuthAccountIds(appkey, token string, page int) (string, error) {
	values := map[string]interface{}{
		"appkey": appkey,
		"page":   page,
		"size":   1000,
		"ts":     carbon.Now().TimestampMilli(),
	}
	// 签名
	values["sign"] = getSign(values, token)

	// GET请求的参数
	params := url.Values{}
	for k, v := range values {
		params.Add(k, fmt.Sprintf("%v", v))
	}

	// 创建PUT请求
	req, err := http.NewRequest("GET", fmt.Sprintf("%s%s?%s", apiUrl, "/open_api/v2/auth/account_ids", params.Encode()), nil)
	if err != nil {
		return ``, err
	}
	// 设置Content-Type
	req.Header.Set("Content-Type", "application/json")
	// 发送请求
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return ``, err
	}
	defer resp.Body.Close()

	// 读取响应体
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ``, err
	}

	// 输出响应
	return string(responseBody), nil
}

// 批量查询账号消耗数据
func ReportV3AccountConsumeData(appkey, token string, accountids []string, timestamp string) (string, error) {
	values := map[string]interface{}{
		"appkey":      appkey,
		"ts":          carbon.Now().TimestampMilli(),
		"account_ids": strings.Join(accountids, ","),
		//"start_time":  carbon.Now().StartOfDay().ToShortDateString(),
		"start_time": timestamp,
		"end_time":   timestamp,
	}
	// 签名
	values["sign"] = getSign(values, token)
	// GET请求的参数
	params := url.Values{}
	for k, v := range values {
		params.Add(k, fmt.Sprintf("%v", v))
	}

	// 创建PUT请求
	req, err := http.NewRequest("GET", fmt.Sprintf("%s%s?%s", apiUrl, "/open_api/report/v3/account/consume_data", params.Encode()), nil)
	if err != nil {
		return ``, err
	}
	// 设置Content-Type
	req.Header.Set("Content-Type", "application/json")
	// 发送请求
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return ``, err
	}
	defer resp.Body.Close()

	// 读取响应体
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ``, err
	}

	// 输出响应
	return string(responseBody), nil
}

// 查看单元投放数据（新）
func ReportV3unit(appkey, token string, accountId int64, page int) (string, error) {
	values := map[string]interface{}{
		"appkey":     appkey,
		"ts":         carbon.Now().TimestampMilli(),
		"account_id": accountId,
		"start_time": carbon.Now().SubDays(6).StartOfDay().ToShortDateString(),
		"end_time":   carbon.Now().StartOfDay().ToShortDateString(),
		"page":       page,
		"size":       100,
	}
	// 签名
	values["sign"] = getSign(values, token)
	// GET请求的参数
	params := url.Values{}
	for k, v := range values {
		params.Add(k, fmt.Sprintf("%v", v))
	}

	// 创建PUT请求
	req, err := http.NewRequest("GET", fmt.Sprintf("%s%s?%s", apiUrl, "/open_api/report/v3/unit", params.Encode()), nil)
	if err != nil {
		return ``, err
	}
	// 设置Content-Type
	req.Header.Set("Content-Type", "application/json")
	// 发送请求
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return ``, err
	}
	defer resp.Body.Close()

	// 读取响应体
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ``, err
	}

	// 输出响应
	return string(responseBody), nil
}

// 账户现金信息
func ReportV2Cash(appkey, token string, accountId int64) (string, error) {
	values := map[string]interface{}{
		"appkey":     appkey,
		"ts":         carbon.Now().TimestampMilli(),
		"account_id": accountId,
	}
	// 签名
	values["sign"] = getSign(values, token)
	// GET请求的参数
	params := url.Values{}
	for k, v := range values {
		params.Add(k, fmt.Sprintf("%v", v))
	}

	// 创建PUT请求
	req, err := http.NewRequest("GET", fmt.Sprintf("%s%s?%s", apiUrl, "/open_api/report/v2/cash", params.Encode()), nil)
	if err != nil {
		return ``, err
	}
	// 设置Content-Type
	req.Header.Set("Content-Type", "application/json")
	// 发送请求
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return ``, err
	}
	defer resp.Body.Close()

	// 读取响应体
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ``, err
	}

	// 输出响应
	return string(responseBody), nil
}

// 单元查询
func CpcV3UnitGet(appkey, token string, unitId int64) (string, error) {
	values := map[string]interface{}{
		"appkey":  appkey,
		"ts":      carbon.Now().TimestampMilli(),
		"unit_id": unitId,
	}
	// 签名
	values["sign"] = getSign(values, token)
	// GET请求的参数
	params := url.Values{}
	for k, v := range values {
		params.Add(k, fmt.Sprintf("%v", v))
	}

	// 创建PUT请求
	req, err := http.NewRequest("GET", fmt.Sprintf("%s%s?%s", apiUrl, "/open_api/cpc/v3/unit/get", params.Encode()), nil)
	if err != nil {
		return ``, err
	}
	// 设置Content-Type
	req.Header.Set("Content-Type", "application/json")
	// 发送请求
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return ``, err
	}
	defer resp.Body.Close()

	// 读取响应体
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ``, err
	}

	// 输出响应
	return string(responseBody), nil
}

// 计划查询
func CpcV3CampaignGet(appkey, token string, unitId int64) (string, error) {
	values := map[string]interface{}{
		"appkey":  appkey,
		"ts":      carbon.Now().TimestampMilli(),
		"unit_id": unitId,
	}
	// 签名
	values["sign"] = getSign(values, token)
	// GET请求的参数
	params := url.Values{}
	for k, v := range values {
		params.Add(k, fmt.Sprintf("%v", v))
	}

	// 创建PUT请求
	req, err := http.NewRequest("GET", fmt.Sprintf("%s%s?%s", apiUrl, "/open_api/cpc/v3/campaign/get", params.Encode()), nil)
	if err != nil {
		return ``, err
	}
	// 设置Content-Type
	req.Header.Set("Content-Type", "application/json")
	// 发送请求
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return ``, err
	}
	defer resp.Body.Close()

	// 读取响应体
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ``, err
	}

	// 输出响应
	return string(responseBody), nil
}

func ReportV2Detail(appkey, token string, accountId int64) (string, error) {
	values := map[string]interface{}{
		"appkey":      appkey,
		"ts":          carbon.Now().TimestampMilli(),
		"launch_type": 0,
		"time_type":   3,
		"account_id":  accountId,
		"start_time":  carbon.Now().SubDays(6).StartOfDay().ToShortDateString(),
		"end_time":    carbon.Now().StartOfDay().ToShortDateString(),
	}
	// 签名
	values["sign"] = getSign(values, token)
	// GET请求的参数
	params := url.Values{}
	for k, v := range values {
		params.Add(k, fmt.Sprintf("%v", v))
	}

	// 创建PUT请求
	req, err := http.NewRequest("GET", fmt.Sprintf("%s%s?%s", apiUrl, "/open_api/report/v2/detail", params.Encode()), nil)
	if err != nil {
		return ``, err
	}
	// 设置Content-Type
	req.Header.Set("Content-Type", "application/json")
	// 发送请求
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return ``, err
	}
	defer resp.Body.Close()

	// 读取响应体
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ``, err
	}

	// 输出响应
	return string(responseBody), nil
}
