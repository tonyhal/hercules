package utils

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/go-kratos/kratos/v2/log"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"runtime/debug"
	"sort"
	"strings"
	"time"
)

// md5加密
func Md5(str string) string {
	hash := md5.New()
	hash.Write([]byte(str))
	cipherStr := hash.Sum(nil)
	return hex.EncodeToString(cipherStr)
}

// post请求
func JsonPost(url string, argv string) string {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("utils.JsonPost: %v", string(debug.Stack()))
		}
	}()

	resp, err := http.Post(url, `application/json`, strings.NewReader(argv))
	if err != nil {
		return ``
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ``
	}
	return string(body)
}

func HttpGet(url string, header map[string]string) (string, error) {
	client := &http.Client{
		Timeout: 45 * time.Second,
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return ``, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ``, err
	}
	return string(body), nil
}

func SendPost(urlStr string, argv map[string]interface{}) (string, string) {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("utils.SendPost: %v", string(debug.Stack()))
		}
	}()

	client := &http.Client{}
	bodyJson, _ := json.Marshal(argv)

	r, _ := http.NewRequest("POST", urlStr, strings.NewReader(string(bodyJson))) // URL-encoded payload
	r.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	r.Header.Add("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")

	resp, err := client.Do(r)
	if err != nil {
	}
	defer resp.Body.Close()

	cookies := []string{}
	for _, cookie := range resp.Cookies() {
		cookies = append(cookies, fmt.Sprintf("%s=%s", cookie.Name, cookie.Value))
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ``, ``
	}

	return string(body), strings.Join(cookies, ";")
}

func HttpPost(urlStr string, header map[string]string, argv map[string]string) string {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("utils.HttpPost: %v", string(debug.Stack()))
		}
	}()

	values := url.Values{}
	for k, v := range argv {
		values.Set(k, v)
	}

	r, _ := http.NewRequest("POST", urlStr, strings.NewReader(values.Encode())) // URL-encoded payload
	r.Header.Set("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")
	for k, v := range header {
		r.Header.Set(k, v)
	}

	client := &http.Client{}
	resp, err := client.Do(r)
	if err != nil {
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ``
	}

	return string(body)
}

func DateStringToUnixTimeStamp(date string) int64 {
	at := int64(0)
	l, _ := time.LoadLocation("Asia/Shanghai")
	t, err := time.ParseInLocation("2006-01-02 15:04:05", date, l)
	if err == nil {
		at = t.Unix()
	}
	return at
}

func RemoveRepeatedElement(arr []string) (newArr []string) {
	newArr = make([]string, 0)
	for i := 0; i < len(arr); i++ {
		repeat := false
		for j := i + 1; j < len(arr); j++ {
			if arr[i] == arr[j] {
				repeat = true
				break
			}
		}
		if !repeat {
			newArr = append(newArr, arr[i])
		}
	}
	return newArr
}

func InSortedArray(arr []int, target int) bool {
	index := sort.SearchInts(arr, target)
	return index < len(arr) && arr[index] == target
}

// 通过user-agent获取设备操作系统版本
func GetDeviceOSVersionFromUserAgent(ua string) string {
	if strings.Contains(ua, "Android") {
		re := regexp.MustCompile(`(?i)Android ([\d.]{1,})`)
		version := re.FindStringSubmatch(ua)
		if len(version) > 1 {
			return "Android " + version[1]
		}

	} else if strings.Contains(ua, "iPhone") {
		re := regexp.MustCompile(`(?i)CPU iPhone OS ([\d_]{1,})`)
		version := re.FindStringSubmatch(ua)
		if len(version) > 1 {
			return "iOS " + strings.Replace(version[1], "_", ".", -1)
		}
	} else if strings.Contains(ua, "iPad") {
		re := regexp.MustCompile(`(?i)CPU OS ([\d_]{1,})`)
		version := re.FindStringSubmatch(ua)
		if len(version) > 1 {
			return "iOS " + strings.Replace(version[1], "_", ".", -1)
		}
	}
	return "unknown"
}
