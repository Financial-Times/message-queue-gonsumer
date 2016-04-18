package consumer

import (
	"encoding/base64"
	"reflect"
	"testing"
)

func TestParseResponse_ResponseContainsMultipleRawMessages_Success(t *testing.T) {
	expected := []Message{
		Message{
			map[string]string{
				"Message-Id":        "c6653374-922c-4b78-927d-15c5125fcd8d",
				"Message-Timestamp": "2015-10-21T14:22:06.270Z",
				"Message-Type":      "cms-content-published",
				"Origin-System-Id":  "http://cmdb.ft.com/systems/methode-web-pub",
				"Content-Type":      "application/json",
				"X-Request-Id":      "SYNTHETIC-REQ-MON_A391MMaVMv",
			},
			`{"contentUri":"http://methode-image-model-transformer-pr-uk-int.svc.ft.com/image/model/c94a3a57-3c99-423c-a6bd-ed8c4c10a3c3",
"uuid":"c94a3a57-3c99-423c-a6bd-ed8c4c10a3c3", "destination":"methode-image-model-transformer", "relativeUrl":"/image/model/c94a3a57-3c99-423c-a6bd-ed8c4c10a3c3"}`,
		},
		Message{
			map[string]string{
				"Message-Id":        "be8132e8-dc95-459f-808f-e6a89e2dc8f0",
				"Message-Timestamp": "2015-10-21T14:22:06.270Z",
				"Message-Type":      "cms-content-published",
				"Origin-System-Id":  "http://cmdb.ft.com/systems/methode-web-pub",
				"Content-Type":      "application/json",
				"X-Request-Id":      "SYNTHETIC-REQ-MON_A391MMaVMv",
			},
			`{"contentUri":"http://methode-image-model-transformer-pr-uk-int.svc.ft.com/image-set/model/c94a3a57-3c99-423c-38db-7a169664088a",
"uuid":"c94a3a57-3c99-423c-38db-7a169664088a", "destination":"methode-image-model-transformer", "relativeUrl":"/image-set/model/c94a3a57-3c99-423c-38db-7a169664088a"}`,
		},
	}

	actual, err := parseResponse([]byte(testRawResp))
	if err != nil {
		t.Fatalf("Error: [%v]", err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("\nExpected: [%v]\nActual: [%v]", expected, actual)
	}
}

func TestParseMessage_RawMessage_Success(t *testing.T) {
	expected := Message{
		map[string]string{
			"Message-Id":        "c4b96810-03e8-4057-84c5-dcc3a8c61a26",
			"Message-Timestamp": "2015-10-19T09:30:29.110Z",
			"Message-Type":      "cms-content-published",
			"Origin-System-Id":  "http://cmdb.ft.com/systems/methode-web-pub",
			"Content-Type":      "application/json",
			"X-Request-Id":      "SYNTHETIC-REQ-MON_Unv1K838lY"},
		testBody4RawMsgValue,
	}

	actual, err := parseMessage(testRawMsgValue)
	if err != nil {
		t.Fatalf("Error: [%v]", err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("\nExpected: [%v]\nActual: [%v]", expected, actual)
	}
}

func TestParseMessage_MsgBodyIsJSON_Success(t *testing.T) {
	testMsg := `FTMSG/1.0
Message-Id: c4b96810-03e8-4057-84c5-dcc3a8c61a26
Message-Timestamp: 2015-10-19T09:30:29.110Z
Message-Type: cms-content-published
Origin-System-Id: http://cmdb.ft.com/systems/methode-web-pub
Content-Type: application/json
X-Request-Id: SYNTHETIC-REQ-MON_Unv1K838lY

{"uuid":"e7a3b814-59ee-459e-8f60-517f3e80ed99", "value":"test","attributes":[]}`
	expected := Message{
		map[string]string{
			"Message-Id":        "c4b96810-03e8-4057-84c5-dcc3a8c61a26",
			"Message-Timestamp": "2015-10-19T09:30:29.110Z",
			"Message-Type":      "cms-content-published",
			"Origin-System-Id":  "http://cmdb.ft.com/systems/methode-web-pub",
			"Content-Type":      "application/json",
			"X-Request-Id":      "SYNTHETIC-REQ-MON_Unv1K838lY",
		},
		`{"uuid":"e7a3b814-59ee-459e-8f60-517f3e80ed99", "value":"test","attributes":[]}`,
	}

	actual, _ := parseMessage(base64.StdEncoding.EncodeToString([]byte(testMsg)))
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected: [%v]\nActual: [%v]", expected, actual)
	}
}

func TestParseMessage_MsgBodyIsText_Success(t *testing.T) {
	testMsg := `FTMSG/1.0
Message-Id: c4b96810-03e8-4057-84c5-dcc3a8c61a26
Message-Timestamp: 2015-10-19T09:30:29.110Z
Message-Type: cms-content-published
Origin-System-Id: http://cmdb.ft.com/systems/methode-web-pub
Content-Type: application/json
X-Request-Id: SYNTHETIC-REQ-MON_Unv1K838lY

foobar`
	expected := Message{
		map[string]string{
			"Message-Id":        "c4b96810-03e8-4057-84c5-dcc3a8c61a26",
			"Message-Timestamp": "2015-10-19T09:30:29.110Z",
			"Message-Type":      "cms-content-published",
			"Origin-System-Id":  "http://cmdb.ft.com/systems/methode-web-pub",
			"Content-Type":      "application/json",
			"X-Request-Id":      "SYNTHETIC-REQ-MON_Unv1K838lY",
		},
		"foobar",
	}
	actual, _ := parseMessage(base64.StdEncoding.EncodeToString([]byte(testMsg)))
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected: [%v]\nActual: [%v]", expected, actual)
	}
}

func TestParseMessage_MsgBodyIsEmpty_Success(t *testing.T) {
	testMsg := `FTMSG/1.0
Message-Id: c4b96810-03e8-4057-84c5-dcc3a8c61a26
Message-Timestamp: 2015-10-19T09:30:29.110Z
Message-Type: cms-content-published
Origin-System-Id: http://cmdb.ft.com/systems/methode-web-pub
Content-Type: application/json
X-Request-Id: SYNTHETIC-REQ-MON_Unv1K838lY
`
	expected := Message{
		map[string]string{
			"Message-Id":        "c4b96810-03e8-4057-84c5-dcc3a8c61a26",
			"Message-Timestamp": "2015-10-19T09:30:29.110Z",
			"Message-Type":      "cms-content-published",
			"Origin-System-Id":  "http://cmdb.ft.com/systems/methode-web-pub",
			"Content-Type":      "application/json",
			"X-Request-Id":      "SYNTHETIC-REQ-MON_Unv1K838lY",
		},

		"",
	}
	actual, _ := parseMessage(base64.StdEncoding.EncodeToString([]byte(testMsg)))
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected: [%v]\nActual: [%v]", expected, actual)
	}
}

// don't validate anything just let the msg body be empty
func TestParseMessage_InvalidMessageFormat_NoErrorReturnedMsgBodyIsEmpty(t *testing.T) {
	testMsg := `FTMSG/1.0
Message-Id: c4b96810-03e8-4057-84c5-dcc3a8c61a26
Message-Timestamp: 2015-10-19T09:30:29.110Z
Message-Type: cms-content-published
Origin-System-Id: http://cmdb.ft.com/systems/methode-web-pub
Content-Type: application/json
X-Request-Id: SYNTHETIC-REQ-MON_Unv1K838lY`
	expected := ""
	actual, err := parseMessage(base64.StdEncoding.EncodeToString([]byte(testMsg)))
	if err != nil {
		t.Fatalf("Error: [%v]", err)
	}
	if actual.Body != expected {
		t.Fatalf("Expected: [%v]. Actual: [%v]", expected, actual)
	}
}

func TestParseHeaders_Success(t *testing.T) {
	testMsg := `FTMSG/1.0
Message-Id: c4b96810-03e8-4057-84c5-dcc3a8c61a26
Message-Timestamp: 2015-10-19T09:30:29.110Z
Message-Type: cms-content-published
Origin-System-Id: http://cmdb.ft.com/systems/methode-web-pub
Content-Type: application/json
X-Request-Id: SYNTHETIC-REQ-MON_Unv1K838lY`
	expected := map[string]string{
		"Message-Id":        "c4b96810-03e8-4057-84c5-dcc3a8c61a26",
		"Message-Timestamp": "2015-10-19T09:30:29.110Z",
		"Message-Type":      "cms-content-published",
		"Origin-System-Id":  "http://cmdb.ft.com/systems/methode-web-pub",
		"Content-Type":      "application/json",
		"X-Request-Id":      "SYNTHETIC-REQ-MON_Unv1K838lY",
	}

	actual, _ := parseHeaders(testMsg)
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected: [%v]\nActual: [%v]", expected, actual)
	}
}

const testRawResp = `[{"key":"Yzk0YTNhNTctM2M5OS00MjNjLWE2YmQtZWQ4YzRjMTBhM2Mz","value":"RlRNU0cvMS4wDQpNZXNzYWdlLUlkOiBjNjY1MzM3NC05MjJjLTRiNzgtOTI3ZC0xNWM1MTI1ZmNkOGQNCk1lc3NhZ2UtVGltZXN0YW1wOiAyMDE1LTEwLTIxVDE0OjIyOjA2LjI3MFoNCk1lc3NhZ2UtVHlwZTogY21zLWNvbnRlbnQtcHVibGlzaGVkDQpPcmlnaW4tU3lzdGVtLUlkOiBodHRwOi8vY21kYi5mdC5jb20vc3lzdGVtcy9tZXRob2RlLXdlYi1wdWINCkNvbnRlbnQtVHlwZTogYXBwbGljYXRpb24vanNvbg0KWC1SZXF1ZXN0LUlkOiBTWU5USEVUSUMtUkVRLU1PTl9BMzkxTU1hVk12DQoNCnsiY29udGVudFVyaSI6Imh0dHA6Ly9tZXRob2RlLWltYWdlLW1vZGVsLXRyYW5zZm9ybWVyLXByLXVrLWludC5zdmMuZnQuY29tL2ltYWdlL21vZGVsL2M5NGEzYTU3LTNjOTktNDIzYy1hNmJkLWVkOGM0YzEwYTNjMyIsCiJ1dWlkIjoiYzk0YTNhNTctM2M5OS00MjNjLWE2YmQtZWQ4YzRjMTBhM2MzIiwgImRlc3RpbmF0aW9uIjoibWV0aG9kZS1pbWFnZS1tb2RlbC10cmFuc2Zvcm1lciIsICJyZWxhdGl2ZVVybCI6Ii9pbWFnZS9tb2RlbC9jOTRhM2E1Ny0zYzk5LTQyM2MtYTZiZC1lZDhjNGMxMGEzYzMifQ==","partition":0,"offset":24461},{"key":"Yzk0YTNhNTctM2M5OS00MjNjLTM4ZGItN2ExNjk2NjQwODhh","value":"RlRNU0cvMS4wDQpNZXNzYWdlLUlkOiBiZTgxMzJlOC1kYzk1LTQ1OWYtODA4Zi1lNmE4OWUyZGM4ZjANCk1lc3NhZ2UtVGltZXN0YW1wOiAyMDE1LTEwLTIxVDE0OjIyOjA2LjI3MFoNCk1lc3NhZ2UtVHlwZTogY21zLWNvbnRlbnQtcHVibGlzaGVkDQpPcmlnaW4tU3lzdGVtLUlkOiBodHRwOi8vY21kYi5mdC5jb20vc3lzdGVtcy9tZXRob2RlLXdlYi1wdWINCkNvbnRlbnQtVHlwZTogYXBwbGljYXRpb24vanNvbg0KWC1SZXF1ZXN0LUlkOiBTWU5USEVUSUMtUkVRLU1PTl9BMzkxTU1hVk12DQoNCnsiY29udGVudFVyaSI6Imh0dHA6Ly9tZXRob2RlLWltYWdlLW1vZGVsLXRyYW5zZm9ybWVyLXByLXVrLWludC5zdmMuZnQuY29tL2ltYWdlLXNldC9tb2RlbC9jOTRhM2E1Ny0zYzk5LTQyM2MtMzhkYi03YTE2OTY2NDA4OGEiLAoidXVpZCI6ImM5NGEzYTU3LTNjOTktNDIzYy0zOGRiLTdhMTY5NjY0MDg4YSIsICJkZXN0aW5hdGlvbiI6Im1ldGhvZGUtaW1hZ2UtbW9kZWwtdHJhbnNmb3JtZXIiLCAicmVsYXRpdmVVcmwiOiIvaW1hZ2Utc2V0L21vZGVsL2M5NGEzYTU3LTNjOTktNDIzYy0zOGRiLTdhMTY5NjY0MDg4YSJ9","partition":0,"offset":24462}]`

const testRawMsgValue = `RlRNU0cvMS4wDQpNZXNzYWdlLUlkOiBjNGI5NjgxMC0wM2U4LTQwNTctODRjNS1kY2MzYThjNjFhMjYNCk1lc3NhZ2UtVGltZXN0YW1wOiAyMDE1LTEwLTE5VDA5OjMwOjI5LjExMFoNCk1lc3NhZ2UtVHlwZTogY21zLWNvbnRlbnQtcHVibGlzaGVkDQpPcmlnaW4tU3lzdGVtLUlkOiBodHRwOi8vY21kYi5mdC5jb20vc3lzdGVtcy9tZXRob2RlLXdlYi1wdWINCkNvbnRlbnQtVHlwZTogYXBwbGljYXRpb24vanNvbg0KWC1SZXF1ZXN0LUlkOiBTWU5USEVUSUMtUkVRLU1PTl9VbnYxSzgzOGxZDQoNCnsidXVpZCI6ImU3YTNiODE0LTU5ZWUtNDU5ZS04ZjYwLTUxN2YzZTgwZWQ5OSIsInR5cGUiOiJJbWFnZSIsInZhbHVlIjoid2tLdDVBVEd1REpZR2Z2VGMyOFZVanM5ZzdTRCtsOGZxY3FkZ0N1cDJ1R2ZiTXlmRXhvSHBOZjI1VmQ5UU1vMEpWZ2pwL1loUWwrMVBnUGZjTGhwSkc0MVQrSzJQTDNKcFhGd1YwVGZBTG4wQnNyYzJwWXRDZzVGTGFsd3RRYlBnN2FIUzU1T1k2UThubDh0K2xoMVJ5amxtMmR2TTVCT2huZmlDVDY3STZYaFk4SU03RnhKSFJWMHlSNEFWWmVPVXdtYms4Ky9zblpIcmFUTGdXUnB2UmxWaDFsSFJ4TEJlK0xrSTFxNDRWUHljdXR6ck16Y0Jta2sza1lzSm5ObmdmWEpCVDNNZlBGeURJSVJoKzdnWlBTK0JDVWQ1UFNNS3JOU0dJUllhczQ4UWZxWnk4YnJ1MzljNHQyc05LRnJtWnJyd05SMDRIc3UxM0kyYnBkSUdXbFdQSTk4dWZvV2hWc1pJdmhMYXV1TS9KZU5zNndHRVNxclJCOEpCekNabWdWY1NIcFNqMWc3dTNKeGd2RDJSd3F0YkZhaGR1NUhZVkEzVEVRbksxaG03WUFNc0ROc0ZZK25MRjJMVFI4ZitBRlVLM0RmN3orWE1vVjA3K1FkVEVGaW9nMmgwTlJ2VXVzYXBhWjZza2I5b3VPMlYyMXRaYjdxdEdYelk1Rm5UdzNwRTVYOVZqVmtnV2xmWGR1bHR3WlhvK1JPY2pGcXhJSVJDbWFIMS9EbEtKaG10ZDB0VzRnc2FYamxGZkpxWXd6RDZXckEwdzRQQlMydjVWb0c0VWFkTE10c25aSFl4Y1dqeHFYbVBiUkhpMmw1MEJxbm9tMXFwZWZVZGh6eC9XcnNuUFp2R2o3ZzMvQXJ5RHNBVXREb0puN0VoSDVZMnB0ZEdjTThmb3FhYStZWWpvbHpFdXhtWjlyUlE2UFpETWcvN3FMZU9JNVMxWXJFRFFxcE5CN1czWlhlUFhKbjNTMlFEeW9yblJrc05MankxaUJEZGdYbm5PSVladFNJOWhPRDBIQnRhaWVla0hWRWtqQmRTeTNXWjB1S2RscWFlOThnZTJmNHVPSmhwdVpaOGlrUlNGbVRCdmprQkRaWmljVVdKUWdCNzM0NjhHeEdUNEJjVWJQSlFmYlFYeEpGWEtSQ01xYStsN2ZBY3JYZk5OZGNESFJuWWRvWHh3QVU3Vlk1V2lFTE85ZmNOTmxWNm1aQnkvYXdCSEw5NHVjN29MWFYyUzNyNUhZZzZsaGhOODVrRUsxdU1pY0lXNkl2eHVJRW0vLzg3QmhRcU0vaGdzbUxIaGNKbUZiVkdUZThGamFJcGp4ckJHLzdtZjY3VzZubFl1dUVxbkdRTWZnWnAxaENic1hXUmZrNnY3NjMwYVdQY1AyRjNwTGxhby9kTjh1dGt3QkZxaVRQMXMxOXV4N1FxcHZVNXhHeTdtK2U4eVA4K25yU3lad2JNRVVVYUpYNjJsb256WU81d3R6RDArakVTQmE2MG0waTR0TlhSVkhwbWY0UXgrUGMya3JHb1dsbTVBV0VqZC8xbFFFWXh0TGpSTjc1TTVrRitEbzg4NTJ5RUFBVW5kRmY1UTZRZUpoWkt6bXNZZTgzUnNkbS9oOGc0MjIvcG5PV2svQ0dXZEVYL2FSQWZtVVRvZTNzdEtIRTdDYzM1dmdXUVlLcU9SQkwwZ2xocWNnNUNMS0NMWm0rcGtMU1Vqb211MXQ3Vk44eTRSZ0VvYS9QcERNaSsySFVKb3Fad3ZFOHNQV2FtVll5VUE9PSIsImF0dHJpYnV0ZXMiOiJcdTAwM2M/eG1sIHZlcnNpb249XCIxLjBcIiBlbmNvZGluZz1cInV0Zi04XCI/XHUwMDNlXG5cdTAwM2MhRE9DVFlQRSBtZXRhIFNZU1RFTSBcIi9TeXNDb25maWcvQ2xhc3NpZnkvRlRJbWFnZXMvY2xhc3NpZnkuZHRkXCJcdTAwM2Vcblx1MDAzY21ldGFcdTAwM2VcbiAgICBcdTAwM2NwaWN0dXJlXHUwMDNlXG4gICAgICAgIFx1MDAzY2NhcHRpb24vXHUwMDNlXG4gICAgICAgIFx1MDAzY2NhdGVnb3J5L1x1MDAzZVxuICAgICAgICBcdTAwM2NrZXl3b3Jkcy9cdTAwM2VcbiAgICAgICAgXHUwMDNjYWRpbmZvL1x1MDAzZVxuICAgICAgICBcdTAwM2NwaG90b2dyYXBoZXIvXHUwMDNlXG4gICAgICAgIFx1MDAzY2NvcHlyaWdodF9pbmZvXHUwMDNlXG4gICAgICAgIFx1MDAzY2NvcHlyaWdodF9zdGF0ZW1lbnQvXHUwMDNlXG4gICAgICAgIFx1MDAzY2NvcHlyaWdodF9ncm91cC9cdTAwM2VcbiAgICAgICAgXHUwMDNjZGlzdHJpYnV0aW9uX3JpZ2h0cy9cdTAwM2VcbiAgICAgICAgXHUwMDNjbGVnYWxfc3RhdHVzL1x1MDAzZVxuICAgICAgICBcdTAwM2MvY29weXJpZ2h0X2luZm9cdTAwM2VcbiAgICAgICAgXHUwMDNjd2ViX2luZm9ybWF0aW9uXHUwMDNlXG4gICAgICAgICAgICBcdTAwM2NjYXB0aW9uL1x1MDAzZVxuICAgICAgICAgICAgXHUwMDNjYWx0X3RhZy9cdTAwM2VcbiAgICAgICAgICAgIFx1MDAzY29ubGluZS1zb3VyY2UvXHUwMDNlXG4gICAgICAgICAgICBcdTAwM2NtYW51YWwtc291cmNlL1x1MDAzZVxuICAgICAgICAgICAgXHUwMDNjRElGVGNvbVdlYlR5cGVcdTAwM2VncmFwaGljXHUwMDNjL0RJRlRjb21XZWJUeXBlXHUwMDNlXG4gICAgICAgIFx1MDAzYy93ZWJfaW5mb3JtYXRpb25cdTAwM2VcbiAgICAgICAgXHUwMDNjcHJvdmlkZXIvXHUwMDNlXG4gICAgICAgIFx1MDAzY2ZpbG1fdHlwZS9cdTAwM2VcbiAgICAgICAgXHUwMDNjZGF0ZV90YWtlbi9cdTAwM2VcbiAgICAgICAgXHUwMDNjZGF0ZV9yZWNlaXZlZC9cdTAwM2VcbiAgICAgICAgXHUwMDNjcmVmZXJlbmNlX251bS9cdTAwM2VcbiAgICAgICAgXHUwMDNjcm9sbF9udW0vXHUwMDNlXG4gICAgICAgIFx1MDAzY2ZyYW1lX251bS9cdTAwM2VcbiAgICAgICAgXHUwMDNjY2hlY2tib3hlcy9cdTAwM2VcbiAgICAgICAgXHUwMDNjd2FybmluZ3MvXHUwMDNlXG4gICAgICAgIFx1MDAzY3NlY3VyaXR5L1x1MDAzZVxuICAgICAgICBcdTAwM2NlbWJhcmdvX2RhdGUvXHUwMDNlXG4gICAgICAgIFx1MDAzY2pvYl9kZXNjcmlwdGlvblx1MDAzZVxuICAgICAgICAgICAgXHUwMDNjY2l0eS9cdTAwM2VcbiAgICAgICAgICAgIFx1MDAzY3Byb3ZpbmNlL1x1MDAzZVxuICAgICAgICAgICAgXHUwMDNjY291bnRyeS9cdTAwM2VcbiAgICAgICAgICAgIFx1MDAzY2luc3RydWN0aW9ucy9cdTAwM2VcbiAgICAgICAgXHUwMDNjL2pvYl9kZXNjcmlwdGlvblx1MDAzZVxuICAgICAgICBcdTAwM2NwcmljZS9cdTAwM2VcbiAgICAgICAgXHUwMDNjZmlsZW5hbWUvXHUwMDNlXG4gICAgICAgIFx1MDAzY2Jhc2tldC9cdTAwM2VcbiAgICAgICAgXHUwMDNjc291cmNlL1x1MDAzZVxuICAgICAgICBcdTAwM2NieWxpbmUvXHUwMDNlXG4gICAgICAgIFx1MDAzY2hlYWRsaW5lL1x1MDAzZVxuICAgICAgICBcdTAwM2N1dWlkXHUwMDNlZTdhM2I4MTQtNTllZS00NTllLThmNjAtNTE3ZjNlODBlZDk5XHUwMDNjL3V1aWRcdTAwM2VcbiAgICAgICAgXHUwMDNjcmF0aW9cdTAwM2UxLjBcdTAwM2MvcmF0aW9cdTAwM2VcbiAgICAgICAgXHUwMDNjaW1hZ2VUeXBlXHUwMDNlQ2hhcnRzXHUwMDNjL2ltYWdlVHlwZVx1MDAzZVxuICAgIFx1MDAzYy9waWN0dXJlXHUwMDNlXG4gICAgXHUwMDNjbWFya0RlbGV0ZWRcdTAwM2VGYWxzZVx1MDAzYy9tYXJrRGVsZXRlZFx1MDAzZVxuXHUwMDNjL21ldGFcdTAwM2VcbiIsIndvcmtmbG93U3RhdHVzIjoiIiwic3lzdGVtQXR0cmlidXRlcyI6Ilx1MDAzY3Byb3BzXHUwMDNlXG4gICAgXHUwMDNjcHJvZHVjdEluZm9cdTAwM2VcbiAgICAgICAgXHUwMDNjbmFtZVx1MDAzZUZpbmFuY2lhbCBUaW1lc1x1MDAzYy9uYW1lXHUwMDNlXG4gICAgICAgIFx1MDAzY2lzc3VlRGF0ZVx1MDAzZTIwMTUxMDE5XHUwMDNjL2lzc3VlRGF0ZVx1MDAzZVxuICAgIFx1MDAzYy9wcm9kdWN0SW5mb1x1MDAzZVxuICAgIFx1MDAzY3dvcmtGb2xkZXJcdTAwM2UvRlRcdTAwM2Mvd29ya0ZvbGRlclx1MDAzZVxuICAgIFx1MDAzY3N1bW1hcnkvXHUwMDNlXG4gICAgXHUwMDNjaW1hZ2VJbmZvXHUwMDNlXG4gICAgICAgIFx1MDAzY3dpZHRoXHUwMDNlMzAyXHUwMDNjL3dpZHRoXHUwMDNlXG4gICAgICAgIFx1MDAzY2hlaWdodFx1MDAzZTI4Mlx1MDAzYy9oZWlnaHRcdTAwM2VcbiAgICAgICAgXHUwMDNjcHRXaWR0aFx1MDAzZTMwMi4wXHUwMDNjL3B0V2lkdGhcdTAwM2VcbiAgICAgICAgXHUwMDNjcHRIZWlnaHRcdTAwM2UyODIuMFx1MDAzYy9wdEhlaWdodFx1MDAzZVxuICAgICAgICBcdTAwM2N4RGltXHUwMDNlMTA2LjU0XHUwMDNjL3hEaW1cdTAwM2VcbiAgICAgICAgXHUwMDNjeURpbVx1MDAzZTk5LjQ4XHUwMDNjL3lEaW1cdTAwM2VcbiAgICAgICAgXHUwMDNjZGltXHUwMDNlMTAuNjU0Y20geCA5Ljk0OGNtXHUwMDNjL2RpbVx1MDAzZVxuICAgICAgICBcdTAwM2N4cmVzXHUwMDNlNzIuMFx1MDAzYy94cmVzXHUwMDNlXG4gICAgICAgIFx1MDAzY3lyZXNcdTAwM2U3Mi4wXHUwMDNjL3lyZXNcdTAwM2VcbiAgICAgICAgXHUwMDNjY29sb3JUeXBlXHUwMDNlUkdCXHUwMDNjL2NvbG9yVHlwZVx1MDAzZVxuICAgICAgICBcdTAwM2NmaWxlVHlwZVx1MDAzZVBOR1x1MDAzYy9maWxlVHlwZVx1MDAzZVxuICAgICAgICBcdTAwM2NhbHBoYUNoYW5uZWxzXHUwMDNlMVx1MDAzYy9hbHBoYUNoYW5uZWxzXHUwMDNlXG4gICAgXHUwMDNjL2ltYWdlSW5mb1x1MDAzZVxuXHUwMDNjL3Byb3BzXHUwMDNlXG4iLCJ1c2FnZVRpY2tldHMiOiJcdTAwM2M/eG1sIHZlcnNpb249JzEuMCcgZW5jb2Rpbmc9J1VURi04Jz9cdTAwM2Vcblx1MDAzY3RsXHUwMDNlXG4gICAgXHUwMDNjdFx1MDAzZVxuICAgICAgICBcdTAwM2NpZFx1MDAzZTFcdTAwM2MvaWRcdTAwM2VcbiAgICAgICAgXHUwMDNjdHBcdTAwM2VQdWJsaXNoZXJcdTAwM2MvdHBcdTAwM2VcbiAgICAgICAgXHUwMDNjY1x1MDAzZXRhc3NlbGx0XHUwMDNjL2NcdTAwM2VcbiAgICAgICAgXHUwMDNjY2RcdTAwM2UyMDE1MTAxOTA5MzAyNVx1MDAzYy9jZFx1MDAzZVxuICAgICAgICBcdTAwM2NkdFx1MDAzZVxuICAgICAgICAgICAgXHUwMDNjcHVibGlzaGVkRGF0ZVx1MDAzZU1vbiBPY3QgMTkgMDk6MzA6MjUgVVRDIDIwMTVcdTAwM2MvcHVibGlzaGVkRGF0ZVx1MDAzZVxuICAgICAgICBcdTAwM2MvZHRcdTAwM2VcbiAgICBcdTAwM2MvdFx1MDAzZVxuICAgIFx1MDAzY3RcdTAwM2VcbiAgICAgICAgXHUwMDNjaWRcdTAwM2UyXHUwMDNjL2lkXHUwMDNlXG4gICAgICAgIFx1MDAzY3RwXHUwMDNld2ViX3B1YmxpY2F0aW9uXHUwMDNjL3RwXHUwMDNlXG4gICAgICAgIFx1MDAzY2NcdTAwM2V0YXNzZWxsdFx1MDAzYy9jXHUwMDNlXG4gICAgICAgIFx1MDAzY2NkXHUwMDNlMjAxNTEwMTkwOTMwMjVcdTAwM2MvY2RcdTAwM2VcbiAgICAgICAgXHUwMDNjZHRcdTAwM2VcbiAgICAgICAgICAgIFx1MDAzY3dlYnB1Ymxpc2hcdTAwM2VcbiAgICAgICAgICAgICAgICBcdTAwM2NzaXRlX3VybFx1MDAzZWh0dHA6Ly93d3cuZnQuY29tL2Ntcy9zL2U3YTNiODE0LTU5ZWUtNDU5ZS04ZjYwLTUxN2YzZTgwZWQ5OS5odG1sXHUwMDNjL3NpdGVfdXJsXHUwMDNlXG4gICAgICAgICAgICAgICAgXHUwMDNjc3luZF91cmxcdTAwM2VodHRwOi8vd3d3LmZ0LmNvbS9jbXMvcy9lN2EzYjgxNC01OWVlLTQ1OWUtOGY2MC01MTdmM2U4MGVkOTksczAxPTEuaHRtbFx1MDAzYy9zeW5kX3VybFx1MDAzZVxuICAgICAgICAgICAgXHUwMDNjL3dlYnB1Ymxpc2hcdTAwM2VcbiAgICAgICAgXHUwMDNjL2R0XHUwMDNlXG4gICAgXHUwMDNjL3RcdTAwM2VcbiAgICBcdTAwM2N0XHUwMDNlXG4gICAgICAgIFx1MDAzY2lkXHUwMDNlM1x1MDAzYy9pZFx1MDAzZVxuICAgICAgICBcdTAwM2N0cFx1MDAzZVdlYkNvcHlcdTAwM2MvdHBcdTAwM2VcbiAgICAgICAgXHUwMDNjY1x1MDAzZXRhc3NlbGx0XHUwMDNjL2NcdTAwM2VcbiAgICAgICAgXHUwMDNjY2RcdTAwM2UyMDE1MTAxOTA5MzAyNVx1MDAzYy9jZFx1MDAzZVxuICAgICAgICBcdTAwM2NkdFx1MDAzZVxuICAgICAgICAgICAgXHUwMDNjcmVwXHUwMDNlY21zQGZ0Y21yMDEtdXZwci11ay1wXHUwMDNjL3JlcFx1MDAzZVxuICAgICAgICAgICAgXHUwMDNjZmlyc3RcdTAwM2UyMDE1MTAxOTA5MzAyNVx1MDAzYy9maXJzdFx1MDAzZVxuICAgICAgICAgICAgXHUwMDNjbGFzdFx1MDAzZTIwMTUxMDE5MDkzMDI1XHUwMDNjL2xhc3RcdTAwM2VcbiAgICAgICAgICAgIFx1MDAzY2NvdW50XHUwMDNlMVx1MDAzYy9jb3VudFx1MDAzZVxuICAgICAgICAgICAgXHUwMDNjY2hhbm5lbFx1MDAzZUZUY29tXHUwMDNjL2NoYW5uZWxcdTAwM2VcbiAgICAgICAgXHUwMDNjL2R0XHUwMDNlXG4gICAgXHUwMDNjL3RcdTAwM2VcbiAgICBcdTAwM2N0XHUwMDNlXG4gICAgICAgIFx1MDAzY2lkXHUwMDNlNFx1MDAzYy9pZFx1MDAzZVxuICAgICAgICBcdTAwM2N0cFx1MDAzZW1tc1x1MDAzYy90cFx1MDAzZVxuICAgICAgICBcdTAwM2NjXHUwMDNlc2VydmxldC1tbXNcdTAwM2MvY1x1MDAzZVxuICAgICAgICBcdTAwM2NjZFx1MDAzZTIwMTUwNjE2MTMxNTAwXHUwMDNjL2NkXHUwMDNlXG4gICAgICAgIFx1MDAzY2R0XHUwMDNlXG4gICAgICAgICAgICBcdTAwM2NzcmNVVUlEXHUwMDNlZTdhM2I4MTQtNTllZS00NTllLThmNjAtNTE3ZjNlODBlZDk5XHUwMDNjL3NyY1VVSURcdTAwM2VcbiAgICAgICAgICAgIFx1MDAzY3RyZ1VVSURcdTAwM2VlN2EzYjgxNC01OWVlLTQ1OWUtOGY2MC01MTdmM2U4MGVkOTlcdTAwM2MvdHJnVVVJRFx1MDAzZVxuICAgICAgICAgICAgXHUwMDNjc3JjUmVwb1x1MDAzZVBST0QtY21zLXJlYWRcdTAwM2Mvc3JjUmVwb1x1MDAzZVxuICAgICAgICAgICAgXHUwMDNjdHJnUmVwb1x1MDAzZVBST0QtY21hLXdyaXRlXHUwMDNjL3RyZ1JlcG9cdTAwM2VcbiAgICAgICAgICAgIFx1MDAzY3RzXHUwMDNlMTIzNDU2Nzg5MFx1MDAzYy90c1x1MDAzZVxuICAgICAgICAgICAgXHUwMDNjY2xzXHUwMDNlY29tLmVpZG9zbWVkaWEubW1zLnRhc2suZXh0ZW5kZWRvYmplY3RtaWdyYXRpb250YXNrLm9iamVjdC5FeHRlbmRlZE9iamVjdEltcGxcdTAwM2MvY2xzXHUwMDNlXG4gICAgICAgICAgICBcdTAwM2NzZXFcdTAwM2VhcmNoaXZlX29uX3B1Ymxpc2hfb3B0aW1pc2VkXHUwMDNjL3NlcVx1MDAzZVxuICAgICAgICAgICAgXHUwMDNjam9iXHUwMDNlYXJjaGl2ZV9vbl9wdWJsaXNoX29wdGltaXNlZF9qb2JcdTAwM2Mvam9iXHUwMDNlXG4gICAgICAgIFx1MDAzYy9kdFx1MDAzZVxuICAgIFx1MDAzYy90XHUwMDNlXG5cdTAwM2MvdGxcdTAwM2VcbiIsImxpbmtlZE9iamVjdHMiOltdfQ==`
const testBody4RawMsgValue = `{"uuid":"e7a3b814-59ee-459e-8f60-517f3e80ed99","type":"Image","value":"wkKt5ATGuDJYGfvTc28VUjs9g7SD+l8fqcqdgCup2uGfbMyfExoHpNf25Vd9QMo0JVgjp/YhQl+1PgPfcLhpJG41T+K2PL3JpXFwV0TfALn0Bsrc2pYtCg5FLalwtQbPg7aHS55OY6Q8nl8t+lh1Ryjlm2dvM5BOhnfiCT67I6XhY8IM7FxJHRV0yR4AVZeOUwmbk8+/snZHraTLgWRpvRlVh1lHRxLBe+LkI1q44VPycutzrMzcBmkk3kYsJnNngfXJBT3MfPFyDIIRh+7gZPS+BCUd5PSMKrNSGIRYas48QfqZy8bru39c4t2sNKFrmZrrwNR04Hsu13I2bpdIGWlWPI98ufoWhVsZIvhLauuM/JeNs6wGESqrRB8JBzCZmgVcSHpSj1g7u3JxgvD2RwqtbFahdu5HYVA3TEQnK1hm7YAMsDNsFY+nLF2LTR8f+AFUK3Df7z+XMoV07+QdTEFiog2h0NRvUusapaZ6skb9ouO2V21tZb7qtGXzY5FnTw3pE5X9VjVkgWlfXdultwZXo+ROcjFqxIIRCmaH1/DlKJhmtd0tW4gsaXjlFfJqYwzD6WrA0w4PBS2v5VoG4UadLMtsnZHYxcWjxqXmPbRHi2l50Bqnom1qpefUdhzx/WrsnPZvGj7g3/AryDsAUtDoJn7EhH5Y2ptdGcM8foqaa+YYjolzEuxmZ9rRQ6PZDMg/7qLeOI5S1YrEDQqpNB7W3ZXePXJn3S2QDyornRksNLjy1iBDdgXnnOIYZtSI9hOD0HBtaieekHVEkjBdSy3WZ0uKdlqae98ge2f4uOJhpuZZ8ikRSFmTBvjkBDZZicUWJQgB73468GxGT4BcUbPJQfbQXxJFXKRCMqa+l7fAcrXfNNdcDHRnYdoXxwAU7VY5WiELO9fcNNlV6mZBy/awBHL94uc7oLXV2S3r5HYg6lhhN85kEK1uMicIW6IvxuIEm//87BhQqM/hgsmLHhcJmFbVGTe8FjaIpjxrBG/7mf67W6nlYuuEqnGQMfgZp1hCbsXWRfk6v7630aWPcP2F3pLlao/dN8utkwBFqiTP1s19ux7QqpvU5xGy7m+e8yP8+nrSyZwbMEUUaJX62lonzYO5wtzD0+jESBa60m0i4tNXRVHpmf4Qx+Pc2krGoWlm5AWEjd/1lQEYxtLjRN75M5kF+Do8852yEAAUndFf5Q6QeJhZKzmsYe83Rsdm/h8g422/pnOWk/CGWdEX/aRAfmUToe3stKHE7Cc35vgWQYKqORBL0glhqcg5CLKCLZm+pkLSUjomu1t7VN8y4RgEoa/PpDMi+2HUJoqZwvE8sPWamVYyUA==","attributes":"\u003c?xml version=\"1.0\" encoding=\"utf-8\"?\u003e\n\u003c!DOCTYPE meta SYSTEM \"/SysConfig/Classify/FTImages/classify.dtd\"\u003e\n\u003cmeta\u003e\n    \u003cpicture\u003e\n        \u003ccaption/\u003e\n        \u003ccategory/\u003e\n        \u003ckeywords/\u003e\n        \u003cadinfo/\u003e\n        \u003cphotographer/\u003e\n        \u003ccopyright_info\u003e\n        \u003ccopyright_statement/\u003e\n        \u003ccopyright_group/\u003e\n        \u003cdistribution_rights/\u003e\n        \u003clegal_status/\u003e\n        \u003c/copyright_info\u003e\n        \u003cweb_information\u003e\n            \u003ccaption/\u003e\n            \u003calt_tag/\u003e\n            \u003conline-source/\u003e\n            \u003cmanual-source/\u003e\n            \u003cDIFTcomWebType\u003egraphic\u003c/DIFTcomWebType\u003e\n        \u003c/web_information\u003e\n        \u003cprovider/\u003e\n        \u003cfilm_type/\u003e\n        \u003cdate_taken/\u003e\n        \u003cdate_received/\u003e\n        \u003creference_num/\u003e\n        \u003croll_num/\u003e\n        \u003cframe_num/\u003e\n        \u003ccheckboxes/\u003e\n        \u003cwarnings/\u003e\n        \u003csecurity/\u003e\n        \u003cembargo_date/\u003e\n        \u003cjob_description\u003e\n            \u003ccity/\u003e\n            \u003cprovince/\u003e\n            \u003ccountry/\u003e\n            \u003cinstructions/\u003e\n        \u003c/job_description\u003e\n        \u003cprice/\u003e\n        \u003cfilename/\u003e\n        \u003cbasket/\u003e\n        \u003csource/\u003e\n        \u003cbyline/\u003e\n        \u003cheadline/\u003e\n        \u003cuuid\u003ee7a3b814-59ee-459e-8f60-517f3e80ed99\u003c/uuid\u003e\n        \u003cratio\u003e1.0\u003c/ratio\u003e\n        \u003cimageType\u003eCharts\u003c/imageType\u003e\n    \u003c/picture\u003e\n    \u003cmarkDeleted\u003eFalse\u003c/markDeleted\u003e\n\u003c/meta\u003e\n","workflowStatus":"","systemAttributes":"\u003cprops\u003e\n    \u003cproductInfo\u003e\n        \u003cname\u003eFinancial Times\u003c/name\u003e\n        \u003cissueDate\u003e20151019\u003c/issueDate\u003e\n    \u003c/productInfo\u003e\n    \u003cworkFolder\u003e/FT\u003c/workFolder\u003e\n    \u003csummary/\u003e\n    \u003cimageInfo\u003e\n        \u003cwidth\u003e302\u003c/width\u003e\n        \u003cheight\u003e282\u003c/height\u003e\n        \u003cptWidth\u003e302.0\u003c/ptWidth\u003e\n        \u003cptHeight\u003e282.0\u003c/ptHeight\u003e\n        \u003cxDim\u003e106.54\u003c/xDim\u003e\n        \u003cyDim\u003e99.48\u003c/yDim\u003e\n        \u003cdim\u003e10.654cm x 9.948cm\u003c/dim\u003e\n        \u003cxres\u003e72.0\u003c/xres\u003e\n        \u003cyres\u003e72.0\u003c/yres\u003e\n        \u003ccolorType\u003eRGB\u003c/colorType\u003e\n        \u003cfileType\u003ePNG\u003c/fileType\u003e\n        \u003calphaChannels\u003e1\u003c/alphaChannels\u003e\n    \u003c/imageInfo\u003e\n\u003c/props\u003e\n","usageTickets":"\u003c?xml version='1.0' encoding='UTF-8'?\u003e\n\u003ctl\u003e\n    \u003ct\u003e\n        \u003cid\u003e1\u003c/id\u003e\n        \u003ctp\u003ePublisher\u003c/tp\u003e\n        \u003cc\u003etassellt\u003c/c\u003e\n        \u003ccd\u003e20151019093025\u003c/cd\u003e\n        \u003cdt\u003e\n            \u003cpublishedDate\u003eMon Oct 19 09:30:25 UTC 2015\u003c/publishedDate\u003e\n        \u003c/dt\u003e\n    \u003c/t\u003e\n    \u003ct\u003e\n        \u003cid\u003e2\u003c/id\u003e\n        \u003ctp\u003eweb_publication\u003c/tp\u003e\n        \u003cc\u003etassellt\u003c/c\u003e\n        \u003ccd\u003e20151019093025\u003c/cd\u003e\n        \u003cdt\u003e\n            \u003cwebpublish\u003e\n                \u003csite_url\u003ehttp://www.ft.com/cms/s/e7a3b814-59ee-459e-8f60-517f3e80ed99.html\u003c/site_url\u003e\n                \u003csynd_url\u003ehttp://www.ft.com/cms/s/e7a3b814-59ee-459e-8f60-517f3e80ed99,s01=1.html\u003c/synd_url\u003e\n            \u003c/webpublish\u003e\n        \u003c/dt\u003e\n    \u003c/t\u003e\n    \u003ct\u003e\n        \u003cid\u003e3\u003c/id\u003e\n        \u003ctp\u003eWebCopy\u003c/tp\u003e\n        \u003cc\u003etassellt\u003c/c\u003e\n        \u003ccd\u003e20151019093025\u003c/cd\u003e\n        \u003cdt\u003e\n            \u003crep\u003ecms@ftcmr01-uvpr-uk-p\u003c/rep\u003e\n            \u003cfirst\u003e20151019093025\u003c/first\u003e\n            \u003clast\u003e20151019093025\u003c/last\u003e\n            \u003ccount\u003e1\u003c/count\u003e\n            \u003cchannel\u003eFTcom\u003c/channel\u003e\n        \u003c/dt\u003e\n    \u003c/t\u003e\n    \u003ct\u003e\n        \u003cid\u003e4\u003c/id\u003e\n        \u003ctp\u003emms\u003c/tp\u003e\n        \u003cc\u003eservlet-mms\u003c/c\u003e\n        \u003ccd\u003e20150616131500\u003c/cd\u003e\n        \u003cdt\u003e\n            \u003csrcUUID\u003ee7a3b814-59ee-459e-8f60-517f3e80ed99\u003c/srcUUID\u003e\n            \u003ctrgUUID\u003ee7a3b814-59ee-459e-8f60-517f3e80ed99\u003c/trgUUID\u003e\n            \u003csrcRepo\u003ePROD-cms-read\u003c/srcRepo\u003e\n            \u003ctrgRepo\u003ePROD-cma-write\u003c/trgRepo\u003e\n            \u003cts\u003e1234567890\u003c/ts\u003e\n            \u003ccls\u003ecom.eidosmedia.mms.task.extendedobjectmigrationtask.object.ExtendedObjectImpl\u003c/cls\u003e\n            \u003cseq\u003earchive_on_publish_optimised\u003c/seq\u003e\n            \u003cjob\u003earchive_on_publish_optimised_job\u003c/job\u003e\n        \u003c/dt\u003e\n    \u003c/t\u003e\n\u003c/tl\u003e\n","linkedObjects":[]}`
