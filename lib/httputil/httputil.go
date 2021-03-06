package httputil

import (
	"net/url"
)

func newUrl(path string, nameValues []string) *url.URL {
	length := len(nameValues)
	if length%2 != 0 {
		panic("nameValues must have even length.")
	}
	values := make(url.Values)
	for i := 0; i < length; i += 2 {
		values.Add(nameValues[i], nameValues[i+1])
	}
	return &url.URL{
		Path:     path,
		RawQuery: values.Encode()}
}

func appendParams(u *url.URL, nameValues []string) *url.URL {
	length := len(nameValues)
	if length%2 != 0 {
		panic("nameValues must have even length.")
	}
	result := *u
	values := result.Query()
	for i := 0; i < length; i += 2 {
		values.Add(nameValues[i], nameValues[i+1])
	}
	result.RawQuery = values.Encode()
	return &result
}

func withParams(u *url.URL, nameValues []string) *url.URL {
	length := len(nameValues)
	if length%2 != 0 {
		panic("nameValues must have even length.")
	}
	result := *u
	values := result.Query()
	for i := 0; i < length; i += 2 {
		values.Set(nameValues[i], nameValues[i+1])
	}
	result.RawQuery = values.Encode()
	return &result
}
