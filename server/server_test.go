package server

import (
	"io/ioutil"
	. "launchpad.net/gocheck"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type MainSuite struct{}

func init() {
	Suite(&MainSuite{})
}

func (this *MainSuite) TestServer(c *C) {
	ts := httptest.NewServer(NewServer().Server.Handler)
	defer ts.Close()

	url := ts.URL + "/mem/table/_id/1"
	req, err := http.NewRequest("GET", url, nil)
	c.Assert(err, IsNil)
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	defer res.Body.Close()

	result, err := ioutil.ReadAll(res.Body)
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusNotFound)

	c.Logf("Result: %s", result)
}
