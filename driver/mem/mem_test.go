package mem

import (
	"github.com/flaub/kissdif/driver/test"
	. "launchpad.net/gocheck"
	"testing"
)

type TestDriver struct {
	*test.TestSuite
}

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

func init() {
	Suite(&TestDriver{TestSuite: test.NewTestSuite("mem")})
}
