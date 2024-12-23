package streamlit

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestJsonError(t *testing.T) {
	faultTests := []struct {
		e    *jsonError
		want Fault
	}{
		{newError().client(), faultClient},
		{newError().server(), faultServer},
	}

	for _, tt := range faultTests {
		t.Run(fmt.Sprintf("%s error", tt.want), func(t *testing.T) {
			assert.Equal(t, tt.e.Fault, tt.want)
		})
	}

	t.Run("status code", func(t *testing.T) {
		assert.Equal(t, newError().status(http.StatusBadRequest).statusCode, http.StatusBadRequest)
	})

	t.Run("message", func(t *testing.T) {
		assert.Equal(t, newError().message("test").Error, "test")
	})
}
