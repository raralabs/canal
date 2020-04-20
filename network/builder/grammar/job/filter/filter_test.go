package filter

import (
	"errors"
	"log"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {

	t.Run("Different Filters", func(t *testing.T) {
		tests := []struct {
			name     string
			desc     string
			parseErr error
			argName  string
			arg      interface{}
			want     bool
			err      error
		}{
			//TODO: add test cases.
			{"Primary Filter", "f1 > \"1.1\"", nil, "f1", 2.2, true, nil},
			{"Wrong Filter", "f1 > ", errors.New("Unk"), "f1", 2.2, true, nil},
			{"Simple String Filter", "name != 'Nischal Nepal'", nil, "name", "Nischal", true, nil},
			{"Complex String Filter", "greet == \"Ho1a Amiho\"", nil, "greet", "Nischal", false, nil},
			{"Bool Filter", "truth == false", nil, "truth", true, false, nil},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := ParseReader("", strings.NewReader(tt.desc))

				if tt.parseErr != nil {
					assert.NotNil(t, err, "Error should not be nil")
					return
				} else {
					assert.Nil(t, err, "Error should be nil")
				}

				input := map[string]interface{}{tt.argName: tt.arg}

				if filt, ok := got.(Filter); ok {
					output, err := filt(input)

					if tt.err != nil {
						assert.NotNil(t, err, "Error should not be nil")
						return
					} else {
						assert.Nil(t, err, "Error should be nil")
					}
					assert.Equal(t, tt.want, output, "Output Should Match with Wanted Result")
				}
			})
		}
	})

	t.Run("Compound Filter", func(t *testing.T) {
		filtDesc := "value/2 == 5 or (value < 5 and value%2 != 0)"

		got, err := ParseReader("", strings.NewReader(filtDesc))
		if err != nil {
			log.Fatalf("%v\n", err)
		}

		tests := []struct {
			name  string
			value interface{}
			want  bool
			err   error
		}{
			// TODO: add test cases.
			{"Int Test", 12, false, nil},
			{"Float Test", 12.2, false, nil},
			{"Int True Test", 10, true, nil},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				input := map[string]interface{}{"value": tt.value}

				if filt, ok := got.(Filter); ok {
					output, err := filt(input)

					if err != tt.err {
						t.Error("Error Should be nil")
					}

					assert.Equal(t, tt.want, output, "Output Should Match with Wanted Result")
				}
			})
		}
	})
}
