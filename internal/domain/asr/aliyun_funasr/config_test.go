package aliyun_funasr

import (
	"reflect"
	"testing"
)

func TestConfigFromMapAcceptsLanguageHints(t *testing.T) {
	tests := []struct {
		name string
		cfg  map[string]interface{}
		want []string
	}{
		{
			name: "array value",
			cfg: map[string]interface{}{
				"language_hints": []interface{}{" zh ", "en", ""},
			},
			want: []string{"zh", "en"},
		},
		{
			name: "comma separated string value",
			cfg: map[string]interface{}{
				"language_hints": "zh,en",
			},
			want: []string{"zh", "en"},
		},
		{
			name: "language compatibility alias",
			cfg: map[string]interface{}{
				"language": "ja",
			},
			want: []string{"ja"},
		},
		{
			name: "nested aliyun_funasr config",
			cfg: map[string]interface{}{
				"aliyun_funasr": map[string]interface{}{
					"language_hints": []interface{}{"zh"},
				},
			},
			want: []string{"zh"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := ConfigFromMap(tt.cfg)
			if !reflect.DeepEqual(conf.LanguageHints, tt.want) {
				t.Fatalf("LanguageHints = %#v, want %#v", conf.LanguageHints, tt.want)
			}
		})
	}
}
