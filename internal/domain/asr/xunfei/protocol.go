package xunfei

type request struct {
	Common   common   `json:"common,omitempty"`
	Business business `json:"business,omitempty"`
	Data     data     `json:"data"`
}

type common struct {
	AppID string `json:"app_id"`
}

type business struct {
	Language string `json:"language"`
	Domain   string `json:"domain"`
	Accent   string `json:"accent,omitempty"`
}

type data struct {
	Status   int    `json:"status"`
	Format   string `json:"format"`
	Audio    string `json:"audio"`
	Encoding string `json:"encoding"`
}

type response struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	SID     string `json:"sid"`
	Data    struct {
		Status int `json:"status"`
		Result struct {
			Sn  int    `json:"sn"`
			Ls  bool   `json:"ls"`
			Pgs string `json:"pgs"`
			Rg  []int  `json:"rg"`
			Ws  []struct {
				Bg int `json:"bg"`
				Cw []struct {
					W  string `json:"w"`
					Sc int    `json:"sc,omitempty"`
				} `json:"cw"`
			} `json:"ws"`
		} `json:"result"`
	} `json:"data"`
}

func (r *response) extractText() string {
	text := ""
	for _, ws := range r.Data.Result.Ws {
		for _, cw := range ws.Cw {
			text += cw.W
			break
		}
	}
	return text
}

func (r *response) candidateCount() int {
	count := 0
	for _, ws := range r.Data.Result.Ws {
		count += len(ws.Cw)
	}
	return count
}
