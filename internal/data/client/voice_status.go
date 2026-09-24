package client

type VoiceStatus struct {
	HaveVoice            bool  //上次是否有说话
	HaveVoiceLastTime    int64 //最后说话时间
	VoiceStop            bool  //是否停止说话
	SilenceThresholdTime int64 //无声音持续时间阈值
}

func (v *VoiceStatus) Reset() {
	v.HaveVoice = false
	v.HaveVoiceLastTime = 0
	v.VoiceStop = false
}

func (v *VoiceStatus) IsSilence(diffMilli int64) bool {
	return diffMilli > v.SilenceThresholdTime
}

func (v *VoiceStatus) GetClientHaveVoice() bool {
	return v.HaveVoice
}

func (v *VoiceStatus) SetClientHaveVoice(haveVoice bool) {
	v.HaveVoice = haveVoice
}

func (v *VoiceStatus) GetClientHaveVoiceLastTime() int64 {
	return v.HaveVoiceLastTime
}

func (v *VoiceStatus) SetClientHaveVoiceLastTime(lastTime int64) {
	v.HaveVoiceLastTime = lastTime
}

func (v *VoiceStatus) GetClientVoiceStop() bool {
	return v.VoiceStop
}

func (v *VoiceStatus) SetClientVoiceStop(voiceStop bool) {
	v.VoiceStop = voiceStop
}
