"""
FunASR WebSocket 端到端测试客户端
用法: python funasr_wss_test.py <wav文件> [host] [port]
连接 ws://host:port/ ，按 xiaozhiServer 协议发送：
  1. 配置消息(JSON): mode/chunk_size/chunk_interval/audio_fs/wav_name/is_speaking
  2. 音频数据(binary, PCM16)
  3. 结束消息(is_speaking=false)
然后打印服务端返回的识别结果
"""
import asyncio
import json
import sys
import wave

import websockets


async def main(wav_path: str, host: str = "127.0.0.1", port: int = 10096):
    # 读取 WAV（要求 16k 单声道 PCM16）
    wf = wave.open(wav_path, "rb")
    fs = wf.getframerate()
    ch = wf.getnchannels()
    sw = wf.getsampwidth()
    data = wf.readframes(wf.getnframes())
    wf.close()
    print(f"WAV: fs={fs} ch={ch} sampwidth={sw} len={len(data)}bytes ({len(data)/2/fs:.2f}s)")

    chunk_ms = 100
    chunk_bytes = int(fs * ch * sw / 1000) * chunk_ms

    url = f"ws://{host}:{port}/"
    print(f"连接 {url} ...")
    async with websockets.connect(url, ping_interval=None, open_timeout=30) as ws:
        # 1) 配置消息
        cfg = {
            "mode": "offline",
            "chunk_size": [5, 10, 5],
            "chunk_interval": 10,
            "audio_fs": fs,
            "wav_name": "test-wav",
            "wav_format": "pcm",
            "is_speaking": True,
            "itn": True,
        }
        await ws.send(json.dumps(cfg, ensure_ascii=False))
        print("已发送配置:", cfg)

        # 2) 流式发送音频
        for i in range(0, len(data), chunk_bytes):
            await ws.send(data[i : i + chunk_bytes])
        print(f"已发送音频 {len(data)} bytes")

        # 3) 结束消息
        end = {"mode": "offline", "chunk_size": [5, 10, 5], "is_speaking": False}
        await ws.send(json.dumps(end, ensure_ascii=False))
        print("已发送结束消息")

        # 4) 读取结果（长超时，CPU 首次推理较慢）
        print("等待识别结果 ...")
        while True:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=180)
            except asyncio.TimeoutError:
                print("!! 等待结果超时(180s)")
                break
            if isinstance(msg, (bytes, bytearray)):
                continue
            try:
                obj = json.loads(msg)
            except Exception:
                continue
            text = obj.get("text", "")
            print(f"收到结果: mode={obj.get('mode')} is_final={obj.get('is_final')} text={text!r}")
            if obj.get("is_final"):
                break


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python funasr_wss_test.py <wav文件> [host] [port]")
        sys.exit(1)
    wav = sys.argv[1]
    h = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1"
    p = int(sys.argv[3]) if len(sys.argv) > 3 else 10096
    asyncio.run(main(wav, h, p))
