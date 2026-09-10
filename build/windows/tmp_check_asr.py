import sqlite3

conn = sqlite3.connect(r'e:\workspace\go\xiaozhiServer\build\windows\data\xiaozhi.db')
conn.row_factory = sqlite3.Row
cur = conn.cursor()
cur.execute("SELECT id, config_id, type, provider, is_default, enabled, json_data FROM configs WHERE type='asr'")
rows = cur.fetchall()
print('ASR config rows:', len(rows))
for r in rows:
    print('---')
    print('id:', r['id'], '| config_id:', r['config_id'], '| provider:', r['provider'],
          '| is_default:', r['is_default'], '| enabled:', r['enabled'])
    jd = r['json_data'] or ''
    print('json_data:', (jd[:400] + '...') if len(jd) > 400 else jd)
conn.close()
