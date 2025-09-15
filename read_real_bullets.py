import datetime
import time

TSV_path = '/Users/brezhrt/Downloads/1dayLogs.tsv'

def GetRealBullets(rows_to_read = None):
    bullets = []
    text_prefix = "text=Requesting rating for places: ["
    text_prefix_len = len(text_prefix)
    
    with open(TSV_path, 'r', encoding='utf-8') as file:
        for line_num, line in enumerate(file, 1):
            if rows_to_read is not None and line_num > rows_to_read:
                break
            
            line = line.strip()
            if not line:
                continue

            parts = line.split('\t')
            if len(parts) < 4:
                continue

            iso_part = parts[1]
            if not iso_part.startswith('iso_eventtime='):
                continue

            iso_time_str = iso_part[14:]  # len("iso_eventtime=") = 14

            dt = datetime.datetime.strptime(iso_time_str, '%Y-%m-%d %H:%M:%S')
            timestamp = dt.timestamp()

            text_part = parts[2]
            if not text_part.startswith(text_prefix):
                continue
            
            keys_str = text_part[text_prefix_len:-1]

            keys = [int(key.strip()) for key in keys_str.split(',') if key.strip()]

            if keys:
                bullets.append({
                    'keys': keys,
                    'timestamp': timestamp
                })

            if line_num % 10000 == 0:
                print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp)))
                print(f"Обработано {line_num:,} строк, найдено {len(bullets):,} bullets...")
    bullets.sort(key=lambda x: x['timestamp'])
    mi = min(bullets, key=lambda x: x['timestamp'])
    ma = max(bullets, key=lambda x: x['timestamp'])
    print('diaposon: ', mi, ma)
    print(f"Всего обработано строк: {line_num:,}")
    print(f"Найдено bullets: {len(bullets):,}")
    
    return bullets
