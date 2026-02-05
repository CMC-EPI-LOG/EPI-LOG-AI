# CSV 데이터를 Python 딕셔너리로 변환하는 스크립트
import csv
import json

# Read CSV
with open('logic.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

# Mappings
age_map = {
    '영아(0-2세)': 'infant',
    '유아(3-6세)': 'toddler',
    '초등저(7-9세)': 'elementary_low',
    '초등고(10-12)': 'elementary_high',
    '청소년/성인': 'teen_adult'
}

cond_map = {
    '일반': 'general',
    '비염': 'rhinitis',
    '천식': 'asthma',
    '아토피': 'atopy'
}

grade_map = {
    '좋음': 'ok',
    '보통': 'caution',
    '나쁨': 'warning',
    '매우나쁨': 'warning'  # Both 나쁨 and 매우나쁨 map to warning
}

# Initialize structures
decision_texts = {}
action_items = {}

for row in rows:
    age_kr = row['연령대']
    cond_kr = row['질환군']
    grade_kr = row['대기등급']
    
    age_key = age_map.get(age_kr)
    cond_key = cond_map.get(cond_kr)
    grade_key = grade_map.get(grade_kr)
    
    if not all([age_key, cond_key, grade_key]):
        continue
    
    # Initialize nested dicts
    if age_key not in decision_texts:
        decision_texts[age_key] = {}
        action_items[age_key] = {}
    
    if cond_key not in decision_texts[age_key]:
        decision_texts[age_key][cond_key] = {}
        action_items[age_key][cond_key] = {}
    
    # Store decision text
    decision_texts[age_key][cond_key][grade_key] = row['메인문구']
    
    # Store action items
    actions = []
    for i in range(1, 4):
        action = row.get(f'행동{i}', '').strip()
        if action:
            actions.append(action)
    
    action_items[age_key][cond_key][grade_key] = actions

# Write to JSON for inspection
with open('decision_logic.json', 'w', encoding='utf-8') as f:
    json.dump({
        'decision_texts': decision_texts,
        'action_items': action_items
    }, f, ensure_ascii=False, indent=2)

print("✅ Conversion complete! Check decision_logic.json")
print(f"Total entries processed: {len(rows)}")
print(f"Age groups: {list(decision_texts.keys())}")
