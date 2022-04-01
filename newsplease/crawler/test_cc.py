from .commoncrawl_extractor import fname_to_date
from .commoncrawl_crawler import get_remote_index, ORACLE_HOSTS
import datetime

min_date = datetime.date(2020, 6, 1)
max_date = datetime.date(2022, 3, 1)
#print(len(get_remote_index(min_date, max_date)))
print(len(ORACLE_HOSTS))
for h in ORACLE_HOSTS:
    cmd = f'sleep 5s && ssh {h} bash /home/sam/news-please/launcher3.sh &'
    print(cmd)

# 2019 -> now = 15617
