# check_robots.py
from urllib import robotparser

robots_url = "https://www.modaoperandi.com/robots.txt"
rp = robotparser.RobotFileParser()
rp.set_url(robots_url)
rp.read()

test_url = "https://www.net-a-porter.com/en-es/shop/new-in"  # replace with real product URL
# Check whether the generic crawler *"*"* may fetch it
print("Allowed for '*':", rp.can_fetch("*", test_url))
# If you plan to identify as a specific UA, check that too:
print("Allowed for 'MyScraper':", rp.can_fetch("MyScraper", test_url))