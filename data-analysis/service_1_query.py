

"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/review_er.csv'


# query to run
query = """

select case when f.uid is null then 'normal'
	else 'black' end as is_black, n.writer_uid,
	count(*) as review
from user_review n
left join (
SELECT DISTINCT uid
FROM 
(
SELECT uid
FROM user_auth_token
WHERE policy_id=2

UNION ALL

SELECT ud.uid
FROM user_device AS ud
  LEFT OUTER JOIN device_group_exception AS dge ON ud.udid_str=dge.udid AND ud.uid=dge.uid
  LEFT OUTER JOIN (SELECT gps.udid
                   FROM device_group_block_policy_status AS gps
                     JOIN block_history_device_group AS gh ON gps.udid=gh.udid
                   WHERE gps.policy_id=2
                     AND gh.is_active=1
                     AND gh.policy_type1_category IN ('거래사기')) AS blocked_udid ON ud.udid_str=blocked_udid.udid
WHERE ud.`status` = 0
  AND ud.udid_str != ''
  AND dge.uid IS NULL
  AND blocked_udid.udid IS NOT NULL
) AS t
) f
on n.writer_uid = f.uid
group by 1, 2

"""
