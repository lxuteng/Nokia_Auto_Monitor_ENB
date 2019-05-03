SELECT
SDATE
,
ENBID
,ENB_CELLID


,RRC连接建立成功率  --0409修改增加了+M8013C31+ M8013C34

from
kpi_list
group by
SDATE
,
ENBID
,ENB_CELLID
where SDATE in (&1)

