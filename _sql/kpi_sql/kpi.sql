SELECT
SDATE
,
ENBID
,ENB_CELLID


,RRC���ӽ����ɹ���  --0409�޸�������+M8013C31+ M8013C34

from
kpi_list
group by
SDATE
,
ENBID
,ENB_CELLID
where SDATE in (&1)

