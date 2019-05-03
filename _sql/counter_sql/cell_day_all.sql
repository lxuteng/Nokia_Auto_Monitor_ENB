SELECT
SDATE
,
ENBID
,ENB_CELLID


,round(100.00*sum(M8013C5)/sum(M8013C17+M8013C18+M8013C19+M8013C20+ M8013C21+M8013C31+ M8013C34),2)  RRC连接建立成功率  --0409修改增加了+M8013C31+ M8013C34
,round(100.00*sum(M8006C0-(M8006C244+M8006C248+M8006C245+M8006C249+M8006C252+M8006C253))/sum(M8006C0),2)  ERAB建立成功率 
,round(100.00*sum(M8013C5)/sum(M8013C17+M8013C18+M8013C19+M8013C20+ M8013C21+M8013C31+ M8013C34)*sum(M8006C0-(M8006C244+M8006C248+M8006C245+M8006C249+M8006C252+M8006C253))/sum(M8006C0),2)  无线接通率
,round(100.00*sum(M8006C176+M8006C177+M8006C178+M8006C179+M8006C180+M8013C59+M8013C60) /sum(M8013C47+(M8051C62/M8051C63)),2) 无线掉线率
,round(100.00*sum(M8006C176+M8006C177+M8006C178+M8006C179+M8006C180+M8013C59+M8013C60)/sum(M8006C1+M8001C223),2) ERAB掉线率   --这公式和亿阳结果基本一致
,round(100.00* sum(M8009C7 + M8014C7 + M8014C19) / sum(M8009C6 + M8014C0 + M8014C14),2) 切换成功率ZB 
,round(100.00* sum(M8009C7 + M8014C7 + M8014C19) / sum(M8009C6 + M8014C6 + M8014C18),2) 切换成功率QQ
,round(1.00*sum(M8012C19+M8012C20)/(1000*1000),2)   用户面PDCP上行数据量MB
   
,max(M8051C58)  最大激活用户数       
,sum(M8013C65+M8013C66+M8013C67+M8013C68+M8013C69) 拥塞次数

,sum(M8013C5)  RRC连接建立成功次数
,sum(M8013C17+M8013C18+M8013C19+M8013C20+ M8013C21+M8013C31+ M8013C34) RRC连接建立请求次数
,sum(M8006C1) ERAB建立成功数real
,sum(M8006C0-(M8006C244+M8006C248+M8006C245+M8006C249+M8006C252+M8006C253)) ERAB建立成功数  -- 2017年7月31日 更新
,sum(M8006C0) ERAB建立请求数
,sum(M8006C176+M8006C177+M8006C178+M8006C179+M8006C180+M8013C59+M8013C60) 无线掉线率分子
,sum(M8013C47+M8051C62/M8051C63) 无线掉线率分母 --分版本
,sum(M8016C25+M8006C176+M8006C177+M8006C178+M8006C179+M8006C180+M8006C257) ERAB掉线次数  --RL55 15A自适应
,sum(M8006C1 + M8001C223) ERAB掉线率分母
,sum(M8009C7 + M8014C7 + M8014C19) 切换成功次数 
,sum(M8009C6 + M8014C0 + M8014C14) 切换请求次数ZB 
,sum(M8009C6 + M8014C6 + M8014C18) 切换请求次数QQ
from
kpi_list
group by
SDATE
,
ENBID
,ENB_CELLID

