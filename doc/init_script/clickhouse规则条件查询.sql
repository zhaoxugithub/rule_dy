-- 查询事件
select
    deviceId,
    count(1) as cnt
from zenniu_detail
where deviceId='000008' and eventId='adShow' and properties['adId']='14'
  and timeStamp between 1615900460000 and 1615900580000
group by deviceId
;

-- 事件序列满足
SELECT
    deviceId,
    sequenceMatch('.*(?1).*(?2).*(?3)')(
    toDateTime(`timeStamp`),
    eventId = 'adShow' and properties['adId']='10',
    eventId = 'addCart' and properties['pageId']='720',
    eventId = 'collect' and properties['pageId']='263'
  ) as is_match3,
    sequenceMatch('.*(?1).*(?2)')(
    toDateTime(`timeStamp`),
    eventId = 'adShow' and properties['adId']='10',
    eventId = 'addCart' and properties['pageId']='720'
  ) as is_match2,
    sequenceMatch('.*(?1).*')(
    toDateTime(`timeStamp`),
    eventId = 'adShow' and properties['adId']='10',
    eventId = 'addCart' and properties['pageId']='720'
  ) as is_match1
from zenniu_detail
where deviceId='rVacGhu7OJgl' and  `timeStamp` > 1615900460000
  and (
        (eventId='adShow' and properties['adId']='10')
        or
        (eventId = 'addCart' and properties['pageId']='720')
        or
        (eventId = 'collect' and properties['pageId']='263')
    )
group by deviceId
;

/*
┌─deviceId─────┬─is_match3─┬─is_match2─┬─is_match1─┐
│ rVacGhu7OJgl │         1 │         1 │         1 │
└──────────────┴───────────┴───────────┴───────────┘

*/

-------------------------------------------------------------------------------------------------------

-- 创建用户行为日志明细表
set allow_experimental_map_type = 1;
drop table if exists default.yinew_detail;
create table default.yinew_detail
(
    account        String,
    appId          String,
    appVersion     String,
    carrier        String,
    deviceId       String,
    deviceType     String,
    eventId        String,
    ip             String,
    latitude       Float64,
    longitude      Float64,
    netType        String,
    osName         String,
    osVersion      String,
    properties Map(String,String),
    releaseChannel String,
    resolution     String,
    sessionId      String,
    timeStamp      Int64,
    INDEX u (deviceId) TYPE minmax GRANULARITY 3,
    INDEX t (timeStamp) TYPE minmax GRANULARITY 3
) ENGINE = MergeTree()
      ORDER BY (deviceId, timeStamp)
;


-- 创建kafka引擎表
drop table if exists default.yinew_detail_kafka;
create table default.yinew_detail_kafka
(
    account        String,
    appId          String,
    appVersion     String,
    carrier        String,
    deviceId       String,
    deviceType     String,
    eventId        String,
    ip             String,
    latitude       Float64,
    longitude      Float64,
    netType        String,
    osName         String,
    osVersion      String,
    properties Map(String,String),
    releaseChannel String,
    resolution     String,
    sessionId      String,
    timeStamp      Int64
) ENGINE = Kafka('centos201:9092,centos203:9092,centos202:9092', 'yinew_applog', 'group1', 'JSONEachRow')
;


-- 创建物化视图（桥接kafka引擎表和事件明细表）
drop view if exists yinew_view;
create MATERIALIZED VIEW yinew_view TO yinew_detail
as
select account,
       appId,
       appVersion,
       carrier,
       deviceId,
       deviceType,
       eventId,
       ip,
       latitude,
       longitude,
       netType,
       osName,
       osVersion,
       properties,
       releaseChannel,
       resolution,
       sessionId,
    timeStamp
from yinew_detail_kafka
;

--tag87=v2, tag26=v1
select *
from yinew_detail
where eventId = 'K';

select count(*)
from yinew_detail;

--  * 画像属性条件: tag87=v2, tag26=v1
--  * 行为次数条件： 2021-06-18 ~ 当前 , 事件 C [p6=v8,p12=v5] 做过 >= 2次
select count(1)
from yinew_detail
where eventId = 'C'
  and properties['p6'] = 'v8'
  and properties['p12'] = 'v5'
  and deviceId = '000099'
  and timeStamp between 1623945600000 and 1660924464000;

-- {'p6': 'v2', 'p7': 'v2'}
select deviceId, count(1)
from yinew_detail
where eventId = 'C'
  and properties['p6'] = 'v2'
  and properties['p7'] = 'v2'
  and timeStamp between 1623945600000 and 1660924464000
group by deviceId;


-- 行为序列规则
select deviceId,
       sequenceMatch('.*(?1).*(?2).*(?3).*')(
                     toDateTime(`timeStamp`),
                     eventId = 'A',
                     eventId = 'B',
                     eventId = 'C'
           --匹配中就返回1，没有返回0
           ) as isMathch3,
       --匹配两种
        sequenceMatch('.*(?1).*(?2).*')(
                     toDateTime(`timeStamp`),
                     eventId = 'A',
                     eventId = 'B',
                     eventId = 'C'
           ) as isMatch2,
       --只匹配一种
        sequenceMatch('.*(?1).*')(
                     toDateTime(`timeStamp`),
                     eventId = 'A',
                     eventId = 'B',
                     eventId = 'C'
           ) as isMatch1
from yinew_detail
where timeStamp between -1 AND 9999999999999
  and (
    (eventId = 'A' and properties['p1'] = 'v2')
   or
    (eventId = 'B' and properties['p2'] = 'v2')
   or
    (eventId = 'C' and properties['p3'] = 'v2')
    )
group by deviceId;



select deviceId, count(1) as ggg
from yinew_detail
group by deviceId
having ggg >= 2;



select deviceId,eventId,timeStamp,properties
from yinew_detail
where deviceId in (select deviceId
    from (select deviceId, count(1) as ggg
    from yinew_detail
    group by deviceId
    having ggg >= 2));





-- 行为序列规则
select deviceId,
       sequenceMatch('.*(?1).*(?2).*')(
                     toDateTime(`timeStamp`),
                     eventId = 'K',
                     eventId = 'X'

           ) as isMatch2,
       --只匹配一种
        sequenceMatch('.*(?1).*')(
                     toDateTime(`timeStamp`),
                     eventId = 'K',
                     eventId = 'X'

           ) as isMatch1
from yinew_detail
where timeStamp between -1 AND 9999999999999
  and (
    (eventId = 'K' and properties['p5'] = 'v2')
   or
    (eventId = 'X' and properties['p3'] = 'v2')

    )
group by deviceId;



























