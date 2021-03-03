##  项目介绍：  
基本逻辑：从交易数据文件中读取交易数据，把数据插入到交易流水表trade_cmf中，交易数据涉及两个账户，需要再账户余额表act_num中更新交易数量.
表结构如下：    
```
create table act_num 
(
account_id char(11),  --账户
sec_code    char(9),  --产品代码
num         bigint,      --交易数量
up_time     timestamp
);

  
CREATE TABLE TRADE_CMF
  ( 
    BUY_SEC_CODE                     CHAR(9)      --buy是买单数据
  , BUY_ACT_NO                       LARGEINT
  , BUY_ACCT_ID                      CHAR(11) 
  , BUY_ORDER_NO                     LARGEINT
  , BUY_TRADE_DIR                    CHAR(1) 
  , BUY_TRADE_PRICE                  LARGEINT
  , BUY_TRADE_VOL                    LARGEINT
  , BUY_TS                           LARGEINT
  , BUY_PBU                          INT
  , BUY_RESERVED                     INT
  , BUY_RSRV                         INT
  , SELL_SEC_CODE                    CHAR(9)     --sell是卖单数据
  , SELL_ACT_NO                      LARGEINT
  , SELL_ACCT_ID                     CHAR(11) 
  , SELL_ORDER_NO                    LARGEINT
  , SELL_TRADE_DIR                   CHAR(1) 
  , SELL_TRADE_PRICE                 LARGEINT
  , SELL_TRADE_VOL                   LARGEINT
  , SELL_TS                          LARGEINT
  , SELL_PBU                         INT
  , SELL_RESERVED                    INT
  , SELL_RSRV                        INT
  , TRADE_TIME                       TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP 
  );
```



### 运行方式：   
```java -cp ./sjs-0.2.jar:.:$CLASSPATCH com.sjs.Trade 10.10.11.16  ./xx.data 1000 seabase```  
参数说明:   
#1: DcsMaster IP地址    
#2：数据文件地址  
#3：批量处理的数据的批大小  
#4：schema名字  


### 校验数据一致性的SQL,打印0条数据表示所有数据一致    
```
select b.*, c.num from (
select a.ACCT_ID acct_id, a.SEC_CODE sec_code, count(1), sum(a.vol) vol from (select BUY_SEC_CODE sec_code, BUY_ACCT_ID acct_id, BUY_TRADE_VOL vol from TRADE_CMF union all
select SELL_SEC_CODE sec_code, SELL_ACCT_ID acct_id,0-SELL_TRADE_VOL vol from TRADE_CMF) a group by a.ACCT_ID, a.SEC_CODE having count(1) >1
) b, act_num c where b.acct_id=c.account_id and b.sec_code=c.sec_code and b.vol <> c.num;
```

### 检查具体不一致的交易数据，第一条sql是查询买卖双方的交易数据，整数为买方，负数为卖方；第二条是查询持仓余额数     
```
select * from (select BUY_SEC_CODE sec_code, BUY_ACCT_ID acct_id, BUY_TRADE_VOL vol from TRADE_CMF union all
select SELL_SEC_CODE sec_code, SELL_ACCT_ID acct_id,0-SELL_TRADE_VOL vol from TRADE_CMF) a, act_num b where a.acct_id=b.account_id and a.acct_id='1132724617';

select * from act_num where account_id='1132724617';
```
