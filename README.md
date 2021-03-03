##  项目介绍：  
基本逻辑：从交易数据文件中读取交易数据，把数据插入到交易流水表trade_cmf中，交易数据涉及两个账户，需要再账户余额表act_num中更新交易数量.
表结构如下：    
```
create table act_num 
(
account_id varchar(11),  --账户
sec_code    varchar(9),  --产品代码
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
