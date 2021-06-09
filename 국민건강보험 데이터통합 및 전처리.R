library(dplyr)
library(readxl)
library(ggplot2)
library(tidyr)
library(data.table)
library(stringr)
library(prettyR)

#시작 전 메모리에 있는 객체들을 모두 삭제
rm(list=ls())

#가비지 컬렉션을 수행해 사용하지 않는 메모리를 해제
gc()

setwd("D:/CloudStation/CloudStation/!6 DB_Dataset/표본코호트1.0DB_DEMO")

nhid_jk_2002 <- read.csv("./01_JK/nhid_jk_2002.csv")
nhid_jk_2002 <- nhid_jk_2002 %>% select(-X)
nhid_jk_2002$PERSON_ID <- as.character(nhid_jk_2002$PERSON_ID)

#데이터 프레임의 형태로 저장되어 있다. 
class(nhid_jk_2002)
nhid_jk_2002_dt<- as.data.table(nhid_jk_2002)

### 3. 유지할 ATT(데이터 통합 2-2) ###
# 01_JK #
BYEAR <- 2002 -nhid_jk_2002$AGE_GROUP +1
BYEAR <- nhid_jk_2002$STND_Y - nhid_jk_2002$AGE_GROUP +1
JK_02 <- cbind(nhid_jk_2002,BYEAR)
JK_02 <- as.data.table(JK_02)
nrow(JK_02)


nhid_gy20_t1_2002 <- read.csv("./02_T120/nhid_gy20_t1_2002.csv")
nhid_gy20_t1_2002 <- as.data.table(nhid_gy20_t1_2002)

nhid_gy20_t1_2002 <- nhid_gy20_t1_2002 %>% select(-X)
nhid_gy20_t1_2002$PERSON_ID <- as.character(nhid_gy20_t1_2002$PERSON_ID)
nhid_gy20_t1_2002$KEY_SEQ <- as.character(nhid_gy20_t1_2002$KEY_SEQ)

# 02_T120 #
T120_02 <- nhid_gy20_t1_2002
class(T120_02)
remove(nhid_gy20_t1_2002)

nhid_gy20_t2_2002 <- read.csv("./06_T220/nhid_gy20_t2_2002.csv")
nhid_gy20_t2_2002 <- as.data.table(nhid_gy20_t2_2002)
nhid_gy20_t2_2002 <- nhid_gy20_t2_2002 %>% select(-X)
nhid_gy20_t2_2002$PERSON_ID <- as.character(nhid_gy20_t2_2002$PERSON_ID)
nhid_gy20_t2_2002$KEY_SEQ <- as.character(nhid_gy20_t2_2002$KEY_SEQ)

# 06_T220 #
T220_02 <- nhid_gy20_t2_2002
class(T220_02)
remove(nhid_gy20_t2_2002)

# 12_GJ #
#GJ_02
# 13_YK #
nhid_yk_2002 <- read.csv("./13_YK/nhid_yk_2002.csv")
nhid_yk_2002 <- as.data.table(nhid_yk_2002)
nhid_yk_2002 <- nhid_yk_2002 %>% select(-X)

YK_02 <- nhid_yk_2002
remove(nhid_yk_2002)
class(JK_02)

### 4. 변환할 ATT(데이터 통합 2-3) ###

# 03_T130 #
nhid_gy30_t1_2002 <- read.csv("./03_T130/nhid_gy30_t1_2002.csv")
nhid_gy30_t1_2002 <- as.data.table(nhid_gy30_t1_2002)
nhid_gy30_t1_2002 <- nhid_gy30_t1_2002 %>% select(-X)
nhid_gy30_t1_2002$KEY_SEQ <- as.character(nhid_gy30_t1_2002$KEY_SEQ)

T130_02 <- nhid_gy30_t1_2002
remove(nhid_gy30_t1_2002)
test <- unique(T130_02$KEY_SEQ) 
class(T130_02)

# TYPE 변환
T130_02$KEY_SEQ <- as.character(T130_02$KEY_SEQ)
T130_02$ITEM_CD <- as.factor(T130_02$ITEM_CD)

head(T130_02)
### 1) 항코드 기준으로 펼치기
# 필요한 변수만 select
CLAUSE_ <- subset(T130_02, select = c("KEY_SEQ","SEQ_NO","RECU_FR_DT","CLAUSE_CD","ITEM_CD"))
CLAUSE_$KEY_SEQ <- as.character((CLAUSE_$KEY_SEQ))
CLAUSE_$CLAUSE_CD <- as.character(CLAUSE_$CLAUSE_CD)
CLAUSE_$CLAUSE_CD <- paste("CLAUSE_CD.",CLAUSE_$CLAUSE_CD,sep='')

# Num 개수 세기
CLAUSE_ <- CLAUSE_ %>%  group_by(KEY_SEQ,CLAUSE_CD) %>% mutate(ITEM_SUM = n())
CLAUSE_ <- subset(CLAUSE_, select = - ITEM_CD)
nrow(CLAUSE_)

#중복제거
CLAUSE_ <- unique(CLAUSE_)
# 청구일련번호 별로 펼치기
CLAUSE_ <- as.data.frame(spread(CLAUSE_ ,CLAUSE_CD,ITEM_SUM,fill=0))
CLAUSE_ <- as.data.table(CLAUSE_)
CLAUSE_ <- unique(CLAUSE_)

### 2) 금액 기준으로 펼치기
# 필요한 변수만 select
AMT_ <- subset(T130_02, select = c("KEY_SEQ","UN_COST", "AMT"))
AMT_ <- AMT_ %>% group_by(KEY_SEQ) %>% summarise(SUM_AMT=sum(AMT),MEAN_UN_COST=mean(UN_COST))

### 3) 파일 합치기
T130_02 <- left_join(CLAUSE_, AMT_, by="KEY_SEQ")
nrow(T130_02)
T130_02 <- as.data.table(T130_02)
class(T130_02)

remove(CLAUSE_)
remove(AMT_)
# 04_T140 #
nhid_gy40_t1_2002 <- read.csv("./04_T140/nhid_gy40_t1_2002.csv")
nhid_gy40_t1_2002 <- as.data.table(nhid_gy40_t1_2002)
nhid_gy40_t1_2002 <- nhid_gy40_t1_2002 %>% select(-X)
nhid_gy40_t1_2002$KEY_SEQ <- as.character(nhid_gy40_t1_2002$KEY_SEQ)

SICK_SYM_0 <- nhid_gy40_t1_2002$SICK_SYM

#nhid_gy40_t1_2002 의 SICK_SYM_0의 D,H를 제외한 것들을 통계표 번호로 바꾸기

SICK_SYM_1 <- ifelse(substr(SICK_SYM_0,1,1)=='A' |substr(SICK_SYM_0,1,1)=='B',1,
                     +                      + ifelse(substr(SICK_SYM_0,1,1)=='C', 2,
                                                     + ifelse(substr(SICK_SYM_0,1,1)=='E',4,
                                                              + ifelse(substr(SICK_SYM_0,1,1)=='F',5,
                                                                       + ifelse(substr(SICK_SYM_0,1,1)=='G',6,
                                                                                + ifelse(substr(SICK_SYM_0,1,1)=='I',9,
                                                                                         + ifelse(substr(SICK_SYM_0,1,1)=='J',10,
                                                                                                  + ifelse(substr(SICK_SYM_0,1,1)=='K',11,
                                                                                                           + ifelse(substr(SICK_SYM_0,1,1)=='L',12,
                                                                                                                    + ifelse(substr(SICK_SYM_0,1,1)=='M',13,
                                                                                                                             + ifelse(substr(SICK_SYM_0,1,1)=='N',14,
                                                                                                                                      + ifelse(substr(SICK_SYM_0,1,1)=='O',15,
                                                                                                                                               + ifelse(substr(SICK_SYM_0,1,1)=='P',16,
                                                                                                                                                        + ifelse(substr(SICK_SYM_0,1,1)=='Q',17,
                                                                                                                                                                 + ifelse(substr(SICK_SYM_0,1,1)=='R',18,
                                                                                                                                                                          + ifelse(substr(SICK_SYM_0,1,1)=='S' | substr(SICK_SYM_0,1,1)=='T',19,
                                                                                                                                                                                   + ifelse(substr(SICK_SYM_0,1,1)=='V' | substr(SICK_SYM_0,1,1)=='Y',20,
                                                                                                                                                                                            + ifelse(substr(SICK_SYM_0,1,1)=='Z',21,NA))))))))))))))))))

#NA값은 D,H에 해당함
summary(SICK_SYM_1)

#기존 테이블과 변환한 것 합치기
nhid_gy40_t1_2002_1 <- cbind(nhid_gy40_t1_2002, SICK_SYM_1)
class(nhid_gy40_t1_2002_1)
remove(SICK_SYM_1)
#D,H에 해당하는 행 제거
nhid_gy40_t1_2002_1 <- nhid_gy40_t1_2002_1 %>% filter(!is.na(SICK_SYM_1))


#nhid_gy40_t1_2002에서 D,H값을 숫자로 바꾸기
SICK_SYM_2 <- ifelse(substr(SICK_SYM_0,1,2)=='D1' | substr(SICK_SYM_0,1,2)=='D2' |substr(SICK_SYM_0,1,2)=='D3' |substr(SICK_SYM_0,1,2)=='D4',2,
                     +                      + ifelse(substr(SICK_SYM_0,1,2)=='D5'| substr(SICK_SYM_0,1,2)=='D6' | substr(SICK_SYM_0,1,2)=='D7' |substr(SICK_SYM_0,1,2)=='D8',3,
                                                     +                               + ifelse(substr(SICK_SYM_0,1,2)=='H0' |substr(SICK_SYM_0,1,2)=='H1'|substr(SICK_SYM_0,1,2)=='H2'|substr(SICK_SYM_0,1,2)=='H3'|substr(SICK_SYM_0,1,2)=='H4'|substr(SICK_SYM_0,1,2)=='H5',7,
                                                                                              +                                        + ifelse(substr(SICK_SYM_0,1,2)=='H6' |substr(SICK_SYM_0,1,2)=='H7' |substr(SICK_SYM_0,1,2)=='H8'|substr(SICK_SYM_0,1,2)=='H9',8,NA))))

#SICK_SYM_2와 기존 테이블 합치기
nhid_gy40_t1_2002_2 <- cbind(nhid_gy40_t1_2002,SICK_SYM_2)
class(nhid_gy40_t1_2002_2)
remove(nhid_gy40_t1_2002)

#D,H외 의 값들 (NA) 제거
nhid_gy40_t1_2002_2 <- nhid_gy40_t1_2002_2 %>% filter(!is.na(SICK_SYM_2))

#이름 변경
nhid_gy40_t1_2002_2 <-nhid_gy40_t1_2002_2 %>% rename(SICK_SYM_1 = "SICK_SYM_2")

#합치기
T140_02 <- rbind(nhid_gy40_t1_2002_1,nhid_gy40_t1_2002_2)
T140_02 <- as.data.table(T140_02)
remove(nhid_gy40_t1_2002_1)
remove(nhid_gy40_t1_2002_2)
nrow(T140_02)
T140_02 <- subset(T140_02,select =-c(DSBJT_CD,SICK_SYM))
T140_02$KEY_SEQ <- as.character(T140_02$KEY_SEQ)
T140_02$SICK_SYM_1 <- paste0("SICK_SYM_1.",T140_02$SICK_SYM_1)
T140_02 <- T140_02 %>%  group_by(KEY_SEQ,SICK_SYM_1) %>% mutate(ITEM_SUM = n())
T140_02 <- unique(T140_02)
T140_02 <- spread(T140_02,SICK_SYM_1,ITEM_SUM,fill=0)

# 05_T160 #
# 처방전 파일 불러오기
nhid_gy60_t1_2002 <- read.csv("./05_T160/nhid_gy60_t1_2002.csv")
nhid_gy60_t1_2002 <- as.data.table(nhid_gy60_t1_2002)
nhid_gy60_t1_2002 <- nhid_gy60_t1_2002 %>% select(-X)
nhid_gy60_t1_2002$KEY_SEQ <- as.character(nhid_gy60_t1_2002$KEY_SEQ)

T160_02 <- nhid_gy60_t1_2002
class(T160_02)
remove(nhid_gy60_t1_2002)

# 약제 파일 불러오기
ATC1 <- read_excel("D:/CloudStation/CloudStation/R code 공유/R code/ATC/ATC 1.xlsx")
ATC2 <- read_excel("D:/CloudStation/CloudStation/R code 공유/R code/ATC/ATC 2.xlsx")
ATC3 <- read_excel("D:/CloudStation/CloudStation/R code 공유/R code/ATC/ATC 3.xlsx")
ATC4 <- read_excel("D:/CloudStation/CloudStation/R code 공유/R code/ATC/ATC 4.xlsx")
### ATC 파일 전처리 ###
ATC <- rbind(ATC1,ATC2,ATC3,ATC4)
ATC <- as.data.table(ATC)
remove(ATC1)
remove(ATC2)
remove(ATC3)
remove(ATC4)

# 결측치 있는 행 제거 #
ATC <- na.omit(ATC)
# 유니크한 값만 남기기 #
ATC <- unique(ATC)
# medicine에 ATC넣기 # 
medicine <- ATC
class(ATC)
remove(ATC)

# 처방전 파일 살펴보기
T160_02$KEY_SEQ <- as.character(T160_02$KEY_SEQ)

test <- unique(T160_02$KEY_SEQ)
nrow(test) # 청구일련번호 고유한 것 3845개.

# 1) 분류유형코드 기준으로 펼치기
# 필요한 변수만 select
DIV_TYPE <- T160_02 %>% select(KEY_SEQ, SEQ_NO, RECU_FR_DT, DIV_TYPE_CD)
DIV_TYPE$DIV_TYPE_CD <- paste0("DIV_TYPE_CD.",DIV_TYPE$DIV_TYPE_CD)

# Num 개수 세기
DIV_TYPE <- DIV_TYPE %>% group_by(KEY_SEQ, DIV_TYPE_CD) %>% mutate(DIV_SUM = n())
#중복제거
DIV_TYPE <- unique(DIV_TYPE)
# 청구일련번호 별로 펼치기
DIV_TYPE <- as.data.frame(spread(DIV_TYPE, DIV_TYPE_CD, DIV_SUM,fill=0))
DIV_TYPE <- unique(DIV_TYPE)


# 2) 일반명코드 기준으로 펼치기
# 필요한 변수만 select
GNL_NM <- T160_02 %>% select(KEY_SEQ, GNL_NM_CD)
GNL_NM$GNL_NM_CD <- as.character(GNL_NM$GNL_NM_CD)
medicine$GNL_NM_CD<- as.character(medicine$GNL_NM_CD)
class(GNL_NM)

# 처방내역과 약제코드 합치기
GNL_NM <- left_join(GNL_NM,medicine,by="GNL_NM_CD")
table(is.na(GNL_NM$NUMBER)) # 옛날파일이라 분류코드에 5352개 NA생김


head(GNL_NM)
# 약제코드의 첫자리만 딴 NUM변수 생성
GNL_NM$NUM0 <- substr(GNL_NM$ATC,1,1)
GNL_NM$NUM <- paste0("GNL_NM_CD.",GNL_NM$NUM0)

# Num 개수 세기
GNL_NM <- GNL_NM %>% group_by(KEY_SEQ, NUM) %>% summarise(GNL_SUM = n())
# 청구일련번호 별로 펼치기
GNL_NM <- as.data.frame(spread(GNL_NM, NUM, GNL_SUM,fill=0))
GNL_NM <- unique(GNL_NM)


# 3) 금액 기준으로 펼치기
# 필요한 변수만 select 및 금액 합계변수 생성
AMT_ <- T160_02 %>% select(KEY_SEQ,UN_COST, AMT)
AMT_ <- AMT_ %>% group_by(KEY_SEQ) %>% summarise(SUM_AMT=sum(AMT),MEAN_UN_COST=mean(UN_COST))


# 4) 1,2,3 합치기
T160_02 <- left_join(DIV_TYPE, GNL_NM, by="KEY_SEQ")
T160_02 <- left_join(T160_02, AMT_,by="KEY_SEQ")
T160_02 <- as.data.table(T160_02)

remove(DIV_TYPE)
remove(GNL_NM)
remove(AMT_)

# 07_T230 #
nhid_gy30_t2_2002 <- read.csv("./07_T230/nhid_gy30_t2_2002.csv")
nhid_gy30_t2_2002 <- as.data.table(nhid_gy30_t2_2002)
nhid_gy30_t2_2002 <- nhid_gy30_t2_2002 %>% select(-X)
nhid_gy30_t2_2002$KEY_SEQ <- as.character(nhid_gy30_t2_2002$KEY_SEQ)

T230_02 <- nhid_gy30_t2_2002
class(T230_02)
#remove(nhid_gy30_t2_2002)
T230_02$CLAUSE_CD <- as.numeric(T230_02$CLAUSE_CD)
T230_02$CLAUSE_CD <- as.character(T230_02$CLAUSE_CD)
T230_02$KEY_SEQ <- as.character(T230_02$KEY_SEQ)
T230_02$ITEM_CD <- as.factor(T230_02$ITEM_CD)

test <- unique(T230_02$KEY_SEQ) 
nrow(test) #고유한 KEY_SEQ 513개

# TYPE 변환
T230_02$KEY_SEQ <- as.character(T230_02$KEY_SEQ)
T230_02$ITEM_CD <- as.factor(T230_02$ITEM_CD)

### 1) 항코드 기준으로 펼치기
# 필요한 변수만 select
CLAUSE_ <- subset(T230_02, select = c("KEY_SEQ","SEQ_NO","RECU_FR_DT","CLAUSE_CD","ITEM_CD"))
CLAUSE_$KEY_SEQ <- as.character((CLAUSE_$KEY_SEQ))
CLAUSE_$CLAUSE_CD <- as.character(CLAUSE_$CLAUSE_CD)
CLAUSE_$CLAUSE_CD <- paste("CLAUSE_CD.",CLAUSE_$CLAUSE_CD,sep='')

# Num 개수 세기
CLAUSE_ <- CLAUSE_ %>%  group_by(KEY_SEQ,CLAUSE_CD) %>% mutate(ITEM_SUM = n())
CLAUSE_ <- subset(CLAUSE_, select = - ITEM_CD)

#중복제거
CLAUSE_ <- unique(CLAUSE_)
# 청구일련번호 별로 펼치기
CLAUSE_ <- as.data.frame(spread(CLAUSE_ ,CLAUSE_CD,ITEM_SUM,fill=0))
CLAUSE_ <- unique(CLAUSE_)


### 2) 금액 기준으로 펼치기
# 필요한 변수만 select
AMT_ <- subset(T230_02, select = c("KEY_SEQ","UN_COST", "AMT"))
AMT_ <- AMT_ %>% group_by(KEY_SEQ) %>% summarise(SUM_AMT=sum(AMT),MEAN_UN_COST=mean(UN_COST))
AMT_ <- as.data.table(AMT_)

### 3) 파일 합치기
T230_02 <- left_join(CLAUSE_, AMT_, by="KEY_SEQ")
T230_02 <- as.data.table(T230_02)
class(T230_02)
remove(CLAUSE_)
remove(AMT_)

# 08_T240 #
nhid_gy40_t2_2002 <- read.csv("./08_T240/nhid_gy40_t2_2002.csv")
nhid_gy40_t2_2002 <- as.data.table(nhid_gy40_t2_2002)
nhid_gy40_t2_2002 <- nhid_gy40_t2_2002 %>% select(-X)
nhid_gy40_t2_2002$KEY_SEQ <- as.character(nhid_gy40_t2_2002$KEY_SEQ)

SICK_SYM_0 <- nhid_gy40_t2_2002$SICK_SYM

SICK_SYM_1 <- ifelse(substr(SICK_SYM_0,1,1)=='A' |substr(SICK_SYM_0,1,1)=='B',1,
                     +                      + ifelse(substr(SICK_SYM_0,1,1)=='C', 2,
                                                     + ifelse(substr(SICK_SYM_0,1,1)=='E',4,
                                                              + ifelse(substr(SICK_SYM_0,1,1)=='F',5,
                                                                       + ifelse(substr(SICK_SYM_0,1,1)=='G',6,
                                                                                + ifelse(substr(SICK_SYM_0,1,1)=='I',9,
                                                                                         + ifelse(substr(SICK_SYM_0,1,1)=='J',10,
                                                                                                  + ifelse(substr(SICK_SYM_0,1,1)=='K',11,
                                                                                                           + ifelse(substr(SICK_SYM_0,1,1)=='L',12,
                                                                                                                    + ifelse(substr(SICK_SYM_0,1,1)=='M',13,
                                                                                                                             + ifelse(substr(SICK_SYM_0,1,1)=='N',14,
                                                                                                                                      + ifelse(substr(SICK_SYM_0,1,1)=='O',15,
                                                                                                                                               + ifelse(substr(SICK_SYM_0,1,1)=='P',16,
                                                                                                                                                        + ifelse(substr(SICK_SYM_0,1,1)=='Q',17,
                                                                                                                                                                 + ifelse(substr(SICK_SYM_0,1,1)=='R',18,
                                                                                                                                                                          + ifelse(substr(SICK_SYM_0,1,1)=='S' | substr(SICK_SYM_0,1,1)=='T',19,
                                                                                                                                                                                   + ifelse(substr(SICK_SYM_0,1,1)=='V' | substr(SICK_SYM_0,1,1)=='Y',20,
                                                                                                                                                                                            + ifelse(substr(SICK_SYM_0,1,1)=='Z',21,NA))))))))))))))))))


summary(SICK_SYM_1)


nhid_gy40_t2_2002_1 <- cbind(nhid_gy40_t2_2002, SICK_SYM_1)
class(nhid_gy40_t2_2002_1)
nhid_gy40_t2_2002_1 <- nhid_gy40_t2_2002_1 %>% filter(!is.na(SICK_SYM_1))
nhid_gy40_t2_2002_1 <- as.data.table(nhid_gy40_t2_2002_1)
SICK_SYM_2 <- ifelse(substr(SICK_SYM_0,1,2)=='D1' | substr(SICK_SYM_0,1,2)=='D2' |substr(SICK_SYM_0,1,2)=='D3' |substr(SICK_SYM_0,1,2)=='D4',2,
                     +                      + ifelse(substr(SICK_SYM_0,1,2)=='D5'| substr(SICK_SYM_0,1,2)=='D6' | substr(SICK_SYM_0,1,2)=='D7' |substr(SICK_SYM_0,1,2)=='D8',3,
                                                     +                               + ifelse(substr(SICK_SYM_0,1,2)=='H0' |substr(SICK_SYM_0,1,2)=='H1'|substr(SICK_SYM_0,1,2)=='H2'|substr(SICK_SYM_0,1,2)=='H3'|substr(SICK_SYM_0,1,2)=='H4'|substr(SICK_SYM_0,1,2)=='H5',7,
                                                                                              +                                        + ifelse(substr(SICK_SYM_0,1,2)=='H6' |substr(SICK_SYM_0,1,2)=='H7' |substr(SICK_SYM_0,1,2)=='H8'|substr(SICK_SYM_0,1,2)=='H9',8,NA))))

nhid_gy40_t2_2002_2 <- cbind(nhid_gy40_t2_2002,SICK_SYM_2)
class(nhid_gy40_t2_2002_2)
nhid_gy40_t2_2002_2 <- nhid_gy40_t2_2002_2 %>% filter(!is.na(SICK_SYM_2))

nhid_gy40_t2_2002_2 <-nhid_gy40_t2_2002_2 %>% rename(SICK_SYM_1 = "SICK_SYM_2")

remove(nhid_gy40_t2_2002)

T240_02 <- rbind(nhid_gy40_t2_2002_1,nhid_gy40_t2_2002_2)
T240_02 <- as.data.table(T240_02)
remove(nhid_gy40_t2_2002_1)
T240_02 <- subset(T240_02,select =-c(DSBJT_CD,SICK_SYM))
class(T240_02)
T240_02$KEY_SEQ <- as.character(T240_02$KEY_SEQ)
T240_02$SICK_SYM_1 <- paste0("SICK_SYM_1.",T240_02$SICK_SYM_1)
T240_02 <- T240_02 %>%  group_by(KEY_SEQ,SICK_SYM_1) %>% mutate(ITEM_SUM = n())
T240_02 <- unique(T240_02)
T240_02 <- spread(T240_02,SICK_SYM_1,ITEM_SUM,fill=0)
nrow(T240_02)

# 09_T260 #

# 처방전 파일 불러오기
nhid_gy60_t2_2002 <- read.csv("./09_T260/nhid_gy60_t2_2002.csv")
nhid_gy60_t2_2002 <- as.data.table(nhid_gy60_t2_2002)
nhid_gy60_t2_2002 <- nhid_gy60_t2_2002 %>% select(-X)
nhid_gy60_t2_2002$KEY_SEQ <- as.character(nhid_gy60_t2_2002$KEY_SEQ)

T260_02 <- nhid_gy60_t2_2002
class(T260_02)
remove(nhid_gy60_t2_2002)

# 1) 분류유형코드 기준으로 펼치기
# 필요한 변수만 select
DIV_TYPE <- T260_02 %>% select(KEY_SEQ, SEQ_NO, RECU_FR_DT, DIV_TYPE_CD)
DIV_TYPE$DIV_TYPE_CD <- paste0("DIV_TYPE_CD.",DIV_TYPE$DIV_TYPE_CD)
class(DIV_TYPE)
# Num 개수 세기
DIV_TYPE <- DIV_TYPE %>% group_by(KEY_SEQ, DIV_TYPE_CD) %>% mutate(DIV_SUM = n())
#중복제거
DIV_TYPE <- unique(DIV_TYPE)
# 청구일련번호 별로 펼치기
DIV_TYPE <- as.data.frame(spread(DIV_TYPE, DIV_TYPE_CD, DIV_SUM,fill=0))
DIV_TYPE <- unique(DIV_TYPE)

# 2) 일반명코드 기준으로 펼치기
# 필요한 변수만 select
GNL_NM <- T260_02 %>% select(KEY_SEQ, GNL_NM_CD)

GNL_NM$GNL_NM_CD <- as.character(GNL_NM$GNL_NM_CD)
medicine$GNL_NM_CD <- as.character(medicine$GNL_NM_CD)

# 처방내역과 약제코드 합치기
GNL_NM <- left_join(GNL_NM,medicine,by="GNL_NM_CD")
GNL_NM <- as.data.table(GNL_NM)
table(is.na(GNL_NM$NUMBER)) # 옛날파일이라 분류코드에 5352개 NA생김

# 약제코드의 첫자리만 딴 NUM변수 생성
GNL_NM$NUM0 <- substr(GNL_NM$ATC,1,1)
GNL_NM$NUM <- paste0("GNL_NM_CD.",GNL_NM$NUM0)

# Num 개수 세기
GNL_NM <- GNL_NM %>% group_by(KEY_SEQ, NUM) %>% summarise(GNL_SUM = n())
# 청구일련번호 별로 펼치기
GNL_NM <- as.data.frame(spread(GNL_NM, NUM, GNL_SUM,fill=0))
GNL_NM <- unique(GNL_NM)

# 3) 금액 기준으로 펼치기
# 필요한 변수만 select 및 금액 합계변수 생성
AMT_ <- T260_02 %>% select(KEY_SEQ,UN_COST, AMT)
AMT_ <- AMT_ %>% group_by(KEY_SEQ) %>% summarise(SUM_AMT=sum(AMT),MEAN_UN_COST=mean(UN_COST))
AMT_<- as.data.table(AMT_)
# KEY_SEQ를 numeric 에서 char 타입으로 
DIV_TYPE$KEY_SEQ <- as.character(DIV_TYPE$KEY_SEQ)
GNL_NM$KEY_SEQ <- as.character(GNL_NM$KEY_SEQ)
AMT_$KEY_SEQ <- as.character(AMT_$KEY_SEQ)

# 4) 1,2,3 합치기
T260_02 <- left_join(DIV_TYPE, GNL_NM, by="KEY_SEQ")
T260_02 <- left_join(T260_02, AMT_,by="KEY_SEQ")
T260_02 <- as.data.table(T260_02)
remove(DIV_TYPE)
remove(GNL_NM)
remove(AMT_)
nrow(T260_02)

### 5. 개인 통합 (ex. T120_02+T220_02 = T20_02 )###
# T20_02 

T20_02 <- rbind(T120_02,T220_02)
class(T20_02)
nrow(T20_02)
# T30_02
T30_02 <- bind_rows(T130_02,T230_02)
class(T30_02)
remove(T130_02)
remove(T230_02)
nrow(T30_02)
# .을 포함하는 열(펼치기할 때 생긴 열) 추출하고 해당 열의 NA를 0으로 바꿔주기
i <- grep("[.]", names(T30_02))
T30_02[,c(i)][is.na(T30_02[,c(i)])]<-0

# T40_02
T40_02 <- bind_rows(T140_02,T240_02)
T40_02 <- as.data.table(T40_02)
remove(T140_02)
remove(T240_02)
i <- grep("[.]", names(T40_02))
T40_02[,c(i)][is.na(T40_02[,c(i)])]<-0
nrow(T40_02)
# T60_02

T60_02 <- bind_rows(T160_02,T260_02)
class(T60_02)

# .을 포함하는 열(펼치기할 때 생긴 열) 추출하고 해당 열의 NA를 0으로 바꿔주기
i <- grep("[.]", names(T60_02))
T60_02[,c(i)][is.na(T60_02[,c(i)])]<-0

nrow(T60_02)


######<>T20_02 NA 처리######
# 최초 입원일 NA처리 : 변수 1(입원여부 1과 0으로 표시), 변수2 (입원 월기입)

T20_02$FST_IN_PAT_DT_1 <- T20_02$FST_IN_PAT_DT
T20_02$FST_IN_PAT_DT_2 <- T20_02$FST_IN_PAT_DT
T20_02 <- subset(T20_02, select = -c(FST_IN_PAT_DT))

T20_02$FST_IN_PAT_DT_1 <- ifelse(is.na(T20_02$FST_IN_PAT_DT_1), 0,1 )
T20_02$FST_IN_PAT_DT_2 <- as.character(T20_02$FST_IN_PAT_DT_2)
T20_02$FST_IN_PAT_DT_2 <- ifelse(is.na(T20_02$FST_IN_PAT_DT_2), 0, substr(T20_02$FST_IN_PAT_DT_2,5,6) )

#나머지 처리(확인후 값에 0있으면 Z처리/ 없으면 0처리)
T20_02$DMD_CT_TOT_AMT <- ifelse(is.na(T20_02$DMD_CT_TOT_AMT), 'Z',T20_02$DMD_CT_TOT_AMT)
T20_02$DMD_MRI_TOT_AMT <- ifelse(is.na(T20_02$DMD_MRI_TOT_AMT),'Z',T20_02$DMD_MRI_TOT_AMT)
T20_02$EDEC_ADD_RT <- ifelse(is.na(T20_02$EDEC_ADD_RT),'Z',T20_02$EDEC_ADD_RT)
T20_02$EDEC_TRAMT <- ifelse(is.na(T20_02$EDEC_TRAMT),'Z',T20_02$EDEC_TRAMT)
T20_02$EDEC_SBRDN_AMT <- ifelse(is.na(T20_02$EDEC_SBRDN_AMT),'Z',T20_02$EDEC_SBRDN_AMT)
T20_02$EDEC_JBRDN_AMT <- ifelse(is.na(T20_02$EDEC_JBRDN_AMT),'Z',T20_02$EDEC_JBRDN_AMT)
T20_02$EDEC_CT_TOT_AMT <- ifelse(is.na(T20_02$EDEC_CT_TOT_AMT),'Z',T20_02$EDEC_CT_TOT_AMT)
T20_02$EDEC_MRI_TOT_AMT <- ifelse(is.na(T20_02$EDEC_MRI_TOT_AMT), 'Z',T20_02$EDEC_MRI_TOT_AMT)
T20_02$TOT_PRES_DD_CNT <- ifelse(is.na(T20_02$TOT_PRES_DD_CNT),'Z',T20_02$TOT_PRES_DD_CNT)

###### <>T20_02 KEY_SEQ 처리 #####
T20_02 <- subset(T20_02, select = -c(DSBJT_CD,MAIN_SICK,SUB_SICK,RECN,DMD_MRI_TOT_AMT))
T20_02$EDEC_TRAMT <- ifelse(is.na(T20_02$EDEC_TRAMT),T20_02$DMD_TRAMT,T20_02$EDEC_TRAMT)
T20_02$EDEC_SBRDN_AMT <- ifelse(is.na(T20_02$EDEC_SBRDN_AMT),T20_02$DMD_SBRDN_AMT,T20_02$EDEC_SBRDN_AMT)
T20_02$EDEC_JBRDN_AMT <- ifelse(is.na(T20_02$EDEC_JBRDN_AMT),T20_02$DMD_JBRDN_AMT,T20_02$EDEC_JBRDN_AMT)
T20_02$EDEC_CT_TOT_AMT <- ifelse(is.na(T20_02$EDEC_CT_TOT_AMT),T20_02$DMD_CT_TOT_AMT,T20_02$EDEC_CT_TOT_AMT)
T20_02$EDEC_MRI_TOT_AMT <- ifelse(is.na(T20_02$EDEC_MRI_TOT_AMT),T20_02$DMD_MRI_TOT_AMT,T20_02$EDEC_MRI_TOT_AMT)

T20_02 <- T20_02 %>% filter(!is.na(EDEC_TRAMT))
T20_02 <- T20_02 %>% filter(!is.na(EDEC_SBRDN_AMT))
T20_02 <- T20_02 %>% filter(!is.na(EDEC_JBRDN_AMT))
T20_02 <- T20_02 %>% filter(!is.na(EDEC_CT_TOT_AMT))
T20_02 <- T20_02 %>% filter(!is.na(EDEC_MRI_TOT_AMT))

##**교수님 코드에서는DMD_MRI_TOT_AMT열도 삭제해야함(데모 데이터에는 없어서 못지웠음)
T20_02 <- subset(T20_02, select = -c(DMD_TRAMT,DMD_SBRDN_AMT,DMD_JBRDN_AMT,DMD_CT_TOT_AMT))

#####><###T30_02 NA처리#####
#**UN_COST NA있는 row 제거#
#T30_02 <- T30_02 %>% filter(!is.na(T30_02$UN_COST))

##### <>T30_02 KEY_SEQ
##**교수님 코드에서는 ITEM_CD,DD_MQTY_EXEC_FREQ,MDCN_EXEC_FREQ,DD_MQTY_FREQ도 삭제해야함
T30_02 <- subset(T30_02, select = -c(SEQ_NO,RECU_FR_DT))


#><###T60_02 NA처리###
#**T60_02 <- na.omit(T60_02$UN_COST)
#**T60_02

##**교수님전용 T60 KEY_SEQ 처리하기
##**T60_02 <- subset(T60_02, select =-c(SEQ_NO,RECU_FR_DT,DIV_CD,DD_MQTY_FREQ,DD_EXEC_FREQ,MDCN_EXEC_FREQ))

#T60 KEY_SEQ 처리하기#
T60_02 <- subset(T60_02, select =-c(SEQ_NO,RECU_FR_DT))

##DD_EXEC_FREQ(1일 투여량) column 제거하기##
#**T60_02 <- subset(T60_02, select =-c(DD_EXEC_FREQ))


#><###JK_02 NA처리###

#DFAB_GRD_CD의 NA를 0으로 바꾸기#
JK_02$DFAB_GRD_CD[is.na(JK_02$DFAB_GRD_CD)] <- 0

#DFAB_REG_YM column 제거하기#
JK_02 <- subset(JK_02, select =-DFAB_REG_YM)

#DTH_CODE1의 NA를 Z로 변경,그외는 그대로#
JK_02$DTH_CODE1 <- ifelse(is.na(JK_02$DTH_CODE1),"Z",JK_02$DTH_CODE1)
#DTH_CODE2의 NA를 0으로 변경#
JK_02$DTH_CODE2 <- ifelse(is.na(JK_02$DTH_CODE2),"Z",JK_02$DTH_CODE2)

#DTH_YM 값이 없으면 0, 값이 있으면 1, 값이 있을경우 사망을 월로 표시하는 변수 생성#
JK_02$DTH_YM[is.na(JK_02$DTH_YM)] <- 0
DTH_M <- JK_02$DTH_YM
DTH_M <- ifelse(DTH_M==0,0,substr(DTH_M,5,6))
JK_02 <- cbind(JK_02,DTH_M)
JK_02$DTH_YM <- ifelse(JK_02$DTH_YM==0,0,1)
환
##jk_02 type 변환 
JK_02$DTH_CODE1 <- as.numeric(as.factor(JK_02$DTH_CODE1))
JK_02$DTH_CODE2 <- as.numeric(as.factor(JK_02$DTH_CODE2))
JK_02$SIDO <- as.factor(JK_02$SIDO)
JK_02$SGG <- as.factor(JK_02$SGG)
JK_02$IPSN_TYPE_CD <- as.factor(JK_02$IPSN_TYPE_CD)
JK_02$CTRB_PT_TYPE_CD <- as.factor(JK_02$CTRB_PT_TYPE_CD)
JK_02$DFAB_GRD_CD <- as.factor(JK_02$DFAB_GRD_CD)
JK_02$DFAB_PTN_CD <- as.factor(JK_02$DFAB_PTN_CD)

### 6. 전체 통합 ###
T20_02$KEY_SEQ <- as.character(T20_02$KEY_SEQ)
T30_02$KEY_SEQ <- as.character(T30_02$KEY_SEQ)
T40_02$KEY_SEQ <- as.character(T40_02$KEY_SEQ)

nrow(T20_02)
nrow(T30_02)
# 명세서(20T) + 진료내역(30T) 청구일련번호(KEY_SEQ)기준으로 합치기
T2030 <- merge(x = T20_02, y = T30_02, by = c('KEY_SEQ'), all.x = TRUE)
class(T2030)
nrow(T2030)
remove(T20_02)
remove(T30_02)

# 개인 일련번호를 기준으로 자격파일과 합치기
JK_02 <- subset(JK_02, select = -STND_Y)
T2030JK <- left_join(T2030,JK_02, by = 'PERSON_ID')
T2030JK <- as.data.table(T2030JK)
head(T2030JK)
remove(T2030)


# 건강검진파일(GJ) 더하기
#GJ_02 <- subset(GJ_02, select = - HCHK_YEAR?)

#T2030JKGJ <- merge(x = T2030JK, y = GJ_02, by = 'PERSON_ID', all.x= TRUE)

# 처방전 더하기 (60T)

T203060JK <- left_join(T2030JK, T60_02, by = c('KEY_SEQ'))
T203060JK <- as.data.table(T203060JK)
nrow(T203060JK)

# 상병내역 40T 더하기 
T20304060JK <- left_join(T203060JK, T40_02,by = c('KEY_SEQ', 'RECU_FR_DT'))
T20304060JK <- as.data.table(T20304060JK)
nrow(T20304060JK)

# YK 더하기 
YK_02 <- subset(YK_02, select = -STND_Y)
class(YK_02)
ALL <- left_join(T20304060JK, YK_02, by = 'YKIHO_ID')
head(ALL)
ALL$KEY_SEQ <- as.character(ALL$KEY_SEQ)
remove(T20304060JK)
remove(YK_02)
### 모든 열 값이 NA인 COLUMN 제거

ncol(ALL)
nrow(ALL)

index_na <- c()

gc()

for (i in 1:ncol(ALL))
{
  if(sum(is.na(ALL[i])) == nrow(ALL))
  {
    index_na <- c(index_na,i)
  }
}

ALL <- ALL[,-index_na]

class(ALL)
### 펼친것 중에 left_join하면서 생긴 NA는 0처리(진료내역X)
i <- grep("[.]", names(ALL))
ALL[,c(i)][is.na(ALL[,c(i)])]<-0

### 7. 고액환자 식별 ###

### 20T의 의과 치과 한방 합치기 ###
X02 <- rbind(T120_02,T220_02)
X02 <- X02 %>% group_by(PERSON_ID) %>% mutate(unique_inst_count = n_distinct(YKIHO_ID))##연간 이용기관수#

####################### 의료기관 방문횟수 및 종합병원 방문횟수 Join 하기#
X02_inst <- subset(X02,select=c(PERSON_ID,unique_inst_count))
high_cost_02 <- X02 %>% group_by(PERSON_ID) %>% summarise(sum_amt=sum(EDEC_TRAMT)) %>% arrange(desc(sum_amt))
high_cost_02 <- unique(left_join(high_cost_02,X02_inst,by='PERSON_ID')) ###
quantile(high_cost_02$sum_amt)
###################### 02년도 진료비 백분위 column 만들기#
high_cost_02_percentile <- high_cost_02
high_cost_02_percentile$num <- c(nrow(high_cost_02_percentile):1)
high_cost_02_percentile$percen <- high_cost_02_percentile$num/nrow(high_cost_02_percentile)*100

###################### 백분위를 토대로 상위 10%의 환자를 고액환자로 식별#
high_cost_02_ox <- high_cost_02_percentile %>% mutate(high_ox=ifelse(percen>=90,'1','0'))
high_cost_02_ox <- subset(high_cost_02_ox,select=-num)
high_cost_02_ox <- subset(high_cost_02_ox,select=-percen)
high_ox <- left_join(nhid_jk_2002, high_cost_02_ox,by='PERSON_ID')### 이용자 테이블에 고액환자 유무 추가#

high_ox$sum_amt <- ifelse(is.na(high_ox$sum_amt),0,high_ox$sum_amt)
high_ox$unique_inst_count <- ifelse(is.na(high_ox$unique_inst_count),0,high_ox$unique_inst_count)
high_ox$high_ox <- ifelse(is.na(high_ox$high_ox),0,high_ox$high_ox)

high_ox <- high_ox %>% select(PERSON_ID,high_ox, sum_amt)
high_ox <- rename(high_ox, CLASS_LABEL=high_ox)

ALL <- merge(x = ALL, y = high_ox, by = 'PERSON_ID', all.x = TRUE)

#파일 저장 
setwd('C:/Users/user/CloudStation/R code 공유/20190227 데이터통합')
write.csv(ALL,file = "TOTAL2002_190407.csv", row.names = FALSE)

#####PERSON_ID로 펼치기#####
View(ALL)
head(ALL$YKIHO_GUBUN_CD)

#PPP KEY_SEQ 삭제
ALL <- subset(ALL, select = -c(KEY_SEQ))

#PPP RECU_FR_DT 임시 삭제
ALL <- subset(ALL, select = -c(RECU_FR_DT))

#PPP 요양기관 병원의 규모에 따라 펼치기(3가지 정도)

ALL$YKIHO_GUBUN_CD <- ALL$YKIHO_GUBUN_CD %/% 10

ALL$YKIHO_CD_NEW <- ifelse(ALL$YKIHO_GUBUN_CD == 1 | ALL$YKIHO_GUBUN_CD == 3 | ALL$YKIHO_GUBUN_CD == 5 | ALL$YKIHO_GUBUN_CD == 7, 1,ifelse(ALL$YKIHO_GUBUN_CD == 2 | ALL$YKIHO_GUBUN_CD == 4,2,ifelse(ALL$YKIHO_GUBUN_CD == 9,3,4)))

ALL <- subset(ALL,select = -c(YKIHO_ID))

###PPP, T120 제거 attribute###
#** 교수님 전용
#**ALL <- subset(ALL, select = -c(DMD_TRAMT,DMD_SBRDN_AMT, DMD_JBRDN_AMT,DMD_DRG_NO))

###PPP, 공상구분 처리(혜택받는사람 1, 그렇지않은사람 0)###
ALL$OFFC_INJ_TYPE <- ifelse(ALL$OFFC_INJ_TYPE==0 | ALL$OFFC_INJ_TYPE== '-',0,1)

#PPP FORM_CD_1
ALL$FORM_CD_1 <- ifelse((ALL$FORM_CD == 2 | ALL$FORM_CD == 3), 1, ifelse((ALL$FORM_CD == 4 |ALL$FORM_CD == 5), 2,ifelse(ALL$FORM_CD == 6, 3, ifelse(ALL$FORM_CD == 7, 4, ifelse(ALL$FORM_CD == 9 | ALL$FORM_CD == 10 | ALL$FORM_CD == 11,5, ifelse(ALL$FORM_CD == 12|ALL$FORM_CD == 13, 6, ifelse(ALL$FORM_CD == 20 | ALL$FORM_CD == 21,7, 'ZZ')))))))


#PPP 입원 OR 외래 ->FORM_CD_2 ( 입원 = 1, 외래 = 2, 나머지 = 0)
ALL$FORM_CD_2 <- ifelse(ALL$FORM_CD == 2 | ALL$FORM_CD == 4 | ALL$FORM_CD == 6 | ALL$FORM_CD == 7 | ALL$FORM_CD == 10, 1,ifelse(ALL$FORM_CD == 3 | ALL$FORM_CD == 5 | ALL$FORM_CD == 8 | ALL$FORM_CD == 11 | ALL$FORM_CD == 13,2,0))

###################################PPP
######### ALL1 (펼친 것 더하기)
# 펼친 ATT(.포함된 것) 인덱스 불러오기
i <- grep("[.]", names(ALL))
ii <- c(i,1)
ALL1 <- ALL[,c(ii)]

ALL1_ <- ALL1 %>% group_by(PERSON_ID) %>% summarise_all(funs(sum))

######### ALL2 (최빈값)
#ALL2 <- ALL %>% select(-c(i))
ALL2 <- ALL %>% select(c(PERSON_ID, SEX, AGE_GROUP, DTH_YM, DTH_CODE1, DTH_CODE2, SGG, IPSN_TYPE_CD, CTRB_PT_TYPE_CD, DFAB_GRD_CD, DFAB_PTN_CD, OFFC_INJ_TYPE))
ALL2_ <- ALL2 %>% group_by(PERSON_ID) %>% summarise_all(funs(Mode))

######### ALL3 (더하기)
#EDEC_CT_TOT_AMT, EDEC_MRI_TOT_AMT Z 너무 많아서 facotr형. 임시삭제.
ALL3 <- ALL %>% select(c(PERSON_ID, FST_IN_PAT_DT_1, FST_IN_PAT_DT_2, EDEC_TRAMT, EDEC_SBRDN_AMT, EDEC_JBRDN_AMT, TOT_PRES_DD_CNT))
ALL3_ <- ALL3 %>% group_by(PERSON_ID) %>% summarise_all(funs(sum))

######### ALL3 (평균)
ALL4 <- ALL %>% select(c(PERSON_ID, EDEC_ADD_RT))
ALL4_ <- ALL4 %>% group_by(PERSON_ID) %>% summarise_all(funs(mean))

ALL <- left_join(ALL2_, ALL1_, by="PERSON_ID")
ALL <- left_join(ALL, ALL3_, by="PERSON_ID")
ALL <- left_join(ALL, ALL4_, by="PERSON_ID")

#파일 저장 
setwd('D:/CloudStation/CloudStation/R code 공유/20190227 데이터통합')
write.csv(ALL,file = "TOTAL2002_190408_PERSONID.csv", row.names = FALSE)
