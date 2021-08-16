### Setting up directory and packages ### 

setwd("~/MM908 - Industrial Project/Project Data")

library(tidyverse)
library(lubridate)
library(sqldf)

### Loading he data and merging ###

df1 <- read.csv("MSC_POLICY_INFO2014_15.csv")
df2 <- read.csv("MSC_POLICY_INFO2016_A.csv")
df3 <- read.csv("MSC_POLICY_INFO2016_B.csv")
df4 <- read.csv("MSC_POLICY_INFO2016_C.csv")
df5 <- read.csv("MSC_POLICY_INFO2017_A.csv")
df6 <- read.csv("MSC_POLICY_INFO2017_B.csv")
df7 <- read.csv("MSC_POLICY_INFO2017_C.csv")
df8 <- read.csv("MSC_POLICY_INFO2017_D.csv")
df9 <- read.csv("MSC_POLICY_INFO2018_A.csv")
df10 <- read.csv("MSC_POLICY_INFO2018_B.csv")
df11 <- read.csv("MSC_POLICY_INFO2018_C.csv")
df12 <- read.csv("MSC_POLICY_INFO2019_A.csv")
df13 <- read.csv("MSC_POLICY_INFO2019_B.csv")
df14 <- read.csv("MSC_POLICY_INFO2019_C.csv")
df15 <- read.csv("MSC_POLICY_INFO2020_A.csv")
df16 <- read.csv("MSC_POLICY_INFO2020_B.csv")
df17 <- read.csv("MSC_POLICY_INFO2020_C.csv")
df18 <- read.csv("MSC_POLICY_INFO2020_D.csv")
df19 <- read.csv("MSC_POLICY_INFO2021_A.csv")
df20 <- read.csv("MSC_POLICY_INFO2021_B.csv")

frames <- list(df1, df2, df3, df4, df5, df6, df7, df8,
               df9, df10, df11, df12, df13, df14, df15,
               df16, df17, df18, df19, df20)

policy_info_df <- bind_rows(frames)

claims_1 <- read.csv("MSC_CLAIMS_INFO_A.csv")
claims_2 <- read.csv("MSC_CLAIMS_INFO_B.csv")

claims_info_df <- bind_rows(claims_1, claims_2)

claims_cost_df <- read.csv("MSC_CLAIMS_COSTS.csv")

## Standardising naming and dating system

policy_info_df <- policy_info_df %>% rename("Authoring_Date" = "Authoring.Date")
policy_info_df <- policy_info_df %>% rename("Product_Code" = "Product.Code")

sapply(policy_info_df, function(x) class(x))
sapply(claims_info_df, function(x) class(x))
sapply(claims_cost_df, function(x) class(x))

policy_info_df$Authoring_Date <- as.Date(
  dmy_hms(policy_info_df$Authoring_Date))

claims_info_df$CREATION_DTTM <- as.Date(
  dmy_hms(claims_info_df$CREATION_DTTM))

claims_info_df$LOSS_DTTM <- as.Date(
  dmy_hms(claims_info_df$LOSS_DTTM))

claims_info_df$ORIG_EFFECTIVE_DTTM <- as.Date(
  dmy_hms(claims_info_df$ORIG_EFFECTIVE_DTTM))

claims_info_df$EXPIRATION_DTTM <- as.Date(
  dmy_hms(claims_info_df$EXPIRATION_DTTM))

claims_cost_df$CLM_DTE_CRRT_CVR <- as.Date(
  dmy(claims_cost_df$CLM_DTE_CRRT_CVR))

claims_cost_df$CLM_DTE_NOTIFIED <- as.Date(
  dmy(claims_cost_df$CLM_DTE_NOTIFIED))

claims_cost_df$CLM_DTE_SETTLED <- as.Date(
  dmy(claims_cost_df$CLM_DTE_SETTLED))

claims_cost_df$CLM_DTE_OCCUR <- as.Date(
  dmy(claims_cost_df$CLM_DTE_OCCUR))

## Creating the Policy table with claimed or not claimed classificaion

policy_df <- policy_info_df %>%
  select(-c("Authoring_Date", "ID"))

policy_df <- policy_df %>%
  group_by(POLICYID) %>% 
  distinct(Text, .keep_all = TRUE)

policy_concat <- policy_df %>% 
  group_by(POLICYID) %>% 
  summarise(Text = toString(Text)) %>% 
  ungroup

non_text_policy <- policy_df %>%
  select(c(POLICYID, Product_Code)) %>% 
  distinct(POLICYID, .keep_all = TRUE)

policy_concat <- inner_join(policy_concat,
                            non_text_policy,
                            by = "POLICYID")

claims_info <- claims_info_df %>%
  group_by(POLICY_ID) %>% 
  mutate(Number_of_Claims = length(unique(CLAIM_ID))) %>% 
  distinct(POLICY_ID, .keep_all = TRUE) %>% 
  rename(POLICYID = POLICY_ID ) %>% 
  select(c(POLICYID, Number_of_Claims))

claims_info <- inner_join(policy_concat,
                          claims_info,
                          by = "POLICYID")

policy_not_claim <- setdiff(policy_concat,
                            claims_info %>% 
                              select(c(POLICYID, Text, Product_Code)))

claims_info <- claims_info %>% 
  mutate(Claim = "Yes")

policy_not_claim <- policy_not_claim %>% 
  mutate(Claim = "No") %>% 
  mutate(Number_of_Claims = 0)

classification_data <- rbind(policy_not_claim,
                           claims_info)

write.csv(classification_data, file = "classification_data.csv", fileEncoding = "UTF-8")

## Making Claims table

claims_table <- claims_info_df %>% select(-LRGLOSS_NETTOTALINCURRED)

count(claims_cost_df, CLM_CVR_TYPE)


PRCL_table <- claims_cost_df %>% filter(CLM_CVR_TYPE == "PRCL")

PRCL_table <- inner_join(PRCL_table,
                         claims_table,
                         by = "CLAIM_ID")

PRCL_table <- inner_join(PRCL_table,
                         policy_concat,
                         by = c("POLICY_ID" = "POLICYID"))

LPL_table <- claims_cost_df %>% filter(CLM_CVR_TYPE == "LPL")

LPL_table <- inner_join(LPL_table,
                        claims_table,
                        by = "CLAIM_ID")

LPL_table <- inner_join(LPL_table,
                        policy_concat,
                        by = c("POLICY_ID" = "POLICYID"))

LEL_table <- claims_cost_df %>% filter(CLM_CVR_TYPE == "LEL")

LEL_table <- inner_join(LEL_table,
                        claims_table,
                        by = "CLAIM_ID")

LEL_table <- inner_join(LEL_table,
                        policy_concat,
                        by = c("POLICY_ID" = "POLICYID"))

MPRO_table <- claims_cost_df %>% filter(CLM_CVR_TYPE == "MPRO")

MPRO_table <- inner_join(MPRO_table,
                        claims_table,
                        by = "CLAIM_ID")

MPRO_table <- inner_join(MPRO_table,
                        policy_concat,
                        by = c("POLICY_ID" = "POLICYID"))

PRMD_table <- claims_cost_df %>% filter(CLM_CVR_TYPE == "PRMD")

PRMD_table <- inner_join(PRMD_table,
                         claims_table,
                         by = "CLAIM_ID")

PRMD_table <- inner_join(PRMD_table,
                         policy_concat,
                         by = c("POLICY_ID" = "POLICYID"))

PCW_table <- claims_cost_df %>% filter(CLM_CVR_TYPE == "PCW")

PCW_table <- inner_join(PCW_table,
                         claims_table,
                         by = "CLAIM_ID")

PCW_table <- inner_join(PCW_table,
                         policy_concat,
                         by = c("POLICY_ID" = "POLICYID"))

TRMD_table <- claims_cost_df %>% filter(CLM_CVR_TYPE == "TRMD")

TRMD_table <- inner_join(TRMD_table,
                        claims_table,
                        by = "CLAIM_ID")

TRMD_table <- inner_join(TRMD_table,
                        policy_concat,
                        by = c("POLICY_ID" = "POLICYID"))

DIOF_table <- claims_cost_df %>% filter(CLM_CVR_TYPE == "DIOF")

DIOF_table <- inner_join(DIOF_table,
                         claims_table,
                         by = "CLAIM_ID")

DIOF_table <- inner_join(DIOF_table,
                         policy_concat,
                         by = c("POLICY_ID" = "POLICYID"))

TRCL_table <- claims_cost_df %>% filter(CLM_CVR_TYPE == "TRCL")

TRCL_table <- inner_join(TRCL_table,
                         claims_table,
                         by = "CLAIM_ID")

TRCL_table <- inner_join(TRCL_table,
                         policy_concat,
                         by = c("POLICY_ID" = "POLICYID"))

LPI_table <- claims_cost_df %>% filter(CLM_CVR_TYPE == "LPI")

LPI_table <- inner_join(LPI_table,
                         claims_table,
                         by = "CLAIM_ID")

LPI_table <- inner_join(LPI_table,
                         policy_concat,
                         by = c("POLICY_ID" = "POLICYID"))


###########################################################

PRMD_table <- PRMD_table %>% 
  select(CLAIM_ID, POLICY_ID, ROW_TOT) %>% 
  group_by(POLICY_ID) %>% 
  mutate(Number_of_Claims = length(unique(CLAIM_ID))) %>% 
  mutate(Total_Claim_Cost = sum(ROW_TOT)) %>% 
  mutate(Average_Claim_Cost = mean(ROW_TOT)) %>% 
  distinct(POLICY_ID, .keep_all = TRUE) %>% 
  select(-ROW_TOT)

write.csv(PRMD_table, file = "PRMD_data.csv", fileEncoding = "UTF-8")

LPL_table <- LPL_table %>% 
  select(CLAIM_ID, POLICY_ID, ROW_TOT) %>% 
  group_by(POLICY_ID) %>% 
  mutate(Number_of_Claims = length(unique(CLAIM_ID))) %>% 
  mutate(Total_Claim_Cost = sum(ROW_TOT)) %>% 
  mutate(Average_Claim_Cost = mean(ROW_TOT)) %>% 
  distinct(POLICY_ID, .keep_all = TRUE) %>% 
  select(-ROW_TOT)

write.csv(LPL_table, file = "LPL_data.csv", fileEncoding = "UTF-8")
