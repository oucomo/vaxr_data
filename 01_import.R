# Modified file of 01_import.R.
# Introduce the following changes:
# - set checkpoints to free up memory 
# - cast binary variables (tetanus_status, fup) to boolean datatype
# - cast gender to factor datatype
# - change "Khong ro" in vacplace to NA for a clearer representation

# ---- Import libraries ------
library(arrow)
library(readxl)
library(lubridate)
library(data.table)
library(stringi)
library(sparklyr)

# ------ Paths ------
# data path
data_path <- file.path("/.", "cluster_data", "vaccine_registry")
## output
save_path <- file.path("/.", "cluster_data", "vrdata", "raw")
## get all files 
files <- list.files(path = data_path, recursive = TRUE)

# ------- Read files ----- 
data <- NULL
# chekpoints to save files, free up memories
checkpoints = c(58, 121, 184, length(files))

for (i in c(1:length(files))){
  ## reading data from excel 
  data_i <- as.data.table(read_excel(path = file.path(data_path, files[i]), sheet = 1))
  
  if (nrow(data_i) > 0) {
    ## rename
    setnames(x = data_i,
             old = c("MA_DOI_TUONG", "HO_TEN", "GIOI_TINH", "TO_CHAR(D.NGAY_SINH,'DD/MM/YYYY')", 
                     "TEN_DAN_TOC", "TINH_TRANG_THEO_DOI", "TEN_TINH", "TEN_HUYEN", "TEN_XA", 
                     "TEN_TINH_DANG_KY", "TEN_HUYEN_DANG_KY", "TEN_XA_DANG_KY", "SO_MUI_UVSS_ME_TIEM", 
                     "TINH_TRANG_BV_UVSS", "NGUOI_CHAM_SOC", "TO_CHAR(D.NGAY_TAO,'DD/MM/YYYY')", 
                     "TEN_VACXIN", "THU_TU_MUI_TIEM", "TO_CHAR(LST.NGAY_TIEM,'DD/MM/YYYY')", 
                     "NOI_TIEM", "HINH_THUC_TIEM_CHUNG", "LOAI_CO_SO_TIEM", "COSO_TIEM", 
                     "TO_CHAR(LST.NGAY_TAO,'DD/MM/YYYY')", "CO_SO_CAP_NHAT", "RN"),
             new = c("pid", "name", "sex", "dob", "ethnic", "fup", 
                     "province", "district", "commune", "province_reg", 
                     "district_reg", "commune_reg", "tetanus_mom", "tetanus_status", 
                     "caregiver", "date0", "vacname", "vacorder", "vacdate", "vacplace0", 
                     "vactype", "vacplace_type", "vacplace", "date1", "place_update", "rn"))

    # Create new column for register place, used as save path parquet
    data_i$province_reg2 <- tolower(stri_trans_general(data_i$province_reg, "Latin-ASCII"))
    data_i$province_reg2 <- gsub(" ", "_", data_i$province_reg2)
    
    ### format date
    data_i$dob <- dmy(data_i$dob)
    data_i$vacdate <- dmy(data_i$vacdate)
    
    ## factorize gender
    data_i$sex <- tolower(stri_trans_general(data_i$sex, "Latin-ASCII"))
    data_i$sex <- factor(data_i$sex, levels=c("nam", "nu"))
    
    ## format binary data 
    data_i$tetanus_status <- tolower(stri_trans_general(data_i$tetanus_status, "Latin-ASCII"))
    data_i$tetanus_status[data_i$tetanus_status == "da duoc bao ve"] <- TRUE
    data_i$tetanus_status[data_i$tetanus_status == "chua duoc bao ve"] <- FALSE
    data_i$tetanus_status <- as.logical(data_i$tetanus_status)
    
    data_i$fup <- tolower(stri_trans_general(data_i$fup, "Latin-ASCII"))
    data_i$fup[data_i$fup == "co"] <- TRUE
    data_i$fup[data_i$fup == "khong"] <- FALSE
    data_i$fup <- as.logical(data_i$fup)
    
    
    ## handle unclear data (Khong ro -> NA)
    data_i$vacplace_type <- stri_trans_general(data_i$vacplace_type, "Latin-ASCII")
    data_i$vacplace_type[data_i$vacplace_type=="Khong ro"] <- NA
    
    ## add source
    data_i$file = files[i]
    
    if(is.null(data)){
      data <- data_i
    }else{
      ## combine
      data <- rbindlist(l = list(data, data_i))
    }
  }
  
  # free up memory used for data at i 
  rm(data_i)
  #collect garbage
  gc()
  
  # ---- Save data and free memories at checkpoints ----
  if (i %in% checkpoints){
    # ---- Drop redundant columns ----
    data[ ,':='(date0 = NULL, date1 = NULL)] 
    
    # ---- Save as parquet file ---- 
    write_dataset(
      dataset = data,
      path = file.path(save_path, "parquet"),
      format = "parquet",
      partitioning = list("province_reg2")
    )
    
    # ----- Free up memory for loading and saving data ---- 
    rm(data)
    gc()
    data <- NULL
  }
  
}





