
# load R packages ---------------------------------------------------------

library(readxl)
library(data.table)
library(lubridate)
library(stringi)
library(arrow)
library(tidyverse)

## input
data_path <- file.path("/", "cluster_data", "vaccine_registry")

## output
save_path <- file.path("/", "cluster_data", "vrdata", "raw")


# import and combine files ------------------------------------------------

## list all files
files <- list.files(path = data_path, recursive = TRUE)

## import and combine files
dat <- NULL
for (i in c(1:161)) {
  cat(i, "\n")

  ## read from excel
  dati <- as.data.table(read_excel(path = file.path(data_path, files[i]), sheet = 1))

  if (nrow(dati) > 0) {
    ## rename
    setnames(x = dati,
             old = c("MA_DOI_TUONG", "HO_TEN", "GIOI_TINH", "TO_CHAR(D.NGAY_SINH,'DD/MM/YYYY')", "TEN_DAN_TOC", "TINH_TRANG_THEO_DOI", "TEN_TINH", "TEN_HUYEN", "TEN_XA", "TEN_TINH_DANG_KY", "TEN_HUYEN_DANG_KY", "TEN_XA_DANG_KY", "SO_MUI_UVSS_ME_TIEM", "TINH_TRANG_BV_UVSS", "NGUOI_CHAM_SOC", "TO_CHAR(D.NGAY_TAO,'DD/MM/YYYY')", "TEN_VACXIN", "THU_TU_MUI_TIEM", "TO_CHAR(LST.NGAY_TIEM,'DD/MM/YYYY')", "NOI_TIEM", "HINH_THUC_TIEM_CHUNG", "LOAI_CO_SO_TIEM", "COSO_TIEM", "TO_CHAR(LST.NGAY_TAO,'DD/MM/YYYY')", "CO_SO_CAP_NHAT", "RN"),
             new = c("pid", "name", "sex", "dob", "ethnic", "fup", "province", "district", "commune", "province_reg", "district_reg", "commune_reg", "tetanus_mom", "tetanus_status", "caregiver", "date0", "vacname", "vacorder", "vacdate", "vacplace0", "vactype", "vacplace_type", "vacplace", "date1", "place_update", "rn"))

    ## remove accent
    dati$province2 <- stri_trans_general(dati$province, "Latin-ASCII")
    dati$district2 <- stri_trans_general(dati$district, "Latin-ASCII")
    dati$commune2 <- stri_trans_general(dati$commune, "Latin-ASCII")
    dati$vacname2 <- stri_trans_general(dati$vacname, "Latin-ASCII")

    ### format date
    dati$dob <- dmy(dati$dob)
    dati$vacdate <- dmy(dati$vacdate)

    ## add source
    dati$file = files[i]

    ## combind
    dat <- rbindlist(l = list(dat, dati))
  }
}

### save
#fwrite(x = dat, file = file.path(save_path, "raw_001_161.csv"))
#fwrite(x = dat, file = file.path("raw_001_161.csv"))
saveRDS(dat, file = file.path("raw_001_161.rds"))

dat <- NULL
for (i in c(162:length(files))) {
  cat(i, "\n")

  ## read from excel
  dati <- as.data.table(read_excel(path = file.path(data_path, files[i]), sheet = 1))

  if (nrow(dati) > 0) {
    ## rename
    setnames(x = dati,
             old = c("MA_DOI_TUONG", "HO_TEN", "GIOI_TINH", "TO_CHAR(D.NGAY_SINH,'DD/MM/YYYY')", "TEN_DAN_TOC", "TINH_TRANG_THEO_DOI", "TEN_TINH", "TEN_HUYEN", "TEN_XA", "TEN_TINH_DANG_KY", "TEN_HUYEN_DANG_KY", "TEN_XA_DANG_KY", "SO_MUI_UVSS_ME_TIEM", "TINH_TRANG_BV_UVSS", "NGUOI_CHAM_SOC", "TO_CHAR(D.NGAY_TAO,'DD/MM/YYYY')", "TEN_VACXIN", "THU_TU_MUI_TIEM", "TO_CHAR(LST.NGAY_TIEM,'DD/MM/YYYY')", "NOI_TIEM", "HINH_THUC_TIEM_CHUNG", "LOAI_CO_SO_TIEM", "COSO_TIEM", "TO_CHAR(LST.NGAY_TAO,'DD/MM/YYYY')", "CO_SO_CAP_NHAT", "RN"),
             new = c("pid", "name", "sex", "dob", "ethnic", "fup", "province", "district", "commune", "province_reg", "district_reg", "commune_reg", "tetanus_mom", "tetanus_status", "caregiver", "date0", "vacname", "vacorder", "vacdate", "vacplace0", "vactype", "vacplace_type", "vacplace", "date1", "place_update", "rn"))

    ## remove accent
    dati$province2 <- stri_trans_general(dati$province, "Latin-ASCII")
    dati$district2 <- stri_trans_general(dati$district, "Latin-ASCII")
    dati$commune2 <- stri_trans_general(dati$commune, "Latin-ASCII")
    dati$vacname2 <- stri_trans_general(dati$vacname, "Latin-ASCII")

    ### format date
    dati$dob <- dmy(dati$dob)
    dati$vacdate <- dmy(dati$vacdate)

    ## add source
    dati$file = files[i]

    ## combind
    dat <- rbindlist(l = list(dat, dati))
  }
}

### save
#fwrite(x = dat, file = file.path(save_path, "alldat_162_207.csv"))
#fwrite(x = dat, file = file.path("alldat_162_207.csv"))
saveRDS(dat, file = file.path("raw_162_207.rds"))

# save into parquet format ------------------------------------------------

dat <- open_dataset(file.path(save_path), format = "csv",
                    schema = schema("pid" = string(),
                                    "name" = string(),
                                    "sex" = string(),
                                    "dob" = string(),
                                    "ethnic" = string(),
                                    "fup" = string(),
                                    "province" = string(),
                                    "district" = string(),
                                    "commune" = string(),
                                    "province_reg" = string(),
                                    "district_reg" = string(),
                                    "commune_reg" = string(),
                                    "tetanus_mom" = string(),
                                    "tetanus_status" = string(),
                                    "caregiver" = string(),
                                    "date0" = string(),
                                    "vacname" = string(),
                                    "vacorder" = string(),
                                    "vacdate" = string(),
                                    "vacplace0" = string(),
                                    "vactype" = string(),
                                    "vacplace_type" = string(),
                                    "vacplace" = string(),
                                    "date1" = string(),
                                    "place_update" = string(),
                                    "rn" = string(),
                                    "province2" = string(),
                                    "district2" = string(),
                                    "commune2" = string(),
                                    "vacname2" = string(),
                                    "file" = string()
                    )) %>%
  select(-date0, -date1)

write_dataset(
  dataset = dat,
  path = file.path(save_path, "parquet"),
  format = "parquet",
  partitioning = "province_reg"
)
