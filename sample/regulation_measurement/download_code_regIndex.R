rm(list = ls())
library(eurlex)

query <- elx_make_query(resource_type = "regulation", include_date = TRUE, include_force = TRUE)
acts_in_force <- elx_run_query(query)
acts_in_force <- subset(acts_in_force, force == "true")

download_legal_act <- function(celex_id, save_dir = "eurlex_downloads") {
  # Create directory if it doesn't exist
  if (!dir.exists(save_dir)) {
    dir.create(save_dir)
  }

  # Fetch the document URL
  pdf_url <- paste0("https://eur-lex.europa.eu/legal-content/EN/TXT/PDF/?uri=CELEX:", celex_id)

  # Download the file
  if (!is.null(pdf_url)) {
    download.file(pdf_url, destfile = paste0(save_dir,"/", celex_id, ".pdf"), mode = "wb")
    message("Downloaded: ", celex_id)
  } else {
    message("No document found for CELEX ID: ", celex_id)
  }
}

legacy_data_dir <- Sys.getenv(
  "EU_REGULATION_LEGACY_DATA_DIR",
  unset = path.expand("~/Dropbox/Projects/EU_Regulation/Data/legacy/regulation_measurement")
)
download_dir <- file.path(legacy_data_dir, "eurlex_downloads")
start_index <- as.integer(Sys.getenv("EU_REGULATION_START_INDEX", unset = "1"))

for (i in start_index:nrow(acts_in_force)) {
  celex_id <- acts_in_force$celex[i]
  download_legal_act(celex_id, save_dir = download_dir)
}
#2985 not found
#3368 not found
#4222 not found
#4532 not found
#4989 not found
#5006 not found
#5310 not found
#5505 not found
#6103 not found
#6527 not found
#7236 not found
#7295 not found
#7323 not found
#7497 not found
#7498 not found
#7506 not found
#7538 not found
#7651 not found
#7813 not found
#7814 not found
#7984 not found
#8053 not found
#9170 not found
#9204 not found
#9256 not found
#9462 not found
#9548 not found
#9559 not found
#9794 not found
#10124 not found
#10225 not found
#10461 not found
#10494 not found
#10583 not found
#10889 not found
#11059 not found
#11216 not found
#11226 not found
#11258 not found
#11372 not found
#11435 not found
#11551 not found
#11682 not found
#11759 not found
#12136 not found
#12210 not found
#12361 not found
#12423 not found
#12433 not found
#12444 not found
#12446 not found
#13029 not found
#14927 not found
