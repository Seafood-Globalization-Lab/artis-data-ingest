# Get latest BACI data
# script to scrape data, version info, and release notes from BACI website
# created 2024-12-09 by AM

## Notes
# - parallel futures not working, needs tuning. 
# - check output dir is working - files are not being writen out 


library(furrr)
library(future)
library(httr)
library(rvest)


# URL of the CEPII BACI database page
url <- "https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37"

# Read the webpage
webpage <- read_html(url)

# Extract the download section using the 'moreOLD' ID and 'content_box' class
download_section <- html_nodes(webpage, xpath = "//div[@class='content_box']//div[@class='moreOLD' and @id='telechargement']")

# Extract all 'a' (anchor) tags from the download section
all_links <- html_nodes(download_section, "a")

# Extract href attributes from all links
file_urls <- html_attr(all_links, "href")

# Remove NA values
file_urls <- file_urls[!is.na(file_urls)]

# Filter only ZIP and PDF files
file_urls <- file_urls[grepl("\\.(zip|pdf)$", file_urls, ignore.case = TRUE)]

# Handle relative paths to make them absolute (just in case, but looks like URLs are absolute already)
file_urls <- ifelse(grepl("^http", file_urls), file_urls, paste0("https://www.cepii.fr", file_urls))

# Remove duplicates just in case
file_urls <- unique(file_urls)

# Display the file URLs
print(file_urls)

# Specify the directory to save the downloaded files
download_dir <- "path/to/your/download/directory"

# Create the directory if it doesn't exist
if (!dir.exists(download_dir)) {
  dir.create(download_dir, recursive = TRUE)
  message("created `baci_raw/` directory")
} else (message("`baci_raw/` directory already exists"))

# Function to download a single file
download_file <- function(file_url) {
  retries <- 3
  file_name <- basename(file_url)
  destfile <- file.path(download_dir, file_name)
  
  for (i in 1:retries) {
    tryCatch({
      GET(file_url, 
          write_disk(destfile, overwrite = TRUE), 
          user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.0.0 Safari/537.36"),
          timeout(60)
      )
      cat("Downloaded:", file_name, "\n")
      return(TRUE)  # Success
    }, error = function(e) {
      cat("Retry", i, "for", file_url, "\n")
      Sys.sleep(2)  # Wait before retrying
    })
  }
  cat("Failed after", retries, "retries for", file_url, "\n")
}
future::plan(multisession, workers = 4)  # Limit to 4 workers

# Use furrr to download files in parallel
future_map(file_urls, download_file, .progress = TRUE)

