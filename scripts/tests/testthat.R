library(testthat)

# Resolve the directory of this script. Works whether invoked via
# Rscript (--file=...) or source()'d interactively.
script_dir <- tryCatch(
  dirname(sys.frame(1)$ofile),
  error = function(e) {
    args <- commandArgs(trailingOnly = FALSE)
    f    <- sub("--file=", "", grep("--file=", args, value = TRUE))
    if (length(f) == 1L && nzchar(f)) dirname(f) else "."
  }
)

test_dir(file.path(script_dir, "testthat"),
         reporter = "summary",
         stop_on_failure = TRUE)
