###--------------------------------------------------------------------------###
# Unnesting Functions for Nested Data Structures ------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' This script contains general-purpose functions for unnesting nested data
#' structures commonly found in API responses (JSON data).
#' 
#' Functions:
#' - unnest_nested_df(): Unnests nested dataframes within a list
#' - unnest_nested_list(): Unnests nested character lists within a list
#------------------------------------------------------------------------------#


#------------------------------------------------------------------------------#
# General function to unnest nested data structures as data.frame -------------#

#' Unnest Nested Data Frames
#'
#' This function unnests a nested data.frame column within a list of data.frames/tables.
#' It's designed to handle complex nested structures from API responses.
#'
#' @param data_list A list of data.frames or data.tables containing nested columns
#' @param group_col A string specifying the column name to use as grouping identifier (default: "id")
#' @param unnest_col A string specifying the name of the nested column to unnest (default: "workHadParticipation")
#'
#' @return A data.table with the unnested data, where rows are duplicated as needed
#'
#' @examples
#' # Assuming pl_docs_list is a list with nested 'workHadParticipation' column
#' # unnested_data <- unnest_nested_df(data_list = pl_docs_list, 
#' #                                    group_col = "id", 
#' #                                    unnest_col = "workHadParticipation")

unnest_nested_df <- function(
    data_list,
    group_col = "id",
    unnest_col = "workHadParticipation"
) {
  
  list_unnest_tmp <- lapply(
    X = data_list, FUN = function(doc) {
      # Check whether the col to be unnested is actually present
      if ( unnest_col %in% names(doc)
           && !is.null(doc[[unnest_col]])
           && length(doc[[unnest_col]]) > 0 ) {
        
        # Check whether data is DT or DF
        if ( data.table::is.data.table(doc) ) {
          cols_tokeep = c(group_col, unnest_col)
          doc_subset <- doc[, ..cols_tokeep, with = FALSE]
        } else if ( !data.table::is.data.table(doc) && is.data.frame(doc) ) {
          doc_subset <- data.table::as.data.table(
            doc[, c(group_col, unnest_col)]
          )
        }
        
        # Unnest data
        unnest_dt = data.table::rbindlist(
          l = setNames(
            object = doc_subset[[unnest_col]],
            nm = doc_subset[[group_col]]), # rename on the fly
          use.names = TRUE, fill = TRUE, idcol = "group_col_tmp"
        )
        
        # Return unnested DT
        return(unnest_dt)
      }
    }
  )
  
  # Append all temporary unnested DT ------------------------------------------#
  result = data.table::rbindlist(list_unnest_tmp, use.names = TRUE, fill = TRUE)
  
  # Check if `group_col` is present in unnested DF
  if ( group_col %in% names(result) ) {
    cat("\nOne of the unnested cols within the DT has the same name of the grouping col. Proceed to rename it...\n")
    data.table::setnames(
      x = result,
      old = c("group_col_tmp", group_col),
      new = c(group_col, paste0(unnest_col, "_id")))
  }
  
  return(result)
}


#------------------------------------------------------------------------------#
# General function to unnest nested data structures as character list ---------#

#' Unnest Nested Character Lists
#'
#' This function unnests a nested list column (typically containing character vectors)
#' within a list of data.frames/tables. It's designed to handle complex nested 
#' structures from API responses where values are stored as lists.
#'
#' @param data_list A list of data.frames or data.tables containing nested list columns
#' @param group_cols A string or vector of strings specifying column names to use as grouping identifiers (default: "id")
#' @param unnest_col A string specifying the name of the nested list column to unnest (default: "is_derivative_of")
#'
#' @return A data.table with the unnested data, where rows are duplicated for each element in the nested lists
#'
#' @examples
#' # Assuming pl_docs_list is a list with nested 'creator' column
#' # unnested_creators <- unnest_nested_list(data_list = pl_docs_list, 
#' #                                          group_cols = "id", 
#' #                                          unnest_col = "creator")

unnest_nested_list <- function(
    data_list,
    group_cols = "id",
    unnest_col = "is_derivative_of"
) {
  
  list_unnest_tmp <- lapply(
    X = data_list, FUN = function(doc) {
      # Check whether the col to be unnested is actually present
      if ( unnest_col %in% names(doc)
           && !is.null(doc[[unnest_col]])
           && length(doc[[unnest_col]]) > 0 ) {
        
        # Check whether data is DT or DF
        if ( data.table::is.data.table(doc) ) {
          cols_tokeep = c(group_cols, unnest_col)
          doc_subset <- doc[, ..cols_tokeep, with = FALSE]
        } else if ( !data.table::is.data.table(doc) && is.data.frame(doc) ) {
          doc_subset <- data.table::as.data.table(
            doc[, c(group_cols, unnest_col)]
          )
        }
        
        # Check if ALL cells are NULL - if so, return NULL
        if ( all(sapply(doc_subset[[unnest_col]], is.null)) ) {
          return(NULL)
        } else if ( any(sapply(doc_subset[[unnest_col]], is.null)) ) {
          # Check if ANY cells are NULL - if so, kick them out
          doc_subset = doc_subset[
            !sapply( doc_subset[[unnest_col]], is.null ),
          ]
        }
        
        # Determine cols to group by (all cols *except* unnest col).
        # This ensures all non-unnested data is carried over and duplicated correctly.
        grouping_cols <- setdiff(names(doc_subset), unnest_col)
        
        # Perform the unnesting using Standard Evaluation (SE)
        # - `by = grouping_cols`: Passes the vector of column names for grouping.
        # - `list(unnest_col_tmp = unlist(get(unnest_col)))`:
        #    - `get(unnest_col)` accesses the list column by its string name.
        #    - `unlist()` flattens the list.
        #    - The result is temporarily named `unnest_col_tmp`.
        unnest_dt <- doc_subset[, list(unnest_col_tmp = unlist(get(unnest_col))),
                                by = grouping_cols]
        
        # Rename the temporary col 'unnest_col_tmp' back to `unnest_col`.
        data.table::setnames(unnest_dt, old = "unnest_col_tmp", new = unnest_col)
        
        return(unnest_dt)
      }
    }
  )
  
  # Append all temporary unnested DT ------------------------------------------#
  result = data.table::rbindlist(list_unnest_tmp, use.names = TRUE, fill = TRUE)
  
  return(result)
}
