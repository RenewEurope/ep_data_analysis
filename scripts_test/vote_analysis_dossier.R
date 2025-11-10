# Quick vote analysis for specific dossiers

tally_bygroup_byrcv_wide <- read_csv("data_out/aggregates/tally_bygroup_byrcv_10.csv") |>
    pivot_wider(id_cols = c(rcv_id, polgroup_id),
                names_from = result_fct, values_from = tally)

mjrt_bygroup <- read_csv("data_out/aggregates/tally_bygroup_byrcv_10.csv") |>
    slice_max(order_by = tally, by = c(rcv_id, polgroup_id))

cop_rcv = pl_votes |>
    filter(
        doc_id %in% "B10-0445/2025"
        & decision_method == "VOTE_ELECTRONIC_ROLLCALL") |>
    left_join(
        y = meps_rcv_mandate[polgroup_id == 7035L],
        by = c("rcv_id", "mandate")
    ) |>
    left_join(
        y = tally_bygroup_byrcv_wide,
        by = c("rcv_id", "polgroup_id")
    ) |>
    left_join(
        y = mjrt_bygroup,
        by = c("rcv_id", "polgroup_id")
    ) |>
    filter(result.x != -3L) |>  # drop absent
    filter(result.x != result.y) |> # keep only divergent votes
    join_meps_names() |>
    select(decision_outcome, number_of_votes_abstention, number_of_votes_against,
           number_of_votes_favor, headingLabel_en, referenceText_en, activity_label_fr,
           responsible_organization_label_fr, doc_id,
           mep_vote = result.x, mep_name,
           absent, no_vote, against, abstain, `for`,
           renew_majority = result_fct,
           majority_count = tally
    )
write_csv(cop_rcv, "cop_rcv.csv")
