LOCAL_SOURCES <- list(
    snhd = list(
        input_dir = "data/clean/SNHD_EMS_CALLS_sheets",
        file_pattern = "^clean_.*\\.csv$",
        id_strategy = "rowhash", # until we find an incident id
        ts = list(
            queued   = "sent_to_queue",
            assigned = "x1st_unit_assigned",
            enroute  = "x1st_unit_enroute",
            arrived  = "x1st_unit_arrived"
        ),
        keep = c(
            "sent_to_queue", "priority_description", "problem", "pro_qa", "jurisdiction",
            "x1st_unit_assigned", "x1st_unit_enroute", "x1st_unit_arrived"
        )
    )
)
