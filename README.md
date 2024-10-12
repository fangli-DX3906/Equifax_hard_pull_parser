# Equifax_hard_pull_parser

If we want to run the code on a monthly frequency, the following two parameters need to be specified: *begin_year* and *begin_month*. However, I also allow other parameters in the constructor.

- `begin_year` and `begin_month` (int). If only these two parameters are given, the class will retrieve the data from the first day to the last day of that month.
- `end_year` and `end_month` (int, Optional=None). If these two parameters are specified, the class will retrieve the data from the first day of `begin_month` to the last day of `end_month`.
- `which_tables` (list(str), Optional = None).  If this parameter is specified, the class will only parse the segments within this list; otherwise, it will parse all possible segments.
- `push_header` (bool, Optional=True). If this parameter is specified, the class will push the header table to GBQ.
- `debug_mode` (bool, Optional=False). This parameter controls whether the parser operates in debug mode. In debug mode, the parser will retrieve only 20 entries of raw data, and the parsed table will not be pushed to GBQ.
- `project_id` and `dataset_id` (str). These two variables control where the parser pushes the parsed table.
