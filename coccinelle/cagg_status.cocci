@cagg_status_eq@
identifier status;
expression E1,E2;
@@
  status = ts_continuous_agg_hypertable_status(E1);
+ #error "use & to check flags of status"
  <+... status == E2 ...+>
@cagg_status_switch1@
identifier status;
expression E1;
@@
  status = ts_continuous_agg_hypertable_status(E1);
  ...
+ #error "don't use switch to handle cagg_status"
  switch1(status);
@cagg_status_switch@
identifier status,i;
symbol s;
expression E1;
@@
  status = ts_continuous_agg_hypertable_status(E1);
  ...
+ #error "don't use switch to handle cagg_status"
  switch (status) {
    default:...
  }
