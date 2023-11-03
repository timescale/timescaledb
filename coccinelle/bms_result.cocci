// Some bitmap set operations recycle one of the input parameters or return
// a reference to a new bitmap set. The following set of rules checks that the
// returned reference is not discarded.
 
@rule_1@
expression E1, E2;
@@

+ /* Result of bms_add_member has to be used */
+ E1 = bms_add_member(E1, E2);
- bms_add_member(E1, E2);

@rule_2@
expression E1, E2;
@@

+ /* Result of bms_del_member has to be used */
+ E1 = bms_del_member(E1, E2);
- bms_del_member(E1, E2);

@rule_3@
expression E1, E2;
@@

+ /* Result of bms_add_members has to be used */
+ E1 = bms_add_members(E1, E2);
- bms_add_members(E1, E2);

@rule_4@
expression E1, E2, E3;
@@

+ /* Result of bms_add_range has to be used */
+ E1 = bms_add_range(E1, E2, E3);
- bms_add_range(E1, E2, E3);

@rule_5@
expression E1, E2;
@@

+ /* Result of bms_int_members has to be used */
+ E1 = bms_int_members(E1, E2);
- bms_int_members(E1, E2);

@rule_6@
expression E1, E2;
@@

+ /* Result of bms_del_members has to be used */
+ E1 = bms_del_members(E1, E2);
- bms_del_members(E1, E2);

@rule_7@
expression E1, E2;
@@

+ /* Result of bms_join has to be used */
+ E1 = bms_join(E1, E2);
- bms_join(E1, E2);

@rule_8@
expression E1, E2;
@@

+ /* Result of bms_union has to be used */
+ E1 = bms_union(E1, E2);
- bms_union(E1, E2);

@rule_9@
expression E1, E2;
@@

+ /* Result of bms_intersect has to be used */
+ E1 = bms_intersect(E1, E2);
- bms_intersect(E1, E2);

@rule_10@
expression E1, E2;
@@

+ /* Result of bms_difference has to be used */
+ E1 = bms_difference(E1, E2);
- bms_difference(E1, E2);

